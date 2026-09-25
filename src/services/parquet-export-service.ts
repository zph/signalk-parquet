/**
 * Parquet Export Service
 *
 * Handles periodic export of data from SQLite buffer to Parquet files.
 * Provides crash recovery by checking for pending records on startup.
 */

import * as fs from 'fs-extra';
import * as path from 'path';
import { SQLiteBuffer } from '../utils/sqlite-buffer';
import { DataRecord, ParquetWriter } from '../types';
import { ServerAPI } from '@signalk/server-api';
import { HivePathBuilder } from '../utils/hive-path-builder';
import { globIn } from '../utils/glob-in';
import { DuckDBPool } from '../utils/duckdb-pool';
import { FileLease } from '../utils/file-lease';

export interface ExportServiceConfig {
  outputDirectory: string;
  filenamePrefix: string;
  useHivePartitioning: boolean;
  dailyExportHour: number;
  s3Upload?: {
    enabled: boolean;
  };
}

export interface ExportResult {
  batchId: string;
  recordsExported: number;
  filesCreated: string[];
  duration: number;
  errors: string[];
}

export class ParquetExportService {
  private readonly sqliteBuffer: SQLiteBuffer;
  private readonly parquetWriter: ParquetWriter;
  private readonly config: ExportServiceConfig;
  private readonly app: ServerAPI;
  private readonly hivePathBuilder: HivePathBuilder;
  private exportInterval: NodeJS.Timeout | null = null;
  private isExporting: boolean = false;
  private lastExportTime: Date | null = null;
  private totalExported: number = 0;
  private lastBatchExported: number = 0;
  private lastExportTrigger: 'startup' | 'hourly' | 'daily' | 'forced' | null =
    null;

  private acquireLease(): FileLease {
    return FileLease.acquire(
      path.join(this.config.outputDirectory, '.parquet-export.lock')
    );
  }

  constructor(
    sqliteBuffer: SQLiteBuffer,
    parquetWriter: ParquetWriter,
    config: ExportServiceConfig,
    app: ServerAPI
  ) {
    this.sqliteBuffer = sqliteBuffer;
    this.parquetWriter = parquetWriter;
    this.config = config;
    this.app = app;
    this.hivePathBuilder = new HivePathBuilder();
  }

  /**
   * Start the export service
   *
   * No startup export — catchup for completed days is handled by
   * exportAllUnexported() called from index.ts after a 10s delay.
   * Today's data stays in SQLite for the History API to read live.
   */
  start(): void {
    this.app.debug(`ParquetExportService started (completed-hour export mode)`);
  }

  /**
   * Stop the export service
   */
  stop(): void {
    if (this.exportInterval) {
      clearInterval(this.exportInterval);
      this.exportInterval = null;
    }
    this.app.debug('ParquetExportService stopped');
  }

  /**
   * Force an immediate export of completed hours (excludes current hour)
   */
  async forceExport(): Promise<ExportResult> {
    this.lastExportTrigger = 'forced';
    return this.exportAllUnexported();
  }

  /**
   * Build a flat-structure file path (legacy compatibility)
   */
  private buildFlatFilePath(context: string, signalkPath: string): string {
    // Clean context for filesystem
    let contextPath: string;
    if (context === 'vessels.self') {
      contextPath = this.app.selfContext.replace(/\./g, '/').replace(/:/g, '_');
    } else if (context.startsWith('vessels.')) {
      const vesselId = context.replace('vessels.', '').replace(/:/g, '_');
      contextPath = `vessels/${vesselId}`;
    } else {
      contextPath = context.replace(/:/g, '_').replace(/\./g, '/');
    }

    const dirPath = path.join(
      this.config.outputDirectory,
      contextPath,
      signalkPath.replace(/\./g, '/')
    );

    const timestamp = new Date()
      .toISOString()
      .replace(/[:.]/g, '')
      .slice(0, 15);

    return path.join(
      dirPath,
      `${this.config.filenamePrefix}_${timestamp}.parquet`
    );
  }

  /**
   * Generate a unique batch ID
   */
  private generateBatchId(): string {
    const timestamp = new Date()
      .toISOString()
      .replace(/[:.]/g, '')
      .slice(0, 15);
    const random = Math.random().toString(36).substring(2, 8);
    return `batch_${timestamp}_${random}`;
  }

  /**
   * Get service status
   */
  getStatus(): {
    isRunning: boolean;
    isExporting: boolean;
    lastExportTime: Date | null;
    lastBatchExported: number;
    totalExported: number;
    pendingRecords: number;
    dailyExportHour: number;
    lastExportTrigger: string | null;
    mode: 'hourly';
  } {
    return {
      isRunning: true,
      isExporting: this.isExporting,
      lastExportTime: this.lastExportTime,
      lastBatchExported: this.lastBatchExported,
      totalExported: this.totalExported,
      pendingRecords: this.sqliteBuffer.getPendingCount(),
      dailyExportHour: this.config.dailyExportHour,
      lastExportTrigger: this.lastExportTrigger,
      mode: 'hourly',
    };
  }

  /**
   * Get health check information
   */
  getHealth(): {
    healthy: boolean;
    lastExportTime: Date | null;
    pendingRecords: number;
    bufferStats: ReturnType<SQLiteBuffer['getStats']>;
  } {
    const stats = this.sqliteBuffer.getStats();
    const healthy = !this.isExporting;

    return {
      healthy,
      lastExportTime: this.lastExportTime,
      pendingRecords: stats.pendingRecords,
      bufferStats: stats,
    };
  }

  /**
   * Export all completed UTC hours, including missed hours after a restart.
   */
  async exportAllUnexported(): Promise<ExportResult> {
    const lease = this.acquireLease();
    try {
      const startTime = Date.now();
      const batchId = this.generateBatchId();
      let totalRecordsExported = 0;
      const allFilesCreated: string[] = [];
      const allErrors: string[] = [];

      const hours = this.sqliteBuffer.getHoursWithUnexportedRecords();

      if (hours.length === 0) {
        this.app.debug('[StartupExport] No unexported records found');
        this.lastExportTime = new Date();
        this.lastBatchExported = 0;
        this.lastExportTrigger = 'startup';
        return {
          batchId,
          recordsExported: 0,
          filesCreated: [],
          duration: Date.now() - startTime,
          errors: [],
        };
      }

      this.app.debug(
        `[StartupExport] Found unexported records for ${hours.length} completed hours`
      );

      for (const hourStr of hours) {
        const targetDate = new Date(hourStr + ':00:00.000Z');

        try {
          const result = await this.exportHourToParquet(targetDate, lease);
          totalRecordsExported += result.recordsExported;
          allFilesCreated.push(...result.filesCreated);
          allErrors.push(...result.errors);

          if (result.recordsExported > 0) {
            this.app.debug(
              `[StartupExport] Exported ${result.recordsExported} records for ${hourStr}`
            );
          }
        } catch (error) {
          const errorMsg = `[StartupExport] Failed to export ${hourStr}: ${(error as Error).message}`;
          this.app.error(errorMsg);
          allErrors.push(errorMsg);
        }
      }

      this.lastExportTime = new Date();
      this.lastBatchExported = totalRecordsExported;
      this.totalExported += totalRecordsExported;
      this.lastExportTrigger = 'startup';

      this.app.debug(
        `[StartupExport] Complete: ${totalRecordsExported} records to ${allFilesCreated.length} files in ${Date.now() - startTime}ms`
      );

      return {
        batchId,
        recordsExported: totalRecordsExported,
        filesCreated: allFilesCreated,
        duration: Date.now() - startTime,
        errors: allErrors,
      };
    } finally {
      lease.release();
    }
  }

  /** Persist one completed UTC hour. Rows remain pending until the file is published. */
  async exportHourToParquet(
    targetHour: Date,
    sharedLease?: FileLease
  ): Promise<ExportResult> {
    const hour = new Date(targetHour);
    hour.setUTCMinutes(0, 0, 0);
    if (hour.getTime() >= Math.floor(Date.now() / 3600000) * 3600000) {
      throw new Error('Cannot export the current or a future UTC hour');
    }
    if (this.isExporting) {
      return {
        batchId: '',
        recordsExported: 0,
        filesCreated: [],
        duration: 0,
        errors: ['Export already in progress'],
      };
    }
    const lease = sharedLease || this.acquireLease();
    this.isExporting = true;
    const started = Date.now();
    const batchId = this.generateBatchId();
    const filesCreated: string[] = [];
    const errors: string[] = [];
    let recordsExported = 0;
    try {
      for (const {
        context,
        path: signalkPath,
      } of this.sqliteBuffer.getPathsForHour(hour)) {
        try {
          const count = this.sqliteBuffer.getRecordCountForPathAndHour(
            context,
            signalkPath,
            hour
          );
          if (!count) continue;
          let offset = 0;
          const nextBatch = (): DataRecord[] => {
            const rows = this.sqliteBuffer.getRecordsForPathAndHourBatched(
              context,
              signalkPath,
              hour,
              5000,
              offset
            );
            offset += rows.length;
            return rows;
          };
          const firstBatch = nextBatch();
          const file = await this.exportDailyGroupBatched(
            context,
            signalkPath,
            firstBatch,
            nextBatch,
            hour,
            lease
          );
          if (!file) continue;
          lease.assertHeld();
          this.sqliteBuffer.markHourExported(
            context,
            signalkPath,
            hour,
            batchId
          );
          filesCreated.push(file);
          recordsExported += count;
        } catch (error) {
          const message = `[HourlyExport] ${context}:${signalkPath}: ${(error as Error).message}`;
          this.app.error(message);
          errors.push(message);
        }
      }
      this.sqliteBuffer.cleanup();
      this.sqliteBuffer.checkpoint();
      this.lastExportTime = new Date();
      this.lastBatchExported = recordsExported;
      this.totalExported += recordsExported;
      this.lastExportTrigger = 'hourly';
      return {
        batchId,
        recordsExported,
        filesCreated,
        duration: Date.now() - started,
        errors,
      };
    } finally {
      this.isExporting = false;
      if (!sharedLease) lease.release();
    }
  }

  /** Merge a completed day's hourly files per context/path, sorted by event time. */
  async compactDay(
    targetDay: Date
  ): Promise<{ filesCompacted: number; errors: string[] }> {
    if (this.isExporting)
      return { filesCompacted: 0, errors: ['Export already in progress'] };
    const lease = this.acquireLease();
    this.isExporting = true;
    try {
      const day = new Date(targetDay);
      day.setUTCHours(0, 0, 0, 0);
      const year = day.getUTCFullYear();
      const dayOfYear =
        Math.floor((day.getTime() - Date.UTC(year, 0, 1)) / 86400000) + 1;
      await this.recoverStrandedCompactionsUnderLease(lease, day);
      const pattern = `tier=raw/context=*/path=*/year=${year}/day=${String(dayOfYear).padStart(3, '0')}/*.parquet`;
      const files = await globIn(this.config.outputDirectory, pattern);
      const groups = new Map<string, string[]>();
      for (const file of files) {
        const dir = path.dirname(file);
        groups.set(dir, [...(groups.get(dir) || []), file]);
      }
      const errors: string[] = [];
      let filesCompacted = 0;
      for (const [dir, group] of groups) {
        if (group.length < 2) continue;
        const stamp = `${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
        const output = path.join(
          dir,
          `daily_compact_${day.toISOString().slice(0, 10)}_${stamp}.parquet`
        );
        const temp = output + '.tmp';
        const quote = (p: string): string => `'${p.replace(/'/g, "''")}'`;
        const sourceSql = group.map(quote).join(', ');
        const sql = `COPY (
        SELECT * FROM read_parquet([${sourceSql}], union_by_name=true, hive_partitioning=false)
        ORDER BY signalk_timestamp, received_timestamp, context, path
      ) TO ${quote(temp)} (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 3);`;
        try {
          const connection = await DuckDBPool.getConnection();
          try {
            await connection.runAndReadAll(sql);
            const counts = await connection.runAndReadAll(`
              SELECT
                (SELECT COUNT(*) FROM read_parquet([${sourceSql}], union_by_name=true, hive_partitioning=false)) AS source_count,
                (SELECT COUNT(*) FROM read_parquet(${quote(temp)}, hive_partitioning=false)) AS output_count
            `);
            const row = counts.getRowObjects()[0] as {
              source_count: bigint;
              output_count: bigint;
            };
            if (row.source_count !== row.output_count) {
              throw new Error(
                `Compaction row count mismatch: ${row.source_count} source, ${row.output_count} output`
              );
            }
          } finally {
            connection.disconnectSync();
          }
          if ((await fs.stat(temp)).size < 100)
            throw new Error('Compacted Parquet is implausibly small');
          lease.assertHeld();
          // Move sources out of query globs before publishing the replacement.
          const trash = path.join(dir, `.daily-compaction-trash-${stamp}`);
          await fs.ensureDir(trash);
          const moved: Array<{ from: string; to: string }> = [];
          try {
            for (const source of group) {
              lease.assertHeld();
              const destination = path.join(trash, path.basename(source));
              await fs.rename(source, destination);
              moved.push({ from: source, to: destination });
            }
            lease.assertHeld();
            await fs.rename(temp, output);
          } catch (error) {
            for (const item of moved.reverse())
              await fs.rename(item.to, item.from);
            throw error;
          }
          await fs.remove(trash);
          filesCompacted++;
        } catch (error) {
          errors.push(`${dir}: ${(error as Error).message}`);
          this.app.error(`[DailyCompaction] ${errors[errors.length - 1]}`);
          await fs.remove(temp).catch(() => undefined);
        }
      }
      return { filesCompacted, errors };
    } finally {
      this.isExporting = false;
      lease.release();
    }
  }

  /** Restore sources from an interrupted compaction before any normal scan. */
  async recoverStrandedCompactions(): Promise<void> {
    const lease = this.acquireLease();
    try {
      await this.recoverStrandedCompactionsUnderLease(lease);
    } finally {
      lease.release();
    }
  }

  /** Completed dates whose raw partitions still contain multiple files. */
  async getDaysNeedingCompaction(): Promise<Date[]> {
    const files = await globIn(
      this.config.outputDirectory,
      'tier=raw/context=*/path=*/year=*/day=*/*.parquet'
    );
    const counts = new Map<string, number>();
    for (const file of files) {
      const dir = path.dirname(file);
      counts.set(dir, (counts.get(dir) || 0) + 1);
    }
    const today = new Date();
    today.setUTCHours(0, 0, 0, 0);
    const days = new Set<number>();
    for (const [dir, count] of counts) {
      if (count < 2) continue;
      const parsed = this.hivePathBuilder.detectPathStyle(
        path.join(dir, 'file.parquet')
      );
      if (!parsed.year || !parsed.dayOfYear) continue;
      const day = Date.UTC(parsed.year, 0, parsed.dayOfYear);
      if (day < today.getTime()) days.add(day);
    }
    return [...days].sort((a, b) => a - b).map(ms => new Date(ms));
  }

  private async recoverStrandedCompactionsUnderLease(
    lease: FileLease,
    targetDay?: Date
  ): Promise<void> {
    const year = targetDay?.getUTCFullYear() ?? '*';
    const dayOfYear = targetDay
      ? String(
          Math.floor(
            (targetDay.getTime() - Date.UTC(targetDay.getUTCFullYear(), 0, 1)) /
              86400000
          ) + 1
        ).padStart(3, '0')
      : '*';
    const dayDirs = await globIn(
      this.config.outputDirectory,
      `tier=raw/context=*/path=*/year=${year}/day=${dayOfYear}`
    );
    for (const dir of dayDirs) {
      for (const name of await fs.readdir(dir)) {
        if (!name.startsWith('.daily-compaction-trash-')) continue;
        lease.assertHeld();
        const trash = path.join(dir, name);
        if (!(await fs.stat(trash)).isDirectory()) continue;
        const stamp = name.slice('.daily-compaction-trash-'.length);
        const published = (await fs.readdir(dir)).some(
          file =>
            file.startsWith('daily_compact_') &&
            file.endsWith(`_${stamp}.parquet`)
        );
        if (!published) {
          for (const file of await fs.readdir(trash)) {
            const destination = path.join(dir, file);
            if (await fs.pathExists(destination)) {
              throw new Error(
                `Cannot recover compaction: destination exists: ${destination}`
              );
            }
            await fs.rename(path.join(trash, file), destination);
          }
          this.app.debug(
            `[DailyCompaction] Restored interrupted sources in ${dir}`
          );
        }
        await fs.remove(trash);
      }
    }
  }

  /**
   * Export a full day's data to Parquet files (one file per context/path)
   * This creates consolidated daily files directly, without needing a separate
   * consolidation step.
   *
   * @param targetDate The date to export (UTC). Typically yesterday.
   * @returns Export result with details about files created
   */
  async exportDayToParquet(targetDate: Date): Promise<ExportResult> {
    if (this.isExporting) {
      this.app.debug('Export already in progress, skipping daily export');
      return {
        batchId: '',
        recordsExported: 0,
        filesCreated: [],
        duration: 0,
        errors: ['Export already in progress'],
      };
    }

    const lease = this.acquireLease();
    this.isExporting = true;
    const startTime = Date.now();
    const batchId = this.generateBatchId();
    const filesCreated: string[] = [];
    const errors: string[] = [];
    let recordsExported = 0;

    const dateStr = targetDate.toISOString().slice(0, 10);
    this.app.debug(`[DailyExport] Starting daily export for ${dateStr}`);

    try {
      // Get all distinct context/path combinations for this date
      const pathsForDate = this.sqliteBuffer.getPathsForDate(targetDate);

      if (pathsForDate.length === 0) {
        this.app.debug(`[DailyExport] No data found for ${dateStr}`);
        this.lastExportTime = new Date();
        this.lastBatchExported = 0;
        this.lastExportTrigger = 'daily';
        return {
          batchId,
          recordsExported: 0,
          filesCreated: [],
          duration: Date.now() - startTime,
          errors: [],
        };
      }

      this.app.debug(
        `[DailyExport] Found ${pathsForDate.length} paths with data for ${dateStr}`
      );

      // Export each context/path to its own file (batched to limit memory)
      const BATCH_SIZE = 5000;

      for (const { context, path: signalkPath } of pathsForDate) {
        try {
          const count = this.sqliteBuffer.getRecordCountForPathAndDate(
            context,
            signalkPath,
            targetDate
          );

          if (count === 0) {
            continue;
          }

          const firstBatch = this.sqliteBuffer.getRecordsForPathAndDateBatched(
            context,
            signalkPath,
            targetDate,
            BATCH_SIZE,
            0
          );
          let offset = BATCH_SIZE;

          const filePath = await this.exportDailyGroupBatched(
            context,
            signalkPath,
            firstBatch,
            () => {
              const batch = this.sqliteBuffer.getRecordsForPathAndDateBatched(
                context,
                signalkPath,
                targetDate,
                BATCH_SIZE,
                offset
              );
              offset += BATCH_SIZE;
              return batch;
            },
            targetDate,
            lease
          );

          if (filePath) {
            lease.assertHeld();
            filesCreated.push(filePath);
            recordsExported += count;

            // Mark records as exported by date range
            this.sqliteBuffer.markDateExported(
              context,
              signalkPath,
              targetDate,
              batchId
            );

            this.app.debug(
              `[DailyExport] Exported ${count} records for ${context}:${signalkPath}`
            );
          }
        } catch (error) {
          const errorMsg = `[DailyExport] Failed to export ${context}:${signalkPath}: ${(error as Error).message}`;
          this.app.error(errorMsg);
          errors.push(errorMsg);
        }
      }

      // Cleanup old exported records
      const cleaned = this.sqliteBuffer.cleanup();
      if (cleaned > 0) {
        this.app.debug(
          `[DailyExport] Cleaned up ${cleaned} old exported records`
        );
      }

      // Truncate WAL after heavy export+cleanup batch
      try {
        this.sqliteBuffer.checkpoint();
      } catch {
        // Non-critical — WAL will be checkpointed eventually
      }

      this.lastExportTime = new Date();
      this.lastBatchExported = recordsExported;
      this.totalExported += recordsExported;
      this.lastExportTrigger = 'daily';

      this.app.debug(
        `[DailyExport] Complete: ${recordsExported} records to ${filesCreated.length} files in ${Date.now() - startTime}ms`
      );

      return {
        batchId,
        recordsExported,
        filesCreated,
        duration: Date.now() - startTime,
        errors,
      };
    } finally {
      this.isExporting = false;
      lease.release();
    }
  }

  /**
   * Export a group of records as a timestamped file
   * Uses consistent timestamped naming (same as startup exports) for simplicity
   */
  private async exportDailyGroup(
    context: string,
    signalkPath: string,
    records: DataRecord[],
    targetDate: Date,
    _batchId: string
  ): Promise<string | null> {
    if (records.length === 0) return null;

    // Build file path with timestamp (consistent with exportGroup)
    let filePath: string;

    if (this.config.useHivePartitioning) {
      // Resolve vessels.self to actual vessel context
      const resolvedContext =
        context === 'vessels.self' ? this.app.selfContext : context;

      // Use Hive-style partitioning
      // Directory is based on targetDate (for correct day partition)
      // Filename uses CURRENT time for uniqueness (avoid overwrites)
      const dirPath = this.hivePathBuilder.buildPath(
        this.config.outputDirectory,
        'raw',
        resolvedContext,
        signalkPath,
        targetDate
      );
      const timestampStr = new Date()
        .toISOString()
        .replace(/[:.]/g, '')
        .slice(0, 15);
      filePath = path.join(
        dirPath,
        `${this.config.filenamePrefix}_h${String(targetDate.getUTCHours()).padStart(2, '0')}_${timestampStr}_${Math.random().toString(36).slice(2, 8)}.parquet`
      );
    } else {
      // Use legacy flat structure with timestamp
      filePath = this.buildFlatFilePath(context, signalkPath);
    }

    // Ensure directory exists
    await fs.ensureDir(path.dirname(filePath));

    // Write to temp file first for atomic operation
    const tempFilePath = filePath + '.tmp';

    try {
      // Write records to temp file
      await this.parquetWriter.writeRecords(tempFilePath, records);

      // Validate the written file
      const stats = await fs.stat(tempFilePath);
      if (stats.size < 100) {
        throw new Error('Written file is too small, likely corrupt');
      }

      // Atomic rename
      await fs.rename(tempFilePath, filePath);

      return filePath;
    } catch (error) {
      // Clean up temp file on failure
      try {
        await fs.remove(tempFilePath);
      } catch {
        // Ignore cleanup errors
      }
      throw error;
    }
  }

  /**
   * Export records in batches to avoid loading all rows into memory at once.
   * firstBatch is used for schema detection; nextBatch callback pulls subsequent chunks.
   */
  private async exportDailyGroupBatched(
    context: string,
    signalkPath: string,
    firstBatch: DataRecord[],
    nextBatch: () => DataRecord[],
    targetDate: Date,
    lease?: FileLease
  ): Promise<string | null> {
    if (firstBatch.length === 0) return null;

    let filePath: string;

    if (this.config.useHivePartitioning) {
      const resolvedContext =
        context === 'vessels.self' ? this.app.selfContext : context;

      const dirPath = this.hivePathBuilder.buildPath(
        this.config.outputDirectory,
        'raw',
        resolvedContext,
        signalkPath,
        targetDate
      );
      const timestampStr = new Date()
        .toISOString()
        .replace(/[:.]/g, '')
        .slice(0, 15);
      filePath = path.join(
        dirPath,
        `${this.config.filenamePrefix}_h${String(targetDate.getUTCHours()).padStart(2, '0')}_${timestampStr}_${Math.random().toString(36).slice(2, 8)}.parquet`
      );
    } else {
      filePath = this.buildFlatFilePath(context, signalkPath);
    }

    await fs.ensureDir(path.dirname(filePath));

    const tempFilePath = filePath + '.tmp';

    try {
      await this.parquetWriter.writeParquetBatched(
        tempFilePath,
        firstBatch,
        nextBatch,
        signalkPath
      );

      const stats = await fs.stat(tempFilePath);
      if (stats.size < 100) {
        throw new Error('Written file is too small, likely corrupt');
      }

      lease?.assertHeld();
      await fs.rename(tempFilePath, filePath);

      return filePath;
    } catch (error) {
      try {
        await fs.remove(tempFilePath);
      } catch {
        // Ignore cleanup errors
      }
      throw error;
    }
  }
}
