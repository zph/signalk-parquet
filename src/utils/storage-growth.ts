import * as fs from 'node:fs/promises';
import * as fsSync from 'node:fs';
import * as path from 'node:path';

export interface StorageSizeSample {
  at: number;
  sqliteBytes: number;
  parquetBytes: number;
}

export interface StorageGrowthEstimate {
  available: boolean;
  sampleCount: number;
  windowHours: number;
  sqliteBytesPerDay: number | null;
  parquetBytesPerDay: number | null;
  totalBytesPerDay: number | null;
}

const HISTORY_FILE = '.storage-size-history.json';
const SAMPLE_EVERY_MS = 30 * 60 * 1000;
const HISTORY_AGE_MS = 7 * 24 * 60 * 60 * 1000;
const MIN_INTERVAL_MS = 15 * 60 * 1000;
const MAX_INTERVAL_MS = 90 * 60 * 1000;
const MIN_INTERVALS_FOR_ESTIMATE = 5;

export function getSQLiteBytes(databasePath: string): number {
  return [databasePath, `${databasePath}-wal`, `${databasePath}-shm`].reduce(
    (total, file) => {
      try {
        return total + fsSync.statSync(file).size;
      } catch {
        return total;
      }
    },
    0
  );
}

function median(values: number[]): number {
  const sorted = [...values].sort((a, b) => a - b);
  const middle = Math.floor(sorted.length / 2);
  return sorted.length % 2
    ? sorted[middle]
    : (sorted[middle - 1] + sorted[middle]) / 2;
}

export async function getParquetBytes(directory: string): Promise<number> {
  let total = 0;
  const visit = async (current: string, root = false): Promise<void> => {
    let entries: import('node:fs').Dirent[];
    try {
      entries = await fs.readdir(current, { withFileTypes: true });
    } catch {
      return;
    }
    for (const entry of entries) {
      if (
        (root && !/^tier=(raw|5s|60s|1h)$/.test(entry.name)) ||
        (entry.isDirectory() &&
          ['processed', 'failed', 'quarantine', 'repaired'].includes(
            entry.name
          ))
      ) {
        continue;
      }
      const fullPath = path.join(current, entry.name);
      if (entry.isDirectory()) {
        await visit(fullPath);
      } else if (entry.isFile() && entry.name.endsWith('.parquet')) {
        try {
          total += (await fs.stat(fullPath)).size;
        } catch {
          // A compaction or retention pass may remove a file during the scan.
        }
      }
    }
  };
  await visit(directory, true);
  return total;
}

/** Persists periodic size snapshots and reports a robust median net growth rate. */
export class StorageGrowthTracker {
  private inFlight?: Promise<StorageGrowthEstimate>;
  private samples: StorageSizeSample[] = [];
  private loaded = false;

  constructor(private readonly dataDirectory: string) {}

  async sample(
    sqliteBytes: number,
    parquetBytes: number,
    now = Date.now()
  ): Promise<StorageGrowthEstimate> {
    if (this.inFlight) return this.inFlight;
    this.inFlight = this.writeSample(sqliteBytes, parquetBytes, now);
    try {
      return await this.inFlight;
    } finally {
      this.inFlight = undefined;
    }
  }

  private async writeSample(
    sqliteBytes: number,
    parquetBytes: number,
    now: number
  ): Promise<StorageGrowthEstimate> {
    const historyPath = path.join(this.dataDirectory, HISTORY_FILE);
    if (!this.loaded) {
      try {
        const parsed = JSON.parse(await fs.readFile(historyPath, 'utf8')) as {
          samples?: StorageSizeSample[];
        };
        this.samples = Array.isArray(parsed.samples)
          ? parsed.samples.filter(
              row =>
                Number.isFinite(row.at) &&
                Number.isFinite(row.sqliteBytes) &&
                Number.isFinite(row.parquetBytes)
            )
          : [];
      } catch {
        this.samples = [];
      }
      this.loaded = true;
    }

    const previous = this.samples[this.samples.length - 1];
    if (!previous || now - previous.at >= SAMPLE_EVERY_MS) {
      this.samples.push({ at: now, sqliteBytes, parquetBytes });
      const cutoff = now - HISTORY_AGE_MS;
      this.samples = this.samples.filter(sample => sample.at >= cutoff);
      const temporaryPath = `${historyPath}.${process.pid}.tmp`;
      await fs.writeFile(
        temporaryPath,
        JSON.stringify({ version: 1, samples: this.samples }),
        'utf8'
      );
      await fs.rename(temporaryPath, historyPath);
    }

    return this.estimate(now);
  }

  estimate(now = Date.now()): StorageGrowthEstimate {
    const recent = this.samples.filter(
      sample => sample.at >= now - HISTORY_AGE_MS
    );
    const intervals: Array<{
      sqliteBytesPerDay: number;
      parquetBytesPerDay: number;
      totalBytesPerDay: number;
    }> = [];
    for (let index = 1; index < recent.length; index++) {
      const previous = recent[index - 1];
      const current = recent[index];
      const elapsed = current.at - previous.at;
      if (elapsed < MIN_INTERVAL_MS || elapsed > MAX_INTERVAL_MS) continue;
      const dailyScale = (24 * 60 * 60 * 1000) / elapsed;
      intervals.push({
        sqliteBytesPerDay:
          (current.sqliteBytes - previous.sqliteBytes) * dailyScale,
        parquetBytesPerDay:
          (current.parquetBytes - previous.parquetBytes) * dailyScale,
        totalBytesPerDay:
          (current.sqliteBytes +
            current.parquetBytes -
            previous.sqliteBytes -
            previous.parquetBytes) *
          dailyScale,
      });
    }
    const first = recent[0];
    const last = recent[recent.length - 1];
    const windowHours =
      first && last ? Math.max(0, (last.at - first.at) / 3600000) : 0;
    if (intervals.length < MIN_INTERVALS_FOR_ESTIMATE) {
      return {
        available: false,
        sampleCount: recent.length,
        windowHours,
        sqliteBytesPerDay: null,
        parquetBytesPerDay: null,
        totalBytesPerDay: null,
      };
    }
    return {
      available: true,
      sampleCount: recent.length,
      windowHours,
      sqliteBytesPerDay: median(intervals.map(row => row.sqliteBytesPerDay)),
      parquetBytesPerDay: median(intervals.map(row => row.parquetBytesPerDay)),
      totalBytesPerDay: median(intervals.map(row => row.totalBytesPerDay)),
    };
  }
}
