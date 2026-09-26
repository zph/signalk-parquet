import * as fs from 'node:fs/promises';
import * as path from 'node:path';
import { randomUUID } from 'node:crypto';
import { DuckDBInstance } from '@duckdb/node-api';
import { globIn } from './glob-in';
import { FileLease } from './file-lease';
import { HivePathBuilder } from './hive-path-builder';
import { SHARED_AIS_CONTEXT } from './ais-shared';

export interface SharedAisMigrationSummary {
  sourceFiles: number;
  sourceBytes: number;
  sharedFiles: number;
  sharedBytes: number;
  rows: number;
  deletedSourceFiles: number;
}

const quote = (value: string): string => `'${value.replace(/'/g, "''")}'`;

/**
 * One-way layout migration. Stage and verify every path/day before publishing
 * the shared partition. Old vessel directories are deleted only after the
 * shared partition is complete. Run with ingestion stopped.
 */
export async function migrateSharedAis(
  directory: string,
  apply = false
): Promise<SharedAisMigrationSummary> {
  const root = path.resolve(directory);
  if (!(await fs.stat(root)).isDirectory())
    throw new Error('Archive root is not a directory');
  const hive = new HivePathBuilder();
  const rawDir = path.join(root, 'tier=raw');
  const sharedDir = path.join(
    rawDir,
    `context=${hive.sanitizeContext(SHARED_AIS_CONTEXT)}`
  );
  const sourceFiles = (
    await globIn(
      root,
      'tier=raw/context=vessels__urn-mrn-imo-mmsi-*/path=*/year=*/day=*/*.parquet'
    )
  ).sort();
  const groups = new Map<string, string[]>();
  const contexts = new Set<string>();
  let sourceBytes = 0;
  for (const file of sourceFiles) {
    const relative = path.relative(rawDir, file).split(path.sep);
    if (relative.length !== 5)
      throw new Error(`Unexpected AIS file layout: ${file}`);
    contexts.add(relative[0]);
    const key = relative.slice(1, 4).join(path.sep);
    groups.set(key, [...(groups.get(key) || []), file]);
    sourceBytes += (await fs.stat(file)).size;
  }
  const summary: SharedAisMigrationSummary = {
    sourceFiles: sourceFiles.length,
    sourceBytes,
    sharedFiles: groups.size,
    sharedBytes: 0,
    rows: 0,
    deletedSourceFiles: 0,
  };
  if (!apply || sourceFiles.length === 0) return summary;
  if (
    await fs.stat(sharedDir).then(
      () => true,
      () => false
    )
  ) {
    throw new Error(`Shared AIS partition already exists: ${sharedDir}`);
  }
  const lease = FileLease.acquire(path.join(root, '.parquet-export.lock'));
  const stageDir = path.join(root, `.ais-shared-stage-${randomUUID()}`);
  const instance = await DuckDBInstance.create(':memory:');
  const connection = await instance.connect();
  let published = false;
  try {
    await connection.runAndReadAll("SET memory_limit = '512MB'");
    for (const [key, files] of groups) {
      lease.assertHeld();
      const outputDir = path.join(stageDir, key);
      await fs.mkdir(outputDir, { recursive: true });
      const output = path.join(outputDir, `shared_${randomUUID()}.parquet`);
      const sources = `[${files.map(quote).join(',')}]`;
      await connection.runAndReadAll(
        `COPY (SELECT * FROM read_parquet(${sources}, union_by_name=true, hive_partitioning=false)
          ORDER BY event_time, received_delay_us, context)
         TO ${quote(output)} (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 3)`
      );
      const counts = await connection.runAndReadAll(
        `SELECT
          (SELECT COUNT(*) FROM read_parquet(${sources}, union_by_name=true, hive_partitioning=false)) AS source_count,
          (SELECT COUNT(*) FROM read_parquet(${quote(output)}, hive_partitioning=false)) AS output_count,
          (SELECT COUNT(*) FROM (
            (SELECT * FROM read_parquet(${sources}, union_by_name=true, hive_partitioning=false)
             EXCEPT ALL SELECT * FROM read_parquet(${quote(output)}, hive_partitioning=false))
            UNION ALL
            (SELECT * FROM read_parquet(${quote(output)}, hive_partitioning=false)
             EXCEPT ALL SELECT * FROM read_parquet(${sources}, union_by_name=true, hive_partitioning=false))
          )) AS changed_count`
      );
      const row = counts.getRowObjects()[0] as {
        source_count: bigint;
        output_count: bigint;
        changed_count: bigint;
      };
      if (row.source_count !== row.output_count || row.changed_count !== 0n) {
        throw new Error(`AIS migration verification failed for ${key}`);
      }
      const codecs = await connection.runAndReadAll(
        `SELECT DISTINCT compression FROM parquet_metadata(${quote(output)})`
      );
      if (codecs.getRowObjects().some(item => item.compression !== 'ZSTD')) {
        throw new Error(`AIS migration output is not ZSTD: ${output}`);
      }
      summary.rows += Number(row.output_count);
      summary.sharedBytes += (await fs.stat(output)).size;
    }
    lease.assertHeld();
    await fs.rename(stageDir, sharedDir);
    published = true;
    for (const context of contexts) {
      lease.assertHeld();
      await fs.rm(path.join(rawDir, context), { recursive: true });
    }
    summary.deletedSourceFiles = sourceFiles.length;
    return summary;
  } finally {
    connection.disconnectSync();
    lease.release();
    if (!published) await fs.rm(stageDir, { recursive: true, force: true });
  }
}
