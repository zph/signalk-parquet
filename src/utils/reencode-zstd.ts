import * as fs from 'node:fs/promises';
import * as path from 'node:path';
import { randomUUID } from 'node:crypto';
import { DuckDBInstance } from '@duckdb/node-api';
import { globIn } from './glob-in';
import { FileLease } from './file-lease';

export interface ReencodeSummary {
  scanned: number;
  converted: number;
  alreadyZstd: number;
  originalBytes: number;
  resultingBytes: number;
}

const sqlPath = (file: string): string => `'${file.replace(/'/g, "''")}'`;

async function metadata(
  connection: Awaited<ReturnType<DuckDBInstance['connect']>>,
  file: string
): Promise<{ rows: bigint; codecs: string[] }> {
  const result = await connection.runAndReadAll(
    `SELECT
       COALESCE(SUM(row_group_num_rows) FILTER (WHERE column_id = 0), 0) AS rows,
       string_agg(DISTINCT compression, ',') AS codecs
     FROM parquet_metadata(${sqlPath(file)})`
  );
  const row = result.getRowObjects()[0] as {
    rows: bigint;
    codecs: string | null;
  };
  return {
    rows: row.rows,
    codecs: row.codecs ? row.codecs.split(',').sort() : [],
  };
}

/**
 * Re-encode active archive files without changing rows. A renewable archive
 * lease excludes the hourly writer and compaction processes while applying.
 * Each replacement is verified before an atomic rename; interruption leaves
 * the original file available and a retry safely skips completed files.
 */
export async function reencodeParquetDirectory(
  directory: string,
  apply = false
): Promise<ReencodeSummary> {
  const root = path.resolve(directory);
  const stat = await fs.stat(root);
  if (!stat.isDirectory()) throw new Error('Archive root is not a directory');
  const files = (
    await globIn(root, 'tier=*/**/*.parquet', {
      ignore: [
        '**/quarantine/**',
        '**/failed/**',
        '**/repaired/**',
        '**/.daily-compaction-trash-*/**',
      ],
    })
  ).sort();
  const summary: ReencodeSummary = {
    scanned: 0,
    converted: 0,
    alreadyZstd: 0,
    originalBytes: 0,
    resultingBytes: 0,
  };
  const lease = apply
    ? FileLease.acquire(path.join(root, '.parquet-export.lock'))
    : undefined;
  let instance: DuckDBInstance | undefined;
  let connection: Awaited<ReturnType<DuckDBInstance['connect']>> | undefined;
  try {
    instance = await DuckDBInstance.create(':memory:');
    connection = await instance.connect();
    await connection.runAndReadAll("SET memory_limit = '512MB'");
    for (const file of files) {
      const before = await fs.stat(file);
      const source = await metadata(connection, file);
      summary.scanned++;
      summary.originalBytes += before.size;
      if (source.codecs.length === 1 && source.codecs[0] === 'ZSTD') {
        summary.alreadyZstd++;
        summary.resultingBytes += before.size;
        continue;
      }
      if (!apply) {
        summary.resultingBytes += before.size;
        continue;
      }
      const temp = `${file}.zstd-${randomUUID()}.tmp`;
      try {
        await connection.runAndReadAll(
          `COPY (SELECT * FROM read_parquet(${sqlPath(file)}, hive_partitioning=false))
           TO ${sqlPath(temp)}
           (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 9)`
        );
        const output = await metadata(connection, temp);
        if (
          output.rows !== source.rows ||
          output.codecs.length !== 1 ||
          output.codecs[0] !== 'ZSTD'
        ) {
          throw new Error(`ZSTD verification failed for ${file}`);
        }
        const difference = await connection.runAndReadAll(
          `SELECT COUNT(*) AS n FROM (
             (SELECT * FROM read_parquet(${sqlPath(file)}, hive_partitioning=false)
              EXCEPT ALL
              SELECT * FROM read_parquet(${sqlPath(temp)}, hive_partitioning=false))
             UNION ALL
             (SELECT * FROM read_parquet(${sqlPath(temp)}, hive_partitioning=false)
              EXCEPT ALL
              SELECT * FROM read_parquet(${sqlPath(file)}, hive_partitioning=false))
           )`
        );
        const count = difference.getRowObjects()[0].n as bigint;
        if (count !== 0n) throw new Error(`Row mismatch for ${file}`);
        const current = await fs.stat(file);
        if (
          current.size !== before.size ||
          current.mtimeMs !== before.mtimeMs
        ) {
          throw new Error(`Source changed during rewrite: ${file}`);
        }
        await fs.chmod(temp, before.mode & 0o777);
        const handle = await fs.open(temp, 'r');
        try {
          await handle.sync();
        } finally {
          await handle.close();
        }
        lease?.assertHeld();
        await fs.rename(temp, file);
        summary.converted++;
        summary.resultingBytes += (await fs.stat(file)).size;
      } finally {
        await fs.rm(temp, { force: true });
      }
    }
    return summary;
  } finally {
    connection?.disconnectSync();
    instance?.closeSync();
    lease?.release();
  }
}
