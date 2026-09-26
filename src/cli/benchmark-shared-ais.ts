import * as fs from 'node:fs/promises';
import * as os from 'node:os';
import * as path from 'node:path';
import { performance } from 'node:perf_hooks';
import { DuckDBInstance } from '@duckdb/node-api';
import { globIn } from '../utils/glob-in';

const quote = (value: string): string => `'${value.replace(/'/g, "''")}'`;
const sourceList = (files: string[]): string =>
  `[${files.map(quote).join(',')}]`;

function median(values: number[]): number {
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.floor(sorted.length / 2)];
}

async function main(): Promise<void> {
  const root = path.resolve(process.argv[2] || '');
  if (!process.argv[2] || !(await fs.stat(root)).isDirectory()) {
    throw new Error('Usage: benchmark-shared-ais <archive-directory>');
  }
  const raw = path.join(root, 'tier=raw');
  const files = (
    await globIn(
      root,
      'tier=raw/context=vessels__urn-mrn-imo-mmsi-*/path=*/year=*/day=*/*.parquet'
    )
  ).sort();
  if (files.length === 0) throw new Error('No per-vessel AIS files found');
  const groups = new Map<string, string[]>();
  let oldBytes = 0;
  for (const file of files) {
    const parts = path.relative(raw, file).split(path.sep);
    const key = parts.slice(1, 4).join(path.sep);
    groups.set(key, [...(groups.get(key) || []), file]);
    oldBytes += (await fs.stat(file)).size;
  }
  const temp = await fs.mkdtemp(
    path.join(os.tmpdir(), 'ais-shared-benchmark-')
  );
  const instance = await DuckDBInstance.create(':memory:');
  const conn = await instance.connect();
  let sharedBytes = 0;
  const sharedPosition: string[] = [];
  try {
    await conn.runAndReadAll("SET memory_limit = '512MB'");
    for (const [key, group] of groups) {
      const dir = path.join(temp, key);
      await fs.mkdir(dir, { recursive: true });
      const output = path.join(dir, 'shared.parquet');
      await conn.runAndReadAll(
        `COPY (SELECT * FROM read_parquet(${sourceList(group)}, union_by_name=true, hive_partitioning=false)
          ORDER BY event_time, received_delay_us, context)
         TO ${quote(output)} (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 9)`
      );
      sharedBytes += (await fs.stat(output)).size;
      if (key.startsWith('path=navigation__position' + path.sep)) {
        sharedPosition.push(output);
      }
    }
    const oldPosition = files.filter(file =>
      file.includes(`${path.sep}path=navigation__position${path.sep}`)
    );
    if (oldPosition.length === 0)
      throw new Error('No AIS position files for query benchmark');
    const sample = await conn.runAndReadAll(
      `SELECT context FROM read_parquet(${quote(oldPosition[0])}, hive_partitioning=false) LIMIT 1`
    );
    const context = String(sample.getRowObjects()[0].context);
    const singleOld = oldPosition.filter(file =>
      file.includes(
        `${path.sep}context=${context.replace(/\./g, '__').replace(/:/g, '-')}${path.sep}`
      )
    );
    const queries = {
      oneVesselOld: `SELECT COUNT(*) FROM read_parquet(${sourceList(singleOld)}, hive_partitioning=false) WHERE context = ${quote(context)}`,
      oneVesselShared: `SELECT COUNT(*) FROM read_parquet(${sourceList(sharedPosition)}, hive_partitioning=false) WHERE context = ${quote(context)}`,
      allVesselsOld: `SELECT COUNT(*) FROM read_parquet(${sourceList(oldPosition)}, hive_partitioning=false)`,
      allVesselsShared: `SELECT COUNT(*) FROM read_parquet(${sourceList(sharedPosition)}, hive_partitioning=false)`,
    };
    const timings: Record<string, { rows: number; medianMs: number }> = {};
    for (const [name, sql] of Object.entries(queries)) {
      const samples: number[] = [];
      let rows = 0;
      for (let i = 0; i < 21; i++) {
        const start = performance.now();
        const result = await conn.runAndReadAll(sql);
        const elapsed = performance.now() - start;
        rows = Number(Object.values(result.getRowObjects()[0])[0]);
        if (i > 0) samples.push(elapsed);
      }
      timings[name] = { rows, medianMs: median(samples) };
    }
    console.log(
      JSON.stringify({
        sourceFiles: files.length,
        sourceBytes: oldBytes,
        sharedFiles: groups.size,
        sharedBytes,
        savedPercent: (100 * (oldBytes - sharedBytes)) / oldBytes,
        timings,
      })
    );
  } finally {
    conn.disconnectSync();
    await fs.rm(temp, { recursive: true, force: true });
  }
}

void main().catch(error => {
  console.error(error);
  process.exitCode = 1;
});
