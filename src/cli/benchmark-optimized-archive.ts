import * as fs from 'node:fs/promises';
import * as path from 'node:path';
import { performance } from 'node:perf_hooks';
import { DuckDBInstance } from '@duckdb/node-api';
import { globIn } from '../utils/glob-in';

interface Column {
  name: string;
  type: string;
}

interface SourceGroup {
  key: string;
  files: string[];
  bytes: number;
  columns: Column[];
  eventColumn: string;
  payload: Column[];
  family: number;
  days: string[];
  pathValue: string;
  contextValue: string;
}

interface Task {
  day: string;
  family: number;
  groups: SourceGroup[];
}

const IDENTITY_COLUMNS = new Set([
  'context',
  'path',
  'source',
  'source_label',
  'source_pgn',
  'source_src',
  'source_type',
  'meta',
  'signalk_timestamp',
  'received_timestamp',
  'event_time',
  'received_delay_us',
]);
const SOURCE_COLUMNS = [
  'source',
  'source_label',
  'source_pgn',
  'source_src',
  'source_type',
];

const quote = (value: string): string => `'${value.replace(/'/g, "''")}'`;
const identifier = (value: string): string => `"${value.replace(/"/g, '""')}"`;
const sourceList = (files: string[]): string =>
  `[${files.map(file => quote(file.split(path.sep).join('/'))).join(',')}]`;
const sourceSql = (group: SourceGroup): string =>
  `read_parquet(${sourceList(group.files)}, union_by_name=true, hive_partitioning=false)`;

function eventExpression(group: SourceGroup, alias = 'src'): string {
  const column = `${alias}.${identifier(group.eventColumn)}`;
  const type = group.columns.find(
    item => item.name === group.eventColumn
  )?.type;
  return type === 'VARCHAR'
    ? `cast(try_cast(${column} AS TIMESTAMPTZ) AS TIMESTAMP_MS)`
    : `cast(${column} AS TIMESTAMP_MS)`;
}

function delayExpression(group: SourceGroup, alias = 'src'): string {
  if (group.columns.some(column => column.name === 'received_delay_us')) {
    return `${alias}.received_delay_us::BIGINT`;
  }
  const event = eventExpression(group, alias);
  return `(epoch_us(try_cast(${alias}.received_timestamp AS TIMESTAMPTZ)) - epoch_us(${event}))::BIGINT`;
}

function sourceField(group: SourceGroup, name: string, alias = 'src'): string {
  return group.columns.some(column => column.name === name)
    ? `cast(${alias}.${identifier(name)} AS VARCHAR)`
    : 'NULL::VARCHAR';
}

function sourceKey(group: SourceGroup, alias = 'src'): string {
  return `to_json(struct_pack(${SOURCE_COLUMNS.map(
    name => `${name} := ${sourceField(group, name, alias)}`
  ).join(', ')}))`;
}

function normalizedSelect(group: SourceGroup, day: string): string {
  const payload = group.payload
    .map(column => `src.${identifier(column.name)}`)
    .join(', ');
  const metaJoin = group.columns.some(column => column.name === 'meta')
    ? 'LEFT JOIN meta_dim md ON md.meta IS NOT DISTINCT FROM cast(src.meta AS VARCHAR)'
    : 'LEFT JOIN meta_dim md ON false';
  const event = eventExpression(group);
  return `SELECT
    ctx.context_id,
    p.path_id,
    sd.source_id,
    md.meta_id,
    ${event} AS event_time,
    ${delayExpression(group)} AS received_delay_us${payload ? `, ${payload}` : ''}
  FROM ${sourceSql(group)} src
  JOIN context_dim ctx ON ctx.context = cast(src.context AS VARCHAR)
  JOIN path_dim p ON p.path = cast(src.path AS VARCHAR)
  JOIN source_dim sd ON sd.source_key = ${sourceKey(group)}
  ${metaJoin}
  WHERE cast(${event} AS DATE) = DATE ${quote(day)}`;
}

function pathDailySelect(
  group: SourceGroup,
  day: string,
  typed: boolean
): string {
  const source = sourceSql(group);
  const event = eventExpression(group);
  if (!typed || group.eventColumn === 'event_time') {
    return `SELECT * FROM ${source} src WHERE cast(${event} AS DATE) = DATE ${quote(day)}`;
  }
  return `SELECT * EXCLUDE (signalk_timestamp, received_timestamp),
    ${event} AS event_time,
    ${delayExpression(group)} AS received_delay_us
    FROM ${source} src
    WHERE cast(${event} AS DATE) = DATE ${quote(day)}`;
}

function median(values: number[]): number {
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.floor(sorted.length / 2)];
}

async function discoverRawGroups(root: string): Promise<SourceGroup[]> {
  const files = Array.from(
    new Set([
      ...(await globIn(root, 'tier=raw/context=*/path=*/year=*/*.parquet')),
      ...(await globIn(
        root,
        'tier=raw/context=*/path=*/year=*/day=*/*.parquet'
      )),
    ])
  ).sort();
  const grouped = new Map<string, string[]>();
  for (const file of files) {
    const parts = path.relative(root, file).split(path.sep);
    const key = parts.slice(0, 4).join(path.sep);
    grouped.set(key, [...(grouped.get(key) || []), file]);
  }
  const result: SourceGroup[] = [];
  for (const [key, groupFiles] of grouped) {
    let bytes = 0;
    for (const file of groupFiles) bytes += (await fs.stat(file)).size;
    result.push({
      key,
      files: groupFiles.sort(),
      bytes,
      columns: [],
      eventColumn: '',
      payload: [],
      family: -1,
      days: [],
      pathValue: '',
      contextValue: '',
    });
  }
  return result.sort((a, b) => a.key.localeCompare(b.key));
}

async function prepareGroups(
  connection: Awaited<ReturnType<DuckDBInstance['connect']>>,
  groups: SourceGroup[]
): Promise<void> {
  const families = new Map<string, number>();
  for (const group of groups) {
    const schema = await connection.runAndReadAll(
      `DESCRIBE SELECT * FROM ${sourceSql(group)}`
    );
    group.columns = schema.getRowObjects().map(row => ({
      name: String(row.column_name),
      type: String(row.column_type),
    }));
    group.eventColumn = group.columns.some(
      column => column.name === 'event_time'
    )
      ? 'event_time'
      : 'signalk_timestamp';
    if (!group.columns.some(column => column.name === group.eventColumn)) {
      throw new Error(`No event timestamp in ${group.key}`);
    }
    group.payload = group.columns.filter(
      column => !IDENTITY_COLUMNS.has(column.name)
    );
    const signature = JSON.stringify(group.payload);
    if (!families.has(signature)) families.set(signature, families.size);
    group.family = families.get(signature)!;
    const sample = await connection.runAndReadAll(
      `SELECT cast(context AS VARCHAR) AS context, cast(path AS VARCHAR) AS path FROM ${sourceSql(group)} LIMIT 1`
    );
    const sampleRow = sample.getRowObjects()[0];
    group.contextValue = String(sampleRow.context);
    group.pathValue = String(sampleRow.path);
    const days = await connection.runAndReadAll(
      `SELECT DISTINCT strftime(cast(${eventExpression(group, 'src')} AS DATE), '%Y-%m-%d') AS day
       FROM ${sourceSql(group)} src ORDER BY day`
    );
    group.days = days.getRowObjects().map(row => String(row.day));
  }
}

async function buildDimensions(
  connection: Awaited<ReturnType<DuckDBInstance['connect']>>,
  groups: SourceGroup[]
): Promise<number> {
  const started = performance.now();
  await connection.runAndReadAll(
    'CREATE TEMP TABLE context_values(context VARCHAR PRIMARY KEY)'
  );
  await connection.runAndReadAll(
    'CREATE TEMP TABLE path_values(path VARCHAR PRIMARY KEY)'
  );
  await connection.runAndReadAll(`CREATE TEMP TABLE source_values(
    source_key VARCHAR PRIMARY KEY,
    source VARCHAR,
    source_label VARCHAR,
    source_pgn VARCHAR,
    source_src VARCHAR,
    source_type VARCHAR
  )`);
  await connection.runAndReadAll(
    'CREATE TEMP TABLE meta_values(meta VARCHAR PRIMARY KEY)'
  );
  for (const group of groups) {
    const source = sourceSql(group);
    await connection.runAndReadAll(
      `INSERT OR IGNORE INTO context_values SELECT DISTINCT cast(context AS VARCHAR) FROM ${source}`
    );
    await connection.runAndReadAll(
      `INSERT OR IGNORE INTO path_values SELECT DISTINCT cast(path AS VARCHAR) FROM ${source}`
    );
    await connection.runAndReadAll(`
      INSERT OR IGNORE INTO source_values
      SELECT DISTINCT ${sourceKey(group)}, ${SOURCE_COLUMNS.map(name => sourceField(group, name)).join(', ')}
      FROM ${source} src
    `);
    if (group.columns.some(column => column.name === 'meta')) {
      await connection.runAndReadAll(
        `INSERT OR IGNORE INTO meta_values SELECT DISTINCT cast(meta AS VARCHAR) FROM ${source} WHERE meta IS NOT NULL`
      );
    }
  }
  await connection.runAndReadAll(`
    CREATE TEMP TABLE context_dim AS
      SELECT (row_number() OVER (ORDER BY context) - 1)::UINTEGER AS context_id, context FROM context_values;
    CREATE TEMP TABLE path_dim AS
      SELECT (row_number() OVER (ORDER BY path) - 1)::UINTEGER AS path_id, path FROM path_values;
    CREATE TEMP TABLE source_dim AS
      SELECT (row_number() OVER (ORDER BY source_key) - 1)::UINTEGER AS source_id, * FROM source_values;
    CREATE TEMP TABLE meta_dim AS
      SELECT (row_number() OVER (ORDER BY meta) - 1)::UINTEGER AS meta_id, meta FROM meta_values;
  `);
  return performance.now() - started;
}

function buildTasks(groups: SourceGroup[]): Task[] {
  const tasks = new Map<string, Task>();
  for (const group of groups) {
    for (const day of group.days) {
      const key = `${day}:${group.family}`;
      const task = tasks.get(key) || { day, family: group.family, groups: [] };
      task.groups.push(group);
      tasks.set(key, task);
    }
  }
  return [...tasks.values()].sort(
    (a, b) => a.day.localeCompare(b.day) || a.family - b.family
  );
}

async function writeDimensions(
  connection: Awaited<ReturnType<DuckDBInstance['connect']>>,
  root: string,
  level: number
): Promise<number> {
  let bytes = 0;
  const catalogDir = path.join(root, 'catalog');
  await fs.mkdir(catalogDir, { recursive: true });
  for (const table of ['context_dim', 'path_dim', 'source_dim', 'meta_dim']) {
    const output = path.join(catalogDir, `${table}.parquet`);
    await connection.runAndReadAll(
      `COPY ${table} TO ${quote(output)} (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL ${level})`
    );
    bytes += (await fs.stat(output)).size;
  }
  return bytes;
}

async function writePathDaily(
  connection: Awaited<ReturnType<DuckDBInstance['connect']>>,
  groups: SourceGroup[],
  outputRoot: string,
  typed: boolean,
  rowGroupSize = 16384
): Promise<{
  bytes: number;
  files: number;
  writeMs: number;
  taskFiles: Map<string, string>;
}> {
  const name = `${typed ? 'typed' : 'current'}-path-daily-zstd9-rg${rowGroupSize}`;
  const root = path.join(outputRoot, name);
  await fs.mkdir(root, { recursive: true });
  const started = performance.now();
  let bytes = 0;
  let files = 0;
  const taskFiles = new Map<string, string>();
  for (const group of groups) {
    for (const day of group.days) {
      const directory = path.join(root, group.key, `date=${day}`);
      await fs.mkdir(directory, { recursive: true });
      const output = path.join(directory, 'archive.parquet');
      const expected = pathDailySelect(group, day, typed);
      const order = typed
        ? 'event_time, received_delay_us, context, path'
        : [
            group.eventColumn,
            group.columns.some(column => column.name === 'received_delay_us')
              ? 'received_delay_us'
              : 'received_timestamp',
            'context',
            'path',
          ]
            .filter(column =>
              group.columns.some(candidate => candidate.name === column)
            )
            .map(identifier)
            .join(', ');
      await connection.runAndReadAll(`
        COPY (SELECT * FROM (${expected}) ORDER BY ${order})
        TO ${quote(output)}
        (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 9, ROW_GROUP_SIZE ${rowGroupSize})
      `);
      const actual = `SELECT * FROM read_parquet(${quote(output)}, hive_partitioning=false)`;
      const difference = await connection.runAndReadAll(`
        SELECT count(*) AS n FROM (
          (SELECT * FROM (${expected}) EXCEPT ALL ${actual})
          UNION ALL
          (${actual} EXCEPT ALL SELECT * FROM (${expected}))
        )
      `);
      if (difference.getRowObjects()[0].n !== 0n) {
        throw new Error(`${name} content mismatch: ${group.key} ${day}`);
      }
      bytes += (await fs.stat(output)).size;
      files++;
      taskFiles.set(`${group.key}:${day}`, output);
    }
  }
  return {
    bytes,
    files,
    writeMs: performance.now() - started,
    taskFiles,
  };
}

async function writeOptimized(
  connection: Awaited<ReturnType<DuckDBInstance['connect']>>,
  tasks: Task[],
  outputRoot: string,
  level: number,
  rowGroupSize: number,
  verify: boolean
): Promise<{
  bytes: number;
  files: number;
  writeMs: number;
  taskFiles: Map<string, string>;
}> {
  const root = path.join(
    outputRoot,
    `optimized-zstd${level}-rg${rowGroupSize}`
  );
  await fs.mkdir(root, { recursive: true });
  const started = performance.now();
  let bytes = await writeDimensions(connection, root, level);
  const taskFiles = new Map<string, string>();
  for (const [taskIndex, task] of tasks.entries()) {
    if (taskIndex % 20 === 0) {
      console.error(
        `Writing ZSTD-${level}/RG-${rowGroupSize} shared task ${taskIndex + 1}/${tasks.length}`
      );
    }
    const directory = path.join(root, `date=${task.day}`);
    await fs.mkdir(directory, { recursive: true });
    const output = path.join(directory, `family=${task.family}.parquet`);
    const expected = task.groups
      .map(group => `(${normalizedSelect(group, task.day)})`)
      .join(' UNION ALL ');
    await connection.runAndReadAll(`
      COPY (SELECT * FROM (${expected}) ORDER BY path_id, context_id, event_time)
      TO ${quote(output)}
      (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL ${level}, ROW_GROUP_SIZE ${rowGroupSize})
    `);
    if (verify) {
      const actual = `SELECT * FROM read_parquet(${quote(output)}, hive_partitioning=false)`;
      const hashColumns = [
        identifier('context_id'),
        identifier('path_id'),
        identifier('source_id'),
        identifier('meta_id'),
        `epoch_us(${identifier('event_time')})`,
        identifier('received_delay_us'),
        ...task.groups[0].payload.map(column => identifier(column.name)),
      ].join(', ');
      const fingerprint = async (
        from: string
      ): Promise<Record<string, unknown>> =>
        (
          await connection.runAndReadAll(`
            SELECT count(*)::HUGEINT AS rows,
                   bit_xor(hash(${hashColumns})) AS xor_hash,
                   sum(hash(${hashColumns}))::HUGEINT AS sum_hash
            FROM (${from})`)
        ).getRowObjects()[0];
      const before = await fingerprint(expected);
      const after = await fingerprint(actual);
      if (
        before.rows !== after.rows ||
        before.xor_hash !== after.xor_hash ||
        before.sum_hash !== after.sum_hash
      ) {
        throw new Error(
          `Content mismatch: day=${task.day} family=${task.family} ` +
            JSON.stringify({ before, after }, (_key, value) =>
              typeof value === 'bigint' ? value.toString() : value
            )
        );
      }
    }
    bytes += (await fs.stat(output)).size;
    taskFiles.set(`${task.day}:${task.family}`, output);
  }
  return {
    bytes,
    files: tasks.length + 4,
    writeMs: performance.now() - started,
    taskFiles,
  };
}

async function timingRoundRobin(
  connection: Awaited<ReturnType<DuckDBInstance['connect']>>,
  statements: Record<string, string>
): Promise<Record<string, { firstMs: number; warmMedianMs: number }>> {
  const keys = Object.keys(statements);
  const samples = new Map(keys.map(key => [key, [] as number[]]));
  for (let round = 0; round < 14; round++) {
    const direction = round % 2 === 0 ? keys : [...keys].reverse();
    const offset = Math.floor(round / 2) % keys.length;
    const order = [...direction.slice(offset), ...direction.slice(0, offset)];
    for (const key of order) {
      const started = performance.now();
      await connection.runAndReadAll(statements[key]);
      samples.get(key)!.push(performance.now() - started);
    }
  }
  return Object.fromEntries(
    keys.map(key => {
      const values = samples.get(key)!;
      return [
        key,
        { firstMs: values[0], warmMedianMs: median(values.slice(1)) },
      ];
    })
  );
}

async function queryBenchmarks(
  connection: Awaited<ReturnType<DuckDBInstance['connect']>>,
  groups: SourceGroup[],
  currentFiles: Map<string, string>,
  currentFiles64k: Map<string, string>,
  typedFiles: Map<string, string>,
  taskFiles6Rg262k: Map<string, string>,
  taskFiles9Rg16k: Map<string, string>,
  taskFiles9Rg64k: Map<string, string>,
  taskFiles9Rg262k: Map<string, string>
): Promise<unknown[]> {
  const selected = [...groups].sort((a, b) => b.bytes - a.bytes).slice(0, 8);
  const output: unknown[] = [];
  for (const group of selected) {
    const source = sourceSql(group);
    const event = eventExpression(group, 'src');
    const bounds = await connection.runAndReadAll(
      `SELECT max(epoch_ms(${event}))::BIGINT AS max_ms FROM ${source} src`
    );
    const maxMs = Number(bounds.getRowObjects()[0].max_ms);
    const minMs = maxMs - 24 * 60 * 60 * 1000;
    const numeric = group.payload.find(column =>
      /DOUBLE|FLOAT|DECIMAL|INT/.test(column.type)
    );
    const baselineAggregate = numeric
      ? `avg(src.${identifier(numeric.name)})`
      : 'count(*)';
    const optimizedAggregate = numeric
      ? `avg(${identifier(numeric.name)})`
      : 'count(*)';
    const relevant6Rg262k = group.days.map(day =>
      taskFiles6Rg262k.get(`${day}:${group.family}`)!
    );
    const relevant9Rg16k = group.days.map(day =>
      taskFiles9Rg16k.get(`${day}:${group.family}`)!
    );
    const relevant9Rg64k = group.days.map(day =>
      taskFiles9Rg64k.get(`${day}:${group.family}`)!
    );
    const relevant9Rg262k = group.days.map(day =>
      taskFiles9Rg262k.get(`${day}:${group.family}`)!
    );
    const current = group.days.map(day =>
      currentFiles.get(`${group.key}:${day}`)!
    );
    const current64k = group.days.map(day =>
      currentFiles64k.get(`${group.key}:${day}`)!
    );
    const typed = group.days.map(day => typedFiles.get(`${group.key}:${day}`)!);
    const pathIdResult = await connection.runAndReadAll(
      `SELECT path_id FROM path_dim WHERE path = ${quote(group.pathValue)}`
    );
    const pathId = Number(pathIdResult.getRowObjects()[0].path_id);
    const contextIdResult = await connection.runAndReadAll(
      `SELECT context_id FROM context_dim WHERE context = ${quote(group.contextValue)}`
    );
    const contextId = Number(contextIdResult.getRowObjects()[0].context_id);
    const optimizedSql = (files: string[]): string => `
      SELECT ${optimizedAggregate} AS value
      FROM read_parquet(${sourceList(files)}, hive_partitioning=false)
      WHERE path_id = ${pathId}
        AND context_id = ${contextId}
        AND event_time >= epoch_ms(${minMs})
        AND event_time < epoch_ms(${maxMs + 1})`;
    const timings = await timingRoundRobin(connection, {
      baseline: `SELECT ${baselineAggregate} AS value FROM ${source} src
         WHERE ${event} >= epoch_ms(${minMs}) AND ${event} < epoch_ms(${maxMs + 1})`,
      currentPathDailyZstd9: `SELECT ${baselineAggregate} AS value
         FROM read_parquet(${sourceList(current)}, hive_partitioning=false) src
         WHERE ${event} >= epoch_ms(${minMs}) AND ${event} < epoch_ms(${maxMs + 1})`,
      currentPathDailyZstd9Rg64k: `SELECT ${baselineAggregate} AS value
         FROM read_parquet(${sourceList(current64k)}, hive_partitioning=false) src
         WHERE ${event} >= epoch_ms(${minMs}) AND ${event} < epoch_ms(${maxMs + 1})`,
      typedPathDailyZstd9: `SELECT ${baselineAggregate.replace(/src\./g, '')} AS value
         FROM read_parquet(${sourceList(typed)}, hive_partitioning=false)
         WHERE event_time >= epoch_ms(${minMs}) AND event_time < epoch_ms(${maxMs + 1})`,
      optimizedZstd6Rg262k: optimizedSql(relevant6Rg262k),
      optimizedZstd9Rg16k: optimizedSql(relevant9Rg16k),
      optimizedZstd9Rg64k: optimizedSql(relevant9Rg64k),
      optimizedZstd9Rg262k: optimizedSql(relevant9Rg262k),
    });
    output.push({
      key: group.key,
      sourceFiles: group.files.length,
      optimizedFiles: relevant9Rg262k.length,
      payload: group.payload,
      ...timings,
    });
  }
  return output;
}

async function main(): Promise<void> {
  const archive = path.resolve(process.argv[2] || '');
  const outputRoot = path.resolve(process.argv[3] || '');
  if (!process.argv[2] || !process.argv[3]) {
    throw new Error(
      'Usage: benchmark-optimized-archive <archive-directory> <empty-output-directory>'
    );
  }
  if (!(await fs.stat(archive)).isDirectory())
    throw new Error('Archive not found');
  await fs.mkdir(outputRoot, { recursive: false });
  const groups = await discoverRawGroups(archive);
  const instance = await DuckDBInstance.create(':memory:');
  const connection = await instance.connect();
  try {
    const spill = path.join(outputRoot, 'spill');
    await fs.mkdir(spill, { recursive: true });
    await connection.runAndReadAll("SET memory_limit = '4GB'");
    await connection.runAndReadAll('SET threads = 2');
    await connection.runAndReadAll('SET preserve_insertion_order = false');
    await connection.runAndReadAll(`SET temp_directory = ${quote(spill)}`);
    await prepareGroups(connection, groups);
    console.error(`Prepared ${groups.length} raw path/year groups`);
    const dimensionBuildMs = await buildDimensions(connection, groups);
    console.error('Built identity dimensions');
    const tasks = buildTasks(groups);
    const currentPathDaily = await writePathDaily(
      connection,
      groups,
      outputRoot,
      false
    );
    console.error('Wrote current ZSTD-9/16K path-daily control');
    const currentPathDaily64k = await writePathDaily(
      connection,
      groups,
      outputRoot,
      false,
      65536
    );
    console.error('Wrote compatible ZSTD-9/64K path-daily candidate');
    const typedPathDaily = await writePathDaily(
      connection,
      groups,
      outputRoot,
      true
    );
    console.error('Wrote typed ZSTD-9/16K path-daily control');
    const zstd6Rg262k = await writeOptimized(
      connection,
      tasks,
      outputRoot,
      6,
      262144,
      true
    );
    const zstd9Rg16k = await writeOptimized(
      connection,
      tasks,
      outputRoot,
      9,
      16384,
      true
    );
    const zstd9Rg64k = await writeOptimized(
      connection,
      tasks,
      outputRoot,
      9,
      65536,
      true
    );
    const zstd9Rg262k = await writeOptimized(
      connection,
      tasks,
      outputRoot,
      9,
      262144,
      true
    );
    const queries = await queryBenchmarks(
      connection,
      groups,
      currentPathDaily.taskFiles,
      currentPathDaily64k.taskFiles,
      typedPathDaily.taskFiles,
      zstd6Rg262k.taskFiles,
      zstd9Rg16k.taskFiles,
      zstd9Rg64k.taskFiles,
      zstd9Rg262k.taskFiles
    );
    console.log(
      JSON.stringify(
        {
          archive,
          baseline: {
            bytes: groups.reduce((sum, group) => sum + group.bytes, 0),
            files: groups.reduce((sum, group) => sum + group.files.length, 0),
          },
          rows: String(
            (
              await connection.runAndReadAll(
                `SELECT sum(n)::HUGEINT AS n FROM (${groups
                  .map(
                    group =>
                      `SELECT count(*)::HUGEINT AS n FROM ${sourceSql(group)}`
                  )
                  .join(' UNION ALL ')})`
              )
            ).getRowObjects()[0].n
          ),
          sourceGroups: groups.length,
          valueFamilies: new Set(groups.map(group => group.family)).size,
          dailyFamilyFiles: tasks.length,
          dimensionBuildMs,
          currentPathDailyZstd9: {
            bytes: currentPathDaily.bytes,
            files: currentPathDaily.files,
            writeMs: currentPathDaily.writeMs,
          },
          currentPathDailyZstd9Rg64k: {
            bytes: currentPathDaily64k.bytes,
            files: currentPathDaily64k.files,
            writeMs: currentPathDaily64k.writeMs,
          },
          typedPathDailyZstd9: {
            bytes: typedPathDaily.bytes,
            files: typedPathDaily.files,
            writeMs: typedPathDaily.writeMs,
          },
          optimizedZstd6Rg262k: {
            bytes: zstd6Rg262k.bytes,
            files: zstd6Rg262k.files,
            writeMs: zstd6Rg262k.writeMs,
          },
          optimizedZstd9Rg16k: {
            bytes: zstd9Rg16k.bytes,
            files: zstd9Rg16k.files,
            writeMs: zstd9Rg16k.writeMs,
          },
          optimizedZstd9Rg64k: {
            bytes: zstd9Rg64k.bytes,
            files: zstd9Rg64k.files,
            writeMs: zstd9Rg64k.writeMs,
          },
          optimizedZstd9Rg262k: {
            bytes: zstd9Rg262k.bytes,
            files: zstd9Rg262k.files,
            writeMs: zstd9Rg262k.writeMs,
          },
          queries,
        },
        null,
        2
      )
    );
  } finally {
    connection.disconnectSync();
    instance.closeSync();
  }
}

void main().catch(error => {
  console.error(error);
  process.exitCode = 1;
});
