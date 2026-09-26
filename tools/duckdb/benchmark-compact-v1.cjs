#!/usr/bin/env node
/* Compare the current Parquet file with a lossless rewrite that keeps every
 * column and path but uses DuckDB Parquet V1/V2 encodings and ZSTD levels 3/9.
 * Prints aggregate measurements only; it does not print recorded values.
 */
const fs = require('node:fs/promises');
const os = require('node:os');
const path = require('node:path');
const { performance } = require('node:perf_hooks');
const { DuckDBInstance } = require('@duckdb/node-api');

const input = process.argv[2];
if (!input) {
  console.error('Usage: node benchmark-compact-v1.cjs INPUT.parquet');
  process.exit(2);
}

const quote = value => `'${value.replace(/'/g, "''")}'`;
const median = values =>
  [...values].sort((a, b) => a - b)[Math.floor(values.length / 2)];

async function main() {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'sk-v1-compact-'));
  const instance = await DuckDBInstance.create(':memory:');
  const connection = await instance.connect();
  try {
    const source = `read_parquet(${quote(input)}, hive_partitioning=false)`;
    const originalCount = Number(
      (
        await connection.runAndReadAll(`SELECT count(*) AS n FROM ${source}`)
      ).getRowObjects()[0].n
    );
    const rewrite = async (output, parquetVersion, level, rowGroupSize) => {
      const relation = `read_parquet(${quote(output)}, hive_partitioning=false)`;
      const writeStart = performance.now();
      await connection.runAndReadAll(`COPY (
        SELECT * FROM ${source}
      ) TO ${quote(output)} (
        FORMAT PARQUET, PARQUET_VERSION '${parquetVersion}',
        COMPRESSION ZSTD, COMPRESSION_LEVEL ${level}, ROW_GROUP_SIZE ${rowGroupSize}
      )`);
      const writeMs = performance.now() - writeStart;
      const row = (
        await connection.runAndReadAll(`SELECT
          (SELECT count(*) FROM ${relation}) AS output_count,
          (SELECT count(*) FROM (SELECT * FROM ${source} EXCEPT ALL SELECT * FROM ${relation})) AS missing_count,
          (SELECT count(*) FROM (SELECT * FROM ${relation} EXCEPT ALL SELECT * FROM ${source})) AS extra_count`)
      ).getRowObjects()[0];
      const outputCount = Number(row.output_count);
      const missingCount = Number(row.missing_count);
      const extraCount = Number(row.extra_count);
      if (
        outputCount !== originalCount ||
        missingCount !== 0 ||
        extraCount !== 0
      ) {
        throw new Error(
          `Lossless ${parquetVersion}/ZSTD-${level} verification failed: ${originalCount} input, ${outputCount} output, ${missingCount} missing, ${extraCount} extra`
        );
      }
      return { relation, writeMs, bytes: (await fs.stat(output)).size };
    };
    const candidates = [
      ['v1Zstd3', 'V1', 3, 4096],
      ['v1Zstd9', 'V1', 9, 4096],
      ['v1Zstd9Rows16k', 'V1', 9, 16384],
      ['v1Zstd9Rows122k', 'V1', 9, 122880],
      ['v2Zstd3', 'V2', 3, 4096],
      ['v2Zstd9', 'V2', 9, 4096],
    ];
    const results = {};
    for (const [name, version, level, rowGroupSize] of candidates) {
      const output = path.join(root, `${name}.parquet`);
      results[name] = {
        output,
        ...(await rewrite(output, version, level, rowGroupSize)),
      };
    }

    const bounds = (
      await connection.runAndReadAll(
        `SELECT max(TRY_CAST(signalk_timestamp AS TIMESTAMP)) AS latest FROM ${source}`
      )
    ).getRowObjects()[0];
    const latest = bounds.latest;
    const schemaRows = (
      await connection.runAndReadAll(`DESCRIBE SELECT * FROM ${source}`)
    ).getRowObjects();
    const schemaNames = new Set(schemaRows.map(column => column.column_name));
    const timestampType = schemaRows.find(
      column => column.column_name === 'signalk_timestamp'
    )?.column_type;
    const valueColumns = schemaNames.has('value')
      ? 'value'
      : schemaNames.has('value_latitude') && schemaNames.has('value_longitude')
        ? 'value_latitude, value_longitude'
        : '*';
    const cutoffIso = latest
      ? new Date(new Date(latest).getTime() - 10 * 60 * 1000).toISOString()
      : null;
    const cutoff = cutoffIso
      ? timestampType === 'VARCHAR'
        ? `WHERE signalk_timestamp >= ${quote(cutoffIso)}`
        : `WHERE signalk_timestamp >= TIMESTAMP ${quote(
            cutoffIso.replace('T', ' ').replace('Z', '')
          )}`
      : '';
    const timeQuery = async relation => {
      const timings = [];
      for (let i = 0; i < 8; i++) {
        const start = performance.now();
        await connection.runAndReadAll(
          `SELECT signalk_timestamp, ${valueColumns} FROM ${relation} ${cutoff} ORDER BY signalk_timestamp LIMIT 600`
        );
        if (i > 0) timings.push(performance.now() - start);
      }
      return median(timings);
    };
    const originalBytes = (await fs.stat(input)).size;
    const metadataSummary = async file => {
      const summary = (
        await connection.runAndReadAll(`SELECT
          count(DISTINCT row_group_id) AS row_groups,
          min(row_group_num_rows) AS min_rows_per_group,
          max(row_group_num_rows) AS max_rows_per_group,
          string_agg(DISTINCT compression, ',') AS compression
          FROM parquet_metadata(${quote(file)})`)
      ).getRowObjects()[0];
      return {
        rowGroups: Number(summary.row_groups),
        minRowsPerGroup: Number(summary.min_rows_per_group),
        maxRowsPerGroup: Number(summary.max_rows_per_group),
        compression: summary.compression,
      };
    };
    const originalWarmReadMedianMs = await timeQuery(source);
    const candidateResults = {};
    for (const [name] of candidates) {
      const result = results[name];
      candidateResults[name] = {
        bytes: result.bytes,
        reductionPercent: (1 - result.bytes / originalBytes) * 100,
        writeMs: result.writeMs,
        warmReadMedianMs: await timeQuery(result.relation),
        metadata: await metadataSummary(result.output),
      };
    }
    console.log(
      JSON.stringify(
        {
          rows: originalCount,
          timestampType,
          originalBytes,
          originalWarmReadMedianMs,
          originalMetadata: await metadataSummary(input),
          candidates: candidateResults,
          verifiedLossless: true,
        },
        null,
        2
      )
    );
  } finally {
    connection.disconnectSync();
    await instance.closeSync();
    await fs.rm(root, { recursive: true, force: true });
  }
}

main().catch(error => {
  console.error(error);
  process.exitCode = 1;
});
