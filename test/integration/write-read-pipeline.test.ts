/**
 * End-to-end integration of the storage pipeline with no mocks on the data
 * path: records are inserted into a real SQLite buffer, exported to real
 * Hive-partitioned Parquet files by the ParquetExportService (which drives
 * the real ParquetWriter and @dsnp/parquetjs), and then read back through a
 * real DuckDB instance — both directly from the Parquet files and through the
 * SQLite buffer federation used for live (not-yet-exported) data.
 *
 * This is the plugin's core promise: "data you write can be queried back",
 * exercised against the real native dependencies rather than test doubles.
 */
import { expect } from 'chai';
import * as os from 'os';
import * as path from 'path';
import * as fs from 'fs-extra';
import { SQLiteBuffer } from '../../src/utils/sqlite-buffer';
import { ParquetWriter } from '../../src/parquet-writer';
import { ParquetExportService } from '../../src/services/parquet-export-service';
import { DuckDBPool } from '../../src/utils/duckdb-pool';
import { stageBufferTable } from '../../src/utils/buffer-staging';
import {
  buildBufferScalarSubquery,
  buildBufferObjectSubquery,
} from '../../src/utils/buffer-sql-builder';
import { HivePathBuilder } from '../../src/utils/hive-path-builder';
import { createFakeSignalK, FakeSignalK } from './helpers/fake-signalk';
import { makeScalarRecord, makePositionRecord } from './helpers/records';
import { ParquetCompression, ParquetField } from '../../src/types';
import { migrateSharedAis } from '../../src/utils/migrate-shared-ais';

// A fixed historical day so export (which excludes "today") always includes it
// and the assertions never depend on the wall clock.
const DAY = new Date('2024-06-01T00:00:00.000Z');
const CONTEXT = 'vessels.test-self';

// Thin wrappers binding this suite's context to the shared builders.
const scalarRecord = (
  signalkPath: string,
  value: number,
  isoTime: string,
  sourceLabel?: string
) => makeScalarRecord(CONTEXT, signalkPath, value, isoTime, sourceLabel);

const positionRecord = (latitude: number, longitude: number, isoTime: string) =>
  makePositionRecord(CONTEXT, latitude, longitude, isoTime);

/** DuckDB accepts forward slashes on every platform; Windows paths use \. */
function toGlob(p: string): string {
  return p.replace(/\\/g, '/');
}

describe('storage pipeline (SQLite buffer -> Parquet -> DuckDB)', function () {
  // Native DuckDB init may install the spatial extension on first run.
  this.timeout(30000);

  let host: FakeSignalK;
  let buffer: SQLiteBuffer;
  let exportService: ParquetExportService;
  const hive = new HivePathBuilder();

  beforeEach(async () => {
    host = createFakeSignalK();
    buffer = new SQLiteBuffer({
      dbPath: path.join(host.dataDir, 'buffer.db'),
    });
    const writer = new ParquetWriter({ format: 'parquet', app: host.app });
    exportService = new ParquetExportService(
      buffer,
      writer,
      {
        outputDirectory: host.dataDir,
        filenamePrefix: 'signalk_data',
        useHivePartitioning: true,
        dailyExportHour: 4,
      },
      host.app
    );
    await DuckDBPool.initialize();
  });

  afterEach(async () => {
    // Guard each resource so a partial beforeEach failure doesn't mask the
    // real setup error with a teardown throw.
    await DuckDBPool.shutdown();
    if (buffer?.isOpen()) buffer.close();
    await host?.cleanup();
  });

  it('writes ZSTD raw files and reads them through DuckDB', async () => {
    const records = Array.from({ length: 1000 }, (_, index) =>
      scalarRecord(
        'navigation.speedOverGround',
        index % 20,
        new Date(DAY.getTime() + index * 1000).toISOString()
      )
    );
    const zstdPath = path.join(host.dataDir, 'zstd.parquet');
    const writer = new ParquetWriter({
      format: 'parquet',
      app: host.app,
    });
    const schema = await writer.createParquetSchema(records);
    expect(
      Object.values(schema.fields as Record<string, ParquetField>).every(
        field => field.compression === ParquetCompression.ZSTD
      )
    ).to.equal(true);
    await writer.writeRecords(zstdPath, records);

    const conn = await DuckDBPool.getConnection();
    try {
      const res = await conn.runAndReadAll(
        `SELECT COUNT(*) AS n
         FROM read_parquet('${toGlob(zstdPath)}')`
      );
      const row = res.getRowObjects()[0] as { n: bigint };
      expect(Number(row.n)).to.equal(records.length);
      const codecs = await conn.runAndReadAll(
        `SELECT DISTINCT compression FROM parquet_metadata('${toGlob(zstdPath)}')`
      );
      expect(
        codecs.getRowObjects().map(codec => codec.compression)
      ).to.deep.equal(['ZSTD']);
    } finally {
      conn.disconnectSync();
    }
  });

  it('exports buffered scalar records to a Hive-partitioned parquet tree', async () => {
    for (let i = 0; i < 5; i++) {
      buffer.insert(
        scalarRecord(
          'navigation.speedOverGround',
          5,
          `2024-06-01T10:0${i}:00.000Z`
        )
      );
    }

    const result = await exportService.exportDayToParquet(DAY);

    expect(result.errors).to.deep.equal([]);
    expect(result.recordsExported).to.equal(5);
    expect(result.filesCreated).to.have.lengthOf(1);

    // The file lands under the documented Hive layout.
    const created = result.filesCreated[0];
    expect(created).to.match(/tier=raw/);
    expect(created).to.match(/context=vessels__test-self/);
    expect(created).to.match(/path=navigation__speedOverGround/);
    expect(created).to.match(/year=2024/);
    expect(created).to.match(/day=153/); // 2024-06-01 is day 153 of a leap year
    expect(await fs.pathExists(created)).to.equal(true);
  });

  it('exports completed hours and compacts them into sorted Zstd daily files', async () => {
    const signalkPath = 'navigation.speedOverGround';
    const day = new Date(Date.now() - 72 * 3600000);
    day.setUTCHours(0, 0, 0, 0);
    const firstHour = new Date(day.getTime() + 10 * 3600000);
    const secondHour = new Date(day.getTime() + 11 * 3600000);
    buffer.insert(scalarRecord(signalkPath, 11, secondHour.toISOString()));
    buffer.insert(scalarRecord(signalkPath, 9, firstHour.toISOString()));
    const first = await exportService.exportHourToParquet(firstHour);
    const second = await exportService.exportHourToParquet(secondHour);
    expect(first.errors).to.deep.equal([]);
    expect(second.errors).to.deep.equal([]);
    expect(first.recordsExported + second.recordsExported).to.equal(2);
    const hourlyConn = await DuckDBPool.getConnection();
    try {
      for (const file of [...first.filesCreated, ...second.filesCreated]) {
        const metadata = await hourlyConn.runAndReadAll(
          `SELECT DISTINCT compression FROM parquet_metadata('${toGlob(file)}')`
        );
        expect(
          metadata.getRowObjects().map(row => row.compression)
        ).to.deep.equal(['ZSTD']);
      }
    } finally {
      hourlyConn.disconnectSync();
    }
    const compacted = await exportService.compactDay(day);
    expect(compacted.errors).to.deep.equal([]);
    expect(compacted.filesCompacted).to.equal(1);
    const dir = path.dirname(first.filesCreated[0]);
    const files = (await fs.readdir(dir)).filter(name =>
      name.endsWith('.parquet')
    );
    expect(files).to.have.lengthOf(1);
    expect(files[0]).to.match(/^daily_compact_/);
    const conn = await DuckDBPool.getConnection();
    try {
      const result = await conn.runAndReadAll(
        `SELECT value, signalk_timestamp FROM read_parquet('${toGlob(path.join(dir, files[0]))}') ORDER BY signalk_timestamp`
      );
      const rows = result.getRowObjects() as Array<{ value: string }>;
      expect(rows.map(row => Number(row.value))).to.deep.equal([9, 11]);
      const metadata = await conn.runAndReadAll(
        `SELECT DISTINCT compression FROM parquet_metadata('${toGlob(path.join(dir, files[0]))}')`
      );
      expect(
        metadata.getRowObjects().map(row => row.compression)
      ).to.deep.equal(['ZSTD']);
    } finally {
      conn.disconnectSync();
    }
  });

  it('shares one AIS file across vessels and verifies its row count before clearing SQLite', async () => {
    const hour = new Date(Date.now() - 72 * 3600000);
    hour.setUTCHours(11, 0, 0, 0);
    const contexts = [
      'vessels.urn:mrn:imo:mmsi:123456789',
      'vessels.urn:mrn:imo:mmsi:987654321',
    ];
    for (const [index, context] of contexts.entries()) {
      buffer.insert(
        makePositionRecord(
          context,
          37 + index,
          -122 - index,
          new Date(hour.getTime() + index * 1000).toISOString()
        )
      );
    }
    const result = await exportService.exportHourToParquet(hour);
    expect(result.errors).to.deep.equal([]);
    expect(result.recordsExported).to.equal(2);
    expect(result.filesCreated).to.have.lengthOf(1);
    expect(result.filesCreated[0]).to.include('context=ais__shared');
    expect(buffer.getStats().pendingRecords).to.equal(0);
    const conn = await DuckDBPool.getConnection();
    try {
      const rows = await conn.runAndReadAll(
        `SELECT context, value_latitude FROM read_parquet('${toGlob(result.filesCreated[0])}', hive_partitioning=false) ORDER BY context`
      );
      expect(rows.getRowObjects().map(row => row.context)).to.deep.equal(
        contexts
      );
    } finally {
      conn.disconnectSync();
    }
  });

  it('keeps SQLite rows pending when the Parquet footer count does not match', async () => {
    const hour = new Date(Date.now() - 72 * 3600000);
    hour.setUTCHours(12, 0, 0, 0);
    buffer.insert(
      scalarRecord('navigation.speedOverGround', 5, hour.toISOString())
    );
    const writer = new ParquetWriter({ format: 'parquet', app: host.app });
    writer.getParquetRowCount = async () => 0;
    const service = new ParquetExportService(
      buffer,
      writer,
      {
        outputDirectory: host.dataDir,
        filenamePrefix: 'signalk_data',
        useHivePartitioning: true,
        dailyExportHour: 4,
      },
      host.app
    );
    const result = await service.exportHourToParquet(hour);
    expect(result.errors).to.have.lengthOf(1);
    expect(result.filesCreated).to.deep.equal([]);
    expect(buffer.getStats().pendingRecords).to.equal(1);
  });

  it('migrates old AIS files into one verified shared ZSTD file and deletes originals', async () => {
    const writer = new ParquetWriter({ format: 'parquet', app: host.app });
    const contexts = [
      'vessels.urn:mrn:imo:mmsi:123456789',
      'vessels.urn:mrn:imo:mmsi:987654321',
    ];
    for (const [index, context] of contexts.entries()) {
      const file = path.join(
        hive.buildPath(
          host.dataDir,
          'raw',
          context,
          'navigation.position',
          DAY
        ),
        `old-${index}.parquet`
      );
      await writer.writeRecords(file, [
        makePositionRecord(
          context,
          37 + index,
          -122,
          '2024-06-01T11:00:00.000Z'
        ),
      ]);
    }
    const preview = await migrateSharedAis(host.dataDir);
    expect(preview.sourceFiles).to.equal(2);
    const result = await migrateSharedAis(host.dataDir, true);
    expect(result.sourceFiles).to.equal(2);
    expect(result.sharedFiles).to.equal(1);
    expect(result.rows).to.equal(2);
    expect(result.deletedSourceFiles).to.equal(2);
    for (const context of contexts) {
      const oldDirectory = hive.buildPath(
        host.dataDir,
        'raw',
        context,
        'navigation.position',
        DAY
      );
      expect(
        await fs.stat(oldDirectory).then(
          () => true,
          () => false
        )
      ).to.equal(false);
    }
    const shared = path.join(
      host.dataDir,
      'tier=raw',
      'context=ais__shared',
      'path=navigation__position',
      'year=2024',
      'day=153'
    );
    const files = await fs.readdir(shared);
    expect(files).to.have.lengthOf(1);
    const conn = await DuckDBPool.getConnection();
    try {
      const rows = await conn.runAndReadAll(
        `SELECT context FROM read_parquet('${toGlob(path.join(shared, files[0]))}', hive_partitioning=false) ORDER BY context`
      );
      expect(rows.getRowObjects().map(row => row.context)).to.deep.equal(
        contexts
      );
    } finally {
      conn.disconnectSync();
    }
  });

  it('restores source files after an interrupted daily compaction', async () => {
    const hour = new Date(Date.now() - 72 * 3600000);
    hour.setUTCHours(10, 0, 0, 0);
    buffer.insert(
      scalarRecord('navigation.speedOverGround', 9, hour.toISOString())
    );
    const exported = await exportService.exportHourToParquet(hour);
    const source = exported.filesCreated[0];
    const dir = path.dirname(source);
    const trash = path.join(dir, '.daily-compaction-trash-interrupted');
    await fs.ensureDir(trash);
    await fs.rename(source, path.join(trash, path.basename(source)));
    await exportService.recoverStrandedCompactions();
    expect(await fs.pathExists(source)).to.equal(true);
    expect(await fs.pathExists(trash)).to.equal(false);
  });

  it('catches up only completed hours, leaving the current hour queryable in SQLite', async () => {
    const old = new Date(Date.now() - 2 * 3600000).toISOString();
    const current = new Date().toISOString();
    buffer.insert(scalarRecord('navigation.speedOverGround', 1, old));
    buffer.insert(scalarRecord('navigation.speedOverGround', 2, current));
    const exported = await exportService.exportAllUnexported();
    expect(exported.errors).to.deep.equal([]);
    expect(exported.recordsExported).to.equal(1);
    expect(buffer.getStats().pendingRecords).to.equal(1);
  });

  it('reads exported scalar values back through DuckDB', async () => {
    for (let i = 0; i < 5; i++) {
      buffer.insert(
        scalarRecord(
          'navigation.speedOverGround',
          4 + i, // 4,5,6,7,8 -> avg 6
          `2024-06-01T10:0${i}:00.000Z`
        )
      );
    }
    await exportService.exportDayToParquet(DAY);

    const glob = toGlob(
      path.join(
        host.dataDir,
        'tier=raw',
        'context=vessels__test-self',
        'path=navigation__speedOverGround',
        'year=2024',
        'day=153',
        '*.parquet'
      )
    );
    const conn = await DuckDBPool.getConnection();
    try {
      const res = await conn.runAndReadAll(
        `SELECT COUNT(*) AS n, AVG(TRY_CAST(value AS DOUBLE)) AS avg_value
         FROM read_parquet('${glob}')`
      );
      const row = res.getRowObjects()[0] as { n: bigint; avg_value: number };
      expect(Number(row.n)).to.equal(5);
      expect(Number(row.avg_value)).to.equal(6);
    } finally {
      conn.disconnectSync();
    }
  });

  it('round-trips object (position) records with component columns', async () => {
    buffer.insert(positionRecord(47.5, 8.7, '2024-06-01T10:00:00.000Z'));
    buffer.insert(positionRecord(47.6, 8.8, '2024-06-01T10:01:00.000Z'));

    const result = await exportService.exportDayToParquet(DAY);
    expect(result.recordsExported).to.equal(2);

    const glob = toGlob(
      path.join(
        host.dataDir,
        'tier=raw',
        'context=vessels__test-self',
        'path=navigation__position',
        '**',
        '*.parquet'
      )
    );
    const conn = await DuckDBPool.getConnection();
    try {
      const res = await conn.runAndReadAll(
        `SELECT
           COUNT(*) AS n,
           MIN(TRY_CAST(value_latitude AS DOUBLE)) AS min_lat,
           MAX(TRY_CAST(value_longitude AS DOUBLE)) AS max_lon
         FROM read_parquet('${glob}')`
      );
      const row = res.getRowObjects()[0] as {
        n: bigint;
        min_lat: number;
        max_lon: number;
      };
      expect(Number(row.n)).to.equal(2);
      expect(Number(row.min_lat)).to.be.closeTo(47.5, 1e-9);
      expect(Number(row.max_lon)).to.be.closeTo(8.8, 1e-9);
    } finally {
      conn.disconnectSync();
    }
  });

  it('separates distinct paths into distinct parquet files', async () => {
    buffer.insert(
      scalarRecord('navigation.speedOverGround', 3, '2024-06-01T10:00:00.000Z')
    );
    buffer.insert(
      scalarRecord(
        'environment.depth.belowTransducer',
        12,
        '2024-06-01T10:00:00.000Z'
      )
    );

    const result = await exportService.exportDayToParquet(DAY);

    expect(result.recordsExported).to.equal(2);
    expect(result.filesCreated).to.have.lengthOf(2);
    expect(
      result.filesCreated.some(f => /path=navigation__speedOverGround/.test(f))
    ).to.equal(true);
    expect(
      result.filesCreated.some(f =>
        /path=environment__depth__belowTransducer/.test(f)
      )
    ).to.equal(true);
  });

  it('marks exported records so a second export is a no-op', async () => {
    buffer.insert(
      scalarRecord('navigation.speedOverGround', 5, '2024-06-01T10:00:00.000Z')
    );

    const first = await exportService.exportDayToParquet(DAY);
    expect(first.recordsExported).to.equal(1);

    const second = await exportService.exportDayToParquet(DAY);
    expect(second.recordsExported).to.equal(0);
    expect(second.filesCreated).to.deep.equal([]);
  });

  it('queries not-yet-exported records through the staged buffer federation', async () => {
    // Live data still in the buffer must be queryable before any export
    // runs; this is the path the History API uses for "today". Buffer rows
    // reach DuckDB via a staged temp table — never via ATTACH of the live
    // buffer.db (two in-process SQLite libraries corrupt each other's WAL).
    buffer.insert(
      scalarRecord('navigation.speedOverGround', 9, '2024-06-01T10:00:00.000Z')
    );
    buffer.insert(
      scalarRecord('navigation.speedOverGround', 11, '2024-06-01T10:01:00.000Z')
    );

    const fromIso = '2024-06-01T00:00:00.000Z';
    const toIso = '2024-06-02T00:00:00.000Z';
    const conn = await DuckDBPool.getConnection();
    try {
      const staged = await stageBufferTable(
        conn,
        buffer,
        CONTEXT,
        'navigation.speedOverGround',
        fromIso,
        toIso
      );
      expect(staged).to.be.a('string');

      const subquery = buildBufferScalarSubquery(
        staged as string,
        CONTEXT,
        'navigation.speedOverGround',
        fromIso,
        toIso
      );
      const res = await conn.runAndReadAll(
        `SELECT AVG(value) AS avg_value FROM ${subquery} AS b`
      );
      const row = res.getRowObjects()[0] as { avg_value: number };
      expect(Number(row.avg_value)).to.equal(10);
    } finally {
      conn.disconnectSync();
    }
  });

  it('staging returns null for unknown paths and empty time windows', async () => {
    buffer.insert(
      scalarRecord('navigation.speedOverGround', 9, '2024-06-01T10:00:00.000Z')
    );

    const conn = await DuckDBPool.getConnection();
    try {
      // Path with no buffer table
      expect(
        await stageBufferTable(
          conn,
          buffer,
          CONTEXT,
          'environment.wind.speedApparent',
          '2024-06-01T00:00:00.000Z',
          '2024-06-02T00:00:00.000Z'
        )
      ).to.equal(null);

      // Known path, but the window holds no rows
      expect(
        await stageBufferTable(
          conn,
          buffer,
          CONTEXT,
          'navigation.speedOverGround',
          '2024-07-01T00:00:00.000Z',
          '2024-07-02T00:00:00.000Z'
        )
      ).to.equal(null);
    } finally {
      conn.disconnectSync();
    }
  });

  it('round-trips object (position) records through the staged federation', async () => {
    buffer.insert(positionRecord(47.5, 8.7, '2024-06-01T10:00:00.000Z'));
    buffer.insert(positionRecord(47.6, 8.8, '2024-06-01T10:01:00.000Z'));

    const fromIso = '2024-06-01T00:00:00.000Z';
    const toIso = '2024-06-02T00:00:00.000Z';
    const conn = await DuckDBPool.getConnection();
    try {
      const staged = await stageBufferTable(
        conn,
        buffer,
        CONTEXT,
        'navigation.position',
        fromIso,
        toIso
      );
      expect(staged).to.be.a('string');

      const subquery = buildBufferObjectSubquery(
        staged as string,
        CONTEXT,
        fromIso,
        toIso,
        new Map([
          [
            'latitude',
            {
              name: 'latitude',
              columnName: 'value_latitude',
              dataType: 'numeric' as const,
            },
          ],
          [
            'longitude',
            {
              name: 'longitude',
              columnName: 'value_longitude',
              dataType: 'numeric' as const,
            },
          ],
        ]),
        buffer.getTableColumns('navigation.position')
      );
      const res = await conn.runAndReadAll(
        `SELECT
           COUNT(*) AS n,
           MIN(value_latitude) AS min_lat,
           MAX(value_longitude) AS max_lon
         FROM ${subquery} AS b`
      );
      const row = res.getRowObjects()[0] as {
        n: bigint;
        min_lat: number;
        max_lon: number;
      };
      expect(Number(row.n)).to.equal(2);
      expect(Number(row.min_lat)).to.be.closeTo(47.5, 1e-9);
      expect(Number(row.max_lon)).to.be.closeTo(8.8, 1e-9);
    } finally {
      conn.disconnectSync();
    }
  });

  it('builds a DuckDB glob for the day that DuckDB can read', async () => {
    buffer.insert(
      scalarRecord('navigation.speedOverGround', 7, '2024-06-01T10:00:00.000Z')
    );
    await exportService.exportDayToParquet(DAY);

    // Mirror how the read path locates files for a single day.
    const glob = hive.buildDuckDBGlob(
      host.dataDir,
      'raw',
      CONTEXT,
      'navigation.speedOverGround',
      DAY,
      DAY
    );
    const globs = Array.isArray(glob) ? glob : [glob];
    const conn = await DuckDBPool.getConnection();
    try {
      let total = 0;
      for (const g of globs) {
        const res = await conn.runAndReadAll(
          `SELECT COUNT(*) AS n FROM read_parquet('${toGlob(g)}')`
        );
        total += Number((res.getRowObjects()[0] as { n: bigint }).n);
      }
      expect(total).to.equal(1);
    } finally {
      conn.disconnectSync();
    }
  });
});
