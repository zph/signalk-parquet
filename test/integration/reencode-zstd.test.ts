import { expect } from 'chai';
import * as fs from 'fs-extra';
import * as os from 'node:os';
import * as path from 'node:path';
import { DuckDBInstance } from '@duckdb/node-api';
import { reencodeParquetDirectory } from '../../src/utils/reencode-zstd';

describe('ZSTD archive migration', function () {
  this.timeout(30000);

  it('rewrites legacy Parquet losslessly and is idempotent', async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), 'parquet-zstd-'));
    const file = path.join(
      root,
      'tier=raw',
      'context=vessels__test',
      'path=navigation__speedOverGround',
      'year=2026',
      'day=268',
      'legacy.parquet'
    );
    await fs.ensureDir(path.dirname(file));
    const instance = await DuckDBInstance.create(':memory:');
    const connection = await instance.connect();
    try {
      await connection.runAndReadAll(
        `COPY (SELECT 1 AS id, 'first' AS value
               UNION ALL SELECT 2, 'second')
         TO '${file}' (FORMAT PARQUET, COMPRESSION SNAPPY)`
      );
      const dryRun = await reencodeParquetDirectory(root);
      expect(dryRun.scanned).to.equal(1);
      expect(dryRun.converted).to.equal(0);
      const applied = await reencodeParquetDirectory(root, true);
      expect(applied.converted).to.equal(1);
      expect(applied.resultingBytes).to.be.greaterThan(0);
      const metadata = await connection.runAndReadAll(
        `SELECT DISTINCT compression FROM parquet_metadata('${file}')`
      );
      expect(
        metadata.getRowObjects().map(row => row.compression)
      ).to.deep.equal(['ZSTD']);
      const rows = await connection.runAndReadAll(
        `SELECT id, value FROM read_parquet('${file}') ORDER BY id`
      );
      expect(rows.getRowObjects().map(row => row.value)).to.deep.equal([
        'first',
        'second',
      ]);
      const repeated = await reencodeParquetDirectory(root, true);
      expect(repeated.converted).to.equal(0);
      expect(repeated.alreadyZstd).to.equal(1);
      expect(
        await fs.pathExists(path.join(root, '.parquet-export.lock'))
      ).to.equal(false);
    } finally {
      connection.disconnectSync();
      instance.closeSync();
      await fs.remove(root);
    }
  });
});
