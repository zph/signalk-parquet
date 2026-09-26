import { expect } from 'chai';
import * as fs from 'fs-extra';
import * as os from 'node:os';
import * as path from 'node:path';
import { randomBytes } from 'node:crypto';
import {
  CreateBucketCommand,
  DeleteBucketCommand,
  DeleteObjectCommand,
  GetObjectCommand,
  PutObjectCommand,
  S3Client,
} from '@aws-sdk/client-s3';
import {
  createCloudClient,
  initializeCloudSDK,
  uploadVerifiedCloudObject,
} from '../../src/data-handler';
import { DuckDBPool } from '../../src/utils/duckdb-pool';
import { S3ReadCache } from '../../src/utils/s3-read-cache';
import { PluginConfig } from '../../src/types';
import { ServerAPI } from '@signalk/server-api';

describe('LocalStack S3 archive round trip', function () {
  this.timeout(60000);
  const endpoint = process.env.LOCALSTACK_S3_ENDPOINT;
  let root: string;
  let file: string;
  let client: S3Client;
  let bucket: string;
  let key: string;

  before(async function () {
    if (!endpoint) this.skip();
    root = await fs.mkdtemp(path.join(os.tmpdir(), 'parquet-localstack-'));
    file = path.join(root, 'sample.parquet');
    bucket = `parquet-test-${randomBytes(6).toString('hex')}`;
    const day = new Date();
    const year = day.getUTCFullYear();
    const doy = String(
      Math.floor((day.getTime() - Date.UTC(year, 0, 1)) / 86400000) + 1
    ).padStart(3, '0');
    key = `history/tier=raw/year=${year}/day=${doy}/sample.parquet`;
    await DuckDBPool.initialize();
    const config = {
      cloudUpload: {
        provider: 's3',
        region: 'us-east-1',
        endpoint,
        allowPrivateEndpoint: true,
        forcePathStyle: true,
        accessKeyId: 'test',
        secretAccessKey: 'test',
      },
    } as PluginConfig;
    const app = {
      error: (message: string) => {
        throw new Error(message);
      },
    } as unknown as ServerAPI;
    await initializeCloudSDK(config, app);
    client = createCloudClient(config, app) as S3Client;
    await client.send(new CreateBucketCommand({ Bucket: bucket }));
    const connection = await DuckDBPool.getConnection();
    try {
      await connection.runAndReadAll(
        `COPY (SELECT * FROM (VALUES (1), (2)) AS t(id)) TO '${file}' (FORMAT PARQUET)`
      );
    } finally {
      connection.disconnectSync();
    }
  });

  after(async () => {
    if (client) {
      await client.send(new DeleteObjectCommand({ Bucket: bucket, Key: key }));
      await client.send(
        new DeleteObjectCommand({ Bucket: bucket, Key: `${key}.manifest.json` })
      );
      await client.send(new DeleteBucketCommand({ Bucket: bucket }));
      client.destroy();
    }
    await DuckDBPool.shutdown();
    if (root) await fs.remove(root);
  });

  it('requires a manifest, verifies reads, detects corruption, and repairs the object', async () => {
    const original = await fs.readFile(file);
    const cache = new S3ReadCache(
      path.join(root, 'cache'),
      original.length * 2
    );
    const day = new Date();
    const from = new Date(
      Date.UTC(day.getUTCFullYear(), day.getUTCMonth(), day.getUTCDate())
    );
    const prefix = 'history/tier=raw';
    const lookup = () => cache.listAndCache(client, bucket, prefix, from, from);

    await client.send(
      new PutObjectCommand({ Bucket: bucket, Key: key, Body: original })
    );
    expect(await lookup()).to.deep.equal({ files: [], incomplete: true });

    await uploadVerifiedCloudObject(file, key, client, bucket);
    const manifestResponse = await client.send(
      new GetObjectCommand({ Bucket: bucket, Key: `${key}.manifest.json` })
    );
    const manifest = JSON.parse(
      await manifestResponse.Body!.transformToString()
    );
    expect(manifest.rows).to.equal(2);
    expect(manifest.bytes).to.equal(original.length);

    const committed = await lookup();
    expect(committed.incomplete).to.equal(false);
    expect(committed.files).to.have.length(1);
    expect(await fs.readFile(committed.files[0])).to.deep.equal(original);
    const connection = await DuckDBPool.getConnection();
    try {
      const reader = await connection.runAndReadAll(
        `SELECT count(*) AS rows FROM read_parquet('${committed.files[0]}')`
      );
      expect(Number(reader.getRows()[0][0])).to.equal(2);
    } finally {
      connection.disconnectSync();
    }

    await client.send(
      new PutObjectCommand({
        Bucket: bucket,
        Key: key,
        Body: Buffer.alloc(original.length),
        Metadata: { sha256: manifest.sha256, rows: '2' },
      })
    );
    const corrupt = await lookup();
    expect(corrupt.files).to.deep.equal([]);
    expect(corrupt.incomplete).to.equal(true);

    await uploadVerifiedCloudObject(file, key, client, bucket, true);
    expect(await fs.pathExists(file)).to.equal(false);
    const repaired = await lookup();
    expect(repaired.incomplete).to.equal(false);
    expect(await fs.readFile(repaired.files[0])).to.deep.equal(original);
  });
});
