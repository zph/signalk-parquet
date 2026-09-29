import { expect } from 'chai';
import * as fs from 'fs-extra';
import * as os from 'node:os';
import * as path from 'node:path';
import { randomBytes } from 'node:crypto';
import {
  CreateBucketCommand,
  DeleteBucketCommand,
  DeleteObjectCommand,
  ListObjectsV2Command,
  PutObjectCommand,
  S3Client,
} from '@aws-sdk/client-s3';
import {
  createCloudClient,
  initializeCloudSDK,
  uploadVerifiedCloudObject,
} from '../../src/data-handler';
import { DuckDBPool } from '../../src/utils/duckdb-pool';
import { S3ManifestReader } from '../../src/utils/s3-manifest-reader';
import { dayManifestBase } from '../../src/utils/s3-day-manifest';
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
    key = `history/tier=raw/context=vessels__self/path=navigation__speedOverGround/year=${year}/day=${doy}/sample.parquet`;
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
    const parsedEndpoint = new URL(endpoint!);
    await DuckDBPool.initializeS3({
      accessKeyId: 'test',
      secretAccessKey: 'test',
      region: 'us-east-1',
      endpoint: parsedEndpoint.host,
      useSSL: parsedEndpoint.protocol === 'https:',
      urlStyle: 'path',
    });
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
      const listed = await client.send(
        new ListObjectsV2Command({ Bucket: bucket })
      );
      for (const object of listed.Contents || []) {
        if (object.Key)
          await client.send(
            new DeleteObjectCommand({ Bucket: bucket, Key: object.Key })
          );
      }
      await client.send(new DeleteBucketCommand({ Bucket: bucket }));
      client.destroy();
    }
    await DuckDBPool.shutdown();
    if (root) await fs.remove(root);
  });

  it('commits a day inventory and lets DuckDB range-read its explicit S3 key', async () => {
    const original = await fs.readFile(file);
    const day = new Date();
    const from = new Date(
      Date.UTC(day.getUTCFullYear(), day.getUTCMonth(), day.getUTCDate())
    );
    const year = day.getUTCFullYear();
    const doy = String(
      Math.floor((day.getTime() - Date.UTC(year, 0, 1)) / 86400000) + 1
    ).padStart(3, '0');
    const prefix =
      'history/tier=raw/context=vessels__self/path=navigation__speedOverGround/';
    const reader = new S3ManifestReader(client, bucket, 'history');
    const lookup = () => reader.listCommitted(prefix, from, from);

    await client.send(
      new PutObjectCommand({ Bucket: bucket, Key: key, Body: original })
    );
    expect(await lookup()).to.deep.equal({ objects: [], incomplete: true });

    await uploadVerifiedCloudObject(file, key, client, bucket);
    const committed = await lookup();
    expect(committed.incomplete).to.equal(false);
    expect(committed.objects).to.have.length(1);
    expect(committed.objects[0].rows).to.equal(2);
    expect(committed.objects[0].bytes).to.equal(original.length);
    const connection = await DuckDBPool.getConnection();
    try {
      const reader = await connection.runAndReadAll(
        `SELECT count(*) AS rows FROM read_parquet('s3://${bucket}/${committed.objects[0].key}')`
      );
      expect(Number(reader.getRows()[0][0])).to.equal(2);
    } finally {
      connection.disconnectSync();
    }

    const pointerKey = `${dayManifestBase('history', year, doy)}/latest.json`;
    await client.send(
      new PutObjectCommand({ Bucket: bucket, Key: pointerKey, Body: '{}' })
    );
    expect(await lookup()).to.deep.equal({ objects: [], incomplete: true });

    await uploadVerifiedCloudObject(file, key, client, bucket, true);
    expect(await fs.pathExists(file)).to.equal(false);
    const repaired = await lookup();
    expect(repaired.incomplete).to.equal(false);
    expect(repaired.objects).to.have.length(1);
  });
});
