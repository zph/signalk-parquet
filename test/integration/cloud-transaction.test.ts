import { expect } from 'chai';
import * as fs from 'fs-extra';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  GetObjectCommand,
  HeadObjectCommand,
  PutObjectCommand,
} from '@aws-sdk/client-s3';
import {
  initializeCloudSDK,
  uploadVerifiedCloudObject,
} from '../../src/data-handler';
import { DuckDBPool } from '../../src/utils/duckdb-pool';
import { PluginConfig } from '../../src/types';
import { ServerAPI } from '@signalk/server-api';

describe('transactional cloud archive upload', function () {
  this.timeout(30000);
  let root: string;
  let file: string;

  beforeEach(async () => {
    root = await fs.mkdtemp(path.join(os.tmpdir(), 'parquet-cloud-tx-'));
    file = path.join(root, 'sample.parquet');
    await DuckDBPool.initialize();
    await initializeCloudSDK(
      { cloudUpload: { provider: 's3' } } as PluginConfig,
      { error: () => undefined } as unknown as ServerAPI
    );
    const connection = await DuckDBPool.getConnection();
    try {
      await connection.runAndReadAll(
        `COPY (SELECT * FROM (VALUES (1), (2)) AS t(id)) TO '${file}' (FORMAT PARQUET)`
      );
    } finally {
      connection.disconnectSync();
    }
  });

  afterEach(async () => {
    await DuckDBPool.shutdown();
    await fs.remove(root);
  });

  function cloudClient(corruptRead = false) {
    const objects = new Map<
      string,
      { body: Buffer; metadata: Record<string, string> }
    >();
    const puts: string[] = [];
    return {
      objects,
      puts,
      send: async (command: unknown) => {
        if (command instanceof PutObjectCommand) {
          const key = String(command.input.Key);
          puts.push(key);
          objects.set(key, {
            body: Buffer.from(command.input.Body as Buffer),
            metadata: (command.input.Metadata || {}) as Record<string, string>,
          });
          return {};
        }
        if (command instanceof HeadObjectCommand) {
          const object = objects.get(String(command.input.Key));
          if (!object) throw new Error('Not found');
          return {
            ContentLength: object.body.length,
            Metadata: object.metadata,
          };
        }
        if (command instanceof GetObjectCommand) {
          const key = String(command.input.Key);
          const object = objects.get(key);
          if (!object) throw new Error('Not found');
          return {
            Body:
              corruptRead && key.endsWith('.parquet')
                ? Buffer.alloc(object.body.length)
                : object.body,
          };
        }
        throw new Error('Unexpected command');
      },
    };
  }

  it('verifies the object, commits a manifest, then permits local deletion', async () => {
    const client = cloudClient();
    const key = 'tier=raw/sample.parquet';
    await uploadVerifiedCloudObject(file, key, client, 'archive', true);

    expect(await fs.pathExists(file)).to.equal(false);
    expect(client.puts).to.deep.equal([key, `${key}.manifest.json`]);
    const manifest = JSON.parse(
      client.objects.get(`${key}.manifest.json`)!.body.toString('utf8')
    );
    expect(manifest.rows).to.equal(2);
    expect(manifest.bytes).to.equal(client.objects.get(key)!.body.length);
    expect(manifest.sha256).to.match(/^[0-9a-f]{64}$/);
  });

  it('keeps the local file and does not commit a manifest after a bad readback', async () => {
    const client = cloudClient(true);
    const key = 'tier=raw/sample.parquet';
    try {
      await uploadVerifiedCloudObject(file, key, client, 'archive', true);
      expect.fail('Expected checksum verification to fail');
    } catch (error) {
      expect((error as Error).message).to.include('verification');
    }
    expect(await fs.pathExists(file)).to.equal(true);
    expect(client.objects.has(`${key}.manifest.json`)).to.equal(false);
  });
});
