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
import { dayManifestBase } from '../../src/utils/s3-day-manifest';

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
    const key = 'tier=raw/year=2026/day=268/sample.parquet';
    await uploadVerifiedCloudObject(file, key, client, 'archive', true);

    expect(await fs.pathExists(file)).to.equal(false);
    const base = dayManifestBase('', 2026, '268');
    expect(client.puts[0]).to.equal(key);
    expect(client.puts[1]).to.match(
      new RegExp(`^${base}/[0-9a-f]{64}\\.json$`)
    );
    expect(client.puts[2]).to.equal(`${base}/latest.json`);
    const pointer = JSON.parse(
      client.objects.get(`${base}/latest.json`)!.body.toString('utf8')
    );
    const manifest = JSON.parse(
      client.objects.get(pointer.manifestKey)!.body.toString('utf8')
    );
    expect(manifest.version).to.equal(2);
    expect(manifest.objects).to.have.length(1);
    expect(manifest.objects[0].rows).to.equal(2);
    expect(manifest.objects[0].bytes).to.equal(
      client.objects.get(key)!.body.length
    );
    expect(manifest.objects[0].sha256).to.match(/^[0-9a-f]{64}$/);
  });

  it('keeps the local file and does not commit a manifest after a bad readback', async () => {
    const client = cloudClient(true);
    const key = 'tier=raw/year=2026/day=268/sample.parquet';
    try {
      await uploadVerifiedCloudObject(file, key, client, 'archive', true);
      expect.fail('Expected checksum verification to fail');
    } catch (error) {
      expect((error as Error).message).to.include('verification');
    }
    expect(await fs.pathExists(file)).to.equal(true);
    expect(
      client.objects.has(`${dayManifestBase('', 2026, '268')}/latest.json`)
    ).to.equal(false);
  });

  it('merges later objects into the immutable day inventory', async () => {
    const client = cloudClient();
    const firstKey = 'tier=raw/year=2026/day=268/first.parquet';
    const secondKey = 'tier=60s/year=2026/day=268/second.parquet';
    const secondFile = path.join(root, 'second.parquet');
    await fs.copy(file, secondFile);

    await uploadVerifiedCloudObject(file, firstKey, client, 'archive');
    await uploadVerifiedCloudObject(secondFile, secondKey, client, 'archive');

    const base = dayManifestBase('', 2026, '268');
    const pointer = JSON.parse(
      client.objects.get(`${base}/latest.json`)!.body.toString('utf8')
    );
    const manifest = JSON.parse(
      client.objects.get(pointer.manifestKey)!.body.toString('utf8')
    );
    expect(
      manifest.objects.map((item: { key: string }) => item.key)
    ).to.deep.equal([secondKey, firstKey].sort());
  });
});
