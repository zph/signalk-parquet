import { expect } from 'chai';
import * as fs from 'node:fs/promises';
import * as path from 'node:path';
import * as os from 'node:os';
import { randomUUID, createHash } from 'node:crypto';
import {
  S3Client,
  CreateBucketCommand,
  ListObjectsV2Command,
  DeleteObjectsCommand,
  DeleteBucketCommand,
} from '@aws-sdk/client-s3';
import createPlugin from '../../src/index';

const endpoint = process.env.HISTORY_SYNC_LOCALSTACK;
const suite = endpoint ? describe : describe.skip;
suite('S3-only replica integration', function () {
  this.timeout(120000);
  it('starts without any live capture and serves verified S3 rows through the History API', async () => {
    if (
      !endpoint ||
      !['127.0.0.1', 'localhost'].includes(new URL(endpoint).hostname)
    )
      throw new Error('Loopback LocalStack required');
    const client = new S3Client({
      endpoint,
      region: 'us-east-1',
      forcePathStyle: true,
      credentials: { accessKeyId: 'test', secretAccessKey: 'test' },
    });
    const bucket = `parquet-replica-test-${randomUUID()}`;
    const root = await fs.mkdtemp(
      path.join(os.tmpdir(), 'parquet-replica-test-')
    );
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    const archive = require('signalk-history-sync/src/archive');
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    const parquet = require('@dsnp/parquetjs');
    let plugin: any, storage: any;
    try {
      await client.send(new CreateBucketCommand({ Bucket: bucket }));
      const rel =
        'tier=raw/context=vessels__self/path=navigation__speedOverGround/year=2026/day=240/sample.parquet';
      const file = path.join(root, 'producer', rel);
      await fs.mkdir(path.dirname(file), { recursive: true });
      const writer = await parquet.ParquetWriter.openFile(
        new parquet.ParquetSchema({
          signalk_timestamp: { type: 'UTF8' },
          value: { type: 'DOUBLE' },
          source_label: { type: 'UTF8' },
        }),
        file
      );
      await writer.appendRow({
        signalk_timestamp: '2026-08-28T12:00:00.000Z',
        value: 4.5,
        source_label: 'sensor',
      });
      await writer.close();
      const bytes = await fs.readFile(file),
        sha256 = createHash('sha256').update(bytes).digest('hex');
      storage = archive.s3Storage({
        endpoint,
        bucket,
        prefix: 'shared',
        forcePathStyle: true,
        allowInsecureLocalTest: true,
        credentials: { accessKeyId: 'test', secretAccessKey: 'test' },
      });
      await archive.publish(
        storage,
        path.join(root, 'producer'),
        {
          schemaVersion: 1,
          source: 'boat',
          generation: 1,
          start: '2026-08-28T00:00:00.000Z',
          end: '2026-08-29T00:00:00.000Z',
          complete: true,
          captureGaps: [],
          files: [
            {
              path: rel,
              key: `objects/${sha256}.parquet`,
              bytes: bytes.length,
              sha256,
              records: 1,
            },
          ],
        },
        'producer'
      );
      let provider: any;
      const deltas: any[] = [];
      const forbidden = () => {
        throw new Error(
          'Replica attempted live capture or source configuration'
        );
      };
      const app: any = {
        selfId: 'self',
        selfContext: 'vessels.self',
        getDataDirPath: () => path.join(root, 'replica'),
        getSelfPath: () => undefined,
        getMetadata: () => undefined,
        debug: () => {},
        error: () => {},
        setPluginStatus: () => {},
        setPluginError: () => {},
        handleMessage: (_id: string, delta: any) => deltas.push(delta),
        registerHistoryApiProvider: (p: any) => {
          provider = p;
        },
        unregisterHistoryApiProvider: () => {},
        subscriptionmanager: { subscribe: forbidden },
        streambundle: { getSelfStream: forbidden, getBus: forbidden },
        registerDeltaInputHandler: forbidden,
        savePluginOptions: forbidden,
      };
      plugin = createPlugin(app);
      await plugin.start({
        archiveMode: 'replica',
        archiveSource: 'boat',
        archiveCoverageStart: '2026-08-28',
        cloudUpload: {
          provider: 's3',
          endpoint,
          bucket,
          keyPrefix: 'shared',
          region: 'us-east-1',
          allowPrivateEndpoint: true,
          forcePathStyle: true,
          accessKeyId: 'test',
          secretAccessKey: 'test',
        },
      });
      expect(provider).to.exist;
      const query = {
        context: 'vessels.self',
        from: '2026-08-28T00:00:00Z',
        to: '2026-08-29T00:00:00Z',
        resolution: 1,
        pathSpecs: [
          {
            path: 'navigation.speedOverGround',
            aggregate: 'average',
            parameter: [],
          },
        ],
      };
      const values = await provider.getValues(query);
      expect(values.data).to.have.length(1);
      expect(values.data[0][1]).to.equal(4.5);
      const paths = await provider.getPaths(query);
      expect(paths).to.include('navigation.speedOverGround');
      expect(
        deltas
          .flatMap(d => d.updates.flatMap((u: any) => u.values))
          .every((v: any) => v.path.startsWith('notifications.'))
      ).to.equal(true);
      let hasBuffer = true;
      try {
        await fs.access(path.join(root, 'replica', 'buffer.db'));
      } catch {
        hasBuffer = false;
      }
      expect(hasBuffer).to.equal(false);
      expect(JSON.stringify(plugin.schema)).to.include(
        'endpoint + bucket + prefix'
      );
    } finally {
      await plugin?.stop();
      storage?.close();
      const keys = await client.send(
        new ListObjectsV2Command({ Bucket: bucket })
      );
      if (keys.Contents?.length)
        await client.send(
          new DeleteObjectsCommand({
            Bucket: bucket,
            Delete: { Objects: keys.Contents.map(o => ({ Key: o.Key! })) },
          })
        );
      await client.send(new DeleteBucketCommand({ Bucket: bucket }));
      client.destroy();
      await fs.rm(root, { recursive: true, force: true });
    }
  });
});
