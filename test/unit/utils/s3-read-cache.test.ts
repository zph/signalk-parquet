import { expect } from 'chai';
import * as fs from 'fs-extra';
import * as os from 'node:os';
import * as path from 'node:path';
import { createHash } from 'node:crypto';
import {
  GetObjectCommand,
  HeadObjectCommand,
  ListObjectsV2Command,
} from '@aws-sdk/client-s3';
import { S3ReadCache } from '../../../src/utils/s3-read-cache';

const BUCKET = 'archive';
const KEY = 'history/tier=raw/year=2026/day=268/sample.parquet';

function sha256(value: Buffer): string {
  return createHash('sha256').update(value).digest('hex');
}

function objectManifest(key: string, body: Buffer): Buffer {
  return Buffer.from(
    JSON.stringify({
      version: 1,
      key,
      sha256: sha256(body),
      bytes: body.length,
      rows: 1,
      committedAt: '2026-09-26T00:00:00.000Z',
    })
  );
}

function makeClient(
  objectBody: Buffer,
  manifestBody = objectManifest(KEY, objectBody)
) {
  const objectGets: string[] = [];
  const client = {
    send: async (command: any) => {
      if (command instanceof ListObjectsV2Command) {
        return {
          Contents: [{ Key: KEY }],
          IsTruncated: false,
        };
      }
      if (command instanceof GetObjectCommand) {
        const requestedKey = command.input.Key || '';
        if (requestedKey === `${KEY}.manifest.json`)
          return { Body: manifestBody };
        objectGets.push(requestedKey);
        return { Body: objectBody };
      }
      if (command instanceof HeadObjectCommand) {
        return {
          ContentLength: objectBody.length,
          ETag: '"stable-etag"',
          Metadata: { sha256: sha256(objectBody), rows: '1' },
        };
      }
      throw new Error(`Unexpected command: ${command.constructor.name}`);
    },
  };
  return { client, objectGets };
}

describe('S3ReadCache', () => {
  let root: string;

  beforeEach(async () => {
    root = await fs.mkdtemp(path.join(os.tmpdir(), 'parquet-s3-cache-'));
  });

  afterEach(async () => {
    await fs.remove(root);
  });

  it('verifies downloaded bytes against the committed checksum', async () => {
    const committed = Buffer.from('valid parquet bytes');
    const corrupt = Buffer.from('wrong parquet bytes');
    expect(corrupt.length).to.equal(committed.length);
    const { client } = makeClient(corrupt, objectManifest(KEY, committed));
    const cache = new S3ReadCache(root, 1024);

    const result = await cache.listAndCache(
      client,
      BUCKET,
      'history/tier=raw',
      new Date('2026-09-25T00:00:00Z'),
      new Date('2026-09-25T23:59:59Z')
    );

    expect(result.files).to.deep.equal([]);
    expect(result.incomplete).to.equal(true);
    expect(
      (await fs.readdir(root)).filter(name => name.endsWith('.parquet'))
    ).to.have.length(0);
  });

  it('revalidates a cache hit and repairs a locally corrupted cached object', async () => {
    const body = Buffer.from('good parquet bytes');
    const { client, objectGets } = makeClient(body);
    const cache = new S3ReadCache(root, 1024, 'endpoint-a|archive');
    const from = new Date('2026-09-25T00:00:00Z');
    const to = new Date('2026-09-25T23:59:59Z');

    const first = await cache.listAndCache(
      client,
      BUCKET,
      'history/tier=raw',
      from,
      to
    );
    expect(first.files).to.have.length(1);
    await fs.writeFile(first.files[0], Buffer.from('evil parquet bytes'));

    const second = await cache.listAndCache(
      client,
      BUCKET,
      'history/tier=raw',
      from,
      to
    );
    expect(second.files).to.have.length(1);
    expect(await fs.readFile(second.files[0])).to.deep.equal(body);
    expect(objectGets).to.deep.equal([KEY, KEY]);
  });

  it('coalesces simultaneous downloads of the same cloud object', async () => {
    const body = Buffer.from('coalesced object');
    const { client, objectGets } = makeClient(body);
    const cache = new S3ReadCache(root, 1024);
    const from = new Date('2026-09-25T00:00:00Z');
    const to = new Date('2026-09-25T23:59:59Z');

    const results = await Promise.all([
      cache.listAndCache(client, BUCKET, 'history/tier=raw', from, to),
      cache.listAndCache(client, BUCKET, 'history/tier=raw', from, to),
    ]);

    expect(results[0].files).to.deep.equal(results[1].files);
    expect(objectGets).to.deep.equal([KEY]);
  });

  it('does not reuse cached objects across cloud namespaces', async () => {
    const body = Buffer.from('namespace scoped object');
    const { client, objectGets } = makeClient(body);
    const from = new Date('2026-09-25T00:00:00Z');
    const to = new Date('2026-09-25T23:59:59Z');

    await new S3ReadCache(root, 1024, 'endpoint-a|archive').listAndCache(
      client,
      BUCKET,
      'history/tier=raw',
      from,
      to
    );
    await new S3ReadCache(root, 1024, 'endpoint-b|archive').listAndCache(
      client,
      BUCKET,
      'history/tier=raw',
      from,
      to
    );

    expect(objectGets).to.deep.equal([KEY, KEY]);
  });
});
