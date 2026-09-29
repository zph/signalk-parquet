import { expect } from 'chai';
import { createHash } from 'node:crypto';
import { GetObjectCommand } from '@aws-sdk/client-s3';
import { S3ManifestReader } from '../../../src/utils/s3-manifest-reader';

describe('S3ManifestReader', () => {
  const bucket = 'archive';
  const base = 'history/_manifests/year=2026/day=268';
  const selected =
    'history/tier=raw/context=vessels__self/path=navigation__speedOverGround/year=2026/day=268/data.parquet';
  const unrelated =
    'history/tier=60s/context=vessels__self/path=navigation__depth/year=2026/day=268/data.parquet';

  function fixture(corrupt = false) {
    const manifest = Buffer.from(
      JSON.stringify({
        version: 2,
        year: 2026,
        day: '268',
        generatedAt: '2026-09-25T00:00:00.000Z',
        objects: [selected, unrelated].map(key => ({
          key,
          sha256: 'a'.repeat(64),
          bytes: 100,
          rows: 10,
        })),
      })
    );
    const manifestKey = `${base}/${createHash('sha256')
      .update(manifest)
      .digest('hex')}.json`;
    const pointer = Buffer.from(
      JSON.stringify({
        version: 1,
        manifestKey,
        sha256: corrupt
          ? 'b'.repeat(64)
          : createHash('sha256').update(manifest).digest('hex'),
        committedAt: '2026-09-25T00:00:01.000Z',
      })
    );
    const gets: string[] = [];
    const client = {
      send: async (command: any) => {
        if (!(command instanceof GetObjectCommand))
          throw new Error('unexpected command');
        const key = String(command.input.Key);
        gets.push(key);
        if (key === `${base}/latest.json`) return { Body: pointer };
        if (key === manifestKey) return { Body: manifest };
        throw new Error('not found');
      },
    };
    return { client, gets };
  }

  it('uses two metadata reads per day and returns only committed path objects', async () => {
    const { client, gets } = fixture();
    const reader = new S3ManifestReader(client as any, bucket, 'history');
    const result = await reader.listCommitted(
      'history/tier=raw/context=vessels__self/path=navigation__speedOverGround',
      new Date('2026-09-25T00:00:00Z'),
      new Date('2026-09-25T23:59:59Z')
    );

    expect(result).to.deep.equal({
      objects: [
        {
          key: selected,
          sha256: 'a'.repeat(64),
          bytes: 100,
          rows: 10,
        },
      ],
      incomplete: false,
    });
    expect(gets).to.have.length(2);
  });

  it('rejects a generation whose checksum does not match its pointer', async () => {
    const { client } = fixture(true);
    const reader = new S3ManifestReader(client as any, bucket, 'history');
    const result = await reader.listCommitted(
      'history/tier=raw',
      new Date('2026-09-25T00:00:00Z'),
      new Date('2026-09-25T23:59:59Z')
    );
    expect(result).to.deep.equal({ objects: [], incomplete: true });
  });
});
