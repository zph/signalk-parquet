import { GetObjectCommand, type S3Client } from '@aws-sdk/client-s3';
import {
  CloudManifestObject,
  dayManifestBase,
  isCloudDayManifest,
  isCloudDayPointer,
  sha256,
} from './s3-day-manifest';

async function bodyToBuffer(body: any): Promise<Buffer> {
  if (Buffer.isBuffer(body) || body instanceof Uint8Array)
    return Buffer.from(body);
  if (body?.transformToByteArray)
    return Buffer.from(await body.transformToByteArray());
  const chunks: Buffer[] = [];
  for await (const chunk of body as AsyncIterable<Uint8Array | Buffer>)
    chunks.push(Buffer.from(chunk));
  return Buffer.concat(chunks);
}

/** Reads one immutable, content-addressed archive inventory per requested day. */
export class S3ManifestReader {
  constructor(
    private readonly client: S3Client,
    private readonly bucket: string,
    private readonly keyPrefix: string
  ) {}

  async listCommitted(
    objectPrefix: string,
    start: Date,
    end: Date
  ): Promise<{ objects: CloudManifestObject[]; incomplete: boolean }> {
    const objects = new Map<string, CloudManifestObject>();
    let incomplete = false;
    const days = this.days(start, end);
    const concurrency = 8;
    for (let offset = 0; offset < days.length; offset += concurrency) {
      const results = await Promise.all(
        days
          .slice(offset, offset + concurrency)
          .map(date => this.readDay(date, objectPrefix))
      );
      for (const result of results) {
        incomplete ||= result.incomplete;
        for (const object of result.objects) objects.set(object.key, object);
      }
    }
    return { objects: Array.from(objects.values()), incomplete };
  }

  private async readDay(
    date: Date,
    objectPrefix: string
  ): Promise<{ objects: CloudManifestObject[]; incomplete: boolean }> {
    const year = date.getUTCFullYear();
    const day = String(
      Math.floor((date.getTime() - Date.UTC(year, 0, 1)) / 86400000) + 1
    ).padStart(3, '0');
    const base = dayManifestBase(this.keyPrefix, year, day);
    try {
      const pointerResponse = await this.client.send(
        new GetObjectCommand({
          Bucket: this.bucket,
          Key: `${base}/latest.json`,
        })
      );
      const pointerBytes = await bodyToBuffer(pointerResponse.Body);
      const pointer = JSON.parse(pointerBytes.toString('utf8'));
      if (!isCloudDayPointer(pointer)) throw new Error('invalid pointer');
      if (pointer.manifestKey !== `${base}/${pointer.sha256}.json`)
        throw new Error('pointer is not content-addressed for this day');

      const manifestResponse = await this.client.send(
        new GetObjectCommand({
          Bucket: this.bucket,
          Key: pointer.manifestKey,
        })
      );
      const manifestBytes = await bodyToBuffer(manifestResponse.Body);
      if (sha256(manifestBytes) !== pointer.sha256)
        throw new Error('manifest checksum mismatch');
      const manifest = JSON.parse(manifestBytes.toString('utf8'));
      if (!isCloudDayManifest(manifest, year, day))
        throw new Error('invalid manifest');
      return {
        objects: manifest.objects.filter(object =>
          object.key.startsWith(objectPrefix)
        ),
        incomplete: false,
      };
    } catch {
      // A missing day manifest is observably different from an empty day.
      return { objects: [], incomplete: true };
    }
  }

  private days(start: Date, end: Date): Date[] {
    const cursor = new Date(
      Date.UTC(start.getUTCFullYear(), start.getUTCMonth(), start.getUTCDate())
    );
    const last = Date.UTC(
      end.getUTCFullYear(),
      end.getUTCMonth(),
      end.getUTCDate()
    );
    const result: Date[] = [];
    while (cursor.getTime() <= last && result.length < 4000) {
      result.push(new Date(cursor));
      cursor.setUTCDate(cursor.getUTCDate() + 1);
    }
    return result;
  }
}
