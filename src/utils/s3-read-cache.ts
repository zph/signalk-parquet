import * as fs from 'fs-extra';
import * as path from 'path';
import { createHash } from 'crypto';
import { CloudManifestObject } from './s3-day-manifest';

/** Disk-backed, byte-bounded LRU cache for committed S3 Parquet objects. */
export class S3ReadCache {
  private readonly inFlight = new Map<string, Promise<string>>();
  private commands?: Promise<any>;

  constructor(
    private readonly directory: string,
    private readonly maxBytes: number,
    private readonly namespace = ''
  ) {}

  private getCommands(): Promise<any> {
    return (this.commands ||= import('@aws-sdk/client-s3'));
  }

  async listAndCache(
    client: any,
    bucket: string,
    prefix: string,
    start: Date,
    end: Date
  ): Promise<{ files: string[]; incomplete: boolean }> {
    if (this.maxBytes <= 0) return { files: [], incomplete: true };
    const aws = await this.getCommands();
    const keys = new Set<string>();
    for (const day of this.days(start, end)) {
      const year = day.getUTCFullYear();
      const doy = String(
        Math.floor((day.getTime() - Date.UTC(year, 0, 1)) / 86400000) + 1
      ).padStart(3, '0');
      let token: string | undefined;
      do {
        const response = await client.send(
          new aws.ListObjectsV2Command({
            Bucket: bucket,
            Prefix: `${prefix.replace(/\/$/, '')}/year=${year}/day=${doy}/`,
            ContinuationToken: token,
          })
        );
        for (const item of response.Contents || []) {
          if (item.Key?.endsWith('.parquet')) keys.add(item.Key);
        }
        token = response.IsTruncated
          ? response.NextContinuationToken
          : undefined;
      } while (token);
    }

    const paths: string[] = [];
    let incomplete = false;
    for (const key of keys) {
      // Only consume objects with an upload-committed sidecar manifest.
      try {
        const manifest = await client.send(
          new aws.GetObjectCommand({
            Bucket: bucket,
            Key: `${key}.manifest.json`,
          })
        );
        const body = await this.toBuffer(manifest.Body);
        const parsed = JSON.parse(body.toString('utf8'));
        if (
          parsed?.version !== 1 ||
          parsed.key !== key ||
          !Number.isFinite(parsed.bytes) ||
          !Number.isInteger(parsed.rows) ||
          parsed.bytes <= 0 ||
          parsed.rows < 0 ||
          typeof parsed.sha256 !== 'string' ||
          !/^[a-f0-9]{64}$/i.test(parsed.sha256)
        ) {
          incomplete = true;
          continue;
        }
        const head = await client.send(
          new aws.HeadObjectCommand({ Bucket: bucket, Key: key })
        );
        if (
          Number(head.ContentLength) !== parsed.bytes ||
          head.Metadata?.sha256 !== parsed.sha256 ||
          Number(head.Metadata?.rows) !== parsed.rows
        ) {
          incomplete = true;
          continue;
        }
        paths.push(
          await this.cacheObject(client, bucket, key, head, parsed.sha256, aws)
        );
      } catch {
        // An absent/unavailable manifest is deliberately not queryable.
        incomplete = true;
      }
    }
    return { files: paths, incomplete };
  }

  /** Optional compatibility/offline mode for objects selected by a day manifest. */
  async cacheCommitted(
    client: any,
    bucket: string,
    objects: CloudManifestObject[]
  ): Promise<{ files: string[]; incomplete: boolean }> {
    if (this.maxBytes <= 0)
      return { files: [], incomplete: objects.length > 0 };
    const aws = await this.getCommands();
    const files: string[] = [];
    let incomplete = false;
    for (const object of objects) {
      try {
        const head = await client.send(
          new aws.HeadObjectCommand({ Bucket: bucket, Key: object.key })
        );
        if (
          Number(head.ContentLength) !== object.bytes ||
          head.Metadata?.sha256 !== object.sha256 ||
          Number(head.Metadata?.rows) !== object.rows
        ) {
          incomplete = true;
          continue;
        }
        files.push(
          await this.cacheObject(
            client,
            bucket,
            object.key,
            head,
            object.sha256,
            aws
          )
        );
      } catch {
        incomplete = true;
      }
    }
    return { files, incomplete };
  }

  private async cacheObject(
    client: any,
    bucket: string,
    key: string,
    head: any,
    expectedSha256: string,
    aws: any
  ): Promise<string> {
    const identity = `${this.namespace}|${bucket}/${key}`;
    const existing = this.inFlight.get(identity);
    if (existing) return existing;
    const task = (async () => {
      await fs.ensureDir(this.directory);
      const name = createHash('sha256').update(identity).digest('hex');
      const file = path.join(this.directory, `${name}.parquet`);
      const metaPath = `${file}.json`;
      const etag = String(head.ETag || '');
      if ((await fs.pathExists(file)) && (await fs.pathExists(metaPath))) {
        try {
          const meta = await fs.readJson(metaPath);
          const stat = await fs.stat(file);
          if (
            meta.etag === etag &&
            meta.size === Number(head.ContentLength) &&
            meta.sha256 === expectedSha256 &&
            stat.size === Number(head.ContentLength)
          ) {
            const cachedBytes = await fs.readFile(file);
            if (
              createHash('sha256').update(cachedBytes).digest('hex') ===
              expectedSha256
            ) {
              const now = new Date();
              await fs.utimes(file, now, now);
              return file;
            }
          }
        } catch {
          /* refresh stale cache entry */
        }
      }
      const response = await client.send(
        new aws.GetObjectCommand({ Bucket: bucket, Key: key })
      );
      const buffer = await this.toBuffer(response.Body);
      if (buffer.length !== Number(head.ContentLength))
        throw new Error(`Cached S3 object size mismatch: ${key}`);
      if (createHash('sha256').update(buffer).digest('hex') !== expectedSha256)
        throw new Error(`Cached S3 object checksum mismatch: ${key}`);
      if (buffer.length > this.maxBytes)
        throw new Error(
          `S3 object exceeds configured local read-cache limit: ${key}`
        );
      const temp = `${file}.${process.pid}.tmp`;
      await fs.writeFile(temp, buffer);
      await fs.move(temp, file, { overwrite: true });
      await fs.writeJson(metaPath, {
        etag,
        size: buffer.length,
        sha256: expectedSha256,
      });
      await this.prune(file);
      return file;
    })().finally(() => this.inFlight.delete(identity));
    this.inFlight.set(identity, task);
    return task;
  }

  private async prune(keepFile: string): Promise<void> {
    const files = (await fs.readdir(this.directory)).filter(name =>
      name.endsWith('.parquet')
    );
    const entries = await Promise.all(
      files.map(async name => {
        const file = path.join(this.directory, name);
        const stat = await fs.stat(file);
        return { file, size: stat.size, mtime: stat.mtimeMs };
      })
    );
    let total = entries.reduce((sum, item) => sum + item.size, 0);
    for (const item of entries.sort((a, b) => a.mtime - b.mtime)) {
      if (total <= this.maxBytes) break;
      if (item.file === keepFile) continue;
      await fs.remove(item.file);
      await fs.remove(`${item.file}.json`);
      total -= item.size;
    }
  }

  private async toBuffer(body: any): Promise<Buffer> {
    if (Buffer.isBuffer(body) || body instanceof Uint8Array)
      return Buffer.from(body);
    if (body?.transformToByteArray)
      return Buffer.from(await body.transformToByteArray());
    const chunks: Buffer[] = [];
    for await (const chunk of body as AsyncIterable<Uint8Array | Buffer>)
      chunks.push(Buffer.from(chunk));
    return Buffer.concat(chunks);
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
