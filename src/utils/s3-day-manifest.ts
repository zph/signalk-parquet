import { createHash } from 'node:crypto';

export interface CloudManifestObject {
  key: string;
  sha256: string;
  bytes: number;
  rows: number;
}

export interface CloudDayManifest {
  version: 2;
  year: number;
  day: string;
  generatedAt: string;
  objects: CloudManifestObject[];
}

export interface CloudDayPointer {
  version: 1;
  manifestKey: string;
  sha256: string;
  committedAt: string;
}

export function sha256(bytes: Buffer): string {
  return createHash('sha256').update(bytes).digest('hex');
}

export function parseCloudDay(key: string): { year: number; day: string } {
  const match = key.match(/(?:^|\/)year=(\d{4})\/day=(\d{3})(?:\/|$)/);
  if (!match) throw new Error(`Cloud object is not day-partitioned: ${key}`);
  return { year: Number(match[1]), day: match[2] };
}

export function dayManifestBase(
  keyPrefix: string,
  year: number,
  day: string
): string {
  return [
    keyPrefix.replace(/^\/+|\/+$/g, ''),
    '_manifests',
    `year=${year}`,
    `day=${day}`,
  ]
    .filter(Boolean)
    .join('/');
}

export function isCloudManifestObject(
  value: unknown
): value is CloudManifestObject {
  const item = value as Partial<CloudManifestObject> | null;
  return Boolean(
    item &&
    typeof item.key === 'string' &&
    typeof item.sha256 === 'string' &&
    /^[a-f0-9]{64}$/i.test(item.sha256) &&
    Number.isFinite(item.bytes) &&
    Number(item.bytes) > 0 &&
    Number.isInteger(item.rows) &&
    Number(item.rows) >= 0
  );
}

export function isCloudDayManifest(
  value: unknown,
  year: number,
  day: string
): value is CloudDayManifest {
  const manifest = value as Partial<CloudDayManifest> | null;
  const valid = Boolean(
    manifest &&
    manifest.version === 2 &&
    manifest.year === year &&
    manifest.day === day &&
    typeof manifest.generatedAt === 'string' &&
    Array.isArray(manifest.objects) &&
    manifest.objects.every(isCloudManifestObject)
  );
  if (!valid) return false;
  return manifest!.objects!.every(object => {
    try {
      const partition = parseCloudDay(object.key);
      return partition.year === year && partition.day === day;
    } catch {
      return false;
    }
  });
}

export function isCloudDayPointer(value: unknown): value is CloudDayPointer {
  const pointer = value as Partial<CloudDayPointer> | null;
  return Boolean(
    pointer &&
    pointer.version === 1 &&
    typeof pointer.manifestKey === 'string' &&
    typeof pointer.sha256 === 'string' &&
    /^[a-f0-9]{64}$/i.test(pointer.sha256) &&
    typeof pointer.committedAt === 'string'
  );
}
