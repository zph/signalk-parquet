import { expect } from 'chai';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { FileLease } from '../../src/utils/file-lease';

describe('archive file lease', () => {
  let dir: string;
  beforeEach(() => {
    dir = fs.mkdtempSync(path.join(os.tmpdir(), 'parquet-lease-'));
  });
  afterEach(() => {
    fs.rmSync(dir, { recursive: true, force: true });
  });

  it('allows one owner and releases its lockfile', () => {
    const file = path.join(dir, '.parquet-export.lock');
    const lease = FileLease.acquire(file);
    expect(fs.existsSync(file)).to.equal(true);
    expect(() => FileLease.acquire(file)).to.throw('Archive lock is held');
    lease.assertHeld();
    lease.release();
    expect(fs.existsSync(file)).to.equal(false);
    const next = FileLease.acquire(file);
    next.release();
  });

  it('reclaims an expired lock and never removes another owner lock', () => {
    const file = path.join(dir, '.parquet-export.lock');
    fs.writeFileSync(file, JSON.stringify({ token: 'crashed-worker' }));
    const old = new Date(Date.now() - 600000);
    fs.utimesSync(file, old, old);
    const lease = FileLease.acquire(file, 60000);
    lease.assertHeld();
    lease.release();
    expect(fs.existsSync(file)).to.equal(false);
  });
});
