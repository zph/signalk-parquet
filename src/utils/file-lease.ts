/** Cross-process lease for a local archive directory.
 *
 * O_EXCL creation is the lock operation. The mtime is renewed while work is
 * active; an expired lock can be reclaimed after a crashed process. Writers
 * must assert ownership again before publishing a file.
 */
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { randomUUID } from 'crypto';

export class FileLease {
  private readonly token = randomUUID();
  private timer?: NodeJS.Timeout;
  private released = false;

  private constructor(
    private readonly file: string,
    private readonly ttlMs: number
  ) {}

  static acquire(file: string, ttlMs = 5 * 60 * 1000): FileLease {
    fs.mkdirSync(path.dirname(file), { recursive: true });
    const lease = new FileLease(file, ttlMs);
    try {
      lease.create();
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== 'EEXIST') throw error;
      const first = fs.statSync(file);
      if (Date.now() - first.mtimeMs <= ttlMs) {
        throw new Error(`Archive lock is held: ${file}`, { cause: error });
      }
      // Recheck both content and mtime immediately before reclaiming. An old
      // worker that wakes up later must fail assertHeld() before publishing.
      const owner = fs.readFileSync(file, 'utf8');
      const second = fs.statSync(file);
      if (
        second.mtimeMs !== first.mtimeMs ||
        fs.readFileSync(file, 'utf8') !== owner
      ) {
        throw new Error(`Archive lock was renewed while reclaiming: ${file}`, {
          cause: error,
        });
      }
      fs.unlinkSync(file);
      lease.create();
    }
    lease.timer = setInterval(
      () => lease.renew(),
      Math.max(1000, Math.floor(ttlMs / 4))
    );
    lease.timer.unref();
    return lease;
  }

  private create(): void {
    const fd = fs.openSync(this.file, 'wx', 0o600);
    try {
      fs.writeFileSync(
        fd,
        JSON.stringify({
          token: this.token,
          pid: process.pid,
          host: os.hostname(),
        })
      );
      fs.fsyncSync(fd);
    } finally {
      fs.closeSync(fd);
    }
  }

  assertHeld(): void {
    if (this.released) throw new Error('Archive lease has been released');
    const owner = JSON.parse(fs.readFileSync(this.file, 'utf8')) as {
      token: string;
    };
    if (owner.token !== this.token)
      throw new Error('Archive lease ownership was lost');
  }

  private renew(): void {
    try {
      this.assertHeld();
      const now = new Date();
      fs.utimesSync(this.file, now, now);
    } catch {
      if (this.timer) clearInterval(this.timer);
      this.timer = undefined;
    }
  }

  release(): void {
    if (this.timer) clearInterval(this.timer);
    try {
      this.assertHeld();
      fs.unlinkSync(this.file);
    } catch {
      // Never remove another process's lock.
    }
    this.released = true;
  }
}
