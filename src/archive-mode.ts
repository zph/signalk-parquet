import * as path from 'node:path';
import * as fs from 'node:fs/promises';
import type { ServerAPI, Path } from '@signalk/server-api';
import type { PluginConfig } from './types';
import { HistoryProvider } from './history-provider';
import { DuckDBPool } from './utils/duckdb-pool';

// Optional peer: installed alongside Parquet only when managed archive modes are used.
function library() {
  // eslint-disable-next-line @typescript-eslint/no-require-imports
  return require('signalk-history-sync/src/parquet-runtime');
}
function monitor() {
  // eslint-disable-next-line @typescript-eslint/no-require-imports
  return require('signalk-history-sync/src/monitor');
}

export class ArchiveMode {
  private bridge: any;
  private timer?: NodeJS.Timeout;
  private snapshot: string;
  private stopped = false;
  private busy = false;
  private lastNotice = '';
  public status: Record<string, unknown>;
  constructor(
    private app: ServerAPI,
    private options: Partial<PluginConfig>
  ) {
    this.snapshot = path.join(app.getDataDirPath(), 'replica-empty');
    this.status = { mode: options.archiveMode, state: 'starting' };
  }
  async start(): Promise<void> {
    const cloud = this.options.cloudUpload;
    if (!cloud || cloud.provider === 'none' || !cloud.bucket)
      throw new Error('ARCHIVE_REQUIRES_CLOUD_STORAGE');
    const source = this.options.archiveSource;
    if (!source || !/^[A-Za-z0-9_-]{1,80}$/.test(source))
      throw new Error('ARCHIVE_SOURCE_REQUIRED');
    const replica = this.options.archiveMode === 'replica';
    if (
      replica &&
      (!this.options.archiveCoverageStart ||
        !/^\d{4}-\d{2}-\d{2}$/.test(this.options.archiveCoverageStart))
    )
      throw new Error('ARCHIVE_COVERAGE_START_REQUIRED');
    const endpoint =
      cloud.provider === 'r2'
        ? `https://${cloud.accountId}.r2.cloudflarestorage.com`
        : cloud.endpoint;
    const { ArchiveBridge, producerIdentity } = library();
    const config = {
      bucket: cloud.bucket,
      prefix: cloud.keyPrefix || '',
      endpoint,
      region: cloud.provider === 'r2' ? 'auto' : cloud.region,
      credentials:
        cloud.accessKeyId && cloud.secretAccessKey
          ? {
              accessKeyId: cloud.accessKeyId,
              secretAccessKey: cloud.secretAccessKey,
            }
          : undefined,
      forcePathStyle: cloud.forcePathStyle ?? true,
      allowInsecureLocalTest: cloud.allowPrivateEndpoint === true,
      source,
      producerId: replica
        ? undefined
        : await producerIdentity(this.app.getDataDirPath()),
      directory: replica
        ? path.join(this.app.getDataDirPath(), 'replica-cache')
        : this.options.outputDirectory?.trim() || this.app.getDataDirPath(),
      coverageStart: replica
        ? `${this.options.archiveCoverageStart}T00:00:00.000Z`
        : undefined,
      dailyExportHour: this.options.dailyExportHour ?? 4,
    };
    this.bridge = new ArchiveBridge(config);
    if (!replica) {
      const receiptFile = path.join(
        this.app.getDataDirPath(),
        'archive-claim.json'
      );
      const identity = JSON.stringify([
        endpoint || cloud.provider,
        cloud.bucket,
        cloud.keyPrefix || '',
        source,
        config.producerId,
      ]);
      try {
        await this.bridge.request('claim');
        await fs.writeFile(receiptFile, identity, { mode: 0o600 });
        this.status = { mode: 'producer', state: 'claimed' };
      } catch (e) {
        // A previously claimed producer must keep collecting locally while offline.
        // Every publish rechecks ownership; the cached receipt never authorizes uploads.
        let receipt = '';
        try {
          receipt = await fs.readFile(receiptFile, 'utf8');
        } catch {}
        if (
          (e as Error).message !== 'PRODUCER_CONFLICT_USE_REPLICA' &&
          receipt === identity
        ) {
          this.status = {
            mode: 'producer',
            state: 'offline',
            advice:
              'Collecting locally; publishing waits for ownership verification.',
          };
          return;
        }
        await this.bridge.stop();
        this.status = {
          mode: 'producer',
          state: 'blocked',
          advice:
            'Another producer or unavailable archive. Select replica for an existing shared endpoint, bucket and prefix.',
        };
        throw e;
      }
      return;
    }
    await fs.mkdir(this.snapshot, { recursive: true });
    await DuckDBPool.initialize(this.app.getDataDirPath(), msg =>
      this.app.error(msg)
    );
    // Each request pins an immutable complete generation tree. No SQLite buffer,
    // live subscriptions, auto-discovery, aggregation or retention writes exist here.
    const provider = () =>
      new HistoryProvider(
        this.app.selfId,
        this.snapshot,
        this.app,
        this.app.debug
      );
    this.app.registerHistoryApiProvider({
      getValues: query => provider().getValues(query),
      getPaths: query => provider().getPaths(query),
      getContexts: query => provider().getContexts(query),
    });
    const tick = async () => {
      if (this.stopped || this.busy) return;
      this.busy = true;
      try {
        const result = await this.bridge.request('reconcile');
        if (this.stopped) return;
        this.snapshot = result.snapshot;
        const health = monitor().coverageStatus(result, {
          warningLagSeconds: (this.options.archiveWarningHours ?? 24) * 3600,
          alarmLagSeconds: (this.options.archiveAlarmHours ?? 72) * 3600,
        });
        this.status = {
          mode: 'replica',
          ...health,
          verifiedThrough: result.verifiedThrough,
        };
        this.notify(health);
        this.app.setPluginStatus('S3 replica; live Signal K capture disabled');
      } catch {
        if (!this.stopped) {
          const health = monitor().coverageStatus(this.status, {
            warningLagSeconds: (this.options.archiveWarningHours ?? 24) * 3600,
            alarmLagSeconds: (this.options.archiveAlarmHours ?? 72) * 3600,
          });
          this.status = {
            ...this.status,
            ...health,
            state: health.state === 'alarm' ? 'alarm' : 'warn',
            code: 'PARQUET_SYNC_FAILED',
          };
          this.notify(this.status);
          this.app.setPluginError(
            'S3 replica is stale or unavailable; retaining verified history'
          );
        }
      } finally {
        this.busy = false;
        if (!this.stopped) this.timer = setTimeout(tick, 60000);
      }
    };
    await tick();
  }
  async publish(excludedDays: string[]): Promise<void> {
    if (this.stopped || this.options.archiveMode !== 'producer') return;
    await this.bridge.request('publish', { excludedDays });
  }
  private notify(status: any): void {
    const identity = `${status.state}:${status.code}`;
    if (identity === this.lastNotice) return;
    this.lastNotice = identity;
    this.app.handleMessage('signalk-parquet', {
      updates: [
        {
          values: [
            {
              path: 'notifications.plugins.historySync.parquet' as Path,
              value: monitor().notification(status),
            },
          ],
        },
      ],
    });
  }
  async stop(): Promise<void> {
    this.stopped = true;
    clearTimeout(this.timer);
    await this.bridge?.stop();
    if (this.options.archiveMode === 'replica') {
      this.app.unregisterHistoryApiProvider();
      await DuckDBPool.shutdown();
    }
  }
}
