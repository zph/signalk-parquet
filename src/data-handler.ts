import * as fs from 'fs-extra';
import * as path from 'path';
import { createHash } from 'crypto';
import { glob as globOriginal } from 'glob';
import { promisify } from 'util';
import { minimatch } from 'minimatch';

// Wrap glob for compatibility with both glob@7.x (callbacks) and glob@11.x (promises)
const glob = async (pattern: string, options?: object): Promise<string[]> => {
  // If glob returns a Promise (glob@11.x), use it directly
  const result = globOriginal(pattern, options || {}) as unknown;
  if (result && typeof (result as Promise<string[]>).then === 'function') {
    return result as Promise<string[]>;
  }
  // Otherwise use promisify for glob@7.x callback style
  const globPromise = promisify(
    globOriginal as (
      pattern: string,
      options: object,
      cb: (err: Error | null, matches: string[]) => void
    ) => void
  );
  return globPromise(pattern, options || {}) as Promise<string[]>;
};
import {
  PluginConfig,
  PathConfig,
  DataRecord,
  PluginState,
  NormalizedDelta,
} from './types';
import { extractCommandName } from './commands';
import { resolveCustomS3Endpoint } from './utils/cloud-endpoint';
import { DuckDBPool } from './utils/duckdb-pool';
import {
  Context,
  Delta,
  hasValues,
  Path,
  PathValue,
  ServerAPI,
  Update,
} from '@signalk/server-api';

// AWS S3 for file upload
// eslint-disable-next-line @typescript-eslint/no-explicit-any
let S3Client: any,
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  PutObjectCommand: any,
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  HeadObjectCommand: any,
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  GetObjectCommand: any;

// Bound network waits for cloud (S3/R2) calls so a stalled endpoint can't block
// the upload/daily-export pipeline indefinitely (a real risk on a vessel uplink).
// throwOnRequestTimeout is required: without it @smithy/node-http-handler only
// logs a warning when requestTimeout elapses and lets the request run on, so
// the ceiling would not actually bound anything.
const CLOUD_CONNECTION_TIMEOUT_MS = 10000;
const CLOUD_REQUEST_TIMEOUT_MS = 60000;
const CLOUD_REQUEST_HANDLER = {
  connectionTimeout: CLOUD_CONNECTION_TIMEOUT_MS,
  requestTimeout: CLOUD_REQUEST_TIMEOUT_MS,
  throwOnRequestTimeout: true,
};

let _appInstance: ServerAPI;

// Generic cloud target for S3-compatible uploads (S3, R2, etc.)
export interface CloudTarget {
  client: any;
  bucket: string;
  keyPrefix: string;
  deleteAfterUpload: boolean;
  label: string; // "S3" or "R2" for logging
}

export async function initializeCloudSDK(
  config: PluginConfig,
  app: ServerAPI
): Promise<void> {
  _appInstance = app;

  if (config.cloudUpload.provider !== 'none') {
    try {
      if (!S3Client) {
        const awsS3 = await import('@aws-sdk/client-s3');
        S3Client = awsS3.S3Client;
        PutObjectCommand = awsS3.PutObjectCommand;
        HeadObjectCommand = awsS3.HeadObjectCommand;
        GetObjectCommand = awsS3.GetObjectCommand;
      }
    } catch (importError) {
      // Cloud upload is configured but the AWS SDK failed to load; leave the
      // client undefined (uploads become no-ops) but make the misconfiguration
      // visible instead of silently disabling sync forever.
      S3Client = undefined;
      app.error(
        `[CloudSync] Cloud upload provider '${config.cloudUpload.provider}' is configured but @aws-sdk/client-s3 failed to load: ${(importError as Error).message}`
      );
    }
  }
}

export function createCloudClient(config: PluginConfig, app: ServerAPI): any {
  const cloud = config.cloudUpload;
  if (cloud.provider === 'none' || !S3Client) {
    return undefined;
  }

  try {
    if (cloud.provider === 'r2') {
      if (!cloud.accountId) {
        app.error('R2 upload enabled but no account ID configured');
        return undefined;
      }
      return new S3Client({
        region: 'auto',
        endpoint: `https://${cloud.accountId}.r2.cloudflarestorage.com`,
        requestHandler: { ...CLOUD_REQUEST_HANDLER },
        credentials:
          cloud.accessKeyId && cloud.secretAccessKey
            ? {
                accessKeyId: cloud.accessKeyId,
                secretAccessKey: cloud.secretAccessKey,
              }
            : undefined,
      });
    }

    // S3 provider
    const s3Config: {
      region: string;
      endpoint?: string;
      forcePathStyle?: boolean;
      credentials?: { accessKeyId: string; secretAccessKey: string };
      requestHandler?: {
        connectionTimeout: number;
        requestTimeout: number;
        throwOnRequestTimeout: boolean;
      };
    } = {
      region: cloud.region || 'us-east-1',
      requestHandler: { ...CLOUD_REQUEST_HANDLER },
    };

    if (cloud.endpoint) {
      const custom = resolveCustomS3Endpoint(
        cloud.endpoint,
        cloud.forcePathStyle,
        cloud.allowPrivateEndpoint
      );
      s3Config.endpoint = custom.url;
      s3Config.forcePathStyle = custom.forcePathStyle;
    } else {
      s3Config.forcePathStyle =
        cloud.forcePathStyle !== undefined ? cloud.forcePathStyle : false;
    }

    if (cloud.accessKeyId && cloud.secretAccessKey) {
      s3Config.credentials = {
        accessKeyId: cloud.accessKeyId,
        secretAccessKey: cloud.secretAccessKey,
      };
    }

    return new S3Client(s3Config);
  } catch (error) {
    app.error(
      `Failed to create ${cloud.provider.toUpperCase()} client: ${(error as Error).message}`
    );
    return undefined;
  }
}

// Build cloud target from config and state
export function getCloudTarget(
  config: PluginConfig,
  state: PluginState
): CloudTarget | null {
  const cloud = config.cloudUpload;
  if (cloud.provider === 'none' || !state.cloudClient) {
    return null;
  }

  return {
    client: state.cloudClient,
    bucket: cloud.bucket || '',
    keyPrefix: cloud.keyPrefix || '',
    deleteAfterUpload: cloud.deleteAfterUpload || false,
    label: cloud.provider.toUpperCase(),
  };
}

// Subscribe to command paths that control regimens using proper subscription manager
export function subscribeToCommandPaths(
  currentPaths: PathConfig[],
  state: PluginState,
  config: PluginConfig,
  app: ServerAPI
): void {
  const commandPaths = currentPaths.filter(
    (pathConfig: PathConfig) =>
      pathConfig &&
      pathConfig.path &&
      pathConfig.path.startsWith('commands.') &&
      pathConfig.enabled
  );

  if (commandPaths.length === 0) return;

  // Create Map for O(1) lookup instead of O(n) .find()
  // This eliminates O(n²) nested loop on every delta message
  const commandPathsMap = new Map<string, PathConfig>(
    commandPaths.map(pathConfig => [pathConfig.path, pathConfig])
  );

  const commandSubscription = {
    context: 'vessels.self' as Context,
    subscribe: commandPaths.map((pathConfig: PathConfig) => ({
      path: pathConfig.path,
      period: 1000, // Check commands every second
      policy: 'fixed' as const,
    })),
  };

  app.subscriptionmanager.subscribe(
    commandSubscription,
    state.unsubscribes,
    (_subscriptionError: unknown) => {},
    (delta: Delta) => {
      // Process each update in the delta
      const prevRegimens = new Set(state.activeRegimens);
      if (delta.updates) {
        delta.updates.forEach((update: Update) => {
          if (hasValues(update)) {
            update.values.forEach((valueUpdate: PathValue) => {
              // O(1) Map lookup instead of O(n) array.find()
              const pathConfig = commandPathsMap.get(valueUpdate.path);
              if (pathConfig) {
                handleCommandMessage(
                  valueUpdate,
                  pathConfig,
                  config,
                  update,
                  state,
                  app
                );
              }
            });
          }
        });
      }
      // If active regimens changed, re-evaluate data subscriptions
      if (
        state.activeRegimens.size !== prevRegimens.size ||
        [...state.activeRegimens].some(r => !prevRegimens.has(r))
      ) {
        const streamSubscriptions: any = (
          state as PluginState & { streamSubscriptions?: unknown }
        ).streamSubscriptions;

        const disposeStreamSubscription = (subscription: unknown): void => {
          if (typeof subscription === 'function') {
            subscription();
            return;
          }

          if (subscription && typeof subscription === 'object') {
            const candidate = subscription as {
              unsubscribe?: () => void;
              dispose?: () => void;
              off?: () => void;
            };

            if (typeof candidate.unsubscribe === 'function') {
              candidate.unsubscribe();
            } else if (typeof candidate.dispose === 'function') {
              candidate.dispose();
            } else if (typeof candidate.off === 'function') {
              candidate.off();
            }
          }
        };

        if (Array.isArray(streamSubscriptions)) {
          streamSubscriptions.forEach(disposeStreamSubscription);
          streamSubscriptions.length = 0;
        } else if (streamSubscriptions instanceof Map) {
          streamSubscriptions.forEach(disposeStreamSubscription);
          streamSubscriptions.clear();
        } else if (streamSubscriptions instanceof Set) {
          streamSubscriptions.forEach(disposeStreamSubscription);
          streamSubscriptions.clear();
        } else if (
          streamSubscriptions &&
          typeof streamSubscriptions === 'object'
        ) {
          Object.values(streamSubscriptions as Record<string, unknown>).forEach(
            disposeStreamSubscription
          );
          Object.keys(streamSubscriptions as Record<string, unknown>).forEach(
            key => {
              delete (streamSubscriptions as Record<string, unknown>)[key];
            }
          );
        }
        updateDataSubscriptions(currentPaths, state, config, app);
      }
    }
  );

  commandPaths.forEach(pathConfig => {
    state.subscribedPaths.add(pathConfig.path);
  });
}

// Handle command messages (regimen control) - now receives complete delta structure
function handleCommandMessage(
  valueUpdate: PathValue,
  pathConfig: PathConfig,
  config: PluginConfig,
  update: Update,
  state: PluginState,
  app: ServerAPI
): void {
  try {
    // Check source filter if specified for commands too
    if (pathConfig.source && pathConfig.source.trim() !== '') {
      const messageSource =
        update.$source || (update.source ? update.source.label : null);
      if (messageSource !== pathConfig.source.trim()) {
        return;
      }
    }

    if (valueUpdate.value !== undefined) {
      const commandName = extractCommandName(pathConfig.path);
      const isActive = Boolean(valueUpdate.value);

      if (isActive) {
        state.activeRegimens.add(commandName);
      } else {
        state.activeRegimens.delete(commandName);
      }

      // Debug active regimens state

      // Capture-all records this same delta through the wildcard subscription.
      // Do not duplicate command rows when both handlers are active.
      if (
        !config.autoDiscovery?.enabled ||
        !config.autoDiscovery.captureAllLivePaths
      ) {
        const bufferKey = `${pathConfig.context || 'vessels.self'}:${pathConfig.path}`;
        bufferData(
          bufferKey,
          {
            received_timestamp: new Date().toISOString(),
            signalk_timestamp: update.timestamp || new Date().toISOString(),
            context: 'vessels.self',
            path: valueUpdate.path,
            value: valueUpdate.value,
            source: update.source || undefined, // Store as object, serialize at write time
            source_label:
              update.$source ||
              (update.source ? update.source.label : undefined),
            source_type: update.source ? update.source.type : undefined,
            source_pgn: update.source ? update.source.pgn : undefined,
            source_src: update.source ? update.source.src : undefined,
          },
          config,
          state,
          app
        );
      }
    }
  } catch (error) {
    app.error(
      `[DataHandler] Failed to handle command message for ${pathConfig.path}: ${(error as Error).message}`
    );
  }
}

// Helper function to handle wildcard contexts
function handleWildcardContext(pathConfig: PathConfig): PathConfig {
  const context = pathConfig.context || 'vessels.self';

  if (context === 'vessels.*') {
    // For vessels.*, we create a subscription that will receive deltas from any vessel
    // The actual filtering by MMSI will happen in the delta handler
    return {
      ...pathConfig,
      context: 'vessels.*' as Context, // Keep the wildcard for the subscription
    };
  }

  // Not a wildcard, return as-is
  return pathConfig;
}

// Helper function to check if a vessel should be excluded based on MMSI
function _shouldExcludeVessel(
  vesselContext: string,
  pathConfig: PathConfig,
  app: ServerAPI
): boolean {
  if (!pathConfig.excludeMMSI || pathConfig.excludeMMSI.length === 0) {
    return false; // No exclusions specified
  }

  try {
    // For vessels.self, use getSelfPath
    // Cast to any for compatibility with different @signalk/server-api versions
    if (vesselContext === 'vessels.self') {
      const mmsiData = app.getSelfPath('mmsi') as any;
      if (mmsiData && mmsiData.value) {
        const mmsi = String(mmsiData.value);
        return pathConfig.excludeMMSI.includes(mmsi);
      }
    } else {
      // For other vessels, we would need to get their MMSI from the delta or other means
      // For now, we'll skip MMSI filtering for other vessels
    }
  } catch (error) {}

  return false; // Don't exclude if we can't determine MMSI
}

// Dispose a single stream subscription returned by streambundle .onValue()
function disposeStreamSubscription(subscription: unknown): void {
  if (typeof subscription === 'function') {
    subscription();
    return;
  }
  if (subscription && typeof subscription === 'object') {
    const candidate = subscription as {
      unsubscribe?: () => void;
      dispose?: () => void;
      end?: () => void;
    };
    if (typeof candidate.unsubscribe === 'function') {
      candidate.unsubscribe();
    } else if (typeof candidate.dispose === 'function') {
      candidate.dispose();
    } else if (typeof candidate.end === 'function') {
      candidate.end();
    }
  }
}

function isPlainRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

/** Root-bus deltas carry identity fields as a nested object at path ''. */
function flattenRootValues(
  value: unknown,
  prefix = ''
): Array<{ path: string; value: unknown }> {
  if (!isPlainRecord(value)) {
    return prefix ? [{ path: prefix, value }] : [];
  }
  if (prefix && 'latitude' in value && 'longitude' in value) {
    return [{ path: prefix, value }];
  }
  return Object.entries(value).flatMap(([key, child]) =>
    flattenRootValues(child, prefix ? `${prefix}.${key}` : key)
  );
}

/** Reconcile cached full-model values that never emitted during this plugin run. */
type CapturedValue = {
  context: string;
  path: string;
  value: unknown;
  timestamp?: string;
  $source?: string;
  source?: NormalizedDelta['source'];
};

function snapshotVesselValues(app: ServerAPI): CapturedValue[] {
  const vessels = app.getPath('vessels');
  if (!isPlainRecord(vessels)) return [];
  const records: CapturedValue[] = [];
  const walk = (context: string, node: unknown, prefix = ''): void => {
    if (!isPlainRecord(node)) {
      if (prefix) records.push({ context, path: prefix, value: node });
      return;
    }
    if (prefix && Object.prototype.hasOwnProperty.call(node, 'value')) {
      records.push({
        context,
        path: prefix,
        value: node.value,
        timestamp:
          typeof node.timestamp === 'string' ? node.timestamp : undefined,
        $source: typeof node.$source === 'string' ? node.$source : undefined,
      });
    }
    if (prefix && 'latitude' in node && 'longitude' in node) {
      records.push({ context, path: prefix, value: node });
      return;
    }
    for (const [key, child] of Object.entries(node)) {
      if (
        ['value', 'meta', 'values', '$source', 'timestamp', 'source'].includes(
          key
        )
      )
        continue;
      walk(context, child, prefix ? `${prefix}.${key}` : key);
    }
  };
  for (const [vesselId, value] of Object.entries(vessels)) {
    walk(vesselId === 'self' ? app.selfContext : `vessels.${vesselId}`, value);
  }
  return records;
}

// Update data path subscriptions based on active regimens
export function updateDataSubscriptions(
  currentPaths: PathConfig[],
  state: PluginState,
  config: PluginConfig,
  app: ServerAPI
): void {
  if (state.captureAllReconcileInterval) {
    clearInterval(state.captureAllReconcileInterval);
    state.captureAllReconcileInterval = undefined;
  }
  // First, unsubscribe from all existing subscriptions
  state.unsubscribes.forEach(unsubscribe => {
    if (typeof unsubscribe === 'function') {
      unsubscribe();
    }
  });
  state.unsubscribes = [];

  // Dispose existing streambundle subscriptions to prevent duplicate handlers
  if (state.streamSubscriptions) {
    state.streamSubscriptions.forEach(disposeStreamSubscription);
    state.streamSubscriptions = [];
  }

  state.subscribedPaths.clear();

  // Re-subscribe to command paths
  subscribeToCommandPaths(currentPaths, state, config, app);

  // Proactive fleet capture uses one wildcard subscription instead of a
  // per-path stream. It preserves every preferred-source value, including
  // distinct AIS contexts that share a path; no cross-vessel debounce.
  if (
    config.autoDiscovery?.enabled &&
    config.autoDiscovery.captureAllLivePaths
  ) {
    const discovery = config.autoDiscovery;
    const bootstrapSeen = new Set<string>();
    const lastCapturedValue = new Map<string, string>();
    let bootstrapping = true;
    const capture = (delta: CapturedValue, cached = false): void => {
      if (!delta.path) return;
      if (
        discovery.excludePatterns?.some(pattern =>
          minimatch(delta.path, pattern)
        ) ||
        (discovery.includePatterns?.length &&
          !discovery.includePatterns.some(pattern =>
            minimatch(delta.path, pattern)
          ))
      )
        return;
      const pathKey = `${delta.context}\0${delta.path}`;
      const version = `${delta.timestamp || ''}\0${delta.$source || ''}\0${JSON.stringify(delta.value)}`;
      if (cached && lastCapturedValue.get(pathKey) === version) return;
      const key = `${delta.context}\0${delta.path}\0${delta.timestamp || ''}\0${delta.$source || ''}`;
      if (bootstrapping) {
        if (bootstrapSeen.has(key)) return;
        bootstrapSeen.add(key);
      }
      handleStreamData(
        delta as NormalizedDelta,
        { path: delta.path as Path },
        config,
        state,
        app
      );
      lastCapturedValue.set(pathKey, version);
      state.subscribedPaths.add(delta.path);
    };
    app.subscriptionmanager.subscribe(
      {
        context: '*' as Context,
        subscribe: [
          { path: '*' as Path, policy: 'instant' as const, minPeriod: 0 },
        ],
        sourcePolicy: 'preferred' as const,
      },
      state.unsubscribes,
      error => app.error(`[CaptureAll] Subscription error: ${String(error)}`),
      (delta: Delta) => {
        for (const update of delta.updates || []) {
          if (!hasValues(update)) continue;
          for (const value of update.values) {
            const signalkPath = value.path;
            const base = {
              context: delta.context || 'vessels.self',
              timestamp: update.timestamp,
              source: update.source,
              $source: update.$source,
            };
            if (signalkPath === '') {
              for (const leaf of flattenRootValues(value.value)) {
                capture({ ...base, ...leaf });
              }
            } else if (signalkPath) {
              capture({ ...base, path: signalkPath, value: value.value });
            }
          }
        }
      }
    );
    const reconcileCachedModel = (): void => {
      try {
        for (const cached of snapshotVesselValues(app)) capture(cached, true);
      } catch (error) {
        app.error(
          `[CaptureAll] Cached-model snapshot failed: ${String(error)}`
        );
      }
    };
    try {
      reconcileCachedModel();
    } finally {
      bootstrapping = false;
      bootstrapSeen.clear();
    }
    // Some plugins populate the full model without emitting a wildcard delta.
    // Reconcile those late static paths without writing duplicates each minute.
    state.captureAllReconcileInterval = setInterval(
      reconcileCachedModel,
      60_000
    );
    state.captureAllReconcileInterval.unref();
    return;
  }

  // Now subscribe to data paths using currentPaths
  const dataPaths = currentPaths.filter(
    (pathConfig: PathConfig) =>
      pathConfig && pathConfig.path && !pathConfig.path.startsWith('commands.')
  );

  const shouldSubscribePaths = dataPaths.filter((pathConfig: PathConfig) =>
    shouldSubscribeToPath(pathConfig, state, app)
  );

  // Handle wildcard contexts (like vessels.*)
  const processedPaths: PathConfig[] = shouldSubscribePaths.map(pathConfig =>
    handleWildcardContext(pathConfig)
  );

  if (processedPaths.length === 0) {
    return;
  }

  // Group paths by context for separate subscriptions
  const contextGroups = new Map<Context, PathConfig[]>();
  processedPaths.forEach((pathConfig: PathConfig) => {
    const context = (pathConfig.context || 'vessels.self') as Context;
    if (!contextGroups.has(context)) {
      contextGroups.set(context, []);
    }
    contextGroups.get(context)!.push(pathConfig);
  });

  // Context filter helper — reused for both normal and root-level paths
  function passesContextFilter(
    normalizedDelta: NormalizedDelta,
    pathConfig: PathConfig
  ): boolean {
    // Filter by source if specified
    if (pathConfig.source && pathConfig.source.trim() !== '') {
      if (normalizedDelta.$source !== pathConfig.source.trim()) return false;
    }

    // Filter by context
    const targetContext = pathConfig.context || 'vessels.self';
    if (targetContext === 'vessels.*') {
      if (!normalizedDelta.context.startsWith('vessels.')) return false;
    } else if (targetContext === 'vessels.self') {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const selfContext = app.selfContext;
      const selfVessel = (app.getSelfPath('') as any) || {};
      const selfMMSI = selfVessel.mmsi;
      const selfUuid = app.getSelfPath('uuid') as any;

      const isSelfVessel =
        normalizedDelta.context === 'vessels.self' ||
        normalizedDelta.context === selfContext ||
        (selfMMSI && normalizedDelta.context.includes(selfMMSI)) ||
        (selfUuid && normalizedDelta.context.includes(selfUuid));

      if (!isSelfVessel) return false;
    } else {
      if (normalizedDelta.context !== targetContext) return false;
    }

    // MMSI exclusion filtering
    if (pathConfig.excludeMMSI && pathConfig.excludeMMSI.length > 0) {
      if (
        pathConfig.excludeMMSI.some(mmsi =>
          normalizedDelta.context.includes(mmsi)
        )
      )
        return false;
    }

    // Skip meta deltas
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    if ((normalizedDelta as any).isMeta) return false;

    return true;
  }

  // Use app.streambundle approach as recommended by SignalK developer
  // This avoids server arbitration and provides true source filtering
  contextGroups.forEach((pathConfigs, _context) => {
    // Root-level paths (no dots, e.g., "name", "mmsi") arrive on the root bus ("")
    // as part of a bundled object update, not as individual path deltas
    const rootPaths = pathConfigs.filter(
      (pc: PathConfig) => !pc.path.includes('.')
    );
    const normalPaths = pathConfigs.filter((pc: PathConfig) =>
      pc.path.includes('.')
    );

    // Subscribe to root bus for root-level paths
    if (rootPaths.length > 0) {
      const rootPathMap = new Map<string, PathConfig>();
      rootPaths.forEach((pc: PathConfig) => rootPathMap.set(pc.path, pc));

      const rootStream = app.streambundle
        .getBus('' as Path)
        .filter((normalizedDelta: NormalizedDelta) => {
          if (
            !normalizedDelta.value ||
            typeof normalizedDelta.value !== 'object'
          )
            return false;
          const valueObj = normalizedDelta.value as Record<string, unknown>;
          return rootPaths.some(
            (pc: PathConfig) => valueObj[pc.path] !== undefined
          );
        })
        .onValue((normalizedDelta: NormalizedDelta) => {
          const valueObj = normalizedDelta.value as Record<string, unknown>;
          for (const [pathName, pathConfig] of rootPathMap) {
            if (valueObj[pathName] === undefined) continue;
            if (!passesContextFilter(normalizedDelta, pathConfig)) continue;
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            const syntheticDelta = {
              ...normalizedDelta,
              path: pathName,
              value: valueObj[pathName],
            } as any as NormalizedDelta;
            handleStreamData(syntheticDelta, pathConfig, config, state, app);
          }
        });

      state.streamSubscriptions = state.streamSubscriptions || [];
      state.streamSubscriptions.push(rootStream);
      rootPaths.forEach((pc: PathConfig) => state.subscribedPaths.add(pc.path));
    }

    // Normal dotted paths — individual stream per path
    normalPaths.forEach((pathConfig: PathConfig) => {
      const stream = app.streambundle
        .getBus(pathConfig.path as Path)
        .filter((normalizedDelta: NormalizedDelta) =>
          passesContextFilter(normalizedDelta, pathConfig)
        )
        .onValue((normalizedDelta: NormalizedDelta) => {
          handleStreamData(normalizedDelta, pathConfig, config, state, app);
        });

      state.streamSubscriptions = state.streamSubscriptions || [];
      state.streamSubscriptions.push(stream);
      state.subscribedPaths.add(pathConfig.path);
    });
  });
}

// Determine if we should subscribe to a path based on regimens
function shouldSubscribeToPath(
  pathConfig: PathConfig,
  state: PluginState,
  _app: ServerAPI
): boolean {
  // Always subscribe if explicitly enabled
  if (pathConfig.enabled) {
    return true;
  }

  // Check if any required regimens are active
  if (pathConfig.regimen) {
    const requiredRegimens = pathConfig.regimen.split(',').map(r => r.trim());
    const hasActiveRegimen = requiredRegimens.some(regimen =>
      state.activeRegimens.has(regimen)
    );
    return hasActiveRegimen;
  }

  return false;
}

// New handler for streambundle data (developer's recommended approach)
function handleStreamData(
  normalizedDelta: NormalizedDelta,
  pathConfig: PathConfig,
  config: PluginConfig,
  state: PluginState,
  app: ServerAPI
): void {
  try {
    const receivedTimestamp = new Date().toISOString();
    const signalKTimestamp = normalizedDelta.timestamp;
    const validTimestamp =
      typeof signalKTimestamp === 'string' &&
      Number.isFinite(Date.parse(signalKTimestamp))
        ? signalKTimestamp
        : receivedTimestamp;
    // Retrieve metadata for this path
    let metadata: object | undefined;
    try {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const pathMetadata = (app as any).getMetadata?.(normalizedDelta.path);
      if (pathMetadata) {
        metadata = pathMetadata; // Store as object, serialize at write time
      }
    } catch (error) {
      // Metadata retrieval failed, continue without it
    }

    const record: DataRecord = {
      received_timestamp: receivedTimestamp,
      signalk_timestamp: validTimestamp,
      context: normalizedDelta.context || pathConfig.context || 'vessels.self',
      path: normalizedDelta.path,
      value: null,
      value_json: undefined,
      source: normalizedDelta.source || undefined, // Store as object, serialize at write time
      source_label: normalizedDelta.$source || undefined,
      source_type: normalizedDelta.source
        ? normalizedDelta.source.type
        : undefined,
      source_pgn: normalizedDelta.source
        ? normalizedDelta.source.pgn
        : undefined,
      source_src: normalizedDelta.source
        ? normalizedDelta.source.src
        : undefined,
      meta: metadata,
    };

    // Handle complex values
    if (
      typeof normalizedDelta.value === 'object' &&
      normalizedDelta.value !== null
    ) {
      const valueObj = normalizedDelta.value as Record<string, unknown>;
      const objKeys = Object.keys(valueObj);

      // Skip if this looks like a meta-only update — every key is a SignalK
      // metadata field, so the object carries no real value to store. 'timeout'
      // is intentionally NOT in this set: it can appear as a real value field,
      // and the worst outcome here is silently dropping archived data, so we
      // err toward keeping an object whose only key is 'timeout'.
      const metaOnlyKeys = [
        'units',
        'meta',
        'description',
        'displayUnits',
        'zones',
      ];
      const isMetaOnly =
        objKeys.length > 0 && objKeys.every(k => metaOnlyKeys.includes(k));

      if (isMetaOnly) {
        // This is a metadata update, not real data - skip it
        return;
      }

      record.value_json = normalizedDelta.value; // Store as object, serialize at write time
      // Extract key properties as columns for easier querying
      Object.entries(normalizedDelta.value).forEach(([key, val]) => {
        if (
          typeof val === 'string' ||
          typeof val === 'number' ||
          typeof val === 'boolean'
        ) {
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          (record as any)[`value_${key}`] = val;
        }
      });
    } else {
      record.value = normalizedDelta.value;
    }

    // Use actual context + path as buffer key to separate data from different vessels
    const bufferKey = `${normalizedDelta.context}:${pathConfig.path}`;
    bufferData(bufferKey, record, config, state, app);
  } catch (error) {
    app.error(
      `[DataHandler] Failed to buffer delta for ${normalizedDelta.context}:${pathConfig.path}: ${(error as Error).message}`
    );
  }
}

// Buffer data and trigger save if buffer is full
function bufferData(
  signalkPath: string,
  record: DataRecord,
  config: PluginConfig,
  state: PluginState,
  app: ServerAPI
): void {
  // Use SQLite buffer if enabled
  if (config.useSqliteBuffer) {
    if (!state.sqliteBuffer) {
      app.error(
        `[DataHandler] SQLite buffer is enabled but not initialized! Data for ${signalkPath} will be lost.`
      );
      return;
    }
    if (!state.sqliteBuffer.isOpen()) {
      app.error(
        `[DataHandler] SQLite buffer is closed! Data for ${signalkPath} will be lost.`
      );
      return;
    }
    state.sqliteBuffer.insert(record);
    return;
  }

  // LRU cache only used when SQLite is disabled
  if (!state.dataBuffers.has(signalkPath)) {
    state.dataBuffers.set(signalkPath, []);
  }

  const buffer = state.dataBuffers.get(signalkPath)!;
  buffer.push(record);

  if (buffer.length >= config.bufferSize) {
    // Extract the actual SignalK path from the buffer key (context:path format)
    // Find the separator between context and path - look for the last colon followed by a valid SignalK path
    const pathMatch = signalkPath.match(/^.*:([a-zA-Z][a-zA-Z0-9._]*)$/);
    const actualPath = pathMatch ? pathMatch[1] : signalkPath;
    saveBufferToParquet(actualPath, buffer, config, state, app);
    state.dataBuffers.set(signalkPath, []); // Clear buffer
  }
}

// Save all buffers (called periodically and on shutdown)
export function saveAllBuffers(
  config: PluginConfig,
  state: PluginState,
  app: ServerAPI
): void {
  state.dataBuffers.forEach((buffer, signalkPath) => {
    if (buffer.length > 0) {
      // Extract the actual SignalK path from the buffer key (context:path format)
      // Find the separator between context and path - look for the last colon followed by a valid SignalK path
      const pathMatch = signalkPath.match(/^.*:([a-zA-Z][a-zA-Z0-9._]*)$/);
      const actualPath = pathMatch ? pathMatch[1] : signalkPath;
      saveBufferToParquet(actualPath, buffer, config, state, app);
      state.dataBuffers.delete(signalkPath); // Delete buffer to free memory
    }
  });
}

// Save buffer to Parquet file
async function saveBufferToParquet(
  signalkPath: string,
  buffer: DataRecord[],
  config: PluginConfig,
  state: PluginState,
  app: ServerAPI
): Promise<void> {
  try {
    // Get context from first record in buffer (all records in buffer have same path/context)
    const context = buffer.length > 0 ? buffer[0].context : 'vessels.self';

    // Create proper directory structure
    let contextPath: string;
    if (context === 'vessels.self') {
      // Clean the self context for filesystem usage (replace dots with slashes, colons with underscores)
      contextPath = app.selfContext.replace(/\./g, '/').replace(/:/g, '_');
    } else if (context.startsWith('vessels.')) {
      // Extract vessel identifier and clean it for filesystem
      const vesselId = context.replace('vessels.', '').replace(/:/g, '_');
      contextPath = `vessels/${vesselId}`;
    } else if (context.startsWith('meteo.')) {
      // Extract meteo station identifier and clean it for filesystem
      const meteoId = context.replace('meteo.', '').replace(/:/g, '_');
      contextPath = `meteo/${meteoId}`;
    } else {
      // Fallback: clean the entire context
      contextPath = context.replace(/:/g, '_').replace(/\./g, '/');
    }

    const dirPath = path.join(
      config.outputDirectory,
      contextPath,
      signalkPath.replace(/\./g, '/')
    );
    await fs.ensureDir(dirPath);

    // Generate filename with timestamp
    const timestamp = new Date()
      .toISOString()
      .replace(/[:.]/g, '')
      .slice(0, 15);
    const fileExt =
      config.fileFormat === 'csv'
        ? 'csv'
        : config.fileFormat === 'parquet'
          ? 'parquet'
          : 'json';
    const filename = `${config.filenamePrefix}_${timestamp}.${fileExt}`;
    const filepath = path.join(dirPath, filename);

    // Use ParquetWriter to save in the configured format
    await state.parquetWriter!.writeRecords(filepath, buffer);
  } catch (error) {
    app.error(
      `[DataHandler] Failed to write buffer for ${signalkPath} to ${config.fileFormat}: ${(error as Error).message}`
    );
  }
}

// Initialize regimen states from current API values at startup
export function initializeRegimenStates(
  currentPaths: PathConfig[],
  state: PluginState,
  app: ServerAPI
): void {
  const commandPaths = currentPaths.filter(
    (pathConfig: PathConfig) =>
      pathConfig &&
      pathConfig.path &&
      pathConfig.path.startsWith('commands.') &&
      pathConfig.enabled
  );

  commandPaths.forEach((pathConfig: PathConfig) => {
    try {
      // Get current value from SignalK API
      // Cast to any for compatibility with different @signalk/server-api versions
      const currentData = app.getSelfPath(pathConfig.path) as any;

      if (currentData !== undefined && currentData !== null) {
        // Check if there's source information
        const shouldProcess = true;

        // If source filter is specified, check it
        if (pathConfig.source && pathConfig.source.trim() !== '') {
          // For startup, we need to check the API source info
          // This is a simplified check - in real deltas we get more source info
          // For now, we'll process the value if it exists and log a warning
          // In practice, you might want to check the source here too
        }

        if (shouldProcess && currentData.value !== undefined) {
          const commandName = extractCommandName(pathConfig.path);
          const isActive = Boolean(currentData.value);

          if (isActive) {
            state.activeRegimens.add(commandName);
          } else {
            state.activeRegimens.delete(commandName);
          }
        }
      } else {
        /* no-op: no matching command for this regimen path */
      }
    } catch (error) {}
  });
}

// REMOVED: consolidateMissedDays() and consolidateYesterday()
// These functions have been replaced by the daily export system in parquet-export-service.ts
// The new exportDayToParquet() creates consolidated daily files directly

interface CloudObjectManifest {
  version: 1;
  key: string;
  sha256: string;
  bytes: number;
  rows: number;
  committedAt: string;
}

async function describeParquet(
  filePath: string
): Promise<Omit<CloudObjectManifest, 'version' | 'key' | 'committedAt'>> {
  const bytes = await fs.readFile(filePath);
  const connection = await DuckDBPool.getConnection();
  try {
    const escaped = filePath.replace(/'/g, "''");
    const result = await connection.runAndReadAll(
      `SELECT count(*) AS row_count FROM read_parquet('${escaped}', hive_partitioning=false)`
    );
    const row = result.getRowObjects()[0] as
      { row_count?: number | bigint } | undefined;
    if (!row || row.row_count === undefined)
      throw new Error('Could not verify Parquet row count');
    return {
      sha256: createHash('sha256').update(bytes).digest('hex'),
      bytes: bytes.length,
      rows: Number(row.row_count),
    };
  } finally {
    connection.disconnectSync();
  }
}

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

async function readCloudManifest(
  client: any,
  target: CloudTarget,
  key: string
): Promise<CloudObjectManifest | undefined> {
  if (!GetObjectCommand) return undefined;
  try {
    const response = await client.send(
      new GetObjectCommand({
        Bucket: target.bucket,
        Key: `${key}.manifest.json`,
      })
    );
    const json = JSON.parse(
      (await bodyToBuffer(response.Body)).toString('utf8')
    ) as CloudObjectManifest;
    return json?.version === 1 && json.key === key ? json : undefined;
  } catch {
    return undefined;
  }
}

async function remoteObjectMatches(
  client: any,
  target: CloudTarget,
  key: string,
  descriptor: Omit<CloudObjectManifest, 'version' | 'key' | 'committedAt'>,
  verifyPayload = false
): Promise<boolean> {
  if (!HeadObjectCommand || !GetObjectCommand) return false;
  try {
    const head = await client.send(
      new HeadObjectCommand({ Bucket: target.bucket, Key: key })
    );
    if (
      Number(head.ContentLength) !== descriptor.bytes ||
      head.Metadata?.sha256 !== descriptor.sha256 ||
      Number(head.Metadata?.rows) !== descriptor.rows
    )
      return false;
    if (!verifyPayload) return true;
    const object = await client.send(
      new GetObjectCommand({ Bucket: target.bucket, Key: key })
    );
    const actual = createHash('sha256')
      .update(await bodyToBuffer(object.Body))
      .digest('hex');
    return actual === descriptor.sha256;
  } catch {
    return false;
  }
}

/** Upload one Parquet object transactionally. The local file is eligible for
 * deletion only after the object is read back and verified and its manifest
 * has been committed and verified remotely. */
export async function uploadVerifiedCloudObject(
  filePath: string,
  cloudKey: string,
  client: any,
  bucket: string,
  deleteAfterUpload = false
): Promise<void> {
  if (!client || !PutObjectCommand || !HeadObjectCommand || !GetObjectCommand)
    throw new Error('Cloud verification SDK is unavailable');
  try {
    const descriptor = await describeParquet(filePath);
    const target = { client, bucket } as CloudTarget;
    const existingManifest = await readCloudManifest(client, target, cloudKey);
    if (
      existingManifest &&
      existingManifest.sha256 === descriptor.sha256 &&
      existingManifest.bytes === descriptor.bytes &&
      existingManifest.rows === descriptor.rows &&
      (await remoteObjectMatches(client, target, cloudKey, descriptor))
    ) {
      if (deleteAfterUpload) await fs.unlink(filePath);
      return;
    }
    const fileContent = await fs.readFile(filePath);
    await client.send(
      new PutObjectCommand({
        Bucket: bucket,
        Key: cloudKey,
        Body: fileContent,
        ContentType: 'application/octet-stream',
        Metadata: { sha256: descriptor.sha256, rows: String(descriptor.rows) },
      })
    );

    if (
      !(await remoteObjectMatches(client, target, cloudKey, descriptor, true))
    ) {
      throw new Error(
        'Uploaded object failed size, checksum, or row-count verification'
      );
    }

    const manifest: CloudObjectManifest = {
      version: 1,
      key: cloudKey,
      ...descriptor,
      committedAt: new Date().toISOString(),
    };
    const manifestBody = Buffer.from(JSON.stringify(manifest));
    await client.send(
      new PutObjectCommand({
        Bucket: bucket,
        Key: `${cloudKey}.manifest.json`,
        Body: manifestBody,
        ContentType: 'application/json',
        Metadata: {
          sha256: createHash('sha256').update(manifestBody).digest('hex'),
        },
      })
    );
    const manifestHead = await client.send(
      new HeadObjectCommand({
        Bucket: bucket,
        Key: `${cloudKey}.manifest.json`,
      })
    );
    if (Number(manifestHead.ContentLength) !== manifestBody.length) {
      throw new Error('Committed manifest failed size verification');
    }
    const committedManifest = await client.send(
      new GetObjectCommand({ Bucket: bucket, Key: `${cloudKey}.manifest.json` })
    );
    const committedBytes = await bodyToBuffer(committedManifest.Body);
    if (
      createHash('sha256').update(committedBytes).digest('hex') !==
      createHash('sha256').update(manifestBody).digest('hex')
    ) {
      throw new Error('Committed manifest failed checksum verification');
    }

    if (deleteAfterUpload) {
      await fs.unlink(filePath);
    }
  } catch (err) {
    throw new Error(
      `Cloud transaction failed for ${cloudKey}: ${(err as Error).message}`,
      { cause: err }
    );
  }
}

async function putToCloud(
  filePath: string,
  target: CloudTarget,
  config: PluginConfig
): Promise<boolean> {
  try {
    await uploadVerifiedCloudObject(
      filePath,
      getCloudKey(filePath, target, config),
      target.client,
      target.bucket,
      target.deleteAfterUpload
    );
    return true;
  } catch (err) {
    _appInstance?.error(
      `[CloudSync] Upload failed for ${filePath}: ${(err as Error).message}`
    );
    return false;
  }
}

/**
 * Object key for a local file: its path under the output directory, with
 * forward slashes, behind the target's key prefix.
 */
function getCloudKey(
  filePath: string,
  target: CloudTarget,
  config: PluginConfig
): string {
  // Object keys use forward slashes whatever the local OS.
  const relativePath = path
    .relative(config.outputDirectory, filePath)
    .split(path.sep)
    .join('/');
  if (target.keyPrefix) {
    const prefix = target.keyPrefix.endsWith('/')
      ? target.keyPrefix
      : `${target.keyPrefix}/`;
    return `${prefix}${relativePath}`;
  }
  return relativePath;
}

/**
 * Reconcile every local Hive file against its own committed object manifest.
 * Directory/object presence is not evidence that this individual file synced.
 */
async function uploadMissingFiles(
  localFiles: string[],
  _existingKeys: Set<string>,
  target: CloudTarget,
  config: PluginConfig,
  app: ServerAPI,
  concurrency = 3
): Promise<number> {
  const excludedDirs = [
    '/processed/',
    '/repaired/',
    '/failed/',
    '/quarantine/',
  ];
  // Compare with forward slashes: glob results are native paths.
  const filtered = localFiles.filter(f => {
    const posixPath = f.split(path.sep).join('/');
    return !excludedDirs.some(dir => posixPath.includes(dir));
  });
  const missing = filtered;
  if (missing.length === 0) return 0;

  app.debug(
    `[CloudSync] ${missing.length} files to upload (${concurrency} concurrent)`
  );

  let uploaded = 0;
  const failures: string[] = [];
  for (let i = 0; i < missing.length; i += concurrency) {
    const batch = missing.slice(i, i + concurrency);
    const results = await Promise.all(
      batch.map(f => putToCloud(f, target, config))
    );
    uploaded += results.filter(Boolean).length;
    batch.forEach((file, index) => {
      if (!results[index]) failures.push(file);
    });
    // Yield to event loop between batches
    await new Promise(resolve => setTimeout(resolve, 10));
  }
  if (failures.length)
    throw new Error(
      `${failures.length} object(s) failed transactional cloud reconciliation; first: ${failures[0]}`
    );
  return uploaded;
}

/**
 * Startup sync reconciles each recent local object independently.
 */
export async function uploadAllConsolidatedFilesToS3(
  config: PluginConfig,
  state: PluginState,
  app: ServerAPI
): Promise<void> {
  const target = getCloudTarget(config, state);
  if (!target) return;

  try {
    const allLocalFiles = await glob('tier=*/**/*.parquet', {
      cwd: config.outputDirectory,
      absolute: true,
      nodir: true,
    });

    app.debug(
      `[StartupSync] Reconciling ${allLocalFiles.length} local Parquet objects individually`
    );
    if (allLocalFiles.length === 0) return;

    const uploaded = await uploadMissingFiles(
      allLocalFiles,
      new Set(),
      target,
      config,
      app
    );

    if (uploaded > 0) {
      app.debug(`[StartupSync] Uploaded ${uploaded} files to ${target.label}`);
    }
  } catch (error) {
    app.error(`[StartupSync] Failed: ${(error as Error).message}`);
    throw error;
  }
}

// Upload hive-partitioned parquet files for a specific date to cloud
export async function uploadConsolidatedFilesToS3(
  config: PluginConfig,
  date: Date,
  state: PluginState,
  app: ServerAPI
): Promise<void> {
  const target = getCloudTarget(config, state);
  if (!target) return;

  try {
    const year = date.getUTCFullYear();
    const dayOfYear = String(
      Math.floor((date.getTime() - Date.UTC(year, 0, 1)) / 86400000) + 1
    ).padStart(3, '0');
    const dateStr = date.toISOString().slice(0, 10);

    const localFiles = await glob(
      `tier=*/**/year=${year}/day=${dayOfYear}/*.parquet`,
      {
        cwd: config.outputDirectory,
        absolute: true,
        nodir: true,
      }
    );

    if (localFiles.length === 0) return;

    const uploaded = await uploadMissingFiles(
      localFiles,
      new Set(),
      target,
      config,
      app
    );

    if (uploaded > 0) {
      app.debug(
        `${target.label}: Uploaded ${uploaded} hive files for ${dateStr}`
      );
    }
  } catch (error) {
    app.error(
      `Cloud upload failed for date ${date.toISOString().slice(0, 10)}: ${(error as Error).message}`
    );
    throw error;
  }
}
