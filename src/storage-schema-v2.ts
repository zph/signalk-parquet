import { DataRecord } from './types';

/** Durable raw Parquet time columns for schema v2. */
export interface StorageTimeV2 {
  event_time: Date;
  received_delay_us: number;
}

/**
 * Convert the ingestion record's ISO timestamps to the compact durable form.
 *
 * SQLite deliberately keeps the original strings: it is a short-lived write
 * buffer. Parquet is the durable boundary and stores one native event time plus
 * the signed receive delay. The original receive time is exactly recoverable
 * to the millisecond precision supplied by Signal K.
 */
export function storageTimeV2(record: DataRecord): StorageTimeV2 {
  const eventTime = new Date(record.signalk_timestamp);
  const receivedTime = new Date(record.received_timestamp);
  if (!Number.isFinite(eventTime.getTime())) {
    throw new Error(`Invalid Signal K timestamp: ${record.signalk_timestamp}`);
  }
  if (!Number.isFinite(receivedTime.getTime())) {
    throw new Error(`Invalid received timestamp: ${record.received_timestamp}`);
  }

  return {
    event_time: eventTime,
    received_delay_us: (receivedTime.getTime() - eventTime.getTime()) * 1000,
  };
}

/** Convert an ingestion record to the schema-v2 durable row shape. */
export function toStorageRecordV2(
  record: DataRecord
): Omit<DataRecord, 'received_timestamp' | 'signalk_timestamp'> &
  StorageTimeV2 {
  const payload = { ...record } as Partial<DataRecord>;
  delete payload.received_timestamp;
  delete payload.signalk_timestamp;
  return {
    ...payload,
    ...storageTimeV2(record),
  } as Omit<DataRecord, 'received_timestamp' | 'signalk_timestamp'> &
    StorageTimeV2;
}

/** DuckDB expression that reconstructs receipt time without storing it twice. */
export const RECEIVED_TIME_V2_SQL =
  "event_time + received_delay_us * INTERVAL '1 microsecond'";
