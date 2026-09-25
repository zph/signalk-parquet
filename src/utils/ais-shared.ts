import { HivePathBuilder } from './hive-path-builder';

export const AIS_VESSEL_CONTEXT_PREFIX = 'vessels.urn:mrn:imo:mmsi:';
export const SHARED_AIS_CONTEXT = 'ais.shared';

export function isAisVesselContext(context: string): boolean {
  return context.startsWith(AIS_VESSEL_CONTEXT_PREFIX);
}

/** AIS history uses one shared directory; the data-column context identifies each vessel. */
export function readContextPartition(context: string): string {
  return isAisVesselContext(context)
    ? new HivePathBuilder().sanitizeContext(SHARED_AIS_CONTEXT)
    : new HivePathBuilder().sanitizeContext(context);
}
