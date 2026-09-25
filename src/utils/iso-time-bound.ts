/**
 * Stored Signal K timestamps generally include milliseconds. js-joda omits
 * a zero fractional part, making ...00Z sort after ...00.000Z as text.
 * Padding query bounds preserves inclusive-start/exclusive-end semantics.
 */
export function isoTimeBound(instant: string): string {
  return instant.replace(
    /(?:\.(\d+))?Z$/,
    (_match, fraction?: string) => `.${(fraction || '').padEnd(3, '0')}Z`
  );
}
