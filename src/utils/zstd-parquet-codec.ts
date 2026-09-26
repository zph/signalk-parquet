import { constants, zstdCompressSync, zstdDecompressSync } from 'node:zlib';
import { PARQUET_COMPRESSION_METHODS } from '@dsnp/parquetjs/dist/lib/compression';

/**
 * parquetjs knows the Parquet ZSTD codec ID but does not provide an encoder.
 * Install one backed by Node's native ZSTD implementation before constructing
 * schemas or opening files. This also lets the existing parquetjs read paths
 * open ZSTD files without a second Parquet implementation.
 */
const codecs = PARQUET_COMPRESSION_METHODS as Record<
  string,
  {
    deflate: (value: Buffer) => Buffer;
    inflate: (value: Buffer) => Buffer;
  }
>;

if (
  typeof zstdCompressSync !== 'function' ||
  typeof zstdDecompressSync !== 'function' ||
  typeof constants.ZSTD_c_compressionLevel !== 'number'
) {
  throw new Error(
    'ZSTD Parquet requires a Node.js runtime with native ZSTD support'
  );
}

codecs.ZSTD = {
  deflate: value =>
    zstdCompressSync(value, {
      params: { [constants.ZSTD_c_compressionLevel]: 9 },
    }),
  inflate: value => zstdDecompressSync(value),
};
