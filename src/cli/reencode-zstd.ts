import { reencodeParquetDirectory } from '../utils/reencode-zstd';

async function main(): Promise<void> {
  const [directory, flag] = process.argv.slice(2);
  if (!directory || (flag !== undefined && flag !== '--apply')) {
    throw new Error('Usage: reencode-zstd <archive-directory> [--apply]');
  }
  const result = await reencodeParquetDirectory(directory, flag === '--apply');
  console.log(JSON.stringify(result));
}

void main().catch(error => {
  console.error(error);
  process.exitCode = 1;
});
