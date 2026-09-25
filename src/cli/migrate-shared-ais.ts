import { migrateSharedAis } from '../utils/migrate-shared-ais';

async function main(): Promise<void> {
  const [directory, flag] = process.argv.slice(2);
  if (!directory || (flag !== undefined && flag !== '--apply')) {
    throw new Error('Usage: migrate-shared-ais <archive-directory> [--apply]');
  }
  console.log(
    JSON.stringify(await migrateSharedAis(directory, flag === '--apply'))
  );
}

void main().catch(error => {
  console.error(error);
  process.exitCode = 1;
});
