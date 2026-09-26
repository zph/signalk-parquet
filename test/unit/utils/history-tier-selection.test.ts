import { expect } from 'chai';
import * as fs from 'fs-extra';
import * as os from 'node:os';
import * as path from 'node:path';
import { HistoryAPI } from '../../../src/HistoryAPI';
import { AggregationTier } from '../../../src/utils/hive-path-builder';

describe('history tier selection', () => {
  let root: string;
  let select: (
    milliseconds: number,
    directory: string
  ) => AggregationTier | undefined;

  beforeEach(async () => {
    root = await fs.mkdtemp(path.join(os.tmpdir(), 'parquet-history-tier-'));
    const api = new HistoryAPI('test', root);
    select = (
      api as unknown as {
        selectOptimalTier: typeof select;
      }
    ).selectOptimalTier.bind(api);
  });

  afterEach(async () => {
    await fs.remove(root);
  });

  it('still selects an older 1-hour tier for hour-resolution queries', async () => {
    await fs.ensureDir(path.join(root, 'tier=1h'));
    expect(select(3600000, root)).to.equal('1h');
  });

  it('uses an older 5-second tier when no new tier exists', async () => {
    await fs.ensureDir(path.join(root, 'tier=5s'));
    expect(select(60000, root)).to.equal('5s');
  });

  it('prefers a new 10-second tier over the older 5-second tier', async () => {
    await fs.ensureDir(path.join(root, 'tier=5s'));
    await fs.ensureDir(path.join(root, 'tier=10s'));
    expect(select(10000, root)).to.equal('10s');
  });
});
