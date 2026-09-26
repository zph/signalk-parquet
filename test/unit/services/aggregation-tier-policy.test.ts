import { expect } from 'chai';
import { buildPerTierRetention } from '../../../src/services/aggregation-service';

describe('tiered archive retention', () => {
  it('keeps the final 60-second tier indefinitely when its duration is zero', () => {
    expect(
      buildPerTierRetention(7, {
        exact: 30,
        tenSecond: 90,
        sixtySecond: 0,
      })
    ).to.deep.equal({
      raw: 30,
      '5s': 120,
      '10s': 120,
      '60s': 0,
      '1h': 0,
    });
  });

  it('uses cumulative expiry ages for finite tiers', () => {
    expect(
      buildPerTierRetention(7, {
        exact: 30,
        tenSecond: 90,
        sixtySecond: 270,
      })
    ).to.deep.equal({
      raw: 30,
      '5s': 120,
      '10s': 120,
      '60s': 390,
      '1h': 390,
    });
  });
});
