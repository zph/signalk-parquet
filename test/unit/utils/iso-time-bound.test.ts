import { expect } from 'chai';
import { isoTimeBound } from '../../../src/utils/iso-time-bound';

describe('ISO timestamp query bounds', () => {
  it('pads whole-second and short fractional UTC instants', () => {
    expect(isoTimeBound('2024-06-01T11:00:00Z')).to.equal(
      '2024-06-01T11:00:00.000Z'
    );
    expect(isoTimeBound('2024-06-01T11:00:00.1Z')).to.equal(
      '2024-06-01T11:00:00.100Z'
    );
  });

  it('preserves millisecond and finer fractions', () => {
    expect(isoTimeBound('2024-06-01T11:00:00.123Z')).to.equal(
      '2024-06-01T11:00:00.123Z'
    );
    expect(isoTimeBound('2024-06-01T11:00:00.123456Z')).to.equal(
      '2024-06-01T11:00:00.123456Z'
    );
  });
});
