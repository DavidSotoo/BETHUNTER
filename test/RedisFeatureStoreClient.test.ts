/**
 * RedisFeatureStoreClient unit tests (ioredis mocked).
 *
 * Validate the mapping from the analytics key scheme to PlayerFeatures and the
 * degradation contract: a missing key, an unusable payload, or a Redis error
 * must all return `null` (never throw) so the FeatureLoader falls back.
 */

import { describe, expect, it, vi } from 'vitest';
import {
  RedisFeatureStoreClient,
  RedisLike,
} from '../src/services/RedisFeatureStoreClient.js';

const silent = { warn: () => {} };

function client(hgetall: RedisLike['hgetall']) {
  const redis: RedisLike = { hgetall };
  return { redis, client: new RedisFeatureStoreClient({ redis, logger: silent }) };
}

const live = (): AbortSignal => new AbortController().signal;

describe('RedisFeatureStoreClient', () => {
  it('maps an analytics hash onto PlayerFeatures', async () => {
    const hgetall = vi.fn(async () => ({
      elo_blend: '1987.5',
      fatigue: '12',
      form_rating: '4.2',
      updated_at: '1730000000000',
    }));
    const { client: c } = client(hgetall);

    const result = await c.fetch('atp:alcaraz-c', live());
    expect(hgetall).toHaveBeenCalledWith('ta:player:atp:alcaraz-c'); // key scheme
    expect(result).toEqual({
      id: 'atp:alcaraz-c',
      elo: 1987.5,
      fatigueScore: 12,
      formRating: 4.2,
      updatedAt: 1730000000000,
    });
  });

  it('returns null when the player key does not exist (empty hash)', async () => {
    const { client: c } = client(vi.fn(async () => ({})));
    expect(await c.fetch('ghost', live())).toBeNull();
  });

  it('returns null (does not throw) on a Redis/network error', async () => {
    const { client: c } = client(
      vi.fn(async () => {
        throw new Error('ECONNREFUSED');
      }),
    );
    await expect(c.fetch('atp:sinner-j', live())).resolves.toBeNull();
  });

  it('returns null when the rating field is missing or unparseable', async () => {
    const { client: c } = client(vi.fn(async () => ({ fatigue: '5', form_rating: '1' })));
    expect(await c.fetch('no-elo', live())).toBeNull();

    const { client: c2 } = client(vi.fn(async () => ({ elo_blend: 'suspended' })));
    expect(await c2.fetch('bad-elo', live())).toBeNull();
  });

  it('defaults fatigue/form/updatedAt to 0 when absent, and honours field aliases', async () => {
    // Uses the alias names (`elo`, `form`) rather than the primaries.
    const { client: c } = client(vi.fn(async () => ({ elo: '1600' })));
    const result = await c.fetch('minimal', live());
    expect(result).toEqual({
      id: 'minimal',
      elo: 1600,
      fatigueScore: 0,
      formRating: 0,
      updatedAt: 0,
    });
  });

  it('respects an already-aborted signal without hitting Redis', async () => {
    const hgetall = vi.fn(async () => ({ elo_blend: '1900' }));
    const { client: c } = client(hgetall);
    const ac = new AbortController();
    ac.abort();

    expect(await c.fetch('aborted', ac.signal)).toBeNull();
    expect(hgetall).not.toHaveBeenCalled();
  });

  it('honours a custom key prefix', async () => {
    const hgetall = vi.fn(async () => ({ elo_blend: '2000' }));
    const redis: RedisLike = { hgetall };
    const c = new RedisFeatureStoreClient({ redis, keyPrefix: 'feat:', logger: silent });
    await c.fetch('x', live());
    expect(hgetall).toHaveBeenCalledWith('feat:x');
  });
});
