/**
 * FeatureLoader unit tests.
 *
 * Covered behaviours:
 *   - cache hit: second read serves from cache, store is not touched again
 *   - timeout -> fallback: a store slower than the deadline degrades, never throws
 *   - store error -> fallback: a rejecting store degrades the same way
 *   - coalescing: concurrent misses for one player share a single fetch
 *   - hydrate: stamps both vectors, sources and metadata
 *
 * Fake timers drive the 50ms deadline so the timeout path is tested with no
 * real wall-clock wait.
 */

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { FeatureLoader, FeatureStoreClient } from '../src/services/FeatureLoader.js';
import { ApprovedOrder, PlayerFeatures } from '../src/types/features.js';

function feat(id: string, overrides: Partial<PlayerFeatures> = {}): PlayerFeatures {
  return { id, elo: 2000, fatigueScore: 10, formRating: 5, updatedAt: 1_700_000_000_000, ...overrides };
}

function order(a = 'player_a', b = 'player_b'): ApprovedOrder {
  return { orderId: 'o-1', playerAId: a, playerBId: b };
}

/** A promise with externally callable resolve/reject. */
function deferred<T>() {
  let resolve!: (v: T) => void;
  let reject!: (e: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

describe('FeatureLoader', () => {
  it('serves a repeated read from cache without re-fetching', async () => {
    const fetch = vi.fn(async (id: string) => feat(id));
    const loader = new FeatureLoader({ client: { fetch } });

    const first = await loader.hydrate(order());
    expect(first.hydrationSource).toEqual({ playerA: 'live', playerB: 'live' });
    expect(fetch).toHaveBeenCalledTimes(2); // one per player, cold cache

    const second = await loader.hydrate(order());
    expect(second.hydrationSource).toEqual({ playerA: 'cache', playerB: 'cache' });
    expect(fetch).toHaveBeenCalledTimes(2); // unchanged: both hits served from cache
    expect(second.features.playerA).toEqual(feat('player_a'));
  });

  it('degrades to a neutral fallback vector when the store exceeds the deadline', async () => {
    vi.useFakeTimers();
    try {
      // Store that never resolves -> only the 50ms deadline can settle the race.
      const fetch = vi.fn(() => new Promise<PlayerFeatures>(() => {}));
      const logger = { warn: vi.fn(), error: vi.fn() };
      const loader = new FeatureLoader({ client: { fetch }, fetchTimeoutMs: 50, logger });

      const promise = loader.hydrate(order());
      await vi.advanceTimersByTimeAsync(50);
      const result = await promise;

      expect(result.hydrationSource).toEqual({ playerA: 'fallback', playerB: 'fallback' });
      // Neutral vector: unrated Elo, no fatigue/form, synthetic updatedAt=0.
      expect(result.features.playerA).toEqual({
        id: 'player_a',
        elo: 1500,
        fatigueScore: 0,
        formRating: 0,
        updatedAt: 0,
      });
      expect(logger.warn).toHaveBeenCalled();
    } finally {
      vi.useRealTimers();
    }
  });

  it('aborts the store fetch on timeout', async () => {
    vi.useFakeTimers();
    try {
      const seen: AbortSignal[] = [];
      const fetch = vi.fn((_id: string, signal: AbortSignal) => {
        seen.push(signal);
        return new Promise<PlayerFeatures>(() => {});
      });
      const loader = new FeatureLoader({ client: { fetch }, fetchTimeoutMs: 50 });

      const promise = loader.hydrate(order('solo', 'solo'));
      await vi.advanceTimersByTimeAsync(50);
      await promise;

      expect(seen.every((s) => s.aborted)).toBe(true);
    } finally {
      vi.useRealTimers();
    }
  });

  it('degrades to fallback when the store rejects', async () => {
    const fetch = vi.fn(async () => {
      throw new Error('store down');
    });
    const loader = new FeatureLoader({ client: { fetch }, logger: { warn: vi.fn(), error: vi.fn() } });

    const result = await loader.hydrate(order());
    expect(result.hydrationSource.playerA).toBe('fallback');
    expect(result.features.playerA.elo).toBe(1500);
  });

  it('coalesces concurrent misses for the same player into one fetch', async () => {
    const d = deferred<PlayerFeatures>();
    const fetch = vi.fn(() => d.promise);
    const loader = new FeatureLoader({ client: { fetch } });

    // Two orders referencing the same two (identical) players, resolved in
    // parallel on a cold cache. Without coalescing this is up to 4 fetches.
    const p1 = loader.hydrate(order('same', 'same'));
    const p2 = loader.hydrate(order('same', 'same'));

    d.resolve(feat('same'));
    await Promise.all([p1, p2]);

    expect(fetch).toHaveBeenCalledTimes(1);
  });

  it('stamps hydration metadata onto the payload', async () => {
    const fetch = vi.fn(async (id: string) => feat(id));
    const loader = new FeatureLoader({ client: { fetch } });

    const result = await loader.hydrate(order());
    expect(result.orderId).toBe('o-1');
    expect(result.hydratedAt).toBeGreaterThan(0);
    expect(result.hydrationLatencyMs).toBeGreaterThanOrEqual(0);
    expect(result.features.playerA.id).toBe('player_a');
    expect(result.features.playerB.id).toBe('player_b');
  });

  it('invalidate() forces the next read back to the store', async () => {
    const fetch = vi.fn(async (id: string) => feat(id));
    const loader = new FeatureLoader({ client: { fetch } });

    await loader.hydrate(order());
    loader.invalidate('player_a');

    const result = await loader.hydrate(order());
    expect(result.hydrationSource.playerA).toBe('live'); // re-fetched
    expect(result.hydrationSource.playerB).toBe('cache'); // untouched
  });
});
