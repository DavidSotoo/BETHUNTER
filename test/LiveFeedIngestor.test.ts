/**
 * LiveFeedIngestor unit tests (network mocked).
 *
 * Validate name canonicalisation, payload normalisation, +EV order publishing,
 * finished-match close publishing, reconnection after a drop (fake timers), and
 * that unusable payloads are ignored.
 */

import { afterEach, describe, expect, it, vi } from 'vitest';
import {
  LiveFeedIngestor,
  canonicalizePlayer,
  normalizeMatch,
  type FeedConnection,
  type IngestorPipeline,
  type IngestorRedis,
} from '../src/feed/LiveFeedIngestor.js';

const silent = { info: () => {}, warn: () => {}, error: () => {} };
const flush = (): Promise<void> => new Promise((r) => setTimeout(r, 0));

/** A controllable in-memory feed connection. */
function makeConn() {
  const cbs = {
    message: [] as ((raw: unknown) => void)[],
    close: [] as (() => void)[],
    error: [] as ((err: Error) => void)[],
  };
  const conn: FeedConnection = {
    onMessage: (cb) => cbs.message.push(cb),
    onClose: (cb) => cbs.close.push(cb),
    onError: (cb) => cbs.error.push(cb),
    close: vi.fn(),
  };
  return {
    conn,
    emitMessage: (raw: unknown) => cbs.message.forEach((f) => f(raw)),
    emitClose: () => cbs.close.forEach((f) => f()),
  };
}

/** A mock Redis recording pipeline operations. */
function makeRedis() {
  const calls = {
    hset: [] as { key: string; data: Record<string, string> }[],
    lpush: [] as { key: string; value: string }[],
  };
  const redis: IngestorRedis = {
    pipeline(): IngestorPipeline {
      const p: IngestorPipeline = {
        hset(key, data) {
          calls.hset.push({ key, data });
          return p;
        },
        lpush(key, value) {
          calls.lpush.push({ key, value });
          return p;
        },
        exec: vi.fn(async () => []),
      };
      return p;
    },
  };
  return { redis, calls };
}

afterEach(() => {
  vi.useRealTimers();
});

describe('canonicalizePlayer', () => {
  it('normalises the common name forms to <tour>:<surname>-<initial>', () => {
    expect(canonicalizePlayer('Carlos Alcaraz')).toBe('atp:alcaraz-c');
    expect(canonicalizePlayer('C. Alcaraz')).toBe('atp:alcaraz-c');
    expect(canonicalizePlayer('Alcaraz C.')).toBe('atp:alcaraz-c');
    expect(canonicalizePlayer('Daniil Medvedev')).toBe('atp:medvedev-d');
  });

  it('strips accents', () => {
    expect(canonicalizePlayer('Novak Đoković'.normalize('NFC'))).toContain('atp:');
    expect(canonicalizePlayer('Stéfanos Tsitsipás')).toBe('atp:tsitsipas-s');
  });
});

describe('normalizeMatch', () => {
  it('parses a match payload and canonicalises players', () => {
    const m = normalizeMatch({
      matchId: 'm1',
      playerA: 'Carlos Alcaraz',
      playerB: 'Daniil Medvedev',
      oddsA: 2.0,
      oddsB: 1.9,
      status: 'inplay',
    });
    expect(m).not.toBeNull();
    expect(m?.playerAId).toBe('atp:alcaraz-c');
    expect(m?.playerBId).toBe('atp:medvedev-d');
    expect(m?.status).toBe('LIVE');
    expect(m?.oddsA).toBe(2.0);
  });

  it('returns null for unusable payloads', () => {
    expect(normalizeMatch(null)).toBeNull();
    expect(normalizeMatch({ playerA: 'A', playerB: 'B' })).toBeNull(); // no matchId
    expect(normalizeMatch({ matchId: 'm', playerA: 'Only One' })).toBeNull(); // missing B
  });
});

describe('LiveFeedIngestor', () => {
  it('writes player hashes and publishes a +EV order', async () => {
    const { redis, calls } = makeRedis();
    const c = makeConn();
    const ing = new LiveFeedIngestor({ redis, connect: () => c.conn, minEv: 0.02, logger: silent });
    await ing.start();

    c.emitMessage({
      matchId: 'm1',
      playerA: 'Carlos Alcaraz',
      playerB: 'Daniil Medvedev',
      oddsA: 2.0, // pA ~0.64 -> EV ~+0.28
      oddsB: 1.9, // pB ~0.36 -> EV ~-0.32
      ratingA: { elo: 2000, fatigue: 5, form: 2 },
      ratingB: { elo: 1900, fatigue: 10, form: -1 },
      status: 'LIVE',
    });
    await flush();

    expect(calls.hset.map((h) => h.key).sort()).toEqual([
      'ta:player:atp:alcaraz-c',
      'ta:player:atp:medvedev-d',
    ]);
    expect(calls.hset[0]?.data.elo_blend).toBeDefined();

    const orders = calls.lpush.filter((l) => l.key === 'ApprovedOrder');
    expect(orders).toHaveLength(1); // only the +EV side (A)
    const order = JSON.parse(orders[0]!.value) as { selection: string; targetOdds: number };
    expect(order.selection).toBe('atp:alcaraz-c');
    expect(order.targetOdds).toBe(2.0);
  });

  it('does not publish an order when no side is +EV', async () => {
    const { redis, calls } = makeRedis();
    const c = makeConn();
    const ing = new LiveFeedIngestor({ redis, connect: () => c.conn, minEv: 0.02, logger: silent });
    await ing.start();

    c.emitMessage({
      matchId: 'm2',
      playerA: 'Carlos Alcaraz',
      playerB: 'Daniil Medvedev',
      oddsA: 1.2,
      oddsB: 1.2, // even elos -> p 0.5 -> EV = -0.4 both
      ratingA: { elo: 1900 },
      ratingB: { elo: 1900 },
      status: 'LIVE',
    });
    await flush();

    expect(calls.lpush.filter((l) => l.key === 'ApprovedOrder')).toHaveLength(0);
    expect(calls.hset).toHaveLength(2); // hashes still updated
  });

  it('publishes a ClosedMatch for a finished match', async () => {
    const { redis, calls } = makeRedis();
    const c = makeConn();
    const ing = new LiveFeedIngestor({ redis, connect: () => c.conn, logger: silent });
    await ing.start();

    c.emitMessage({
      matchId: 'm3',
      playerA: 'Carlos Alcaraz',
      playerB: 'Daniil Medvedev',
      oddsA: 2.1,
      oddsB: 1.8,
      status: 'finished',
      winner: 'Carlos Alcaraz',
    });
    await flush();

    const close = calls.lpush.find((l) => l.key === 'ClosedMatch');
    expect(close).toBeDefined();
    expect(JSON.parse(close!.value)).toMatchObject({
      matchId: 'm3',
      winner: 'atp:alcaraz-c',
      closingOdds: 2.1,
    });
    // a finished match must not also emit an order
    expect(calls.lpush.filter((l) => l.key === 'ApprovedOrder')).toHaveLength(0);
  });

  it('ignores unusable payloads without touching Redis', async () => {
    const { redis, calls } = makeRedis();
    const c = makeConn();
    const ing = new LiveFeedIngestor({ redis, connect: () => c.conn, logger: silent });
    await ing.start();

    c.emitMessage({ garbage: true });
    c.emitMessage(null);
    await flush();

    expect(calls.hset).toHaveLength(0);
    expect(calls.lpush).toHaveLength(0);
  });

  it('reconnects with backoff after the feed drops, and stops cleanly', async () => {
    vi.useFakeTimers();
    const { redis } = makeRedis();
    const conns: ReturnType<typeof makeConn>[] = [];
    const connect = vi.fn(() => {
      const c = makeConn();
      conns.push(c);
      return c.conn;
    });
    const ing = new LiveFeedIngestor({
      redis,
      connect,
      backoff: { baseMs: 100, factor: 2, jitter: false },
      logger: silent,
    });

    await ing.start();
    expect(connect).toHaveBeenCalledTimes(1);

    conns[0]!.emitClose(); // feed dropped
    await vi.advanceTimersByTimeAsync(100); // first backoff
    expect(connect).toHaveBeenCalledTimes(2);

    ing.stop(); // no more reconnects after stop
    conns[1]!.emitClose();
    await vi.advanceTimersByTimeAsync(5000);
    expect(connect).toHaveBeenCalledTimes(2);
  });
});
