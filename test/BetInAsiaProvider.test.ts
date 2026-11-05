/**
 * BetInAsiaProvider unit tests (MollyBet API mocked -- no network).
 *
 * The stream frames below follow the official MollyBet message format
 * (`{ts, data: [[type, payload], ...]}` with `event` / `offer` / `event_score`
 * payloads). Covered: login + tokenised stream URL, live-event registration,
 * odds mapping into the LiveFeedIngestor format (including a round-trip through
 * its `normalizeMatch`), suspension on price removal, filtering, de-duplication,
 * pre-attach buffering, and close / error behaviour.
 */

import { describe, expect, it, vi } from 'vitest';
import {
  BetInAsiaProvider,
  bestPrice,
  parseFrame,
  type WebSocketLike,
} from '../src/feed/providers/BetInAsiaProvider.js';
import { normalizeMatch } from '../src/feed/LiveFeedIngestor.js';
import type { MollyBetClient } from '../src/services/MollyBetClient.js';

const silent = { info: () => {}, warn: () => {}, error: () => {} };

/** Controllable fake socket + the factory that hands it to the provider. */
function makeSocket(opts: { failToOpen?: boolean; neverOpen?: boolean } = {}) {
  const sent: string[] = [];
  const urls: string[] = [];
  const ws: WebSocketLike = {
    send: (d) => void sent.push(d),
    close: vi.fn(),
    onopen: null,
    onmessage: null,
    onclose: null,
    onerror: null,
  };
  const factory = (url: string): WebSocketLike => {
    urls.push(url);
    if (!opts.neverOpen) {
      setTimeout(() => (opts.failToOpen ? ws.onerror?.({}) : ws.onopen?.({})), 0);
    }
    return ws;
  };
  const frame = (...items: [string, unknown][]) =>
    ws.onmessage?.({ data: JSON.stringify({ ts: 1, data: items }) });
  return { ws, sent, urls, factory, frame };
}

function makeClient() {
  const client = {
    login: vi.fn(async () => 'tok-123'),
    keepAlive: vi.fn(async () => {}),
  } as unknown as MollyBetClient;
  return client;
}

const EVENT = (over: Record<string, unknown> = {}): [string, unknown] => [
  'event',
  {
    event_type: 'normal',
    sport: 'tennis',
    event_id: '2026-11-05,64630,64456',
    event_name: 'Carlos Alcaraz vs. Jannik Sinner',
    home: 'Carlos Alcaraz',
    away: 'Jannik Sinner',
    competition_id: '1',
    competition_name: 'ATP Finals',
    competition_country: 'IT',
    ir_status: 'in_running',
    start_time: '2026-11-05T16:00:00+00:00',
    ...over,
  },
];

const OFFER = (
  betType: string,
  priceList: unknown,
  over: Record<string, unknown> = {},
): [string, unknown] => [
  'offer',
  {
    bookie: 'ps3838',
    market: 'm1',
    selection: betType === 'for,h' ? 's1' : 's2',
    sport: 'tennis',
    event_id: '2026-11-05,64630,64456',
    bet_type: betType,
    market_type: 'ml',
    in_running: true,
    price_list: priceList,
    ...over,
  },
];

const quote = (bookie: string, price: number) => ({ [bookie]: { price, min: ['EUR', 10], max: ['EUR', 5000] } });

async function setup(opts: { includePrematch?: boolean } = {}) {
  const sock = makeSocket();
  const client = makeClient();
  const provider = new BetInAsiaProvider({
    client,
    wsUrl: 'wss://api.mollybet.com/v1/stream/',
    wsFactory: sock.factory,
    includePrematch: opts.includePrematch,
    logger: silent,
  });
  const conn = await provider.connect();
  const msgs: any[] = [];
  conn.onMessage((m) => msgs.push(m));
  return { sock, client, provider, conn, msgs };
}

describe('pure helpers', () => {
  it('bestPrice picks the highest valid price across bookies', () => {
    expect(bestPrice([quote('ps3838', 1.9), quote('matchbook', 1.95), quote('orbitx', 1.88)])).toBe(1.95);
  });

  it('bestPrice ignores junk and returns undefined for empty lists', () => {
    expect(bestPrice([])).toBeUndefined();
    expect(bestPrice([{ ps3838: { price: 'x' } }, { b: { price: 0.9 } }, 5])).toBeUndefined();
    expect(bestPrice(undefined)).toBeUndefined();
  });

  it('parseFrame unpacks [type, payload] pairs from strings or objects and drops junk', () => {
    expect(parseFrame('{"ts":1,"data":[["sync",{"token":"t"}],["offer",{}]]}')).toEqual([
      ['sync', { token: 't' }],
      ['offer', {}],
    ]);
    expect(parseFrame({ ts: 1, data: [[1, 2], 'x', ['info', { queue_size: 1 }]] })).toEqual([
      ['info', { queue_size: 1 }],
    ]);
    expect(parseFrame('not json')).toEqual([]);
    expect(parseFrame({ ts: 1 })).toEqual([]);
  });
});

describe('connect', () => {
  it('logs in and opens the stream with the session token', async () => {
    const { sock, client, conn } = await setup();
    expect(client.login).toHaveBeenCalledTimes(1);
    expect(sock.urls).toEqual(['wss://api.mollybet.com/v1/stream/?token=tok-123']);
    conn.close();
  });

  it('rejects when the socket errors before opening', async () => {
    const sock = makeSocket({ failToOpen: true });
    const provider = new BetInAsiaProvider({
      client: makeClient(),
      wsUrl: 'wss://x/v1/stream/',
      wsFactory: sock.factory,
      logger: silent,
    });
    await expect(provider.connect()).rejects.toThrow(/connection failed/);
  });

  it('rejects when the socket never opens within the budget', async () => {
    const sock = makeSocket({ neverOpen: true });
    const provider = new BetInAsiaProvider({
      client: makeClient(),
      wsUrl: 'wss://x/v1/stream/',
      wsFactory: sock.factory,
      connectTimeoutMs: 20,
      logger: silent,
    });
    await expect(provider.connect()).rejects.toThrow(/did not open/);
  });

  it('propagates a login failure (bad credentials) so the ingestor backs off', async () => {
    const client = { login: vi.fn(async () => { throw new Error('login failed: HTTP 401'); }) } as unknown as MollyBetClient;
    const provider = new BetInAsiaProvider({
      client,
      wsUrl: 'wss://x/v1/stream/',
      wsFactory: makeSocket().factory,
      logger: silent,
    });
    await expect(provider.connect()).rejects.toThrow(/401/);
  });
});

describe('event registration', () => {
  it('registers in-running tennis events once', async () => {
    const { sock, conn } = await setup();
    sock.frame(EVENT());
    sock.frame(EVENT()); // update of the same event
    expect(sock.sent).toEqual([JSON.stringify(['register_event', 'tennis', '2026-11-05,64630,64456'])]);
    conn.close();
  });

  it('skips pre-match events unless includePrematch is set', async () => {
    const a = await setup();
    a.sock.frame(EVENT({ ir_status: 'pre_event' }));
    expect(a.sock.sent).toEqual([]);
    a.conn.close();

    const b = await setup({ includePrematch: true });
    b.sock.frame(EVENT({ ir_status: 'pre_event' }));
    expect(b.sock.sent).toHaveLength(1);
    b.conn.close();
  });

  it('ignores other sports and multirunner events', async () => {
    const { sock, conn } = await setup();
    sock.frame(EVENT({ sport: 'fb' }));
    sock.frame(EVENT({ event_type: 'multirunner' }));
    expect(sock.sent).toEqual([]);
    conn.close();
  });
});

describe('odds mapping', () => {
  it('emits the LiveFeedIngestor format with best prices once both sides are priced', async () => {
    const { sock, msgs, conn } = await setup();
    sock.frame(EVENT());
    sock.frame(OFFER('for,h', [quote('ps3838', 1.9), quote('matchbook', 1.95)]));
    // Only one side priced: the market is not tradable yet -> SUSPENDED.
    expect(msgs).toHaveLength(1);
    expect(msgs[0]).toMatchObject({ status: 'SUSPENDED', odds: { a: 1.95, b: undefined } });

    sock.frame(OFFER('for,a', [quote('ps3838', 2.1)]));
    const last = msgs[msgs.length - 1];
    expect(last).toMatchObject({
      matchId: '2026-11-05,64630,64456',
      playerA: 'Carlos Alcaraz',
      playerB: 'Jannik Sinner',
      odds: { a: 1.95, b: 2.1 },
      status: 'LIVE',
      marketStatus: 'LIVE',
    });
    conn.close();
  });

  it('is consumable by LiveFeedIngestor.normalizeMatch', async () => {
    const { sock, msgs, conn } = await setup();
    sock.frame(EVENT());
    sock.frame(OFFER('for,h', [quote('ps3838', 1.9)]));
    sock.frame(OFFER('for,a', [quote('ps3838', 2.1)]));
    const n = normalizeMatch(msgs[msgs.length - 1]);
    expect(n).toMatchObject({
      matchId: '2026-11-05,64630,64456',
      playerAId: 'atp:alcaraz-c',
      playerBId: 'atp:sinner-j',
      oddsA: 1.9,
      oddsB: 2.1,
      status: 'LIVE',
    });
    conn.close();
  });

  it('maps event_score into the score field', async () => {
    const { sock, msgs, conn } = await setup();
    sock.frame(EVENT());
    sock.frame(OFFER('for,h', [quote('ps3838', 1.9)]));
    sock.frame(OFFER('for,a', [quote('ps3838', 2.1)]));
    sock.frame(['event_score', { sport: 'tennis', event_id: '2026-11-05,64630,64456', score: [1, 0] }]);
    expect(msgs[msgs.length - 1].score).toBe('1-0');
    conn.close();
  });

  it('flags SUSPENDED when a side loses all prices', async () => {
    const { sock, msgs, conn } = await setup();
    sock.frame(EVENT());
    sock.frame(OFFER('for,h', [quote('ps3838', 1.9)]));
    sock.frame(OFFER('for,a', [quote('ps3838', 2.1)]));
    sock.frame(OFFER('for,a', [])); // exchange line emptied
    const last = msgs[msgs.length - 1];
    expect(last.status).toBe('SUSPENDED');
    expect(last.odds).toEqual({ a: 1.9, b: undefined });
    conn.close();
  });

  it('does not re-emit an unchanged state', async () => {
    const { sock, msgs, conn } = await setup();
    sock.frame(EVENT());
    sock.frame(OFFER('for,h', [quote('ps3838', 1.9)]));
    sock.frame(OFFER('for,a', [quote('ps3838', 2.1)]));
    const n = msgs.length;
    sock.frame(OFFER('for,a', [quote('ps3838', 2.1)]));
    sock.frame(EVENT());
    expect(msgs).toHaveLength(n);
    conn.close();
  });

  it('ignores offers for other bet types and unknown events', async () => {
    const { sock, msgs, conn } = await setup();
    sock.frame(EVENT());
    sock.frame(OFFER('for,over', [quote('ps3838', 1.9)]));
    sock.frame(OFFER('for,h', [quote('ps3838', 1.9)], { event_id: 'unknown' }));
    expect(msgs).toHaveLength(0);
    conn.close();
  });

  it('exposes the live best price for the executor slippage guard', async () => {
    const { sock, provider, conn } = await setup();
    sock.frame(EVENT());
    sock.frame(OFFER('for,h', [quote('ps3838', 1.9), quote('matchbook', 1.97)]));
    expect(provider.getBestPrice('2026-11-05,64630,64456', 'home')).toBe(1.97);
    expect(provider.getBestPrice('2026-11-05,64630,64456', 'away')).toBeUndefined();
    expect(provider.getBestPrice('nope', 'home')).toBeUndefined();
    sock.frame(['remove_event', { sport: 'tennis', event_id: '2026-11-05,64630,64456' }]);
    expect(provider.getBestPrice('2026-11-05,64630,64456', 'home')).toBeUndefined();
    conn.close();
  });
});

describe('connection lifecycle', () => {
  it('buffers messages that arrive before the ingestor attaches', async () => {
    const sock = makeSocket();
    const provider = new BetInAsiaProvider({
      client: makeClient(),
      wsUrl: 'wss://x/v1/stream/',
      wsFactory: sock.factory,
      logger: silent,
    });
    const conn = await provider.connect();
    sock.frame(EVENT());
    sock.frame(OFFER('for,h', [quote('ps3838', 1.9)]));
    sock.frame(OFFER('for,a', [quote('ps3838', 2.1)]));

    const got: any[] = [];
    conn.onMessage((m) => got.push(m));
    expect(got).toHaveLength(2); // SUSPENDED (one side) then LIVE (both sides)
    expect(got[0].status).toBe('SUSPENDED');
    expect(got[1]).toMatchObject({ status: 'LIVE', odds: { a: 1.9, b: 2.1 } });
    conn.close();
  });

  it('notifies onClose when the server drops the socket, but not after our own close()', async () => {
    const a = await setup();
    const onClose = vi.fn();
    a.conn.onClose(onClose);
    a.sock.ws.onclose?.({});
    expect(onClose).toHaveBeenCalledTimes(1);

    const b = await setup();
    const onCloseB = vi.fn();
    b.conn.onClose(onCloseB);
    b.conn.close();
    b.sock.ws.onclose?.({});
    expect(onCloseB).not.toHaveBeenCalled();
    expect(b.sock.ws.close).toHaveBeenCalled();
  });

  it('survives malformed frames and rejected commands', async () => {
    const { sock, msgs, conn } = await setup();
    sock.ws.onmessage?.({ data: '{broken' });
    sock.frame(['response', { status: 'error', code: 'invalid_event' }]);
    sock.frame(['info', { queue_size: 1 }]);
    expect(msgs).toHaveLength(0);
    conn.close();
  });

  it('forwards stream errors to onError listeners', async () => {
    const { sock, conn } = await setup();
    const onError = vi.fn();
    conn.onError(onError);
    sock.ws.onerror?.({});
    expect(onError).toHaveBeenCalledTimes(1);
    conn.close();
  });
});
