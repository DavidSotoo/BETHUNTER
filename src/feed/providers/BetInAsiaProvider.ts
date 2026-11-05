/**
 * BetInAsiaProvider -- live tennis odds from the official MollyBet API
 * (BetInAsia BLACK) over its authorised websocket stream.
 *
 * It plugs into the LiveFeedIngestor as a `FeedConnector`:
 *
 *   const provider = new BetInAsiaProvider({ client, wsUrl, sport: 'tennis' });
 *   new LiveFeedIngestor({ redis, connect: provider.connector });
 *
 * STREAM PROTOCOL (https://api.mollybet.com/docs)
 *
 *   1. POST /v1/sessions/ (via MollyBetClient) -> session token.
 *   2. Open  wss://.../v1/stream/?token=<token>.
 *   3. Every server frame is `{ts, data: [[type, payload], ...]}`.
 *      - `event`            event metadata (home / away / ir_status)
 *      - `offer`            prices per (bookie, market, selection); the
 *                           `price_list` holds one `{bookie: {price,min,max}}`
 *                           entry per price -- the consolidated book
 *                           (Pinnacle/PS3838, Matchbook, ...)
 *      - `event_score`      [home, away]
 *      - `remove_event`     event no longer offered
 *      - `sync` / `response` / `info` / `connection_closed`  connection control
 *   4. Offers only flow for events the socket has registered, so the provider
 *      sends `["register_event", sport, event_id]` for each live tennis event.
 *
 * OUTPUT (what the LiveFeedIngestor's normalizeMatch consumes)
 *
 *   { matchId, playerA, playerB, odds: {a, b}, status, marketStatus, score? }
 *
 *   matchId  = the MollyBet event_id  (the executor reads it back to open a betslip)
 *   playerA  = home, playerB = away   (the executor relies on this ordering)
 *   odds     = best decimal back price across the book for home / away
 *   status / marketStatus = LIVE | SUSPENDED | PREMATCH
 *
 * No scraping, no browser emulation: login, stream and orders all go through
 * the documented endpoints. Credentials and the token are never logged.
 */

import type { FeedConnection, FeedConnector } from '../LiveFeedIngestor.js';
import type { MollyBetClient } from '../../services/MollyBetClient.js';

/** The subset of the WHATWG WebSocket the provider uses (also what tests mock). */
export interface WebSocketLike {
  send(data: string): void;
  close(): void;
  onopen: ((ev: unknown) => void) | null;
  onmessage: ((ev: { data: unknown }) => void) | null;
  onclose: ((ev: unknown) => void) | null;
  onerror: ((ev: unknown) => void) | null;
}
export type WebSocketFactory = (url: string) => WebSocketLike;

/** Back-home / back-away bet types for the match-winner market. */
export interface BackBetTypes {
  home: string;
  away: string;
}

export interface BetInAsiaProviderOptions {
  client: MollyBetClient;
  /** BIA_WS_URL, without the token. */
  wsUrl: string;
  /** Sport code in MollyBet event messages. Default "tennis". */
  sport?: string;
  /**
   * `bet_type` codes for backing the home / away side of the match-winner
   * market. Default `for,h` / `for,a` (the documented back-home form).
   */
  backBetTypes?: BackBetTypes;
  /** Also register pre-match events. Default false (live engine). */
  includePrematch?: boolean;
  /** Keep-alive HTTP call cadence; sessions idle out after 24h. Default 6h. */
  keepAliveMs?: number;
  /** Budget to reach an open socket. Default 10000. */
  connectTimeoutMs?: number;
  /** Injectable for tests. Default: global WebSocket (Node >= 22). */
  wsFactory?: WebSocketFactory;
  logger?: Pick<Console, 'info' | 'warn' | 'error'>;
}

/** The message handed to LiveFeedIngestor. */
export interface BetInAsiaFeedMessage {
  matchId: string;
  playerA: string;
  playerB: string;
  odds: { a?: number; b?: number };
  status: 'LIVE' | 'SUSPENDED' | 'PREMATCH';
  marketStatus: 'LIVE' | 'SUSPENDED' | 'PREMATCH';
  score?: string;
  sport: string;
}

// --- pure helpers (exported for direct testing) ---

const isRecord = (v: unknown): v is Record<string, unknown> =>
  typeof v === 'object' && v !== null && !Array.isArray(v);
const str = (v: unknown): string | undefined =>
  typeof v === 'string' && v.trim() !== '' ? v : undefined;

/**
 * Best (highest) decimal price in an offer's `price_list`. Each entry is an
 * object keyed by bookie name whose value carries `price`; entries without a
 * finite price > 1 are ignored. Empty list (exchange, no liquidity) -> undefined.
 */
export function bestPrice(priceList: unknown): number | undefined {
  if (!Array.isArray(priceList)) return undefined;
  let best: number | undefined;
  for (const entry of priceList) {
    if (!isRecord(entry)) continue;
    for (const quote of Object.values(entry)) {
      if (!isRecord(quote)) continue;
      const p = quote.price;
      if (typeof p === 'number' && Number.isFinite(p) && p > 1 && (best === undefined || p > best)) {
        best = p;
      }
    }
  }
  return best;
}

/** Parse a stream frame into its `[type, payload]` items. Malformed frames -> []. */
export function parseFrame(raw: unknown): [string, unknown][] {
  let obj: unknown = raw;
  if (typeof raw === 'string') {
    try {
      obj = JSON.parse(raw);
    } catch {
      return [];
    }
  }
  if (!isRecord(obj) || !Array.isArray(obj.data)) return [];
  const items: [string, unknown][] = [];
  for (const pair of obj.data) {
    if (Array.isArray(pair) && typeof pair[0] === 'string') items.push([pair[0], pair[1]]);
  }
  return items;
}

interface OfferState {
  betType: string;
  price: number;
}

interface EventState {
  eventId: string;
  home: string;
  away: string;
  irStatus: 'pre_event' | 'in_running' | 'unknown';
  score?: string;
  registered: boolean;
  /** key: `${bookie}|${market}|${selection}` */
  offers: Map<string, OfferState>;
  lastEmitted?: string;
}

export class BetInAsiaProvider {
  /** Hand this to `new LiveFeedIngestor({ connect: provider.connector })`. */
  readonly connector: FeedConnector = () => this.connect();

  private readonly client: MollyBetClient;
  private readonly wsUrl: string;
  private readonly sport: string;
  private readonly bet: BackBetTypes;
  private readonly includePrematch: boolean;
  private readonly keepAliveMs: number;
  private readonly connectTimeoutMs: number;
  private readonly wsFactory: WebSocketFactory;
  private readonly logger: Pick<Console, 'info' | 'warn' | 'error'>;

  /** Live state of the current connection; replaced on every reconnect. */
  private events = new Map<string, EventState>();

  constructor(opts: BetInAsiaProviderOptions) {
    this.client = opts.client;
    this.wsUrl = opts.wsUrl;
    this.sport = opts.sport ?? 'tennis';
    this.bet = opts.backBetTypes ?? { home: 'for,h', away: 'for,a' };
    this.includePrematch = opts.includePrematch ?? false;
    this.keepAliveMs = opts.keepAliveMs ?? 6 * 60 * 60 * 1000;
    this.connectTimeoutMs = opts.connectTimeoutMs ?? 10_000;
    this.wsFactory =
      opts.wsFactory ??
      ((url) => {
        const Ctor = (globalThis as { WebSocket?: new (u: string) => WebSocketLike }).WebSocket;
        if (!Ctor) {
          throw new Error('global WebSocket unavailable (needs Node >= 22): pass wsFactory');
        }
        return new Ctor(url);
      });
    this.logger = opts.logger ?? console;
  }

  /**
   * Best current back price for one side of an event, from the live stream.
   * Used by the executor's slippage guard; undefined when unknown/suspended.
   */
  getBestPrice(eventId: string, side: 'home' | 'away'): number | undefined {
    const ev = this.events.get(eventId);
    return ev ? this.sidePrice(ev, side) : undefined;
  }

  /** Authenticate, open the stream and resolve once the socket is open. */
  async connect(): Promise<FeedConnection> {
    // A fresh login per (re)connect: an expired session would otherwise loop on 401.
    const token = await this.client.login();
    const url = new URL(this.wsUrl);
    url.searchParams.set('token', token);

    this.events = new Map();
    const ws = this.wsFactory(url.toString());

    const listeners = {
      message: [] as ((raw: unknown) => void)[],
      close: [] as (() => void)[],
      error: [] as ((err: Error) => void)[],
    };
    const pending: unknown[] = [];
    let closedByUs = false;
    let keepAlive: ReturnType<typeof setInterval> | undefined;

    const emit = (msg: BetInAsiaFeedMessage): void => {
      if (listeners.message.length === 0) pending.push(msg);
      else listeners.message.forEach((f) => f(msg));
    };
    const cleanup = (): void => {
      if (keepAlive) clearInterval(keepAlive);
      keepAlive = undefined;
    };

    await new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => {
        closedByUs = true;
        try {
          ws.close();
        } catch {
          /* already closed */
        }
        reject(new Error(`stream did not open within ${this.connectTimeoutMs}ms`));
      }, this.connectTimeoutMs);

      ws.onopen = () => {
        clearTimeout(timer);
        resolve();
      };
      ws.onerror = () => {
        clearTimeout(timer);
        reject(new Error('stream connection failed (check BIA_WS_URL / session)'));
      };
      ws.onclose = () => {
        clearTimeout(timer);
        reject(new Error('stream closed before opening'));
      };
    });

    ws.onmessage = (ev) => {
      for (const [type, payload] of parseFrame(ev.data)) {
        this.handleItem(type, payload, ws, emit);
      }
    };
    ws.onerror = () => {
      listeners.error.forEach((f) => f(new Error('stream error')));
    };
    ws.onclose = () => {
      cleanup();
      if (!closedByUs) listeners.close.forEach((f) => f());
    };

    keepAlive = setInterval(() => {
      this.client
        .keepAlive()
        .catch((err) => this.logger.warn(`BetInAsiaProvider: keep-alive failed: ${(err as Error).message}`));
    }, this.keepAliveMs);
    keepAlive.unref?.();

    this.logger.info('BetInAsiaProvider: stream open');

    return {
      onMessage: (cb) => {
        listeners.message.push(cb);
        // Flush anything that arrived before the ingestor attached.
        while (pending.length > 0) cb(pending.shift());
      },
      onClose: (cb) => void listeners.close.push(cb),
      onError: (cb) => void listeners.error.push(cb),
      close: () => {
        closedByUs = true;
        cleanup();
        try {
          ws.close();
        } catch {
          /* already closed */
        }
      },
    };
  }

  // --- message handling ---

  private handleItem(
    type: string,
    payload: unknown,
    ws: WebSocketLike,
    emit: (m: BetInAsiaFeedMessage) => void,
  ): void {
    if (!isRecord(payload) && type !== 'clear_events') return;
    const p = (isRecord(payload) ? payload : {}) as Record<string, unknown>;

    switch (type) {
      case 'sync':
        this.logger.info(`BetInAsiaProvider: initial state synced (${this.events.size} tennis events)`);
        break;
      case 'response':
        if (p.status === 'error') {
          this.logger.warn(`BetInAsiaProvider: command rejected: ${String(p.code ?? 'unknown')}`);
        }
        break;
      case 'connection_closed':
        this.logger.warn('BetInAsiaProvider: server is closing the connection');
        break;
      case 'clear_events':
        this.events.clear();
        break;
      case 'event':
        this.onEvent(p, ws, emit);
        break;
      case 'remove_event': {
        const id = str(p.event_id);
        if (id && p.sport === this.sport) this.events.delete(id);
        break;
      }
      case 'event_score':
        this.onScore(p, emit);
        break;
      case 'offer':
        this.onOffer(p, emit);
        break;
      default:
        break; // info, event_time, ir_info, ... not needed for odds ingestion
    }
  }

  private onEvent(p: Record<string, unknown>, ws: WebSocketLike, emit: (m: BetInAsiaFeedMessage) => void): void {
    if (p.sport !== this.sport || p.event_type === 'multirunner') return;
    const eventId = str(p.event_id);
    const home = str(p.home);
    const away = str(p.away);
    if (!eventId || !home || !away) return;

    const prev = this.events.get(eventId);
    const irStatus = p.ir_status === 'in_running' ? 'in_running' : p.ir_status === 'pre_event' ? 'pre_event' : 'unknown';
    const ev: EventState = prev ?? {
      eventId,
      home,
      away,
      irStatus,
      registered: false,
      offers: new Map(),
    };
    ev.home = home;
    ev.away = away;
    ev.irStatus = irStatus;
    this.events.set(eventId, ev);

    if (!ev.registered && (irStatus === 'in_running' || this.includePrematch)) {
      ev.registered = true;
      ws.send(JSON.stringify(['register_event', this.sport, eventId]));
    }
    this.maybeEmit(ev, emit);
  }

  private onScore(p: Record<string, unknown>, emit: (m: BetInAsiaFeedMessage) => void): void {
    const ev = this.lookup(p);
    if (!ev) return;
    const s = p.score;
    ev.score = Array.isArray(s) && s.length === 2 ? `${String(s[0])}-${String(s[1])}` : undefined;
    this.maybeEmit(ev, emit);
  }

  private onOffer(p: Record<string, unknown>, emit: (m: BetInAsiaFeedMessage) => void): void {
    const ev = this.lookup(p);
    const betType = str(p.bet_type);
    if (!ev || !betType) return;
    if (betType !== this.bet.home && betType !== this.bet.away) return;

    const key = `${String(p.bookie)}|${String(p.market)}|${String(p.selection)}`;
    const price = bestPrice(p.price_list);
    if (price === undefined) ev.offers.delete(key);
    else ev.offers.set(key, { betType, price });
    this.maybeEmit(ev, emit);
  }

  private lookup(p: Record<string, unknown>): EventState | undefined {
    const id = str(p.event_id);
    return id && p.sport === this.sport ? this.events.get(id) : undefined;
  }

  private sidePrice(ev: EventState, side: 'home' | 'away'): number | undefined {
    const wanted = side === 'home' ? this.bet.home : this.bet.away;
    let best: number | undefined;
    for (const o of ev.offers.values()) {
      if (o.betType === wanted && (best === undefined || o.price > best)) best = o.price;
    }
    return best;
  }

  /** Emit a normalised message when something the ingestor cares about changed. */
  private maybeEmit(ev: EventState, emit: (m: BetInAsiaFeedMessage) => void): void {
    const a = this.sidePrice(ev, 'home');
    const b = this.sidePrice(ev, 'away');
    // Nothing priced yet and nothing sent before: no news worth emitting.
    if (ev.offers.size === 0 && ev.lastEmitted === undefined) return;

    const live = ev.irStatus === 'in_running';
    const status: BetInAsiaFeedMessage['status'] = !live
      ? 'PREMATCH'
      : a !== undefined && b !== undefined
        ? 'LIVE'
        : 'SUSPENDED';

    const sig = `${status}|${a ?? ''}|${b ?? ''}|${ev.score ?? ''}`;
    if (sig === ev.lastEmitted) return;
    ev.lastEmitted = sig;

    emit({
      matchId: ev.eventId,
      playerA: ev.home,
      playerB: ev.away,
      odds: { a, b },
      status,
      marketStatus: status,
      score: ev.score,
      sport: this.sport,
    });
  }
}
