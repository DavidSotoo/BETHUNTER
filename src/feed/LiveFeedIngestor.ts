/**
 * LiveFeedIngestor -- real live tennis feed adapter.
 *
 * Connects to a WebSocket / polling feed, normalises match & odds payloads,
 * canonicalises player names to the `ta:player:<id>` key scheme, and pushes the
 * results into Redis for the rest of the gateway:
 *
 *   - HSET ta:player:<id>   hydrated elo / fatigue / form
 *   - LPUSH ApprovedOrder   qualified (+EV) orders -> OrderWorker
 *   - LPUSH ClosedMatch     finished matches -> SettlementService
 *
 * The network transport is injected (`connect`), so the ingestor is agnostic to
 * WebSocket vs polling and is unit-testable with a mocked connection. It
 * reconnects on close/error with a defensive exponential backoff.
 */

/** A live connection: the ingestor only needs to observe its events. */
export interface FeedConnection {
  onMessage(cb: (raw: unknown) => void): void;
  onClose(cb: () => void): void;
  onError(cb: (err: Error) => void): void;
  close(): void;
}
export type FeedConnector = () => FeedConnection | Promise<FeedConnection>;

/** The slice of an ioredis pipeline the ingestor uses. */
export interface IngestorPipeline {
  hset(key: string, data: Record<string, string>): IngestorPipeline;
  lpush(key: string, value: string): IngestorPipeline;
  exec(): Promise<unknown>;
}
export interface IngestorRedis {
  pipeline(): IngestorPipeline;
}

export type MatchStatus = 'PREMATCH' | 'LIVE' | 'SUSPENDED' | 'FINISHED' | 'UNKNOWN';

export interface PlayerRating {
  elo?: number;
  fatigue?: number;
  form?: number;
}

export interface NormalizedMatch {
  matchId: string;
  playerAId: string;
  playerBId: string;
  oddsA?: number;
  oddsB?: number;
  ratingA?: PlayerRating;
  ratingB?: PlayerRating;
  status: MatchStatus;
  score?: string;
  /** Canonical id of the winner, when FINISHED. */
  winnerId?: string;
  targetUrl?: string;
}

export interface BackoffOptions {
  baseMs?: number;
  factor?: number;
  maxMs?: number;
  jitter?: boolean;
}

export interface LiveFeedIngestorOptions {
  redis: IngestorRedis;
  connect: FeedConnector;
  playerPrefix?: string;
  approvedOrderQueue?: string;
  closedMatchQueue?: string;
  /** Minimum +EV to qualify an order. Default 0.02 (2%). */
  minEv?: number;
  /** Stake stamped on emitted orders. Default 10. */
  stakeUnit?: number;
  /** Namespace for canonical player ids. Default "atp". */
  tour?: string;
  backoff?: BackoffOptions;
  logger?: Pick<Console, 'info' | 'warn' | 'error'>;
}

// --- parsing helpers (pure, exported for direct testing) ---

const num = (v: unknown): number | undefined =>
  typeof v === 'number' && Number.isFinite(v) ? v : undefined;
const str = (v: unknown): string | undefined =>
  typeof v === 'string' && v.trim() !== '' ? v : undefined;
const asRecord = (v: unknown): Record<string, unknown> | null =>
  typeof v === 'object' && v !== null ? (v as Record<string, unknown>) : null;
const round = (n: number, dp = 2): number => {
  const f = 10 ** dp;
  return Math.round(n * f) / f;
};

/**
 * Canonicalise a display name to `<tour>:<surname>-<initial>`, matching the
 * analytics key scheme. Handles "First Last", "F. Last" and "Last F." forms,
 * and strips accents.
 */
export function canonicalizePlayer(name: string, tour = 'atp'): string {
  const cleaned = name
    .normalize('NFD')
    .replace(/[̀-ͯ]/g, '')
    .toLowerCase()
    .trim();
  const tokens = cleaned.split(/[\s.]+/).filter(Boolean);
  if (tokens.length === 0) return `${tour}:unknown`;

  let surname: string;
  let initial = '';

  if (tokens.length === 1) {
    surname = tokens[0] as string;
  } else {
    const initialIdx = tokens.findIndex((t) => t.length === 1);
    if (initialIdx >= 0) {
      initial = (tokens[initialIdx] as string)[0] ?? '';
      surname = tokens[initialIdx === 0 ? tokens.length - 1 : 0] as string;
    } else {
      // "First Last[...]" -> initial of the first token, last token is surname.
      initial = (tokens[0] as string)[0] ?? '';
      surname = tokens[tokens.length - 1] as string;
    }
  }

  surname = surname.replace(/[^a-z0-9]/g, '');
  return initial ? `${tour}:${surname}-${initial}` : `${tour}:${surname}`;
}

function normalizeStatus(raw: string | undefined): MatchStatus {
  const s = (raw ?? '').toLowerCase();
  if (/(finished|final|ft|ended|closed|complete)/.test(s)) return 'FINISHED';
  if (/(suspend|pause|interrupt)/.test(s)) return 'SUSPENDED';
  if (/(live|inplay|in_play|playing)/.test(s)) return 'LIVE';
  if (/(prematch|scheduled|upcoming|not_started)/.test(s)) return 'PREMATCH';
  return 'UNKNOWN';
}

function readRating(v: unknown): PlayerRating | undefined {
  const r = asRecord(v);
  if (!r) return undefined;
  const rating: PlayerRating = {
    elo: num(r.elo) ?? num(r.elo_blend),
    fatigue: num(r.fatigue) ?? num(r.fatigue_score),
    form: num(r.form) ?? num(r.form_rating),
  };
  if (rating.elo === undefined && rating.fatigue === undefined && rating.form === undefined) {
    return undefined;
  }
  return rating;
}

/**
 * Normalise a raw feed message into a NormalizedMatch, or `null` if it is not a
 * usable match payload (missing ids / both players). Pure and side-effect free.
 */
export function normalizeMatch(raw: unknown, tour = 'atp'): NormalizedMatch | null {
  const r = asRecord(raw);
  if (!r) return null;

  const matchId = str(r.matchId) ?? str(r.id);
  const players = Array.isArray(r.players) ? (r.players as unknown[]) : [];
  const nameA = str(r.playerA) ?? str(r.home) ?? str(players[0]);
  const nameB = str(r.playerB) ?? str(r.away) ?? str(players[1]);
  if (!matchId || !nameA || !nameB) return null;

  const odds = asRecord(r.odds);
  const oddsA = num(r.oddsA) ?? (odds ? num(odds.a) : undefined);
  const oddsB = num(r.oddsB) ?? (odds ? num(odds.b) : undefined);

  const playerAId = canonicalizePlayer(nameA, tour);
  const playerBId = canonicalizePlayer(nameB, tour);
  const status = normalizeStatus(str(r.status));

  let winnerId: string | undefined;
  const winnerName = str(r.winner);
  if (winnerName) {
    const wid = canonicalizePlayer(winnerName, tour);
    winnerId = wid === playerBId ? playerBId : playerAId; // map to a known side
    if (wid !== playerAId && wid !== playerBId) winnerId = wid; // unknown -> keep as-is
  }

  return {
    matchId,
    playerAId,
    playerBId,
    oddsA,
    oddsB,
    ratingA: readRating(r.ratingA),
    ratingB: readRating(r.ratingB),
    status,
    score: str(r.score),
    winnerId,
    targetUrl: str(r.url),
  };
}

export class LiveFeedIngestor {
  private readonly redis: IngestorRedis;
  private readonly connect: FeedConnector;
  private readonly playerPrefix: string;
  private readonly approvedOrderQueue: string;
  private readonly closedMatchQueue: string;
  private readonly minEv: number;
  private readonly stakeUnit: number;
  private readonly tour: string;
  private readonly baseMs: number;
  private readonly factor: number;
  private readonly maxMs: number;
  private readonly jitter: boolean;
  private readonly logger: Pick<Console, 'info' | 'warn' | 'error'>;

  private conn?: FeedConnection;
  private timer?: ReturnType<typeof setTimeout>;
  private attempt = 0;
  private stopped = false;

  constructor(opts: LiveFeedIngestorOptions) {
    this.redis = opts.redis;
    this.connect = opts.connect;
    this.playerPrefix = opts.playerPrefix ?? 'ta:player:';
    this.approvedOrderQueue = opts.approvedOrderQueue ?? 'ApprovedOrder';
    this.closedMatchQueue = opts.closedMatchQueue ?? 'ClosedMatch';
    this.minEv = opts.minEv ?? 0.02;
    this.stakeUnit = opts.stakeUnit ?? 10;
    this.tour = opts.tour ?? 'atp';
    this.baseMs = opts.backoff?.baseMs ?? 500;
    this.factor = opts.backoff?.factor ?? 2;
    this.maxMs = opts.backoff?.maxMs ?? 30_000;
    this.jitter = opts.backoff?.jitter ?? true;
    this.logger = opts.logger ?? console;
  }

  /** Open the feed and start ingesting. Reconnects on failure. */
  async start(): Promise<void> {
    this.stopped = false;
    await this.establish();
  }

  /** Stop ingesting; cancels any pending reconnect and closes the connection. */
  stop(): void {
    this.stopped = true;
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = undefined;
    }
    this.conn?.close();
    this.conn = undefined;
  }

  private async establish(): Promise<void> {
    if (this.stopped) return;
    try {
      const conn = await this.connect();
      if (this.stopped) {
        conn.close();
        return;
      }
      this.conn = conn;
      conn.onMessage((raw) => {
        this.attempt = 0; // a live message proves the link is healthy
        void this.handleMessage(raw);
      });
      conn.onError((err) => this.logger.warn(`LiveFeedIngestor: feed error: ${err.message}`));
      conn.onClose(() => {
        this.conn = undefined;
        this.scheduleReconnect();
      });
      this.attempt = 0;
      this.logger.info('LiveFeedIngestor: connected');
    } catch (err) {
      this.logger.warn(`LiveFeedIngestor: connect failed: ${(err as Error).message}`);
      this.scheduleReconnect();
    }
  }

  private scheduleReconnect(): void {
    if (this.stopped) return;
    if (this.timer) clearTimeout(this.timer);
    const raw = Math.min(this.maxMs, this.baseMs * this.factor ** this.attempt);
    const delay = this.jitter ? raw * (0.5 + Math.random() * 0.5) : raw;
    this.attempt++;
    this.logger.warn(`LiveFeedIngestor: reconnecting in ${Math.round(delay)}ms (attempt ${this.attempt})`);
    this.timer = setTimeout(() => {
      this.timer = undefined;
      void this.establish();
    }, delay);
  }

  private async handleMessage(raw: unknown): Promise<void> {
    const match = normalizeMatch(raw, this.tour);
    if (!match) return; // not a usable match payload
    try {
      await this.processMatch(match);
    } catch (err) {
      this.logger.error(`LiveFeedIngestor: process failed: ${(err as Error).message}`);
    }
  }

  private async processMatch(m: NormalizedMatch): Promise<void> {
    const pipeline = this.redis.pipeline();
    let ops = 0;

    // Hydrate player hashes with whatever ratings arrived.
    const hashA = this.serializeRating(m.ratingA);
    if (hashA) {
      pipeline.hset(this.playerPrefix + m.playerAId, hashA);
      ops++;
    }
    const hashB = this.serializeRating(m.ratingB);
    if (hashB) {
      pipeline.hset(this.playerPrefix + m.playerBId, hashB);
      ops++;
    }

    if (m.status === 'FINISHED') {
      const closingOdds = m.winnerId === m.playerBId ? m.oddsB : m.oddsA;
      if (m.winnerId && closingOdds !== undefined && closingOdds > 0) {
        pipeline.lpush(
          this.closedMatchQueue,
          JSON.stringify({ matchId: m.matchId, winner: m.winnerId, closingOdds: round(closingOdds) }),
        );
        ops++;
      } else {
        this.logger.warn(`LiveFeedIngestor: ${m.matchId} finished without a settleable winner/odds`);
      }
    } else if (m.status !== 'SUSPENDED') {
      for (const order of this.qualifyOrders(m)) {
        pipeline.lpush(this.approvedOrderQueue, JSON.stringify(order));
        ops++;
      }
    }

    if (ops > 0) await pipeline.exec();
  }

  /** +EV screen: back a side only when model prob * market odds beats minEv. */
  private qualifyOrders(m: NormalizedMatch): Record<string, unknown>[] {
    const eloA = m.ratingA?.elo;
    const eloB = m.ratingB?.elo;
    if (eloA === undefined || eloB === undefined) return [];
    const pA = 1 / (1 + 10 ** ((eloB - eloA) / 400));

    const sides: { selection: string; odds: number | undefined; p: number }[] = [
      { selection: m.playerAId, odds: m.oddsA, p: pA },
      { selection: m.playerBId, odds: m.oddsB, p: 1 - pA },
    ];

    const orders: Record<string, unknown>[] = [];
    for (const s of sides) {
      if (s.odds === undefined || s.odds <= 1) continue;
      const ev = s.p * s.odds - 1;
      if (ev < this.minEv) continue;
      orders.push({
        orderId: `ord-${m.matchId}-${s.selection}`,
        matchId: m.matchId,
        playerAId: m.playerAId,
        playerBId: m.playerBId,
        selection: s.selection,
        targetOdds: round(s.odds),
        stake: this.stakeUnit,
        market: 'match_winner',
        ev: round(ev, 4),
        ...(m.targetUrl ? { targetUrl: m.targetUrl } : {}),
      });
    }
    return orders;
  }

  private serializeRating(r: PlayerRating | undefined): Record<string, string> | undefined {
    if (!r) return undefined;
    const h: Record<string, string> = {};
    if (r.elo !== undefined) h.elo_blend = String(r.elo);
    if (r.fatigue !== undefined) h.fatigue = String(r.fatigue);
    if (r.form !== undefined) h.form_rating = String(r.form);
    if (Object.keys(h).length === 0) return undefined;
    h.updated_at = String(Date.now());
    return h;
  }
}
