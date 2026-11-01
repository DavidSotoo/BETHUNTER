/**
 * FeatureLoader -- hydrates live competitor metrics for the Execution Gateway.
 *
 * DESIGN CONSTRAINTS
 *
 *   - Cached reads must resolve in < 2 ms. We keep the hot path synchronous
 *     in spirit: an LRU hit returns without ever awaiting I/O. The public API
 *     is async (it may miss), but a hit resolves on the microtask queue with
 *     no timer, no socket, no serialization.
 *   - A miss may touch the Feature Store, which is an external dependency and
 *     therefore untrusted for latency. Every store call is wrapped in a hard
 *     50 ms deadline. On timeout or failure we degrade gracefully with a
 *     neutral fallback vector -- the worker loop must never stall or crash on
 *     a slow Feature Store.
 *   - The store client is behind an interface (anti-corruption layer): the
 *     gateway speaks `PlayerFeatures`, and the adapter is the only place that
 *     knows the store is REST/gRPC/DB and what its column names are.
 */

import { LRUCache } from 'lru-cache';
import {
  HydratedOrderPayload,
  HydrationSource,
  PlayerFeatures,
  ApprovedOrder,
} from '../types/features.js';

/**
 * Anti-corruption boundary to the Feature Store. Implementations translate
 * the store's wire shape (REST body, gRPC message, DB row -- with its
 * `a_elo_blend` / `a_fatigue` / `a_form_matches` column vocabulary) into the
 * gateway's `PlayerFeatures`. Nothing above this interface knows the transport.
 */
export interface FeatureStoreClient {
  /**
   * Fetch one competitor's live features. MUST respect the AbortSignal: the
   * loader arms a deadline and aborts on it, so a well-behaved client stops
   * waiting on the wire instead of leaking a pending request.
   */
  fetch(playerId: string, signal: AbortSignal): Promise<PlayerFeatures>;
}

export interface FeatureLoaderOptions {
  client: FeatureStoreClient;
  /** Max distinct players held in cache. Default 10k. */
  maxEntries?: number;
  /** Cache TTL in ms. Short by design (30-60s). Default 45s. */
  ttlMs?: number;
  /** Hard per-fetch deadline in ms. Default 50ms. */
  fetchTimeoutMs?: number;
  /** Optional structured logger; defaults to console. */
  logger?: Pick<Console, 'warn' | 'error'>;
}

/**
 * Neutral vector used when the store cannot answer within budget. Elo 1500 is
 * the conventional "unrated" baseline; zeroed fatigue/form means "no signal",
 * which downstream risk sizing already treats conservatively. `updatedAt: 0`
 * is the tell that this is synthetic, not fresh.
 */
function neutralFeatures(id: string): PlayerFeatures {
  return { id, elo: 1500, fatigueScore: 0, formRating: 0, updatedAt: 0 };
}

export class FeatureLoader {
  private readonly client: FeatureStoreClient;
  private readonly cache: LRUCache<string, PlayerFeatures>;
  private readonly fetchTimeoutMs: number;
  private readonly logger: Pick<Console, 'warn' | 'error'>;

  /**
   * Coalesces concurrent misses for the same player into one in-flight store
   * call. Without this, N orders referencing a hot player on a cold cache
   * would each open a socket; with it, they share the single pending promise.
   */
  private readonly inFlight = new Map<string, Promise<PlayerFeatures>>();

  constructor(opts: FeatureLoaderOptions) {
    this.client = opts.client;
    this.fetchTimeoutMs = opts.fetchTimeoutMs ?? 50;
    this.logger = opts.logger ?? console;
    this.cache = new LRUCache<string, PlayerFeatures>({
      max: opts.maxEntries ?? 10_000,
      ttl: opts.ttlMs ?? 45_000,
      // Keep a stale entry readable; freshness is governed by `updatedAt`,
      // and a slightly-stale hit beats a 50ms store round trip on the hot path.
      allowStale: false,
    });
  }

  /**
   * Enrich an order with both competitors' live vectors. Resolves the two
   * sides in parallel and stamps hydration metadata (source, read time,
   * latency) onto the returned payload. Never rejects on a store failure --
   * it degrades to a neutral vector and marks that side `fallback`.
   */
  async hydrate(order: ApprovedOrder): Promise<HydratedOrderPayload> {
    const start = performance.now();

    const [a, b] = await Promise.all([
      this.resolve(order.playerAId),
      this.resolve(order.playerBId),
    ]);

    const hydrationLatencyMs = performance.now() - start;

    return {
      ...order,
      features: { playerA: a.features, playerB: b.features },
      hydratedAt: Date.now(),
      hydrationSource: { playerA: a.source, playerB: b.source },
      hydrationLatencyMs,
    };
  }

  /**
   * Resolve one player: cache first (the <2ms path), then a deadlined,
   * coalesced store fetch, then a neutral fallback. Returns the vector plus
   * how it was obtained so `hydrate` can record the source.
   */
  private async resolve(
    playerId: string,
  ): Promise<{ features: PlayerFeatures; source: HydrationSource }> {
    const cached = this.cache.get(playerId);
    if (cached !== undefined) {
      return { features: cached, source: 'cache' };
    }

    try {
      const features = await this.fetchCoalesced(playerId);
      this.cache.set(playerId, features);
      return { features, source: 'live' };
    } catch (err) {
      // Timeout or store failure: degrade, do not throw. The worker loop
      // keeps running; the payload carries the `fallback` flag so the
      // executor can apply its own policy to a degraded vector.
      this.logger.warn(
        `FeatureLoader: fallback for player ${playerId}: ${(err as Error).message}`,
      );
      return { features: neutralFeatures(playerId), source: 'fallback' };
    }
  }

  /** Share a single in-flight store call across concurrent misses. */
  private fetchCoalesced(playerId: string): Promise<PlayerFeatures> {
    const existing = this.inFlight.get(playerId);
    if (existing) return existing;

    const p = this.fetchWithDeadline(playerId).finally(() => {
      this.inFlight.delete(playerId);
    });
    this.inFlight.set(playerId, p);
    return p;
  }

  /**
   * Wrap a store fetch in a hard deadline. We own the timer so the guarantee
   * holds even against a client that ignores its AbortSignal: the race rejects
   * at `fetchTimeoutMs` regardless, and the abort is a courtesy to let a
   * well-behaved client release the socket.
   */
  private fetchWithDeadline(playerId: string): Promise<PlayerFeatures> {
    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout>;

    const deadline = new Promise<never>((_, reject) => {
      timer = setTimeout(() => {
        controller.abort();
        reject(new Error(`feature-store timeout after ${this.fetchTimeoutMs}ms`));
      }, this.fetchTimeoutMs);
    });

    return Promise.race([
      this.client.fetch(playerId, controller.signal),
      deadline,
    ]).finally(() => clearTimeout(timer));
  }

  /** Test/ops hook: drop a player (or everything) from the cache. */
  invalidate(playerId?: string): void {
    if (playerId) this.cache.delete(playerId);
    else this.cache.clear();
  }
}
