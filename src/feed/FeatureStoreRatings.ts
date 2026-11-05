/**
 * featureStoreRatingResolver -- adapts the Feature Store to the ingestor's
 * `RatingResolver`.
 *
 * An odds-only feed (MollyBet / BetInAsia BLACK) carries no Elo, fatigue or
 * form, so the LiveFeedIngestor cannot screen for +EV from the message alone.
 * This resolver reads the player's vector from the store -- in production the
 * `RedisFeatureStoreClient` over `ta:player:<id>` -- and hands back the three
 * metrics the screen needs.
 *
 * TWO GUARDS, because this sits on a live stream:
 *
 *   - a deadline per lookup (AbortSignal), so a stalled Redis cannot wedge
 *     ingestion; a timeout is a miss, and a miss simply leaves the match
 *     unqualified rather than mispriced.
 *   - a short TTL cache, so a match ticking several times a second costs one
 *     read per player per window instead of one per tick.
 *
 * The store client's own contract is to return `null` and never throw on a
 * miss or a failure; the try/catch here is belt-and-braces for a custom client.
 */

import { LRUCache } from 'lru-cache';
import type { FeatureStoreClient } from '../services/FeatureLoader.js';
import type { PlayerRating, RatingResolver } from './LiveFeedIngestor.js';

export interface FeatureStoreRatingsOptions {
  client: FeatureStoreClient;
  /** Per-lookup deadline in ms. Default 50 (the hot-path budget). */
  timeoutMs?: number;
  /** How long a resolved rating stays reusable. Default 10000. */
  cacheTtlMs?: number;
  /** Cache capacity. Default 5000 players. */
  maxEntries?: number;
  logger?: Pick<Console, 'warn'>;
}

export function featureStoreRatingResolver(opts: FeatureStoreRatingsOptions): RatingResolver {
  const timeoutMs = opts.timeoutMs ?? 50;
  const logger = opts.logger ?? console;
  const cache = new LRUCache<string, PlayerRating>({
    max: opts.maxEntries ?? 5_000,
    ttl: opts.cacheTtlMs ?? 10_000,
  });

  return async (playerId: string): Promise<PlayerRating | undefined> => {
    const hit = cache.get(playerId);
    if (hit) return hit;

    const ac = new AbortController();
    const timer = setTimeout(() => ac.abort(), timeoutMs);
    try {
      const f = await opts.client.fetch(playerId, ac.signal);
      if (!f) return undefined; // clean miss: leave the side unpriced
      const rating: PlayerRating = {
        elo: f.elo,
        fatigue: f.fatigueScore,
        form: f.formRating,
      };
      cache.set(playerId, rating);
      return rating;
    } catch (err) {
      logger.warn(
        `featureStoreRatingResolver: lookup for ${playerId} failed: ${(err as Error).message}`,
      );
      return undefined;
    } finally {
      clearTimeout(timer);
    }
  };
}
