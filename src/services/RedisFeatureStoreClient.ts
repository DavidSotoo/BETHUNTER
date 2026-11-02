/**
 * RedisFeatureStoreClient -- the real Feature Store adapter.
 *
 * Reads a competitor's live metrics from a Redis HASH under the system key
 * scheme `ta:player:<id>` and maps the analytics vocabulary (`elo_blend`,
 * `fatigue`, `form_rating`, `updated_at`) onto the gateway's `PlayerFeatures`.
 *
 * DEGRADATION CONTRACT
 *
 * It returns `null` -- never throws -- when the key is absent, the payload is
 * unusable, or the lookup fails (network, parse). `null` is a clean miss: the
 * FeatureLoader turns it into the neutral fallback vector. Combined with the
 * loader's 50 ms deadline (which aborts a hanging read), this keeps the hot
 * path degrading in well under 50 ms.
 *
 * The Redis connection is injected: it MUST be a connection dedicated to
 * feature reads, never the one the OrderWorker uses for blocking BRPOP.
 */

import { FeatureStoreClient } from './FeatureLoader.js';
import { PlayerFeatures } from '../types/features.js';

/** The slice of ioredis this client needs; keeps it trivially mockable. */
export interface RedisLike {
  hgetall(key: string): Promise<Record<string, string>>;
}

/** Field-name candidates per metric, tried in order (first present wins). */
export interface FeatureFieldMap {
  elo: string[];
  fatigue: string[];
  form: string[];
  updatedAt: string[];
}

const DEFAULT_FIELDS: FeatureFieldMap = {
  elo: ['elo_blend', 'elo'],
  fatigue: ['fatigue', 'fatigue_score'],
  form: ['form_rating', 'form'],
  updatedAt: ['updated_at', 'updatedAt'],
};

export interface RedisFeatureStoreClientOptions {
  redis: RedisLike;
  /** Key prefix for a player's feature hash. Default "ta:player:". */
  keyPrefix?: string;
  /** Override the analytics->PlayerFeatures field mapping. */
  fields?: Partial<FeatureFieldMap>;
  logger?: Pick<Console, 'warn'>;
}

export class RedisFeatureStoreClient implements FeatureStoreClient {
  private readonly redis: RedisLike;
  private readonly keyPrefix: string;
  private readonly fields: FeatureFieldMap;
  private readonly logger: Pick<Console, 'warn'>;

  constructor(opts: RedisFeatureStoreClientOptions) {
    this.redis = opts.redis;
    this.keyPrefix = opts.keyPrefix ?? 'ta:player:';
    this.fields = { ...DEFAULT_FIELDS, ...opts.fields };
    this.logger = opts.logger ?? console;
  }

  async fetch(playerId: string, signal: AbortSignal): Promise<PlayerFeatures | null> {
    // The loader may have aborted before we even started; don't hit the wire.
    if (signal.aborted) return null;

    const key = this.keyPrefix + playerId;
    try {
      const hash = await this.redis.hgetall(key);
      if (!hash || Object.keys(hash).length === 0) {
        return null; // unknown player -> clean miss -> loader fallback
      }

      const elo = this.pickNumber(hash, this.fields.elo);
      if (elo === undefined) {
        // No usable rating; treat as a miss rather than fabricating one.
        return null;
      }

      return {
        id: playerId,
        elo,
        fatigueScore: this.pickNumber(hash, this.fields.fatigue) ?? 0,
        formRating: this.pickNumber(hash, this.fields.form) ?? 0,
        updatedAt: this.pickNumber(hash, this.fields.updatedAt) ?? 0,
      };
    } catch (err) {
      // Network / Redis error: degrade, never throw. The loader falls back.
      this.logger.warn(
        `RedisFeatureStoreClient: lookup for ${key} failed: ${(err as Error).message}`,
      );
      return null;
    }
  }

  /** First present, finite numeric field among `names`, else undefined. */
  private pickNumber(hash: Record<string, string>, names: string[]): number | undefined {
    for (const name of names) {
      const raw = hash[name];
      if (raw === undefined || raw === '') continue;
      const n = Number(raw);
      if (Number.isFinite(n)) return n;
    }
    return undefined;
  }
}
