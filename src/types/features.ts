/**
 * Shared feature/order contracts for the Execution Gateway.
 *
 * These types are the boundary between the Feature Store (which owns the
 * raw tennis signals -- Elo, fatigue, form) and the OrderWorker hot path.
 * The names here (`elo`, `fatigueScore`, `formRating`) are the gateway's
 * own vocabulary; mapping from the analytics contract's column names
 * (`a_elo_blend`, `a_fatigue`, `a_form_matches`, ...) lives in the
 * anti-corruption layer inside FeatureLoader, never here.
 */

/**
 * Live, hydrated metrics for a single competitor.
 *
 * `updatedAt` is epoch milliseconds as reported by the Feature Store (the
 * freshness of the underlying signal), NOT the time we read it. The gateway
 * records its own read time separately in `HydratedOrderPayload.hydratedAt`,
 * because the two answer different questions: "how stale is the data" vs.
 * "when did we enrich this order".
 */
export interface PlayerFeatures {
  /** Stable competitor id, matching the id used in the order payload. */
  id: string;
  /** Pre-match blended Elo rating. */
  elo: number;
  /** Rolling fatigue score; higher means more accumulated load. */
  fatigueScore: number;
  /** Recent form/trend rating. */
  formRating: number;
  /** Epoch ms of the signal's last recompute (source freshness). */
  updatedAt: number;
}

/**
 * The order as it arrives on the Redis `ApprovedOrder` queue, before
 * enrichment. Kept intentionally minimal and open (`[key: string]`) so the
 * gateway does not have to model every upstream field it does not read; it
 * only needs the two competitor ids to hydrate.
 */
export interface ApprovedOrder {
  /** Idempotency / correlation id for the order. */
  orderId: string;
  /** Competitor A id (maps to the analytics `player_a`). */
  playerAId: string;
  /** Competitor B id (maps to the analytics `player_b`). */
  playerBId: string;
  /** Any further upstream fields are preserved untouched. */
  [key: string]: unknown;
}

/**
 * Why a hydration was or was not fully served from live data. Carried on the
 * payload so the downstream executor (and risk checks) can decide whether a
 * degraded vector is acceptable rather than discovering it implicitly.
 */
export type HydrationSource = 'live' | 'cache' | 'fallback';

/**
 * The original order extended with both competitors' live vectors and the
 * gateway's own hydration metadata. This is what the OrderWorker hands to the
 * Playwright execution step.
 */
export interface HydratedOrderPayload extends ApprovedOrder {
  features: {
    playerA: PlayerFeatures;
    playerB: PlayerFeatures;
  };
  /** Epoch ms when the gateway enriched this order (our read time). */
  hydratedAt: number;
  /** How each side was resolved; `fallback` flags a degraded vector. */
  hydrationSource: {
    playerA: HydrationSource;
    playerB: HydrationSource;
  };
  /** Wall-clock cost of the hydration leg in milliseconds. */
  hydrationLatencyMs: number;
}
