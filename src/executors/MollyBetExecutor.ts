/**
 * MollyBetExecutor -- places orders through the official MollyBet API
 * (BetInAsia BLACK). Replaces the DOM/Playwright path of `OrderExecutor`: there
 * is no browser, no selector and no click, just two authenticated HTTP calls.
 *
 * FLOW (live)
 *
 *   1. Validate the order and resolve the market:
 *        order.matchId   -> MollyBet event_id   (set by BetInAsiaProvider)
 *        order.selection -> home (= playerAId) or away (= playerBId)
 *                           -> bet_type `for,h` / `for,a`
 *   2. CIRCUIT BREAKER gate: if the breaker is OPEN nothing is sent.
 *   3. SLIPPAGE GUARD (client side, before any payload leaves): when a live
 *      price source is wired (`liveOdds`, e.g. BetInAsiaProvider.getBestPrice),
 *      slippage = (requested - live) / requested; beyond tolerance ->
 *      REJECTED_SLIPPAGE, nothing sent.
 *   4. POST /v1/betslips/ -> betslip_id.
 *   5. POST /v1/orders/ with `price` = the slippage FLOOR
 *        floor = requested * (1 - maxSlippageTolerance)
 *      `price` is the minimum acceptable price, so the venue itself refuses to
 *      fill below the floor: the guard holds even if the price moves between
 *      step 3 and the fill.
 *   6. Return the API's own order object as `confirmation`.
 *
 * `request_uuid` is derived from the orderId, so a redelivered order is acted on
 * once by the API (it dedupes the same UUID for 24h).
 *
 * STATUS SEMANTICS
 *
 *   EXECUTED          the API accepted the order and returned its confirmation.
 *                     Fills are asynchronous (the order may still be open with
 *                     `bets` filling in); the confirmation carries order_id so
 *                     it can be tracked with MollyBetClient.getOrder().
 *   REJECTED_SLIPPAGE business decision, does not count against the breaker.
 *   FAILED_DOM        kept for contract compatibility with the worker/metrics:
 *                     here it means "downstream API fault" (HTTP error, timeout,
 *                     invalid order, breaker open). Error text containing
 *                     403 / forbidden / too many requests trips the breaker.
 *
 * The breaker's outcome is recorded by OrderWorker; this executor only reads
 * its state, so a failure is never counted twice.
 */

import { createHash } from 'node:crypto';
import { performance } from 'node:perf_hooks';
import { CircuitBreaker, CircuitState } from '../services/CircuitBreaker.js';
import { MollyBetClient, MollyBetError, type MollyBetOrder } from '../services/MollyBetClient.js';
import type { HydratedOrderPayload } from '../types/features.js';
import type { OrderExecutionResult, OrderExecutorPort } from '../types/execution.js';

export interface MollyBetExecutorOptions {
  client: MollyBetClient;
  /** Sport code for betslips. Default "tennis". */
  sport?: string;
  /** Stake currency (ISO 4217). Default "EUR". */
  currency?: string;
  /** Seconds the order stays open trying to fill. Default 15. */
  orderDurationSec?: number;
  /** Max tolerated adverse relative slippage. Default 0.02 (2%). */
  maxSlippageTolerance?: number;
  /** When true: run every check, build the payload, never call the order API. */
  shadowMode?: boolean;
  /** Read-only: an OPEN breaker blocks sending. */
  breaker?: CircuitBreaker;
  /** Live best back price for a side of an event (e.g. provider.getBestPrice). */
  liveOdds?: (eventId: string, side: 'home' | 'away') => number | undefined;
  /** bet_type codes for backing home / away. Default `for,h` / `for,a`. */
  backBetTypes?: { home: string; away: string };
  logger?: Pick<Console, 'info' | 'warn' | 'error'>;
}

/** Deterministic UUID (v4-shaped) from an order id, for API-side deduplication. */
export function orderUuid(orderId: string): string {
  const h = createHash('sha256').update(`tennis-alpha:${orderId}`).digest('hex');
  const variant = ((parseInt(h.slice(16, 17), 16) & 0x3) | 0x8).toString(16);
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-4${h.slice(13, 16)}-${variant}${h.slice(17, 20)}-${h.slice(20, 32)}`;
}

export class MollyBetExecutor implements OrderExecutorPort {
  /**
   * API executor: the worker skips browser acquisition and navigation entirely
   * and passes `undefined` for the page.
   */
  readonly needsBrowser = false;

  private readonly client: MollyBetClient;
  private readonly sport: string;
  private readonly currency: string;
  private readonly durationSec: number;
  private readonly maxSlippage: number;
  private readonly shadowMode: boolean;
  private readonly breaker?: CircuitBreaker;
  private readonly liveOdds?: MollyBetExecutorOptions['liveOdds'];
  private readonly bet: { home: string; away: string };
  private readonly logger: Pick<Console, 'info' | 'warn' | 'error'>;

  constructor(opts: MollyBetExecutorOptions) {
    this.client = opts.client;
    this.sport = opts.sport ?? 'tennis';
    this.currency = opts.currency ?? 'EUR';
    this.durationSec = opts.orderDurationSec ?? 15;
    this.maxSlippage = opts.maxSlippageTolerance ?? 0.02;
    this.shadowMode = opts.shadowMode ?? false;
    this.breaker = opts.breaker;
    this.liveOdds = opts.liveOdds;
    this.bet = opts.backBetTypes ?? { home: 'for,h', away: 'for,a' };
    this.logger = opts.logger ?? console;
  }

  /** `_page` is always ignored: this executor is API-only. */
  async executeOrder(_page: unknown, order: HydratedOrderPayload): Promise<OrderExecutionResult> {
    const start = performance.now();
    const elapsed = (): number => performance.now() - start;
    const fail = (
      requestedOdds: number,
      error: string,
      extra: Partial<OrderExecutionResult> = {},
    ): OrderExecutionResult => ({
      success: false,
      requestedOdds,
      slippage: 0,
      status: 'FAILED_DOM',
      latencyMs: elapsed(),
      error,
      ...extra,
    });

    // --- 1. Validate & resolve the market. ---
    const requested = Number(order.targetOdds);
    if (!Number.isFinite(requested) || requested <= 1) {
      return fail(Number.isFinite(requested) ? requested : 0, 'order.targetOdds must be a number > 1');
    }
    const stake = Number(order.stake);
    if (!Number.isFinite(stake) || stake <= 0) {
      return fail(requested, 'order.stake must be a positive number');
    }
    // `marketId` is the explicit MollyBet event id stamped by the ingestor;
    // `matchId` is the fallback for orders queued before that field existed.
    const eventId =
      (typeof order.marketId === 'string' && order.marketId.trim() !== ''
        ? order.marketId
        : undefined) ??
      (typeof order.matchId === 'string' && order.matchId.trim() !== ''
        ? order.matchId
        : undefined);
    if (!eventId) {
      return fail(requested, 'order.marketId / order.matchId (MollyBet event_id) is missing');
    }

    const side =
      order.selection === order.playerAId ? 'home' : order.selection === order.playerBId ? 'away' : undefined;
    if (!side) return fail(requested, 'order.selection matches neither playerAId nor playerBId');
    const betType = side === 'home' ? this.bet.home : this.bet.away;

    // --- 2. Circuit breaker gate (read-only: the worker records the outcome). ---
    if (this.breaker && this.breaker.getState() === CircuitState.OPEN) {
      return fail(requested, 'circuit breaker OPEN: order not sent');
    }

    // --- 3. Slippage guard against the live stream price, before sending. ---
    const live = this.liveOdds?.(eventId, side);
    let slippage = 0;
    if (live !== undefined) {
      slippage = (requested - live) / requested;
      if (slippage > this.maxSlippage) {
        this.logger.warn(
          `MollyBetExecutor: REJECTED_SLIPPAGE order=${order.orderId} requested=${requested} ` +
            `live=${live} slippage=${(slippage * 100).toFixed(2)}% > tol=${(this.maxSlippage * 100).toFixed(2)}%`,
        );
        return {
          success: false,
          executedOdds: live,
          requestedOdds: requested,
          slippage,
          status: 'REJECTED_SLIPPAGE',
          latencyMs: elapsed(),
        };
      }
    }

    // Server-side floor: the venue will not fill below this price.
    const floor = Math.max(1, Math.ceil(requested * (1 - this.maxSlippage) * 1000) / 1000);
    const orderRequest = {
      price: floor,
      stake: [this.currency, stake] as [string, number],
      duration: this.durationSec,
      request_uuid: orderUuid(order.orderId),
      user_data: order.orderId.slice(0, 512),
    };

    // --- SHADOW: everything above ran; stop before touching the order API. ---
    if (this.shadowMode) {
      this.logger.info(
        `MollyBetExecutor[SHADOW]: order=${order.orderId} event=${eventId} bet_type=${betType} ` +
          `requested=${requested} floor=${floor} live=${live ?? 'n/a'} ` +
          `slippage=${(slippage * 100).toFixed(2)}% (nothing sent)`,
      );
      return {
        success: true,
        executedOdds: live ?? requested,
        requestedOdds: requested,
        slippage,
        status: 'SHADOW_SIMULATED',
        latencyMs: elapsed(),
      };
    }

    // --- 4 + 5. Open the betslip, then place the order. ---
    let confirmation: MollyBetOrder;
    try {
      const slip = await this.client.openBetslip({ sport: this.sport, event_id: eventId, bet_type: betType });
      if (!slip.betslip_id || slip.is_open === false) {
        return fail(requested, `betslip not open for event ${eventId} (${betType})`, { slippage });
      }
      confirmation = await this.client.placeOrder({ betslip_id: slip.betslip_id, ...orderRequest });
    } catch (err) {
      return fail(requested, this.describeError(err), { slippage });
    }

    // Placement is asynchronous: `bets` may be absent; report fills when present.
    const executedOdds = this.filledPrice(confirmation);
    this.logger.info(
      `MollyBetExecutor: EXECUTED order=${order.orderId} api_order=${confirmation.order_id} ` +
        `requested=${requested} floor=${floor}${executedOdds !== undefined ? ` filled@${executedOdds}` : ''}`,
    );
    return {
      success: true,
      executedOdds,
      requestedOdds: requested,
      slippage: executedOdds !== undefined ? (requested - executedOdds) / requested : slippage,
      status: 'EXECUTED',
      latencyMs: elapsed(),
      confirmation: confirmation as Record<string, unknown>,
    };
  }

  /** Error text for the worker. Keeps words its block heuristic recognises. */
  private describeError(err: unknown): string {
    if (err instanceof MollyBetError) {
      const base = `MollyBet API error: ${err.message}`;
      if (err.httpStatus === 429) return `${base} (rate limited: too many requests)`;
      if (err.httpStatus === 403) return `${base} (forbidden)`;
      return base;
    }
    return `order placement failed: ${(err as Error).message}`;
  }

  /** Stake-weighted average of the filled bets' prices, if any have filled. */
  private filledPrice(order: MollyBetOrder): number | undefined {
    let stakeSum = 0;
    let weighted = 0;
    for (const b of order.bets ?? []) {
      const price = b.got_price;
      const amt = b.got_stake?.[1];
      if (typeof price === 'number' && typeof amt === 'number' && amt > 0) {
        stakeSum += amt;
        weighted += price * amt;
      }
    }
    return stakeSum > 0 ? weighted / stakeSum : undefined;
  }
}
