/**
 * SettlementService -- Shadow-Mode settlement & CLV audit.
 *
 * Records the odds each Shadow order would have been emitted at, then
 * reconciles them when a match settles: it computes the Closing Line Value
 * (did we take better odds than the market close?) and the PnL from the
 * fractional-Kelly stake that was applied, and rolls everything up into a
 * financial report. This is how the strategy is proven on paper before real
 * capital is risked.
 *
 * PERSISTENCE
 *
 * Records are always held in memory (the source of truth for the report).
 * When a Redis client is supplied, each record is additionally written through
 * to `ta:settlement:<orderId>` as JSON for durable audit; a write failure is
 * logged, never thrown -- the audit must not take down the hot path.
 */

import { HydratedOrderPayload } from '../types/features.js';
import {
  FinancialReport,
  OrderSettlement,
  SettlementStatus,
} from '../types/settlement.js';

/** The slice of ioredis used for write-through persistence. */
export interface SettlementRedisLike {
  set(key: string, value: string): Promise<unknown>;
}

export interface SettlementServiceOptions {
  redis?: SettlementRedisLike;
  /** Key prefix for persisted records. Default "ta:settlement:". */
  keyPrefix?: string;
  /** Kelly fraction applied when sizing from a model probability. Default 0.25. */
  kellyFraction?: number;
  /** Bankroll used for fractional-Kelly sizing. Default 1000. */
  bankroll?: number;
  /** Stake used when an order carries neither a stake nor a model probability. */
  defaultStake?: number;
  logger?: Pick<Console, 'warn'>;
}

/** Internal record = the public settlement plus the stake behind its PnL. */
interface SettlementRecord extends OrderSettlement {
  stake: number;
}

const round = (n: number, dp: number): number => {
  const f = 10 ** dp;
  return Math.round(n * f) / f;
};
const num = (v: unknown): number | undefined =>
  typeof v === 'number' && Number.isFinite(v) ? v : undefined;
const str = (v: unknown): string | undefined => (typeof v === 'string' ? v : undefined);

export class SettlementService {
  private readonly redis?: SettlementRedisLike;
  private readonly keyPrefix: string;
  private readonly kellyFraction: number;
  private readonly bankroll: number;
  private readonly defaultStake: number;
  private readonly logger: Pick<Console, 'warn'>;

  private readonly records = new Map<string, SettlementRecord>();
  private readonly byMatch = new Map<string, Set<string>>();

  constructor(opts: SettlementServiceOptions = {}) {
    this.redis = opts.redis;
    this.keyPrefix = opts.keyPrefix ?? 'ta:settlement:';
    this.kellyFraction = opts.kellyFraction ?? 0.25;
    this.bankroll = opts.bankroll ?? 1000;
    this.defaultStake = opts.defaultStake ?? 1;
    this.logger = opts.logger ?? console;
  }

  /** Record an emitted (Shadow) order at its entry odds, status PENDING. */
  recordEmittedOrder(order: HydratedOrderPayload): OrderSettlement {
    const emittedOdds = num(order.targetOdds);
    if (emittedOdds === undefined || emittedOdds <= 0) {
      throw new Error(`SettlementService: order ${order.orderId} has no valid targetOdds`);
    }

    const matchId = str(order.matchId) ?? `${order.playerAId}:${order.playerBId}`;
    const selection = str(order.selection) ?? str(order.side) ?? order.playerAId;
    const stake = this.resolveStake(order, emittedOdds);

    const record: SettlementRecord = {
      orderId: order.orderId,
      matchId,
      selection,
      emittedOdds,
      closingOdds: 0,
      clvEdgePct: 0,
      status: 'PENDING',
      pnl: 0,
      timestamp: Date.now(),
      stake,
    };

    this.records.set(record.orderId, record);
    let ids = this.byMatch.get(matchId);
    if (!ids) {
      ids = new Set<string>();
      this.byMatch.set(matchId, ids);
    }
    ids.add(record.orderId);

    void this.persist(record);
    return this.publicView(record);
  }

  /**
   * Reconcile all PENDING orders of a match: stamp the closing odds, compute
   * CLV, resolve WON/LOST/VOID and the PnL from the applied stake.
   *
   * `winner` is the winning selection, or "VOID" to void the match.
   */
  settleMatch(matchId: string, winner: string, closingOdds: number): OrderSettlement[] {
    const ids = this.byMatch.get(matchId);
    if (!ids || closingOdds <= 0) return [];

    const settled: OrderSettlement[] = [];
    for (const id of ids) {
      const rec = this.records.get(id);
      if (!rec || rec.status !== 'PENDING') continue;

      rec.closingOdds = closingOdds;
      // CLV: positive means we took better odds than the market close.
      rec.clvEdgePct = round((rec.emittedOdds / closingOdds - 1) * 100, 4);

      const status = this.resolveStatus(rec.selection, winner);
      rec.status = status;
      rec.pnl = this.resolvePnl(status, rec.stake, rec.emittedOdds);

      void this.persist(rec);
      settled.push(this.publicView(rec));
    }
    return settled;
  }

  /** Consolidated financial audit across all recorded orders. */
  getFinancialReport(): FinancialReport {
    let won = 0;
    let lost = 0;
    let voided = 0;
    let pending = 0;
    let totalPnl = 0;
    let totalStaked = 0;
    let clvSum = 0;
    let clvCount = 0;
    let btclCount = 0;

    for (const rec of this.records.values()) {
      switch (rec.status) {
        case 'WON':
          won++;
          break;
        case 'LOST':
          lost++;
          break;
        case 'VOID':
          voided++;
          break;
        case 'PENDING':
          pending++;
          break;
      }
      if (rec.status === 'WON' || rec.status === 'LOST') {
        totalPnl += rec.pnl;
        totalStaked += rec.stake;
        clvSum += rec.clvEdgePct;
        clvCount++;
        if (rec.clvEdgePct > 0) btclCount++;
      } else if (rec.status === 'VOID') {
        clvSum += rec.clvEdgePct;
        clvCount++;
        if (rec.clvEdgePct > 0) btclCount++;
      }
    }

    const decided = won + lost;
    const totalOrders = this.records.size;
    return {
      totalOrders,
      settledOrders: won + lost + voided,
      pending,
      won,
      lost,
      void: voided,
      winRatePct: decided === 0 ? 0 : round((won / decided) * 100, 2),
      yieldPct: totalStaked === 0 ? 0 : round((totalPnl / totalStaked) * 100, 2),
      totalPnl: round(totalPnl, 2),
      totalStaked: round(totalStaked, 2),
      avgClvEdgePct: clvCount === 0 ? 0 : round(clvSum / clvCount, 4),
      btclRatePct: clvCount === 0 ? 0 : round((btclCount / clvCount) * 100, 2),
    };
  }

  /** Look up a single settlement (e.g. for audit). */
  getSettlement(orderId: string): OrderSettlement | undefined {
    const rec = this.records.get(orderId);
    return rec ? this.publicView(rec) : undefined;
  }

  // --- internals ---

  private resolveStatus(selection: string, winner: string): SettlementStatus {
    if (winner === 'VOID') return 'VOID';
    return selection === winner ? 'WON' : 'LOST';
  }

  private resolvePnl(status: SettlementStatus, stake: number, emittedOdds: number): number {
    if (status === 'WON') return round(stake * (emittedOdds - 1), 2);
    if (status === 'LOST') return round(-stake, 2);
    return 0; // VOID / PENDING -> stake returned / not yet resolved
  }

  /**
   * Stake behind the PnL. Prefer the stake the risk module already applied
   * (`order.stake`); else size it with fractional Kelly from a model
   * probability (`order.pFair`) if present; else a flat default.
   */
  private resolveStake(order: HydratedOrderPayload, emittedOdds: number): number {
    const explicit = num(order.stake);
    if (explicit !== undefined && explicit > 0) return explicit;

    const kelly = this.fractionalKellyStake(order, emittedOdds);
    return kelly ?? this.defaultStake;
  }

  private fractionalKellyStake(
    order: HydratedOrderPayload,
    emittedOdds: number,
  ): number | undefined {
    const p = num(order.pFair) ?? num(order.modelProb);
    if (p === undefined || p <= 0 || p >= 1) return undefined;
    const b = emittedOdds - 1;
    if (b <= 0) return 0;
    const full = (b * p - (1 - p)) / b; // full-Kelly fraction of bankroll
    if (full <= 0) return 0; // no edge -> no stake
    return round(full * this.kellyFraction * this.bankroll, 2);
  }

  private publicView(rec: SettlementRecord): OrderSettlement {
    const { stake: _stake, ...settlement } = rec;
    return { ...settlement };
  }

  private async persist(rec: SettlementRecord): Promise<void> {
    if (!this.redis) return;
    try {
      await this.redis.set(this.keyPrefix + rec.orderId, JSON.stringify(rec));
    } catch (err) {
      this.logger.warn(
        `SettlementService: persist ${rec.orderId} failed: ${(err as Error).message}`,
      );
    }
  }
}
