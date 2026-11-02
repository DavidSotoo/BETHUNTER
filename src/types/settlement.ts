/**
 * Settlement & CLV (Closing Line Value) contracts.
 *
 * These drive the quantitative audit of Shadow-Mode orders: we record the odds
 * we would have taken, reconcile them against the market's closing odds and the
 * actual result, and measure whether the strategy beats the closing line before
 * any real capital is committed.
 */

export type SettlementStatus = 'PENDING' | 'WON' | 'LOST' | 'VOID';

export interface OrderSettlement {
  orderId: string;
  matchId: string;
  /** The competitor / outcome the order backed. */
  selection: string;
  /** Odds the order was emitted at (entry). */
  emittedOdds: number;
  /** Market odds at close (0 until settled). */
  closingOdds: number;
  /** Closing Line Value edge: ((emittedOdds / closingOdds) - 1) * 100. */
  clvEdgePct: number;
  status: SettlementStatus;
  /** Profit/loss in stake units (0 until settled / for VOID). */
  pnl: number;
  /** Epoch ms when the order was recorded. */
  timestamp: number;
}

/** Consolidated financial audit across recorded orders. */
export interface FinancialReport {
  totalOrders: number;
  settledOrders: number;
  pending: number;
  won: number;
  lost: number;
  void: number;
  /** WON / (WON + LOST) * 100. */
  winRatePct: number;
  /** totalPnl / totalStaked * 100. */
  yieldPct: number;
  totalPnl: number;
  /** Turnover across settled, non-void orders. */
  totalStaked: number;
  /** Mean CLV edge across settled orders. */
  avgClvEdgePct: number;
  /** "Beat The Closing Line": % of settled orders with positive CLV. */
  btclRatePct: number;
}
