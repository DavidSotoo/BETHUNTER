/**
 * SettlementService unit tests.
 *
 * Cover CLV (positive / negative), PnL resolution for WON / LOST / VOID, the
 * consolidated financial report, and Redis write-through persistence.
 */

import { describe, expect, it, vi } from 'vitest';
import { SettlementService } from '../src/services/SettlementService.js';
import type { HydratedOrderPayload, PlayerFeatures } from '../src/types/features.js';

const silent = { warn: () => {} };

/** Assert a single settled order came back, and return it (non-undefined). */
function only<T>(arr: T[]): T {
  expect(arr).toHaveLength(1);
  return arr[0] as T;
}

function feat(id: string): PlayerFeatures {
  return { id, elo: 1900, fatigueScore: 0, formRating: 0, updatedAt: 0 };
}

function order(opts: {
  orderId: string;
  matchId: string;
  selection: string;
  targetOdds: number;
  stake?: number;
  pFair?: number;
}): HydratedOrderPayload {
  return {
    orderId: opts.orderId,
    playerAId: 'pa',
    playerBId: 'pb',
    matchId: opts.matchId,
    selection: opts.selection,
    targetOdds: opts.targetOdds,
    ...(opts.stake !== undefined ? { stake: opts.stake } : {}),
    ...(opts.pFair !== undefined ? { pFair: opts.pFair } : {}),
    features: { playerA: feat('pa'), playerB: feat('pb') },
    hydratedAt: 0,
    hydrationSource: { playerA: 'cache', playerB: 'cache' },
    hydrationLatencyMs: 0,
  };
}

describe('SettlementService', () => {
  it('records an emitted order as PENDING at its entry odds', () => {
    const svc = new SettlementService({ logger: silent });
    const s = svc.recordEmittedOrder(
      order({ orderId: 'o1', matchId: 'm1', selection: 'home', targetOdds: 2.0, stake: 10 }),
    );
    expect(s.status).toBe('PENDING');
    expect(s.emittedOdds).toBe(2.0);
    expect(s.closingOdds).toBe(0);
    expect(s.pnl).toBe(0);
  });

  it('settles a WON bet with positive CLV and correct PnL', () => {
    const svc = new SettlementService({ logger: silent });
    // emitted 2.10 vs closing 2.00 -> CLV = ((2.10/2.00)-1)*100 = +5%
    svc.recordEmittedOrder(
      order({ orderId: 'o1', matchId: 'm1', selection: 'home', targetOdds: 2.1, stake: 10 }),
    );
    const s = only(svc.settleMatch('m1', 'home', 2.0));
    expect(s.status).toBe('WON');
    expect(s.clvEdgePct).toBeCloseTo(5, 4); // beat the closing line
    expect(s.pnl).toBeCloseTo(11, 2); // 10 * (2.10 - 1)
  });

  it('settles a LOST bet with negative CLV and loses the stake', () => {
    const svc = new SettlementService({ logger: silent });
    // emitted 1.90 vs closing 2.00 -> CLV = ((1.90/2.00)-1)*100 = -5%
    svc.recordEmittedOrder(
      order({ orderId: 'o1', matchId: 'm1', selection: 'away', targetOdds: 1.9, stake: 10 }),
    );
    const s = only(svc.settleMatch('m1', 'home', 2.0));
    expect(s.status).toBe('LOST');
    expect(s.clvEdgePct).toBeCloseTo(-5, 4);
    expect(s.pnl).toBeCloseTo(-10, 2);
  });

  it('voids a match with zero PnL', () => {
    const svc = new SettlementService({ logger: silent });
    svc.recordEmittedOrder(
      order({ orderId: 'o1', matchId: 'm1', selection: 'home', targetOdds: 2.0, stake: 10 }),
    );
    const s = only(svc.settleMatch('m1', 'VOID', 2.0));
    expect(s.status).toBe('VOID');
    expect(s.pnl).toBe(0);
  });

  it('only settles PENDING orders (idempotent re-settle is a no-op)', () => {
    const svc = new SettlementService({ logger: silent });
    svc.recordEmittedOrder(
      order({ orderId: 'o1', matchId: 'm1', selection: 'home', targetOdds: 2.0, stake: 10 }),
    );
    expect(svc.settleMatch('m1', 'home', 2.0)).toHaveLength(1);
    expect(svc.settleMatch('m1', 'home', 2.0)).toHaveLength(0);
  });

  it('produces a consolidated financial report', () => {
    const svc = new SettlementService({ logger: silent });
    svc.recordEmittedOrder(
      order({ orderId: 'o1', matchId: 'm1', selection: 'home', targetOdds: 2.1, stake: 10 }),
    );
    svc.recordEmittedOrder(
      order({ orderId: 'o2', matchId: 'm1', selection: 'away', targetOdds: 1.9, stake: 10 }),
    );
    svc.settleMatch('m1', 'home', 2.0); // o1 WON (+CLV), o2 LOST (-CLV)

    const r = svc.getFinancialReport();
    expect(r.totalOrders).toBe(2);
    expect(r.settledOrders).toBe(2);
    expect(r.won).toBe(1);
    expect(r.lost).toBe(1);
    expect(r.winRatePct).toBeCloseTo(50, 2);
    expect(r.totalPnl).toBeCloseTo(1, 2); // +11 - 10
    expect(r.totalStaked).toBeCloseTo(20, 2);
    expect(r.yieldPct).toBeCloseTo(5, 2); // 1 / 20
    expect(r.btclRatePct).toBeCloseTo(50, 2); // 1 of 2 beat the close
  });

  it('sizes the stake with fractional Kelly when no explicit stake is given', () => {
    // p=0.6, odds=2.0 -> b=1, full Kelly f=(1*0.6-0.4)/1=0.2
    // stake = 0.2 * kellyFraction(0.25) * bankroll(1000) = 50
    const svc = new SettlementService({
      logger: silent,
      kellyFraction: 0.25,
      bankroll: 1000,
    });
    svc.recordEmittedOrder(
      order({ orderId: 'o1', matchId: 'm1', selection: 'home', targetOdds: 2.0, pFair: 0.6 }),
    );
    const s = only(svc.settleMatch('m1', 'home', 2.0));
    expect(s.pnl).toBeCloseTo(50, 2); // 50 * (2.0 - 1)
  });

  it('writes each record through to Redis under ta:settlement:<orderId>', async () => {
    const set = vi.fn(async () => 'OK');
    const svc = new SettlementService({ logger: silent, redis: { set } });
    svc.recordEmittedOrder(
      order({ orderId: 'o1', matchId: 'm1', selection: 'home', targetOdds: 2.0, stake: 10 }),
    );
    // persistence is fire-and-forget; let the microtask settle
    await Promise.resolve();
    expect(set).toHaveBeenCalledWith('ta:settlement:o1', expect.stringContaining('"orderId":"o1"'));
  });
});
