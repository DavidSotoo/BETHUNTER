/**
 * MollyBetExecutor unit tests (MollyBet API mocked -- no network).
 *
 * Verify the order payload sent to the official API (slippage-floor price,
 * [ccy, stake], deduplicating request_uuid), the pre-send guards (Slippage
 * Guard, Circuit Breaker), shadow mode, and error mapping for the worker.
 */

import { describe, expect, it, vi } from 'vitest';
import { MollyBetExecutor, orderUuid } from '../src/executors/MollyBetExecutor.js';
import { MollyBetError, type MollyBetClient } from '../src/services/MollyBetClient.js';
import { CircuitBreaker } from '../src/services/CircuitBreaker.js';
import type { HydratedOrderPayload, PlayerFeatures } from '../src/types/features.js';

const silent = { info: vi.fn(), warn: vi.fn(), error: vi.fn() };

const feat = (id: string): PlayerFeatures => ({ id, elo: 1900, fatigueScore: 10, formRating: 2, updatedAt: Date.now() });

function order(over: Record<string, unknown> = {}): HydratedOrderPayload {
  return {
    orderId: 'ord-evt1-atp:alcaraz-c',
    matchId: 'evt1',
    playerAId: 'atp:alcaraz-c',
    playerBId: 'atp:sinner-j',
    selection: 'atp:alcaraz-c',
    targetOdds: 2.0,
    stake: 10,
    features: { playerA: feat('atp:alcaraz-c'), playerB: feat('atp:sinner-j') },
    hydratedAt: Date.now(),
    hydrationSource: { playerA: 'cache', playerB: 'cache' },
    hydrationLatencyMs: 1,
    ...over,
  };
}

function makeClient(over: Partial<Record<'openBetslip' | 'placeOrder', unknown>> = {}) {
  const openBetslip = vi.fn(async () => ({ betslip_id: 'slip-1', is_open: true }));
  const placeOrder = vi.fn(async () => ({ order_id: 42, status: 'open', closed: false, want_price: 1.96 }));
  const client = { openBetslip, placeOrder, ...over } as unknown as MollyBetClient;
  return { client, openBetslip: (client as any).openBetslip as ReturnType<typeof vi.fn>, placeOrder: (client as any).placeOrder as ReturnType<typeof vi.fn> };
}

function makeExecutor(client: MollyBetClient, extra: Record<string, unknown> = {}) {
  return new MollyBetExecutor({ client, currency: 'EUR', orderDurationSec: 15, logger: silent, ...extra });
}

describe('MollyBetExecutor -- placement', () => {
  it('opens a betslip then places the order with the slippage floor, and returns the API confirmation', async () => {
    const { client, openBetslip, placeOrder } = makeClient();
    const res = await makeExecutor(client).executeOrder(undefined, order());

    expect(openBetslip).toHaveBeenCalledWith({ sport: 'tennis', event_id: 'evt1', bet_type: 'for,h' });
    expect(placeOrder).toHaveBeenCalledTimes(1);
    const sent = placeOrder.mock.calls[0]![0] as Record<string, unknown>;
    expect(sent).toMatchObject({
      betslip_id: 'slip-1',
      price: 1.96, // 2.0 * (1 - 0.02)
      stake: ['EUR', 10],
      duration: 15,
      user_data: 'ord-evt1-atp:alcaraz-c',
    });
    expect(sent.request_uuid).toBe(orderUuid('ord-evt1-atp:alcaraz-c'));

    expect(res.status).toBe('EXECUTED');
    expect(res.success).toBe(true);
    expect(res.confirmation).toMatchObject({ order_id: 42, status: 'open' });
  });

  it('backs the away side with for,a', async () => {
    const { client, openBetslip } = makeClient();
    await makeExecutor(client).executeOrder(undefined, order({ selection: 'atp:sinner-j' }));
    expect(openBetslip).toHaveBeenCalledWith(expect.objectContaining({ bet_type: 'for,a' }));
  });

  it('reports the stake-weighted fill price when bets are already present', async () => {
    const { client } = makeClient({
      placeOrder: vi.fn(async () => ({
        order_id: 7,
        bets: [
          { got_price: 2.0, got_stake: ['EUR', 6] },
          { got_price: 1.9, got_stake: ['EUR', 4] },
          { got_price: null, got_stake: null },
        ],
      })),
    });
    const res = await makeExecutor(client).executeOrder(undefined, order());
    expect(res.executedOdds).toBeCloseTo(1.96, 6);
    expect(res.slippage).toBeCloseTo(0.02, 6);
  });

  it('derives a stable, UUID-shaped request_uuid from the order id', () => {
    const u = orderUuid('ord-1');
    expect(u).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
    expect(orderUuid('ord-1')).toBe(u);
    expect(orderUuid('ord-2')).not.toBe(u);
  });
});

describe('MollyBetExecutor -- Slippage Guard', () => {
  it('rejects before sending anything when the live price dropped beyond tolerance', async () => {
    const { client, openBetslip, placeOrder } = makeClient();
    const ex = makeExecutor(client, { liveOdds: () => 1.9 }); // (2.0-1.9)/2.0 = 5% > 2%
    const res = await ex.executeOrder(undefined, order());
    expect(res.status).toBe('REJECTED_SLIPPAGE');
    expect(res.success).toBe(false);
    expect(res.slippage).toBeCloseTo(0.05, 6);
    expect(openBetslip).not.toHaveBeenCalled();
    expect(placeOrder).not.toHaveBeenCalled();
  });

  it('proceeds when the live price is within tolerance or better', async () => {
    const { client, placeOrder } = makeClient();
    const ex = makeExecutor(client, { liveOdds: () => 1.98 });
    expect((await ex.executeOrder(undefined, order())).status).toBe('EXECUTED');
    expect(placeOrder).toHaveBeenCalledTimes(1);

    const better = makeClient();
    const ex2 = makeExecutor(better.client, { liveOdds: () => 2.2 });
    const res = await ex2.executeOrder(undefined, order());
    expect(res.status).toBe('EXECUTED');
  });

  it('honours a custom tolerance for the server-side floor', async () => {
    const { client, placeOrder } = makeClient();
    await makeExecutor(client, { maxSlippageTolerance: 0.05 }).executeOrder(undefined, order());
    expect((placeOrder.mock.calls[0]![0] as { price: number }).price).toBe(1.9);
  });
});

describe('MollyBetExecutor -- Circuit Breaker', () => {
  it('sends nothing while the breaker is OPEN', async () => {
    const breaker = new CircuitBreaker({ logger: silent });
    breaker.recordOutcome({ success: false, blocked: true }); // trips immediately
    const { client, openBetslip } = makeClient();
    const res = await makeExecutor(client, { breaker }).executeOrder(undefined, order());
    expect(res.status).toBe('FAILED_DOM');
    expect(res.error).toMatch(/circuit breaker OPEN/);
    expect(openBetslip).not.toHaveBeenCalled();
  });

  it('still sends the HALF-OPEN probe the worker already reserved a slot for', async () => {
    let now = 0;
    const breaker = new CircuitBreaker({ resetTimeoutMs: 1000, clock: () => now, logger: silent });
    breaker.recordOutcome({ success: false, blocked: true });
    now = 1500; // cooldown elapsed -> HALF_OPEN on next read
    expect(breaker.allowsRequest()).toBe(true);
    breaker.onExecuteStart(); // what OrderWorker does before executing

    const { client, placeOrder } = makeClient();
    const res = await makeExecutor(client, { breaker }).executeOrder(undefined, order());
    expect(res.status).toBe('EXECUTED');
    expect(placeOrder).toHaveBeenCalledTimes(1);
  });
});

describe('MollyBetExecutor -- shadow mode', () => {
  it('runs the checks but never calls the API', async () => {
    const { client, openBetslip, placeOrder } = makeClient();
    const res = await makeExecutor(client, { shadowMode: true }).executeOrder(undefined, order());
    expect(res.status).toBe('SHADOW_SIMULATED');
    expect(res.success).toBe(true);
    expect(openBetslip).not.toHaveBeenCalled();
    expect(placeOrder).not.toHaveBeenCalled();
  });

  it('still applies the slippage guard in shadow mode', async () => {
    const { client } = makeClient();
    const res = await makeExecutor(client, { shadowMode: true, liveOdds: () => 1.5 }).executeOrder(undefined, order());
    expect(res.status).toBe('REJECTED_SLIPPAGE');
  });
});

describe('MollyBetExecutor -- failures', () => {
  it('maps API errors to FAILED_DOM with words the worker block-heuristic recognises', async () => {
    const mk = (status: number, code: string) =>
      makeClient({ placeOrder: vi.fn(async () => { throw new MollyBetError(`POST /v1/orders/ failed: HTTP ${status} ${code}`, status, code); }) });

    const r429 = await makeExecutor(mk(429, 'throttled').client).executeOrder(undefined, order());
    expect(r429.status).toBe('FAILED_DOM');
    expect(r429.error).toMatch(/too many requests/);

    const r403 = await makeExecutor(mk(403, 'forbidden').client).executeOrder(undefined, order());
    expect(r403.error).toMatch(/403|forbidden/i);

    const r400 = await makeExecutor(mk(400, 'validation_error').client).executeOrder(undefined, order());
    expect(r400.error).toMatch(/validation_error/);
    expect(r400.error).not.toMatch(/too many requests|forbidden/i);
  });

  it('fails cleanly on transport errors and when the betslip is not open', async () => {
    const down = makeClient({ openBetslip: vi.fn(async () => { throw new Error('socket hang up'); }) });
    const r1 = await makeExecutor(down.client).executeOrder(undefined, order());
    expect(r1.status).toBe('FAILED_DOM');
    expect(r1.error).toMatch(/socket hang up/);

    const closed = makeClient({ openBetslip: vi.fn(async () => ({ betslip_id: 'x', is_open: false })) });
    const r2 = await makeExecutor(closed.client).executeOrder(undefined, order());
    expect(r2.status).toBe('FAILED_DOM');
    expect(r2.error).toMatch(/not open/);
    expect(closed.placeOrder).not.toHaveBeenCalled();
  });

  it.each([
    ['bad odds', { targetOdds: 1 }, /targetOdds/],
    ['non-numeric odds', { targetOdds: 'x' }, /targetOdds/],
    ['bad stake', { stake: 0 }, /stake/],
    ['missing matchId', { matchId: undefined }, /matchId/],
    ['unknown selection', { selection: 'atp:nobody' }, /selection/],
  ])('rejects an invalid order without calling the API (%s)', async (_n, over, re) => {
    const { client, openBetslip } = makeClient();
    const res = await makeExecutor(client).executeOrder(undefined, order(over));
    expect(res.status).toBe('FAILED_DOM');
    expect(res.error).toMatch(re);
    expect(openBetslip).not.toHaveBeenCalled();
  });
});
