/**
 * OrderExecutor unit tests.
 *
 * Playwright is mocked: `page.waitForSelector` yields an element whose
 * `textContent` is the live odds, and `page.click` commits the bet. The tests
 * cover the three required paths -- successful execution, slippage abort, and a
 * DOM selector timeout -- plus the shadow-mode simulation.
 */

import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { Page } from 'playwright';
import { OrderExecutor } from '../src/executors/OrderExecutor.js';
import type { HydratedOrderPayload, PlayerFeatures } from '../src/types/features.js';

const silent = { info: vi.fn(), warn: vi.fn(), error: vi.fn() };

function feat(id: string): PlayerFeatures {
  return { id, elo: 1900, fatigueScore: 10, formRating: 2, updatedAt: Date.now() };
}

/** A hydrated order carrying the requested odds the guard checks against. */
function order(targetOdds = 2.0): HydratedOrderPayload {
  return {
    orderId: 'ord-exec-1',
    playerAId: 'atp:alcaraz-c',
    playerBId: 'atp:sinner-j',
    targetOdds,
    features: { playerA: feat('atp:alcaraz-c'), playerB: feat('atp:sinner-j') },
    hydratedAt: Date.now(),
    hydrationSource: { playerA: 'cache', playerB: 'cache' },
    hydrationLatencyMs: 1,
  };
}

/** Build a mocked Playwright Page plus handles to assert on. */
function mockPage(opts: {
  oddsText?: string | null;
  selectorThrows?: boolean;
  clickThrows?: boolean;
}) {
  const click = vi.fn(async () => {
    if (opts.clickThrows) throw new Error('place-bet click: Timeout 3000ms exceeded');
  });
  const waitForSelector = vi.fn(async (_sel: string, _o?: unknown) => {
    if (opts.selectorThrows) {
      throw new Error('Timeout 3000ms exceeded waiting for selector');
    }
    return { textContent: async () => opts.oddsText ?? null };
  });
  const page = { waitForSelector, click } as unknown as Page;
  return { page, click, waitForSelector };
}

function makeExecutor(overrides = {}) {
  return new OrderExecutor({
    maxSlippageTolerance: 0.02,
    domSelectorTimeoutMs: 3000,
    shadowMode: false,
    oddsSelector: '.odds',
    placeBetSelector: '.place-bet',
    logger: silent,
    ...overrides,
  });
}

describe('OrderExecutor', () => {
  beforeEach(() => {
    silent.info.mockClear();
    silent.warn.mockClear();
    silent.error.mockClear();
  });

  it('executes when the live odds are within tolerance', async () => {
    // requested 2.00, live 1.98 -> slippage 1% (<= 2%) -> EXECUTED.
    const { page, click } = mockPage({ oddsText: '1.98' });
    const result = await makeExecutor().executeOrder(page, order(2.0));

    expect(result.status).toBe('EXECUTED');
    expect(result.success).toBe(true);
    expect(result.executedOdds).toBe(1.98);
    expect(result.requestedOdds).toBe(2.0);
    expect(result.slippage).toBeCloseTo(0.01, 5);
    expect(click).toHaveBeenCalledTimes(1); // the bet was placed
  });

  it('aborts on slippage beyond tolerance and never clicks', async () => {
    // requested 2.00, live 1.90 -> slippage 5% (> 2%) -> REJECTED_SLIPPAGE.
    const { page, click } = mockPage({ oddsText: '1.90' });
    const result = await makeExecutor().executeOrder(page, order(2.0));

    expect(result.status).toBe('REJECTED_SLIPPAGE');
    expect(result.success).toBe(false);
    expect(result.executedOdds).toBe(1.9);
    expect(result.slippage).toBeCloseTo(0.05, 5);
    expect(click).not.toHaveBeenCalled(); // guard fired BEFORE the click
  });

  it('returns FAILED_DOM when the odds selector times out', async () => {
    const { page, click } = mockPage({ selectorThrows: true });
    const result = await makeExecutor().executeOrder(page, order(2.0));

    expect(result.status).toBe('FAILED_DOM');
    expect(result.success).toBe(false);
    expect(result.error).toMatch(/timeout/i);
    expect(click).not.toHaveBeenCalled();
  });

  it('favourable odds (live > requested) execute with negative slippage', async () => {
    const { page, click } = mockPage({ oddsText: '2.20' });
    const result = await makeExecutor().executeOrder(page, order(2.0));

    expect(result.status).toBe('EXECUTED');
    expect(result.slippage).toBeLessThan(0);
    expect(click).toHaveBeenCalledTimes(1);
  });

  it('flags unparseable odds as FAILED_DOM', async () => {
    const { page, click } = mockPage({ oddsText: 'suspended' });
    const result = await makeExecutor().executeOrder(page, order(2.0));

    expect(result.status).toBe('FAILED_DOM');
    expect(click).not.toHaveBeenCalled();
  });

  it('simulates in shadow mode without touching the DOM', async () => {
    const { page, click, waitForSelector } = mockPage({ oddsText: '1.50' });
    const executor = makeExecutor({ shadowMode: true });
    const result = await executor.executeOrder(page, order(2.0));

    expect(result.status).toBe('SHADOW_SIMULATED');
    expect(result.success).toBe(true);
    expect(waitForSelector).not.toHaveBeenCalled(); // no DOM read
    expect(click).not.toHaveBeenCalled(); // no bet placed
  });
});
