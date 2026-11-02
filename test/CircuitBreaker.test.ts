/**
 * CircuitBreaker unit tests.
 *
 * The headline test walks the full lifecycle CLOSED -> OPEN -> HALF-OPEN ->
 * CLOSED. Supporting tests cover each trigger (consecutive failures, latency,
 * explicit block), the HALF-OPEN probe limit, and a failed probe re-opening.
 *
 * Time is driven through an injected `clock`, so the OPEN cooldown is exercised
 * deterministically without fake timers.
 */

import { beforeEach, describe, expect, it, vi } from 'vitest';
import { CircuitBreaker, CircuitState } from '../src/services/CircuitBreaker.js';

const silent = { info: vi.fn(), warn: vi.fn() };

/** A breaker with a controllable clock; returns the breaker and a time cursor. */
function makeBreaker(overrides = {}) {
  let now = 0;
  const breaker = new CircuitBreaker({
    failureThreshold: 3,
    resetTimeoutMs: 30_000,
    latencyThresholdMs: 500,
    clock: () => now,
    logger: silent,
    ...overrides,
  });
  return {
    breaker,
    advance: (ms: number) => {
      now += ms;
    },
  };
}

describe('CircuitBreaker', () => {
  beforeEach(() => {
    silent.info.mockClear();
    silent.warn.mockClear();
  });

  it('starts CLOSED and allows requests', () => {
    const { breaker } = makeBreaker();
    expect(breaker.getState()).toBe(CircuitState.CLOSED);
    expect(breaker.allowsRequest()).toBe(true);
  });

  it('walks CLOSED -> OPEN -> HALF-OPEN -> CLOSED', () => {
    const { breaker, advance } = makeBreaker();

    // CLOSED -> OPEN: three consecutive failures hit the threshold.
    breaker.recordOutcome({ success: false });
    breaker.recordOutcome({ success: false });
    expect(breaker.getState()).toBe(CircuitState.CLOSED); // 2 < 3
    breaker.recordOutcome({ success: false });
    expect(breaker.getState()).toBe(CircuitState.OPEN);
    expect(breaker.allowsRequest()).toBe(false); // rejects while OPEN

    // OPEN -> HALF-OPEN: only after the reset timeout elapses.
    advance(29_999);
    expect(breaker.allowsRequest()).toBe(false);
    advance(1);
    expect(breaker.allowsRequest()).toBe(true);
    expect(breaker.getState()).toBe(CircuitState.HALF_OPEN);

    // HALF-OPEN -> CLOSED: a successful probe closes the circuit.
    breaker.onExecuteStart();
    breaker.recordOutcome({ success: true, latencyMs: 10 });
    expect(breaker.getState()).toBe(CircuitState.CLOSED);
    expect(breaker.allowsRequest()).toBe(true);
  });

  it('re-opens when the HALF-OPEN probe fails', () => {
    const { breaker, advance } = makeBreaker();
    breaker.recordOutcome({ success: false });
    breaker.recordOutcome({ success: false });
    breaker.recordOutcome({ success: false });
    expect(breaker.getState()).toBe(CircuitState.OPEN);

    advance(30_000);
    expect(breaker.allowsRequest()).toBe(true); // now HALF-OPEN
    breaker.onExecuteStart();
    breaker.recordOutcome({ success: false }); // failed probe
    expect(breaker.getState()).toBe(CircuitState.OPEN);
    expect(breaker.allowsRequest()).toBe(false);
  });

  it('resets the failure streak on a success while CLOSED', () => {
    const { breaker } = makeBreaker();
    breaker.recordOutcome({ success: false });
    breaker.recordOutcome({ success: false });
    breaker.recordOutcome({ success: true }); // clears the streak
    breaker.recordOutcome({ success: false });
    breaker.recordOutcome({ success: false });
    expect(breaker.getState()).toBe(CircuitState.CLOSED); // only 2 since reset
  });

  it('treats an E2E latency over the threshold as a failure', () => {
    const { breaker } = makeBreaker();
    breaker.recordOutcome({ success: true, latencyMs: 600 });
    breaker.recordOutcome({ success: true, latencyMs: 700 });
    expect(breaker.getState()).toBe(CircuitState.CLOSED);
    breaker.recordOutcome({ success: true, latencyMs: 501 });
    expect(breaker.getState()).toBe(CircuitState.OPEN); // 3 slow calls
  });

  it('trips immediately on an explicit block, regardless of the streak', () => {
    const { breaker } = makeBreaker();
    expect(breaker.getState()).toBe(CircuitState.CLOSED);
    breaker.recordOutcome({ success: false, blocked: true }); // WAF/Captcha/403
    expect(breaker.getState()).toBe(CircuitState.OPEN);
    expect(breaker.getStats().lastReason).toMatch(/block/i);
  });

  it('limits concurrent probes in HALF-OPEN to halfOpenMaxCalls', () => {
    const { breaker, advance } = makeBreaker({ halfOpenMaxCalls: 1 });
    breaker.recordOutcome({ success: false });
    breaker.recordOutcome({ success: false });
    breaker.recordOutcome({ success: false });
    advance(30_000);

    expect(breaker.allowsRequest()).toBe(true); // first probe allowed
    breaker.onExecuteStart(); // slot reserved, probe in flight
    expect(breaker.allowsRequest()).toBe(false); // second probe refused
  });

  it('reset() forces the circuit back to CLOSED', () => {
    const { breaker } = makeBreaker();
    breaker.recordOutcome({ success: false, blocked: true });
    expect(breaker.getState()).toBe(CircuitState.OPEN);
    breaker.reset();
    expect(breaker.getState()).toBe(CircuitState.CLOSED);
    expect(breaker.allowsRequest()).toBe(true);
  });
});
