/**
 * MetricsCollector unit tests.
 *
 * Verify the aggregate maths: percentiles (p50/p95/p99), success / slippage /
 * failure rates, execution counters, and circuit-breaker transition counters.
 */

import { describe, expect, it } from 'vitest';
import { MetricsCollector, OrderMetricSample, OrderOutcome } from '../src/telemetry/MetricsCollector.js';
import { CircuitState } from '../src/services/CircuitBreaker.js';

const silent = { info: () => {} };

function sample(overrides: Partial<OrderMetricSample> = {}): OrderMetricSample {
  return {
    orderId: 'o',
    outcome: 'EXECUTED',
    shadow: false,
    hydrationMs: 1,
    browserAcquisitionMs: 1,
    domCheckMs: 1,
    totalE2EMs: 1,
    circuitState: CircuitState.CLOSED,
    ...overrides,
  };
}

describe('MetricsCollector', () => {
  it('computes nearest-rank percentiles for a latency series', () => {
    const m = new MetricsCollector({ logger: silent });
    // totalE2E latencies 1..100.
    for (let i = 1; i <= 100; i++) {
      m.record(sample({ orderId: `o${i}`, totalE2EMs: i }));
    }
    const e2e = m.exportMetricsSummary().latencyMs.totalE2E;
    expect(e2e.count).toBe(100);
    expect(e2e.p50).toBe(50);
    expect(e2e.p95).toBe(95);
    expect(e2e.p99).toBe(99);
    expect(e2e.max).toBe(100);
    expect(e2e.avg).toBe(50.5);
  });

  it('counts outcomes and computes success / slippage / failure rates', () => {
    const m = new MetricsCollector({ logger: silent });
    const outcomes: OrderOutcome[] = [
      'EXECUTED',
      'EXECUTED',
      'EXECUTED',
      'EXECUTED',
      'EXECUTED',
      'EXECUTED', // 6 executed
      'REJECTED_SLIPPAGE',
      'REJECTED_SLIPPAGE', // 2 slippage
      'FAILED_DOM', // 1 DOM/WAF failure
      'SHADOW_SIMULATED', // 1 shadow
    ];
    for (const o of outcomes) {
      m.record(sample({ outcome: o, shadow: o === 'SHADOW_SIMULATED' }));
    }

    const s = m.exportMetricsSummary();
    expect(s.counters.totalOrders).toBe(10);
    expect(s.counters.executed).toBe(6);
    expect(s.counters.rejectedSlippage).toBe(2);
    expect(s.counters.failedDomOrWaf).toBe(1);
    expect(s.counters.shadowInvocations).toBe(1);

    // success = (executed 6 + shadow 1) / 10 = 70%
    expect(s.rates.successPct).toBe(70);
    // slippage = 2 / 10 = 20%
    expect(s.rates.slippagePct).toBe(20);
    // failure = (DOM/WAF 1 + pipeline 0) / 10 = 10%
    expect(s.rates.failurePct).toBe(10);
  });

  it('counts a WAF/403 block as a DOM/WAF failure even on a pipeline error', () => {
    const m = new MetricsCollector({ logger: silent });
    m.record(sample({ outcome: 'PIPELINE_ERROR', blocked: true }));
    const s = m.exportMetricsSummary();
    expect(s.counters.blocked).toBe(1);
    expect(s.counters.pipelineErrors).toBe(1);
    expect(s.counters.failedDomOrWaf).toBe(1); // block lifted it into DOM/WAF
  });

  it('does not double-count a FAILED_DOM that is also blocked', () => {
    const m = new MetricsCollector({ logger: silent });
    m.record(sample({ outcome: 'FAILED_DOM', blocked: true }));
    const s = m.exportMetricsSummary();
    expect(s.counters.failedDomOrWaf).toBe(1);
    expect(s.counters.blocked).toBe(1);
  });

  it('tracks circuit-breaker transition counts and current state', () => {
    const m = new MetricsCollector({ logger: silent });
    m.recordCircuitTransition(CircuitState.CLOSED, CircuitState.OPEN);
    m.recordCircuitTransition(CircuitState.OPEN, CircuitState.HALF_OPEN);
    m.recordCircuitTransition(CircuitState.HALF_OPEN, CircuitState.OPEN);
    m.recordCircuitTransition(CircuitState.OPEN, CircuitState.HALF_OPEN);
    m.recordCircuitTransition(CircuitState.HALF_OPEN, CircuitState.CLOSED);

    const s = m.exportMetricsSummary();
    expect(s.circuit.transitions.toOpen).toBe(2);
    expect(s.circuit.transitions.toHalfOpen).toBe(2);
    expect(s.circuit.transitions.toClosed).toBe(1);
    expect(s.circuit.currentState).toBe(CircuitState.CLOSED);
  });

  it('omits the DOM-check series when no execution was attempted', () => {
    const m = new MetricsCollector({ logger: silent });
    m.record(sample({ outcome: 'NO_OP', domCheckMs: undefined }));
    m.record(sample({ outcome: 'EXECUTED', domCheckMs: 42 }));
    const s = m.exportMetricsSummary();
    expect(s.latencyMs.domCheck.count).toBe(1); // only the executed order
    expect(s.latencyMs.domCheck.p50).toBe(42);
  });

  it('emits a structured JSON summary log', () => {
    const lines: string[] = [];
    const m = new MetricsCollector({ logger: { info: (l: string) => lines.push(l) } });
    m.record(sample());
    m.logSummary();
    const parsed = JSON.parse(lines[lines.length - 1] as string);
    expect(parsed.type).toBe('gateway.metrics.summary');
    expect(parsed.counters.totalOrders).toBe(1);
    expect(parsed.rates.successPct).toBe(100);
  });

  it('reset() clears counters and transitions', () => {
    const m = new MetricsCollector({ logger: silent });
    m.record(sample());
    m.recordCircuitTransition(CircuitState.CLOSED, CircuitState.OPEN);
    m.reset();
    const s = m.exportMetricsSummary();
    expect(s.counters.totalOrders).toBe(0);
    expect(s.circuit.transitions.toOpen).toBe(0);
    expect(s.circuit.currentState).toBe(CircuitState.CLOSED);
  });
});
