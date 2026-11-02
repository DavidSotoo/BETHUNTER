/**
 * MetricsCollector -- operational & latency telemetry for the Execution Gateway.
 *
 * It aggregates, per order cycle:
 *   - latency series per leg (hydration, browser acquisition, DOM check, E2E),
 *     each summarised as avg / p50 / p95 / p99 / max over a bounded window;
 *   - execution counters (total, executed, slippage-rejected, DOM/WAF failures,
 *     shadow invocations, pipeline errors);
 *   - circuit-breaker state and transition counts.
 *
 * It exposes `exportMetricsSummary()` for a full snapshot and emits structured
 * JSON log lines (type-tagged, flat-ish) ready for Datadog / Elasticsearch
 * ingestion. The hot path (`record`) is O(1): percentiles are computed only on
 * export, never per order.
 */

import { CircuitState } from '../services/CircuitBreaker.js';

/** The outcome of one order cycle, as classified by the worker. */
export type OrderOutcome =
  | 'EXECUTED'
  | 'REJECTED_SLIPPAGE'
  | 'FAILED_DOM'
  | 'SHADOW_SIMULATED'
  | 'PIPELINE_ERROR'
  | 'NO_OP';

/** One order's measurements, handed to `record` at the end of its cycle. */
export interface OrderMetricSample {
  orderId: string;
  outcome: OrderOutcome;
  /** WAF / Captcha / 403 detected. */
  blocked?: boolean;
  /** Processed while SHADOW_MODE was on. */
  shadow: boolean;
  hydrationMs: number;
  browserAcquisitionMs: number;
  /** The executor (DOM) leg; absent when no execution was attempted. */
  domCheckMs?: number;
  totalE2EMs: number;
  circuitState: CircuitState;
}

export interface LatencySnapshot {
  count: number;
  lifetime: number;
  avg: number;
  p50: number;
  p95: number;
  p99: number;
  max: number;
}

export interface MetricsSummary {
  timestamp: string;
  uptimeMs: number;
  counters: {
    totalOrders: number;
    executed: number;
    rejectedSlippage: number;
    failedDomOrWaf: number;
    blocked: number;
    shadowInvocations: number;
    pipelineErrors: number;
    noOp: number;
  };
  rates: {
    /** (executed + shadow simulated) / total. */
    successPct: number;
    /** slippage rejections / total. */
    slippagePct: number;
    /** (DOM/WAF failures + pipeline errors) / total. */
    failurePct: number;
  };
  latencyMs: {
    hydration: LatencySnapshot;
    browserAcquisition: LatencySnapshot;
    domCheck: LatencySnapshot;
    totalE2E: LatencySnapshot;
  };
  circuit: {
    currentState: CircuitState;
    transitions: { toClosed: number; toOpen: number; toHalfOpen: number };
  };
}

export interface MetricsCollectorOptions {
  /** Rolling window size per latency series. Default 4096. */
  windowSize?: number;
  logger?: Pick<Console, 'info'>;
  /** Emit a structured JSON log line on every `record`. Default false. */
  emitPerOrderLog?: boolean;
}

/** Nearest-rank percentile over an already-sorted ascending array. */
function percentile(sorted: number[], p: number): number {
  if (sorted.length === 0) return 0;
  const rank = Math.ceil((p / 100) * sorted.length);
  const idx = Math.min(sorted.length - 1, Math.max(0, rank - 1));
  return sorted[idx] as number;
}

/** Fixed-size ring buffer of latency samples; summarised lazily on export. */
class LatencySeries {
  private readonly buf: number[];
  private idx = 0;
  private len = 0;
  private lifetime = 0;

  constructor(private readonly cap: number) {
    this.buf = new Array<number>(cap);
  }

  add(v: number): void {
    this.buf[this.idx] = v;
    this.idx = (this.idx + 1) % this.cap;
    if (this.len < this.cap) this.len++;
    this.lifetime++;
  }

  snapshot(): LatencySnapshot {
    const window = this.len < this.cap ? this.buf.slice(0, this.len) : this.buf.slice();
    if (window.length === 0) {
      return { count: 0, lifetime: this.lifetime, avg: 0, p50: 0, p95: 0, p99: 0, max: 0 };
    }
    const sorted = [...window].sort((a, b) => a - b);
    const sum = window.reduce((a, b) => a + b, 0);
    return {
      count: window.length,
      lifetime: this.lifetime,
      avg: round(sum / window.length),
      p50: round(percentile(sorted, 50)),
      p95: round(percentile(sorted, 95)),
      p99: round(percentile(sorted, 99)),
      max: round(sorted[sorted.length - 1] as number),
    };
  }
}

const round = (n: number): number => Math.round(n * 1000) / 1000;
const pct = (num: number, den: number): number => (den === 0 ? 0 : round((num / den) * 100));

export class MetricsCollector {
  private readonly logger: Pick<Console, 'info'>;
  private readonly emitPerOrderLog: boolean;
  private readonly startedAt = Date.now();

  private readonly counters = {
    totalOrders: 0,
    executed: 0,
    rejectedSlippage: 0,
    failedDomOrWaf: 0,
    blocked: 0,
    shadowInvocations: 0,
    pipelineErrors: 0,
    noOp: 0,
  };

  private readonly transitions = { toClosed: 0, toOpen: 0, toHalfOpen: 0 };
  private currentState: CircuitState = CircuitState.CLOSED;

  private readonly hydration: LatencySeries;
  private readonly browserAcquisition: LatencySeries;
  private readonly domCheck: LatencySeries;
  private readonly totalE2E: LatencySeries;

  constructor(opts: MetricsCollectorOptions = {}) {
    const cap = opts.windowSize ?? 4096;
    this.logger = opts.logger ?? console;
    this.emitPerOrderLog = opts.emitPerOrderLog ?? false;
    this.hydration = new LatencySeries(cap);
    this.browserAcquisition = new LatencySeries(cap);
    this.domCheck = new LatencySeries(cap);
    this.totalE2E = new LatencySeries(cap);
  }

  /** Record one completed order cycle. O(1). */
  record(sample: OrderMetricSample): void {
    this.counters.totalOrders++;

    switch (sample.outcome) {
      case 'EXECUTED':
        this.counters.executed++;
        break;
      case 'REJECTED_SLIPPAGE':
        this.counters.rejectedSlippage++;
        break;
      case 'SHADOW_SIMULATED':
        this.counters.shadowInvocations++;
        break;
      case 'FAILED_DOM':
        this.counters.failedDomOrWaf++;
        break;
      case 'PIPELINE_ERROR':
        this.counters.pipelineErrors++;
        break;
      case 'NO_OP':
        this.counters.noOp++;
        break;
    }

    // A WAF/403 block is a DOM/WAF failure regardless of which leg raised it.
    if (sample.blocked) {
      this.counters.blocked++;
      if (sample.outcome !== 'FAILED_DOM') this.counters.failedDomOrWaf++;
    }

    this.hydration.add(sample.hydrationMs);
    this.browserAcquisition.add(sample.browserAcquisitionMs);
    this.totalE2E.add(sample.totalE2EMs);
    if (sample.domCheckMs !== undefined) this.domCheck.add(sample.domCheckMs);

    this.currentState = sample.circuitState;

    if (this.emitPerOrderLog) {
      this.logger.info(JSON.stringify(this.formatOrderLog(sample)));
    }
  }

  /** Record a circuit-breaker state change (wire to breaker.onStateChange). */
  recordCircuitTransition(_from: CircuitState, to: CircuitState): void {
    if (to === CircuitState.CLOSED) this.transitions.toClosed++;
    else if (to === CircuitState.OPEN) this.transitions.toOpen++;
    else if (to === CircuitState.HALF_OPEN) this.transitions.toHalfOpen++;
    this.currentState = to;
  }

  /** Full performance snapshot of the gateway. */
  exportMetricsSummary(): MetricsSummary {
    const c = this.counters;
    const total = c.totalOrders;
    return {
      timestamp: new Date().toISOString(),
      uptimeMs: Date.now() - this.startedAt,
      counters: { ...c },
      rates: {
        successPct: pct(c.executed + c.shadowInvocations, total),
        slippagePct: pct(c.rejectedSlippage, total),
        failurePct: pct(c.failedDomOrWaf + c.pipelineErrors, total),
      },
      latencyMs: {
        hydration: this.hydration.snapshot(),
        browserAcquisition: this.browserAcquisition.snapshot(),
        domCheck: this.domCheck.snapshot(),
        totalE2E: this.totalE2E.snapshot(),
      },
      circuit: {
        currentState: this.currentState,
        transitions: { ...this.transitions },
      },
    };
  }

  /** Summary as a single-line structured JSON log (Datadog/ES friendly). */
  toStructuredLog(): string {
    return JSON.stringify({ type: 'gateway.metrics.summary', ...this.exportMetricsSummary() });
  }

  /** Emit the summary through the logger. */
  logSummary(): void {
    this.logger.info(this.toStructuredLog());
  }

  /** Flat per-order record for log ingestion. */
  formatOrderLog(sample: OrderMetricSample): Record<string, unknown> {
    return {
      type: 'gateway.order',
      ts: new Date().toISOString(),
      order_id: sample.orderId,
      outcome: sample.outcome,
      shadow: sample.shadow,
      blocked: Boolean(sample.blocked),
      circuit_state: sample.circuitState,
      latency_ms: {
        hydration: round(sample.hydrationMs),
        browser_acquisition: round(sample.browserAcquisitionMs),
        dom_check: sample.domCheckMs === undefined ? null : round(sample.domCheckMs),
        total_e2e: round(sample.totalE2EMs),
      },
    };
  }

  /** Test/ops hook: wipe all state. */
  reset(): void {
    const keys = Object.keys(this.counters) as (keyof typeof this.counters)[];
    for (const k of keys) this.counters[k] = 0;
    this.transitions.toClosed = 0;
    this.transitions.toOpen = 0;
    this.transitions.toHalfOpen = 0;
    this.currentState = CircuitState.CLOSED;
  }
}
