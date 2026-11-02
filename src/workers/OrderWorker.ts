/**
 * OrderWorker -- drains the Redis `ApprovedOrder` queue and runs each order
 * through the execution pipeline:
 *
 *   BRPOP ApprovedOrder
 *     -> FeatureLoader.hydrate        (live competitor vectors)
 *     -> BrowserManager.acquire       (isolated Playwright context)
 *     -> navigate                     (load the execution surface)
 *     -> OrderExecutor.executeOrder   (slippage guard + place-bet, DOM)
 *
 * SHADOW MODE
 *
 * When `config.shadowMode` is on, the OrderExecutor simulates the slippage
 * verification and never clicks; the rest of the pipeline still runs so the
 * latency breakdown stays representative.
 *
 * CIRCUIT BREAKER
 *
 * A CircuitBreaker guards the pipeline. While OPEN the worker pauses
 * consumption (orders stay queued). Each order's outcome is fed back:
 *   - a thrown pipeline error, or an executor FAILED_DOM  -> breaker failure
 *   - an E2E latency over the threshold                   -> breaker failure
 *   - an explicit WAF/Captcha/403 block                   -> immediate trip
 *   - EXECUTED / SHADOW_SIMULATED / REJECTED_SLIPPAGE     -> healthy (no trip)
 * REJECTED_SLIPPAGE is a deliberate decision, so it never trips the breaker.
 *
 * Every step is timed and each order ends with a structured telemetry block.
 */

import { Redis } from 'ioredis';
import { performance } from 'node:perf_hooks';
import { FeatureLoader } from '../services/FeatureLoader.js';
import { BrowserManager, BrowserSession } from '../browser/BrowserManager.js';
import { CircuitBreaker, CircuitState } from '../services/CircuitBreaker.js';
import { OrderExecutor } from '../executors/OrderExecutor.js';
import { OrderExecutionResult, OrderExecutionStatus } from '../types/execution.js';
import { config as defaultConfig, AppConfig } from '../config/index.js';
import {
  ApprovedOrder,
  HydratedOrderPayload,
  HydrationSource,
} from '../types/features.js';

export interface OrderWorkerOptions {
  redis: Redis;
  loader: FeatureLoader;
  browser: BrowserManager;
  /** Places the order (with slippage guard). Omitted = pipeline runs, no bet. */
  executor?: OrderExecutor;
  /** Guards the pipeline. Defaults to one built from config. */
  breaker?: CircuitBreaker;
  /** Defaults are taken from the shared config singleton. */
  config?: AppConfig;
  logger?: Pick<Console, 'info' | 'warn' | 'error'>;
}

/** Per-order latency breakdown, in milliseconds. */
interface OrderTelemetry {
  orderId: string;
  hydrationLatencyMs: number;
  hydrationSource: HydrationSource;
  browserAcquisitionMs: number;
  totalE2EMs: number;
  shadow: boolean;
  circuitState: CircuitState;
  execution?: OrderExecutionResult;
}

const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

/**
 * Collapse the two per-player sources into one headline label:
 * a single fallback taints the order; otherwise a single live read means the
 * order touched the store; only all-cache counts as a pure cache order.
 */
function aggregateSource(s: {
  playerA: HydrationSource;
  playerB: HydrationSource;
}): HydrationSource {
  if (s.playerA === 'fallback' || s.playerB === 'fallback') return 'fallback';
  if (s.playerA === 'live' || s.playerB === 'live') return 'live';
  return 'cache';
}

/** Heuristic: does this error look like a WAF / Captcha / 403 wall? */
function isBlockError(msg: string | undefined): boolean {
  if (!msg) return false;
  return /\b403\b|forbidden|captcha|\bwaf\b|access denied|blocked|rate.?limit|too many requests/i.test(
    msg,
  );
}

export class OrderWorker {
  private readonly redis: Redis;
  private readonly loader: FeatureLoader;
  private readonly browser: BrowserManager;
  private readonly executor?: OrderExecutor;
  private readonly breaker: CircuitBreaker;
  private readonly config: AppConfig;
  private readonly logger: Pick<Console, 'info' | 'warn' | 'error'>;
  private running = false;

  constructor(opts: OrderWorkerOptions) {
    this.redis = opts.redis;
    this.loader = opts.loader;
    this.browser = opts.browser;
    this.executor = opts.executor;
    this.config = opts.config ?? defaultConfig;
    this.logger = opts.logger ?? console;
    this.breaker =
      opts.breaker ??
      new CircuitBreaker({
        name: 'order-exec',
        failureThreshold: this.config.circuitBreaker.failureThreshold,
        resetTimeoutMs: this.config.circuitBreaker.resetTimeoutMs,
        latencyThresholdMs: this.config.circuitBreaker.maxE2ELatencyMs,
        logger: this.logger,
      });
  }

  /** Begin draining the queue. Resolves when `stop()` is called. */
  async run(): Promise<void> {
    this.running = true;
    const { queueKey } = this.config.redis;
    this.logger.info(
      `OrderWorker: draining "${queueKey}" ` +
        `(shadowMode=${this.config.shadowMode}, browserSimulate=${this.config.browser.simulate})`,
    );

    while (this.running) {
      // Circuit OPEN: pause consumption so orders stay queued and we stop
      // hammering a degraded downstream. Re-check after a short backoff.
      if (!this.breaker.allowsRequest()) {
        await sleep(250);
        continue;
      }

      let raw: [string, string] | null;
      try {
        raw = await this.redis.brpop(queueKey, this.config.redis.blockTimeoutSec);
      } catch (err) {
        this.logger.error(`OrderWorker: BRPOP failed: ${(err as Error).message}`);
        continue;
      }
      if (!raw) continue; // block window elapsed on an empty queue
      await this.process(raw[1]);
    }
  }

  stop(): void {
    this.running = false;
  }

  /** Parse, run the pipeline (timed, breaker-guarded), and emit telemetry. */
  private async process(payload: string): Promise<void> {
    let order: ApprovedOrder;
    try {
      order = JSON.parse(payload) as ApprovedOrder;
    } catch (err) {
      this.logger.error(`OrderWorker: dropping malformed order: ${(err as Error).message}`);
      return; // a malformed payload is not a downstream fault; don't trip the breaker
    }

    const shadow = this.config.shadowMode;
    this.breaker.onExecuteStart(); // reserve a HALF-OPEN probe slot if applicable

    const e2eStart = performance.now();
    let session: BrowserSession | undefined;
    let success = false;
    let blocked = false;
    let execution: OrderExecutionResult | undefined;

    let hydrationLatencyMs = 0;
    let hydrationSource: HydrationSource = 'fallback';
    let browserAcquisitionMs = 0;

    try {
      // --- 1. Hydration leg ---
      const hydrated = await this.loader.hydrate(order);
      hydrationLatencyMs = hydrated.hydrationLatencyMs;
      hydrationSource = aggregateSource(hydrated.hydrationSource);

      // --- 2. Browser acquisition leg ---
      const acqStart = performance.now();
      session = await this.browser.acquire();
      browserAcquisitionMs = performance.now() - acqStart;

      // --- 3. Navigate to the execution surface ---
      const targetUrl =
        typeof order.targetUrl === 'string'
          ? order.targetUrl
          : this.config.browser.defaultTargetUrl;
      await session.navigate(targetUrl);

      // --- 4. Execute with the slippage guard (shadow-aware inside) ---
      if (this.executor) {
        execution = await this.executor.executeOrder(session.page, hydrated);
        // Only a genuine DOM/execution fault counts against the breaker; a
        // slippage rejection is a healthy, intentional outcome.
        success = execution.status !== 'FAILED_DOM';
        if (execution.status === 'FAILED_DOM') {
          blocked = isBlockError(execution.error);
        }
      } else {
        this.logger.warn(
          `OrderWorker: order=${hydrated.orderId} has no executor configured; nothing placed`,
        );
        success = true; // the pipeline itself was healthy
      }
    } catch (err) {
      const msg = (err as Error).message;
      blocked = isBlockError(msg);
      this.logger.error(
        `OrderWorker: pipeline failed for order=${order.orderId}` +
          `${blocked ? ' [BLOCK detected]' : ''}: ${msg}`,
      );
    } finally {
      if (session) {
        await session
          .close()
          .catch((err) =>
            this.logger.warn(`OrderWorker: session close failed: ${(err as Error).message}`),
          );
      }
    }

    const totalE2EMs = performance.now() - e2eStart;

    // Feed the breaker: a block trips immediately; a success slower than the
    // configured E2E ceiling is treated as a (latency) failure.
    this.breaker.recordOutcome({ success, latencyMs: totalE2EMs, blocked });

    this.reportTelemetry({
      orderId: order.orderId,
      hydrationLatencyMs,
      hydrationSource,
      browserAcquisitionMs,
      totalE2EMs,
      shadow,
      circuitState: this.breaker.getState(),
      execution,
    });
  }

  /** Structured, human-readable latency breakdown per order. */
  private reportTelemetry(t: OrderTelemetry): void {
    const mode = modeLabel(t.shadow, t.execution?.status);
    const lines = [
      '──────── Execution Telemetry ────────',
      `[Order ID]                    ${t.orderId}`,
      `Hydration Latency             ${t.hydrationLatencyMs.toFixed(3)} ms (Source: ${t.hydrationSource})`,
      `Browser Acquisition Latency   ${t.browserAcquisitionMs.toFixed(3)} ms`,
      `Total E2E Execution Latency   ${t.totalE2EMs.toFixed(3)} ms`,
      `Circuit State                 ${t.circuitState}`,
    ];
    if (t.execution) {
      const e = t.execution;
      lines.push(
        `Execution Status              ${e.status}`,
        `Odds (req -> exec)            ${e.requestedOdds}` +
          `${e.executedOdds !== undefined ? ` -> ${e.executedOdds}` : ''} ` +
          `(slippage ${(e.slippage * 100).toFixed(2)}%)`,
      );
      if (e.error) lines.push(`Execution Error               ${e.error}`);
    }
    lines.push(`Mode                          ${mode}`, '─────────────────────────────────────');
    this.logger.info(lines.join('\n'));
  }
}

function modeLabel(shadow: boolean, status?: OrderExecutionStatus): string {
  if (shadow) return 'SHADOW (transaction skipped)';
  switch (status) {
    case 'EXECUTED':
      return 'LIVE (executed)';
    case 'REJECTED_SLIPPAGE':
      return 'LIVE (rejected: slippage)';
    case 'FAILED_DOM':
      return 'LIVE (failed: DOM)';
    default:
      return 'LIVE (no-op)';
  }
}
