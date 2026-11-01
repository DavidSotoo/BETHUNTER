/**
 * OrderWorker -- drains the Redis `ApprovedOrder` queue and runs each order
 * through the execution pipeline:
 *
 *   BRPOP ApprovedOrder
 *     -> FeatureLoader.hydrate        (live competitor vectors)
 *     -> BrowserManager.acquire       (isolated Playwright context)
 *     -> navigate                     (load the execution surface)
 *     -> TransactionExecutor.execute  (the final, irreversible action)
 *
 * SHADOW MODE
 *
 * When `config.shadowMode` is on, every step runs EXCEPT the final
 * `TransactionExecutor.execute`. We still hydrate, still open a real/simulated
 * browser context, still navigate -- so the latency breakdown is representative
 * -- but we never place the bet. This is the safe way to measure and validate
 * the live path.
 *
 * Every step is timed with performance.now() and each order ends with a
 * structured telemetry block on the console.
 */

import { Redis } from 'ioredis';
import { performance } from 'node:perf_hooks';
import { FeatureLoader } from '../services/FeatureLoader.js';
import { BrowserManager, BrowserSession } from '../browser/BrowserManager.js';
import { config as defaultConfig, AppConfig } from '../config/index.js';
import {
  ApprovedOrder,
  HydratedOrderPayload,
  HydrationSource,
} from '../types/features.js';

/** The final, irreversible action, isolated so SHADOW_MODE can skip it. */
export interface TransactionExecutor {
  execute(session: BrowserSession, order: HydratedOrderPayload): Promise<void>;
}

export interface OrderWorkerOptions {
  redis: Redis;
  loader: FeatureLoader;
  browser: BrowserManager;
  /** Omitted (or skipped by shadow mode) means no final transaction runs. */
  executor?: TransactionExecutor;
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
  executed: boolean;
}

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

export class OrderWorker {
  private readonly redis: Redis;
  private readonly loader: FeatureLoader;
  private readonly browser: BrowserManager;
  private readonly executor?: TransactionExecutor;
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
      let raw: [string, string] | null;
      try {
        // Blocking pop. null when the block window elapses on an empty queue --
        // that is the loop's chance to observe a stop signal, not an error.
        raw = await this.redis.brpop(queueKey, this.config.redis.blockTimeoutSec);
      } catch (err) {
        this.logger.error(`OrderWorker: BRPOP failed: ${(err as Error).message}`);
        continue;
      }
      if (!raw) continue;
      await this.process(raw[1]);
    }
  }

  stop(): void {
    this.running = false;
  }

  /** Parse, run the pipeline (timed), and emit telemetry for one order. */
  private async process(payload: string): Promise<void> {
    let order: ApprovedOrder;
    try {
      order = JSON.parse(payload) as ApprovedOrder;
    } catch (err) {
      this.logger.error(`OrderWorker: dropping malformed order: ${(err as Error).message}`);
      return;
    }

    const shadow = this.config.shadowMode;
    const e2eStart = performance.now();
    let session: BrowserSession | undefined;
    let executed = false;

    try {
      // --- 1. Hydration leg (right after the pop) ---
      const hydrated = await this.loader.hydrate(order);

      // --- 2. Browser acquisition leg ---
      const acqStart = performance.now();
      session = await this.browser.acquire();
      const browserAcquisitionMs = performance.now() - acqStart;

      // --- 3. Navigate to the execution surface (counts toward E2E) ---
      const targetUrl =
        typeof order.targetUrl === 'string'
          ? order.targetUrl
          : this.config.browser.defaultTargetUrl;
      await session.navigate(targetUrl);

      // --- 4. Final transaction (skipped in shadow mode) ---
      if (shadow) {
        this.logger.info(
          `OrderWorker: [SHADOW] order=${hydrated.orderId} navigated to ${targetUrl}; transaction NOT executed`,
        );
      } else if (this.executor) {
        await this.executor.execute(session, hydrated);
        executed = true;
      } else {
        this.logger.warn(
          `OrderWorker: order=${hydrated.orderId} has no executor configured; transaction skipped`,
        );
      }

      this.reportTelemetry({
        orderId: hydrated.orderId,
        hydrationLatencyMs: hydrated.hydrationLatencyMs,
        hydrationSource: aggregateSource(hydrated.hydrationSource),
        browserAcquisitionMs,
        totalE2EMs: performance.now() - e2eStart,
        shadow,
        executed,
      });
    } catch (err) {
      this.logger.error(
        `OrderWorker: pipeline failed for order=${order.orderId}: ${(err as Error).message}`,
      );
    } finally {
      // Always release the context, even on failure, so we never leak browsers.
      if (session) {
        await session.close().catch((err) =>
          this.logger.warn(`OrderWorker: session close failed: ${(err as Error).message}`),
        );
      }
    }
  }

  /** Structured, human-readable latency breakdown per order. */
  private reportTelemetry(t: OrderTelemetry): void {
    const mode = t.shadow ? 'SHADOW (transaction skipped)' : t.executed ? 'LIVE (executed)' : 'LIVE (no-op)';
    const lines = [
      '──────── Execution Telemetry ────────',
      `[Order ID]                    ${t.orderId}`,
      `Hydration Latency             ${t.hydrationLatencyMs.toFixed(3)} ms (Source: ${t.hydrationSource})`,
      `Browser Acquisition Latency   ${t.browserAcquisitionMs.toFixed(3)} ms`,
      `Total E2E Execution Latency   ${t.totalE2EMs.toFixed(3)} ms`,
      `Mode                          ${mode}`,
      '─────────────────────────────────────',
    ];
    this.logger.info(lines.join('\n'));
  }
}
