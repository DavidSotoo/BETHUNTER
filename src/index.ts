/**
 * Production entrypoint for the Execution Gateway.
 *
 * Wires the full pipeline -- config, MetricsCollector, CircuitBreaker,
 * BrowserManager, FeatureLoader, OrderExecutor, OrderWorker -- starts the
 * health/metrics HTTP server, then drains the Redis queue. Handles SIGINT /
 * SIGTERM with a graceful shutdown: stop consuming, close the HTTP server,
 * release the browser and Redis connection, then exit cleanly.
 */

import { Redis } from 'ioredis';
import { config } from './config/index.js';
import { MetricsCollector } from './telemetry/MetricsCollector.js';
import { CircuitBreaker } from './services/CircuitBreaker.js';
import { BrowserManager } from './browser/BrowserManager.js';
import { FeatureLoader, FeatureStoreClient } from './services/FeatureLoader.js';
import { OrderExecutor } from './executors/OrderExecutor.js';
import { OrderWorker } from './workers/OrderWorker.js';
import { createHealthServer } from './server.js';
import type { PlayerFeatures } from './types/features.js';

const logger = console;

/**
 * PLACEHOLDER Feature Store client.
 *
 * TODO: replace with the real REST/gRPC/DB adapter before production. It
 * returns deterministic neutral-ish vectors so the gateway is runnable end to
 * end; a startup warning is logged so this never ships silently.
 */
function createFeatureStoreClient(): FeatureStoreClient {
  logger.warn(
    'index: using PLACEHOLDER Feature Store client -- wire the real adapter before production',
  );
  return {
    async fetch(playerId: string, signal: AbortSignal): Promise<PlayerFeatures> {
      // Honour the loader's deadline even in the placeholder.
      await new Promise<void>((resolve, reject) => {
        const t = setTimeout(resolve, 5);
        signal.addEventListener('abort', () => {
          clearTimeout(t);
          reject(new Error('aborted'));
        });
      });
      const seed = [...playerId].reduce((a, c) => a + c.charCodeAt(0), 0);
      return {
        id: playerId,
        elo: 1800 + (seed % 400),
        fatigueScore: seed % 50,
        formRating: (seed % 20) - 10,
        updatedAt: Date.now(),
      };
    },
  };
}

async function main(): Promise<void> {
  // --- Shared telemetry + resilience (constructed first so both the worker
  //     and the health server observe the same state). ---
  const metrics = new MetricsCollector({ logger });
  const breaker = new CircuitBreaker({
    name: 'order-exec',
    failureThreshold: config.circuitBreaker.failureThreshold,
    resetTimeoutMs: config.circuitBreaker.resetTimeoutMs,
    latencyThresholdMs: config.circuitBreaker.maxE2ELatencyMs,
    logger,
    onStateChange: (from, to) => metrics.recordCircuitTransition(from, to),
  });

  // --- Pipeline dependencies ---
  const redis = new Redis(config.redis.url, { maxRetriesPerRequest: null });
  const loader = new FeatureLoader({
    client: createFeatureStoreClient(),
    ttlMs: config.featureStore.ttlMs,
    fetchTimeoutMs: config.featureStore.fetchTimeoutMs,
    maxEntries: config.featureStore.maxEntries,
  });
  const browser = new BrowserManager({
    simulate: config.browser.simulate,
    headless: config.browser.headless,
    navigationTimeoutMs: config.browser.navigationTimeoutMs,
  });
  const executor = new OrderExecutor({
    maxSlippageTolerance: config.execution.maxSlippageTolerance,
    domSelectorTimeoutMs: config.execution.domSelectorTimeoutMs,
    shadowMode: config.shadowMode,
  });
  const worker = new OrderWorker({ redis, loader, browser, executor, breaker, metrics });

  // --- Health / metrics server ---
  const server = createHealthServer({
    metrics,
    getCircuitState: () => breaker.getState(),
    logger,
  });
  await new Promise<void>((resolve) => server.listen(config.server.httpPort, resolve));
  logger.info(`index: health/metrics server listening on :${config.server.httpPort}`);

  // --- Graceful shutdown ---
  let shuttingDown = false;
  const shutdown = async (signal: string): Promise<void> => {
    if (shuttingDown) return;
    shuttingDown = true;
    logger.info(`index: received ${signal}, shutting down gracefully...`);
    try {
      worker.stop(); // stop pulling new work
      await new Promise<void>((resolve) => server.close(() => resolve())); // drain HTTP
      await browser.close(); // release Chromium / contexts
      redis.disconnect(); // drop the (possibly blocked BRPOP) connection
      logger.info('index: shutdown complete');
      process.exit(0);
    } catch (err) {
      logger.error(`index: error during shutdown: ${(err as Error).message}`);
      process.exit(1);
    }
  };
  process.on('SIGINT', () => void shutdown('SIGINT'));
  process.on('SIGTERM', () => void shutdown('SIGTERM'));

  // --- Run (blocks until worker.stop()) ---
  logger.info(
    `index: Execution Gateway starting (shadowMode=${config.shadowMode}, browserSimulate=${config.browser.simulate})`,
  );
  await worker.run();
}

main().catch((err) => {
  logger.error('index: fatal:', err);
  process.exit(1);
});
