/**
 * Production entrypoint for the Execution Gateway.
 *
 * Wires the full pipeline -- config, MetricsCollector, CircuitBreaker,
 * FeatureLoader, MollyBetClient, MollyBetExecutor, OrderWorker -- starts the
 * health/metrics HTTP server, then drains the Redis queue. Handles SIGINT /
 * SIGTERM with a graceful shutdown: stop consuming, close the HTTP server,
 * disconnect Redis, then exit cleanly.
 *
 * Execution goes through the official BetInAsia BLACK (MollyBet) API, so there
 * is no browser in this path at all.
 *
 * SLIPPAGE GUARD: this entrypoint consumes a queue and has no odds stream of
 * its own, so the executor's client-side live-price check has nothing to
 * compare against; the server-side minimum-price floor still applies to every
 * order. For the full guard (live price + floor) run scripts/runMollyBetLive.ts,
 * which owns the stream and feeds it to the executor.
 */

import { Redis } from 'ioredis';
import { config, assertBetInAsiaCredentials } from './config/index.js';
import { MetricsCollector } from './telemetry/MetricsCollector.js';
import { CircuitBreaker } from './services/CircuitBreaker.js';
import { FeatureLoader } from './services/FeatureLoader.js';
import { RedisFeatureStoreClient } from './services/RedisFeatureStoreClient.js';
import { MollyBetClient } from './services/MollyBetClient.js';
import { MollyBetExecutor } from './executors/MollyBetExecutor.js';
import { OrderWorker } from './workers/OrderWorker.js';
import { SettlementService } from './services/SettlementService.js';
import { createHealthServer } from './server.js';

const logger = console;

async function main(): Promise<void> {
  // Stop a misconfigured deploy at startup, not on the first order.
  assertBetInAsiaCredentials(config, logger);

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
  // The worker's connection does blocking BRPOP, which holds the socket; the
  // Feature Store needs its own connection so reads are never queued behind it.
  const redis = new Redis(config.redis.url, { maxRetriesPerRequest: null });
  const featureRedis = new Redis(config.redis.url);
  const loader = new FeatureLoader({
    client: new RedisFeatureStoreClient({ redis: featureRedis, logger }),
    ttlMs: config.featureStore.ttlMs,
    fetchTimeoutMs: config.featureStore.fetchTimeoutMs,
    maxEntries: config.featureStore.maxEntries,
  });
  // Official BetInAsia BLACK (MollyBet) API: one authenticated session shared
  // by the executor. In shadow mode nothing is ever posted to it.
  const mollybet = new MollyBetClient({
    apiUrl: config.betinasia.apiUrl,
    username: config.betinasia.accountId,
    password: config.betinasia.apiKey,
    timeoutMs: config.betinasia.requestTimeoutMs,
    logger,
  });
  const executor = new MollyBetExecutor({
    client: mollybet,
    sport: config.betinasia.sport,
    currency: config.betinasia.currency,
    orderDurationSec: config.betinasia.orderDurationSec,
    maxSlippageTolerance: config.execution.maxSlippageTolerance,
    shadowMode: config.shadowMode,
    breaker,
    logger,
  });
  // Shared across worker (records emitted orders) and server (reports); writes
  // through to Redis for durable audit on the feature-store connection.
  const settlement = new SettlementService({ redis: featureRedis, logger });
  // No `browser`: MollyBetExecutor is API-based, so the worker skips browser
  // acquisition and navigation entirely.
  const worker = new OrderWorker({
    redis,
    loader,
    executor,
    breaker,
    metrics,
    settlement,
  });

  // --- Health / metrics server ---
  const server = createHealthServer({
    metrics,
    getCircuitState: () => breaker.getState(),
    getFinancialReport: () => settlement.getFinancialReport(),
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
      redis.disconnect(); // drop the (possibly blocked BRPOP) connection
      featureRedis.disconnect(); // drop the Feature Store connection
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
    `index: Execution Gateway starting (shadowMode=${config.shadowMode}, ` +
      `executor=MollyBet API, account=${config.betinasia.accountId || 'unset'})`,
  );
  await worker.run();
}

main().catch((err) => {
  logger.error('index: fatal:', err);
  process.exit(1);
});
