/**
 * runWorker -- paper-trading consumer for the live-feed harness.
 *
 * Wires the full gateway against a real Redis: reads competitor features from
 * `ta:player:<id>` (RedisFeatureStoreClient), drains the ApprovedOrder queue,
 * records every emitted/simulated order in the SettlementService, settles
 * matches from a `ClosedMatch` event stream, and serves /health, /metrics and
 * /financial-report so the CLV & PnL audit can be watched live.
 *
 * Pair it with scripts/liveFeedSimulator.ts (the producer).
 *
 * Run (paper trading -- shadow mode, simulated browser):
 *   SHADOW_MODE=true tsx scripts/runWorker.ts
 */

import { Redis } from 'ioredis';
import { config } from '../src/config/index.js';
import { FeatureLoader } from '../src/services/FeatureLoader.js';
import { RedisFeatureStoreClient } from '../src/services/RedisFeatureStoreClient.js';
import { BrowserManager } from '../src/browser/BrowserManager.js';
import { CircuitBreaker } from '../src/services/CircuitBreaker.js';
import { MetricsCollector } from '../src/telemetry/MetricsCollector.js';
import { OrderExecutor } from '../src/executors/OrderExecutor.js';
import { SettlementService } from '../src/services/SettlementService.js';
import { OrderWorker } from '../src/workers/OrderWorker.js';
import { createHealthServer } from '../src/server.js';

const logger = console;
const CLOSED_MATCH_QUEUE = 'ClosedMatch';

interface CloseEvent {
  matchId: string;
  winner: string;
  closingOdds: number;
}

async function main(): Promise<void> {
  // Three connections: the worker's blocking BRPOP holds its socket, so feature
  // reads and the close-event consumer each need their own.
  const redis = new Redis(config.redis.url, { maxRetriesPerRequest: null });
  const featureRedis = new Redis(config.redis.url);
  const closeRedis = new Redis(config.redis.url, { maxRetriesPerRequest: null });

  const metrics = new MetricsCollector({ logger });
  const breaker = new CircuitBreaker({
    name: 'order-exec',
    failureThreshold: config.circuitBreaker.failureThreshold,
    resetTimeoutMs: config.circuitBreaker.resetTimeoutMs,
    latencyThresholdMs: config.circuitBreaker.maxE2ELatencyMs,
    logger,
    onStateChange: (from, to) => metrics.recordCircuitTransition(from, to),
  });
  const settlement = new SettlementService({ redis: featureRedis, logger });
  const loader = new FeatureLoader({
    client: new RedisFeatureStoreClient({ redis: featureRedis, logger }),
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
  const worker = new OrderWorker({ redis, loader, browser, executor, breaker, metrics, settlement });

  // --- Health / metrics / financial-report server ---
  const server = createHealthServer({
    metrics,
    getCircuitState: () => breaker.getState(),
    getFinancialReport: () => settlement.getFinancialReport(),
    logger,
  });
  await new Promise<void>((resolve) => server.listen(config.server.httpPort, resolve));
  logger.info(
    `runWorker: serving /health /metrics /financial-report on :${config.server.httpPort} ` +
      `(shadowMode=${config.shadowMode})`,
  );

  // --- Settlement consumer: close events published by the simulator ---
  let settling = true;
  const settleLoop = (async () => {
    while (settling) {
      let raw: [string, string] | null;
      try {
        raw = await closeRedis.brpop(CLOSED_MATCH_QUEUE, 5);
      } catch (err) {
        if (settling) logger.error(`runWorker: close BRPOP failed: ${(err as Error).message}`);
        continue;
      }
      if (!raw) continue;
      try {
        const ev = JSON.parse(raw[1]) as CloseEvent;
        const settled = settlement.settleMatch(ev.matchId, ev.winner, ev.closingOdds);
        if (settled.length > 0) {
          const r = settlement.getFinancialReport();
          logger.info(
            `runWorker: settled ${ev.matchId} (${settled.length}) | ` +
              `PnL=${r.totalPnl} winRate=${r.winRatePct}% yield=${r.yieldPct}% BTCL=${r.btclRatePct}%`,
          );
        }
      } catch (err) {
        logger.error(`runWorker: settle failed: ${(err as Error).message}`);
      }
    }
  })();

  const shutdown = async (signal: string): Promise<void> => {
    logger.info(`\nrunWorker: ${signal}, shutting down...`);
    settling = false;
    worker.stop();
    await new Promise<void>((resolve) => server.close(() => resolve()));
    await browser.close();
    redis.disconnect();
    featureRedis.disconnect();
    closeRedis.disconnect();
    await settleLoop.catch(() => undefined);
    process.exit(0);
  };
  process.on('SIGINT', () => void shutdown('SIGINT'));
  process.on('SIGTERM', () => void shutdown('SIGTERM'));

  await worker.run();
}

main().catch((err) => {
  logger.error('runWorker: fatal:', err);
  process.exitCode = 1;
});
