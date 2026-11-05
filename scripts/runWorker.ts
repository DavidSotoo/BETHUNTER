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
 * Execution goes through MollyBetExecutor (official BetInAsia BLACK API), so
 * there is no browser in this path. Keep SHADOW_MODE=true for paper trading:
 * the executor then runs every check and posts nothing, which is also why this
 * harness needs no BetInAsia credentials.
 *
 * Run (paper trading -- shadow mode, nothing placed):
 *   SHADOW_MODE=true tsx scripts/runWorker.ts
 */

import { Redis } from 'ioredis';
import { config, assertBetInAsiaCredentials } from '../src/config/index.js';
import { FeatureLoader } from '../src/services/FeatureLoader.js';
import { RedisFeatureStoreClient } from '../src/services/RedisFeatureStoreClient.js';
import { CircuitBreaker } from '../src/services/CircuitBreaker.js';
import { MetricsCollector } from '../src/telemetry/MetricsCollector.js';
import { MollyBetClient } from '../src/services/MollyBetClient.js';
import { MollyBetExecutor } from '../src/executors/MollyBetExecutor.js';
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
  // Warns under SHADOW_MODE, throws otherwise: this harness is meant to be run
  // as paper trading, but it refuses to go live half-configured.
  assertBetInAsiaCredentials(config, logger);

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
  // API executor: no BrowserManager, so no context is acquired per order.
  const worker = new OrderWorker({ redis, loader, executor, breaker, metrics, settlement });

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
      `(shadowMode=${config.shadowMode}, executor=MollyBet API)`,
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
