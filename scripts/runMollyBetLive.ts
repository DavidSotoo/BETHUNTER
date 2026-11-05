/**
 * runMollyBetLive -- single production entrypoint for the BetInAsia BLACK
 * (MollyBet) pipeline. One process, end to end:
 *
 *   BetInAsiaProvider  (official MollyBet websocket: consolidated live odds)
 *     -> LiveFeedIngestor   (+EV screen; ratings resolved from the Feature Store)
 *        -> LPUSH ApprovedOrder
 *           -> OrderWorker  (hydrate -> MollyBetExecutor)
 *              -> POST /v1/betslips/ + /v1/orders/   (Slippage Guard, Breaker)
 *                 -> SettlementService  (CLV & PnL audit)
 *
 * The odds stream and the executor share one process on purpose: the executor's
 * Slippage Guard compares the order's target price against the *live* best
 * price from that stream (`provider.getBestPrice`) before anything is sent, and
 * then pins a minimum-price floor on the order itself. Split across processes,
 * the client-side half of that guard has nothing to read.
 *
 * Ratings: MollyBet carries odds only, so the ingestor resolves Elo / fatigue /
 * form per player from `ta:player:<id>` through the RedisFeatureStoreClient.
 * Without a rating for both sides a match is simply not qualified.
 *
 * SHADOW_MODE=true runs everything -- stream, screen, hydration, guards,
 * telemetry, settlement -- and posts nothing to the order API.
 *
 * Run:
 *   SHADOW_MODE=true BIA_API_KEY=... BIA_ACCOUNT_ID=... npm run live-mollybet
 */

import { Redis } from 'ioredis';
import { config, assertBetInAsiaCredentials } from '../src/config/index.js';
import { MetricsCollector } from '../src/telemetry/MetricsCollector.js';
import { CircuitBreaker } from '../src/services/CircuitBreaker.js';
import { FeatureLoader } from '../src/services/FeatureLoader.js';
import { RedisFeatureStoreClient } from '../src/services/RedisFeatureStoreClient.js';
import { MollyBetClient } from '../src/services/MollyBetClient.js';
import { MollyBetExecutor } from '../src/executors/MollyBetExecutor.js';
import { SettlementService } from '../src/services/SettlementService.js';
import { OrderWorker } from '../src/workers/OrderWorker.js';
import { BetInAsiaProvider } from '../src/feed/providers/BetInAsiaProvider.js';
import { featureStoreRatingResolver } from '../src/feed/FeatureStoreRatings.js';
import {
  LiveFeedIngestor,
  type IngestorPipeline,
  type IngestorRedis,
} from '../src/feed/LiveFeedIngestor.js';
import { createHealthServer } from '../src/server.js';

const logger = console;
const CLOSED_MATCH_QUEUE = 'ClosedMatch';

interface CloseEvent {
  matchId: string;
  winner: string;
  closingOdds: number;
}

async function main(): Promise<void> {
  // Refuse to start live without credentials; warn (and carry on) in shadow.
  assertBetInAsiaCredentials(config, logger);

  // --- Redis connections. The worker's BRPOP blocks and holds its socket, so
  //     feature reads, the ingestor's writes and the close consumer each get
  //     their own connection. ---
  const workerRedis = new Redis(config.redis.url, { maxRetriesPerRequest: null });
  const featureRedis = new Redis(config.redis.url);
  const ingestRedis = new Redis(config.redis.url);
  const closeRedis = new Redis(config.redis.url, { maxRetriesPerRequest: null });

  // --- Telemetry + resilience, shared by the worker, executor and server. ---
  const metrics = new MetricsCollector({ logger });
  const breaker = new CircuitBreaker({
    name: 'order-exec',
    failureThreshold: config.circuitBreaker.failureThreshold,
    resetTimeoutMs: config.circuitBreaker.resetTimeoutMs,
    latencyThresholdMs: config.circuitBreaker.maxE2ELatencyMs,
    logger,
    onStateChange: (from, to) => metrics.recordCircuitTransition(from, to),
  });

  // --- Feature Store: hydration for the worker, ratings for the +EV screen. ---
  const featureStore = new RedisFeatureStoreClient({ redis: featureRedis, logger });
  const loader = new FeatureLoader({
    client: featureStore,
    ttlMs: config.featureStore.ttlMs,
    fetchTimeoutMs: config.featureStore.fetchTimeoutMs,
    maxEntries: config.featureStore.maxEntries,
  });
  const settlement = new SettlementService({ redis: featureRedis, logger });

  // --- Official MollyBet API session, shared by the stream and the executor. ---
  const mollybet = new MollyBetClient({
    apiUrl: config.betinasia.apiUrl,
    username: config.betinasia.accountId,
    password: config.betinasia.apiKey,
    timeoutMs: config.betinasia.requestTimeoutMs,
    logger,
  });

  // --- Live odds stream ---
  const provider = new BetInAsiaProvider({
    client: mollybet,
    wsUrl: config.betinasia.wsUrl,
    sport: config.betinasia.sport,
    logger,
  });

  // --- Execution: the provider's live price is the guard's reference. ---
  const executor = new MollyBetExecutor({
    client: mollybet,
    sport: config.betinasia.sport,
    currency: config.betinasia.currency,
    orderDurationSec: config.betinasia.orderDurationSec,
    maxSlippageTolerance: config.execution.maxSlippageTolerance,
    shadowMode: config.shadowMode,
    breaker,
    liveOdds: (eventId, side) => provider.getBestPrice(eventId, side),
    logger,
  });
  // API executor: no BrowserManager is wired, so the worker skips browser
  // acquisition and navigation for every order.
  const worker = new OrderWorker({
    redis: workerRedis,
    loader,
    executor,
    breaker,
    metrics,
    settlement,
    logger,
  });

  // --- Ingestion: stream -> +EV screen -> ApprovedOrder queue. ---
  const ingestorRedis: IngestorRedis = {
    pipeline: () => ingestRedis.pipeline() as unknown as IngestorPipeline,
  };
  const ingestor = new LiveFeedIngestor({
    redis: ingestorRedis,
    connect: provider.connector,
    ratings: featureStoreRatingResolver({ client: featureStore, logger }),
    minEv: config.execution.minEv,
    stakeUnit: config.execution.stakeUnit,
    logger,
  });

  // --- Health / metrics / financial-report server ---
  const server = createHealthServer({
    metrics,
    getCircuitState: () => breaker.getState(),
    getFinancialReport: () => settlement.getFinancialReport(),
    logger,
  });
  await new Promise<void>((resolve) => server.listen(config.server.httpPort, resolve));

  // --- Settlement consumer: ClosedMatch events from the ingestor. ---
  let settling = true;
  const settleLoop = (async () => {
    while (settling) {
      let raw: [string, string] | null;
      try {
        raw = await closeRedis.brpop(CLOSED_MATCH_QUEUE, 5);
      } catch (err) {
        if (settling) {
          logger.error(`runMollyBetLive: close BRPOP failed: ${(err as Error).message}`);
        }
        continue;
      }
      if (!raw) continue;
      try {
        const ev = JSON.parse(raw[1]) as CloseEvent;
        const settled = settlement.settleMatch(ev.matchId, ev.winner, ev.closingOdds);
        if (settled.length > 0) {
          const r = settlement.getFinancialReport();
          logger.info(
            `runMollyBetLive: settled ${ev.matchId} (${settled.length}) | ` +
              `PnL=${r.totalPnl} winRate=${r.winRatePct}% yield=${r.yieldPct}% BTCL=${r.btclRatePct}%`,
          );
        }
      } catch (err) {
        logger.error(`runMollyBetLive: settle failed: ${(err as Error).message}`);
      }
    }
  })();

  // --- Graceful shutdown: stop intake first, then drain, then drop sockets. ---
  let shuttingDown = false;
  const shutdown = async (signal: string): Promise<void> => {
    if (shuttingDown) return;
    shuttingDown = true;
    logger.info(`\nrunMollyBetLive: received ${signal}, shutting down...`);
    try {
      ingestor.stop(); // stop reconnecting; closes the MollyBet websocket
      worker.stop(); // stop pulling new orders
      settling = false; // stop the settlement consumer
      await new Promise<void>((resolve) => server.close(() => resolve()));
      workerRedis.disconnect();
      featureRedis.disconnect();
      ingestRedis.disconnect();
      closeRedis.disconnect();
      await settleLoop.catch(() => undefined);
      logger.info('runMollyBetLive: shutdown complete');
      process.exit(0);
    } catch (err) {
      logger.error(`runMollyBetLive: error during shutdown: ${(err as Error).message}`);
      process.exit(1);
    }
  };
  process.on('SIGINT', () => void shutdown('SIGINT'));
  process.on('SIGTERM', () => void shutdown('SIGTERM'));

  logger.info(
    `runMollyBetLive: starting (shadowMode=${config.shadowMode}, ` +
      `sport=${config.betinasia.sport}, currency=${config.betinasia.currency}, ` +
      `minEv=${config.execution.minEv}, stake=${config.execution.stakeUnit}, ` +
      `maxSlippage=${config.execution.maxSlippageTolerance}) | ` +
      `/health /metrics /financial-report on :${config.server.httpPort}`,
  );

  // Open the stream, then block draining the queue until shutdown.
  await ingestor.start();
  await worker.run();
}

main().catch((err) => {
  logger.error('runMollyBetLive: fatal:', err);
  process.exitCode = 1;
});
