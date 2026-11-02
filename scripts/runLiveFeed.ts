/**
 * runLiveFeed -- real live-feed producer entrypoint.
 *
 * Opens the WebSocketTransport, drives the LiveFeedIngestor, and streams
 * normalised player hashes, +EV ApprovedOrders and ClosedMatch events into
 * Redis for the OrderWorker / SettlementService. Handles SIGINT / SIGTERM with
 * a graceful shutdown (stop ingesting, close the socket, disconnect Redis).
 *
 * Run:  FEED_WS_URL=wss://host/feed FEED_API_KEY=... tsx scripts/runLiveFeed.ts
 */

import { Redis } from 'ioredis';
import { config } from '../src/config/index.js';
import {
  LiveFeedIngestor,
  type IngestorPipeline,
  type IngestorRedis,
} from '../src/feed/LiveFeedIngestor.js';
import { WebSocketTransport } from '../src/feed/transports/WebSocketTransport.js';

const logger = console;

async function main(): Promise<void> {
  const redis = new Redis(config.redis.url, { maxRetriesPerRequest: null });
  // Adapt ioredis to the ingestor's minimal pipeline surface.
  const redisAdapter: IngestorRedis = {
    pipeline: () => redis.pipeline() as unknown as IngestorPipeline,
  };

  const transport = new WebSocketTransport({
    url: config.feed.wsUrl,
    apiKey: config.feed.apiKey,
    logger,
  });

  const ingestor = new LiveFeedIngestor({
    redis: redisAdapter,
    connect: () => transport.connect(),
    logger,
  });

  let shuttingDown = false;
  const shutdown = async (signal: string): Promise<void> => {
    if (shuttingDown) return;
    shuttingDown = true;
    logger.info(`\nrunLiveFeed: received ${signal}, shutting down...`);
    ingestor.stop(); // stops reconnecting and closes the socket
    redis.disconnect();
    logger.info('runLiveFeed: shutdown complete');
    process.exit(0);
  };
  process.on('SIGINT', () => void shutdown('SIGINT'));
  process.on('SIGTERM', () => void shutdown('SIGTERM'));

  logger.info(`runLiveFeed: connecting to ${config.feed.wsUrl} and ingesting into Redis...`);
  await ingestor.start();
}

main().catch((err) => {
  logger.error('runLiveFeed: fatal:', err);
  process.exitCode = 1;
});
