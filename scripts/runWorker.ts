/**
 * runWorker -- integration harness consumer.
 *
 * Wires the real OrderWorker to a real Redis connection, a FeatureLoader backed
 * by a demo Feature Store (artificial latency so misses are visible against the
 * 50ms deadline), and a BrowserManager. Run this in one terminal, then run
 * `simulateOrder` in another to push work at it.
 *
 * Run (shadow mode, simulated browser -- no binaries needed):
 *   SHADOW_MODE=true tsx scripts/runWorker.ts
 *
 * Drive a real browser instead:
 *   SHADOW_MODE=true BROWSER_SIMULATE=false tsx scripts/runWorker.ts
 */

import { Redis } from 'ioredis';
import { config } from '../src/config/index.js';
import { FeatureLoader, FeatureStoreClient } from '../src/services/FeatureLoader.js';
import { BrowserManager } from '../src/browser/BrowserManager.js';
import { OrderWorker, TransactionExecutor } from '../src/workers/OrderWorker.js';
import type { PlayerFeatures } from '../src/types/features.js';

/** Stand-in Feature Store: deterministic vectors with a realistic I/O delay. */
function createDemoStore(delayMs = 25): FeatureStoreClient {
  let fetches = 0;
  return {
    async fetch(playerId: string, signal: AbortSignal): Promise<PlayerFeatures> {
      fetches++;
      await new Promise<void>((resolve, reject) => {
        const t = setTimeout(resolve, delayMs);
        signal.addEventListener('abort', () => {
          clearTimeout(t);
          reject(new Error('aborted'));
        });
      });
      // A stable pseudo-rating derived from the id, so runs are reproducible.
      const seed = [...playerId].reduce((a, c) => a + c.charCodeAt(0), 0);
      return {
        id: playerId,
        elo: 1800 + (seed % 400),
        fatigueScore: seed % 50,
        formRating: (seed % 20) - 10,
        updatedAt: Date.now(),
      };
    },
    get fetches(): number {
      return fetches;
    },
  } as FeatureStoreClient & { fetches: number };
}

/** Demo final transaction. Never reached in SHADOW_MODE. */
const demoExecutor: TransactionExecutor = {
  async execute(session, order): Promise<void> {
    console.log(`demoExecutor: placing bet for ${order.orderId} on ${session.id}`);
  },
};

async function main(): Promise<void> {
  const redis = new Redis(config.redis.url, { maxRetriesPerRequest: null });
  const loader = new FeatureLoader({
    client: createDemoStore(),
    ttlMs: config.featureStore.ttlMs,
    fetchTimeoutMs: config.featureStore.fetchTimeoutMs,
    maxEntries: config.featureStore.maxEntries,
  });
  const browser = new BrowserManager({
    simulate: config.browser.simulate,
    headless: config.browser.headless,
    navigationTimeoutMs: config.browser.navigationTimeoutMs,
  });
  const worker = new OrderWorker({ redis, loader, browser, executor: demoExecutor });

  const shutdown = async (): Promise<void> => {
    console.log('\nrunWorker: shutting down...');
    worker.stop();
    await browser.close();
    await redis.quit();
    process.exit(0);
  };
  process.on('SIGINT', () => void shutdown());
  process.on('SIGTERM', () => void shutdown());

  await worker.run();
}

main().catch((err) => {
  console.error('runWorker: fatal:', err);
  process.exitCode = 1;
});
