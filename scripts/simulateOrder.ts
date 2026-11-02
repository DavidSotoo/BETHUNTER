/**
 * simulateOrder -- integration harness producer.
 *
 * Publishes 5 synthetic orders onto the Redis `ApprovedOrder` queue, crafted
 * so a worker draining the queue in order exercises three distinct cache
 * behaviours in the FeatureLoader:
 *
 *   1. cold miss          -- two never-seen competitors (both fetch the store)
 *   2. immediate hit      -- the same pair again (both served from cache)
 *   3. concurrent miss    -- one new competitor on BOTH sides: hydrate() resolves
 *                            the two sides in parallel, so the two misses collapse
 *                            into ONE store fetch (coalescing, exercised live)
 *   4. mixed hit          -- competitors now all warm (cache on both sides)
 *   5. cold miss (second) -- two more new competitors
 *
 * The queue is FIFO for the worker: it BRPOPs (tail), so we LPUSH (head).
 *
 * Run:  tsx scripts/simulateOrder.ts     (REDIS_URL optional, defaults to 127.0.0.1)
 */

import { Redis } from 'ioredis';
import { config } from '../src/config/index.js';
import type { ApprovedOrder } from '../src/types/features.js';

/** An order plus a harness-only label describing the scenario it provokes. */
interface SyntheticOrder extends ApprovedOrder {
  scenario: string;
  targetUrl: string;
  targetOdds: number;
}

// Valid-looking competitor ids (namespaced, as the analytics side keys them).
const P = {
  alcaraz: 'atp:alcaraz-c',
  sinner: 'atp:sinner-j',
  djokovic: 'atp:djokovic-n',
  medvedev: 'atp:medvedev-d',
  zverev: 'atp:zverev-a',
} as const;

function order(
  orderId: string,
  scenario: string,
  playerAId: string,
  playerBId: string,
): SyntheticOrder {
  return {
    orderId,
    scenario,
    playerAId,
    playerBId,
    // about:blank keeps the harness offline; the worker treats this as the
    // execution surface to "navigate" to.
    targetUrl: 'about:blank',
    // Odds we want the bet placed against; the slippage guard compares the
    // live DOM odds against this.
    targetOdds: 2.0,
    market: 'match_winner',
    side: 'A',
    stake: 10,
  };
}

const ORDERS: SyntheticOrder[] = [
  order('ord-0001', 'cold-miss', P.alcaraz, P.sinner),
  order('ord-0002', 'immediate-hit', P.alcaraz, P.sinner),
  order('ord-0003', 'concurrent-miss-coalescing', P.djokovic, P.djokovic),
  order('ord-0004', 'mixed-hit', P.djokovic, P.alcaraz),
  order('ord-0005', 'cold-miss-2', P.medvedev, P.zverev),
];

async function main(): Promise<void> {
  const redis = new Redis(config.redis.url);
  const queueKey = config.redis.queueKey;

  try {
    console.log(`simulateOrder: publishing ${ORDERS.length} orders to "${queueKey}" at ${config.redis.url}`);
    for (const o of ORDERS) {
      await redis.lpush(queueKey, JSON.stringify(o));
      console.log(`  -> ${o.orderId}  [${o.scenario}]  A=${o.playerAId}  B=${o.playerBId}`);
    }
    const depth = await redis.llen(queueKey);
    console.log(`simulateOrder: done. Queue "${queueKey}" depth is now ${depth}.`);
  } finally {
    await redis.quit();
  }
}

main().catch((err) => {
  console.error('simulateOrder: failed:', err);
  process.exitCode = 1;
});
