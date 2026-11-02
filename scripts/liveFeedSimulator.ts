/**
 * liveFeedSimulator -- continuous live-feed producer for paper trading.
 *
 * On every tick it:
 *   1. refreshes a couple of player feature hashes (ta:player:<id>) with drifting
 *      elo / fatigue / form_rating -- the data RedisFeatureStoreClient reads;
 *   2. builds a live match between two players, prices our backed selection and
 *      LPUSHes an ApprovedOrder onto the queue (this activates the OrderWorker);
 *   3. schedules the match "close" after a delay, LPUSHing a ClosedMatch event
 *      (winner + closing odds) so the consumer can settle it and the CLV/PnL
 *      audit at /financial-report moves in real time.
 *
 * Pair it with scripts/runWorker.ts (the consumer/server).
 *
 * Run:  tsx scripts/liveFeedSimulator.ts
 * Env:  FEED_ORDER_INTERVAL_MS (default 3000), FEED_CLOSE_DELAY_MS (default 8000)
 */

import { Redis } from 'ioredis';
import { config } from '../src/config/index.js';

const logger = console;
const CLOSED_MATCH_QUEUE = 'ClosedMatch';
const PLAYER_PREFIX = 'ta:player:';

function envNumber(name: string, def: number): number {
  const v = process.env[name];
  if (v === undefined || v === '') return def;
  const n = Number(v);
  return Number.isFinite(n) && n > 0 ? n : def;
}

const ORDER_INTERVAL_MS = envNumber('FEED_ORDER_INTERVAL_MS', 3000);
const CLOSE_DELAY_MS = envNumber('FEED_CLOSE_DELAY_MS', 8000);

interface Player {
  id: string;
  elo: number;
  fatigue: number;
  form: number;
}

const ROSTER: Player[] = [
  'alcaraz-c',
  'sinner-j',
  'djokovic-n',
  'medvedev-d',
  'zverev-a',
  'rublev-a',
].map((name, i) => ({ id: `atp:${name}`, elo: 1850 + i * 25, fatigue: 10, form: 0 }));

const rand = (min: number, max: number): number => min + Math.random() * (max - min);
const clamp = (v: number, lo: number, hi: number): number => Math.max(lo, Math.min(hi, v));
const pickPlayer = (): Player => ROSTER[Math.floor(Math.random() * ROSTER.length)] as Player;
/** Elo win probability of a over b. */
const winProb = (a: Player, b: Player): number => 1 / (1 + 10 ** ((b.elo - a.elo) / 400));

async function main(): Promise<void> {
  const redis = new Redis(config.redis.url, { maxRetriesPerRequest: null });
  const timers = new Set<ReturnType<typeof setTimeout>>();
  let seq = 0;

  async function writePlayer(p: Player): Promise<void> {
    // Random walk so cached features go stale and the loader re-reads.
    p.elo = clamp(p.elo + rand(-8, 8), 1400, 2400);
    p.fatigue = clamp(p.fatigue + rand(-3, 4), 0, 100);
    p.form = clamp(p.form + rand(-1, 1), -10, 10);
    await redis.hset(PLAYER_PREFIX + p.id, {
      elo_blend: p.elo.toFixed(2),
      fatigue: p.fatigue.toFixed(2),
      form_rating: p.form.toFixed(3),
      updated_at: String(Date.now()),
    });
  }

  async function tick(): Promise<void> {
    const a = pickPlayer();
    let b = pickPlayer();
    while (b.id === a.id) b = pickPlayer();
    await writePlayer(a);
    await writePlayer(b);

    seq += 1;
    const matchId = `m-${String(seq).padStart(4, '0')}`;
    const pA = winProb(a, b);
    const fairOddsA = 1 / pA;
    // Emit slightly above fair odds: we only fire when we think there is value.
    const targetOdds = Number((fairOddsA * (1 + rand(0, 0.06))).toFixed(2));
    const selection = a.id; // we back player A

    const order = {
      orderId: `ord-${matchId}`,
      matchId,
      playerAId: a.id,
      playerBId: b.id,
      selection,
      targetOdds,
      stake: 10,
      market: 'match_winner',
      targetUrl: 'about:blank',
    };
    await redis.lpush(config.redis.queueKey, JSON.stringify(order));
    logger.info(
      `feed: order ${order.orderId}  ${a.id} vs ${b.id}  @${targetOdds}  (pA=${(pA * 100).toFixed(1)}%)`,
    );

    // Schedule the close: favourite wins proportionally; closing odds drift
    // either side of our entry so CLV comes out positive or negative.
    const t = setTimeout(() => {
      timers.delete(t);
      void publishClose(matchId, a, b, pA, targetOdds, selection);
    }, CLOSE_DELAY_MS);
    timers.add(t);
  }

  async function publishClose(
    matchId: string,
    a: Player,
    b: Player,
    pA: number,
    targetOdds: number,
    selection: string,
  ): Promise<void> {
    try {
      const winner = Math.random() < pA ? a.id : b.id;
      const closingOdds = Number(clamp(targetOdds * (1 + rand(-0.08, 0.08)), 1.01, 100).toFixed(2));
      await redis.lpush(CLOSED_MATCH_QUEUE, JSON.stringify({ matchId, winner, closingOdds }));
      logger.info(
        `feed: close ${matchId}  winner=${winner}  closing=${closingOdds}  (bet ${selection})`,
      );
    } catch (err) {
      logger.error(`feed: close publish failed: ${(err as Error).message}`);
    }
  }

  logger.info(
    `liveFeedSimulator: feeding "${config.redis.queueKey}" every ${ORDER_INTERVAL_MS}ms, ` +
      `closing after ${CLOSE_DELAY_MS}ms, at ${config.redis.url}`,
  );
  await tick();
  const interval = setInterval(() => void tick(), ORDER_INTERVAL_MS);

  const shutdown = async (signal: string): Promise<void> => {
    logger.info(`\nliveFeedSimulator: ${signal}, stopping...`);
    clearInterval(interval);
    for (const t of timers) clearTimeout(t);
    await redis.quit().catch(() => undefined);
    process.exit(0);
  };
  process.on('SIGINT', () => void shutdown('SIGINT'));
  process.on('SIGTERM', () => void shutdown('SIGTERM'));
}

main().catch((err) => {
  logger.error('liveFeedSimulator: fatal:', err);
  process.exitCode = 1;
});
