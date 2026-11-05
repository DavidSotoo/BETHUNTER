/**
 * End-to-end integration: BetInAsia BLACK (MollyBet) through the whole engine,
 * with the official API mocked and an in-memory Redis.
 *
 *   BetInAsiaProvider (mocked websocket)
 *     -> LiveFeedIngestor (+EV screen, ratings from the Feature Store)
 *        -> ApprovedOrder queue
 *           -> OrderWorker (hydration, no browser)
 *              -> MollyBetExecutor (Slippage Guard, Breaker, mocked REST)
 *                 -> SettlementService (CLV & PnL)
 *
 * Nothing here touches the network or Playwright.
 */

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { BetInAsiaProvider, type WebSocketLike } from '../src/feed/providers/BetInAsiaProvider.js';
import { LiveFeedIngestor, type IngestorPipeline, type IngestorRedis } from '../src/feed/LiveFeedIngestor.js';
import { featureStoreRatingResolver } from '../src/feed/FeatureStoreRatings.js';
import { RedisFeatureStoreClient } from '../src/services/RedisFeatureStoreClient.js';
import { FeatureLoader } from '../src/services/FeatureLoader.js';
import { MollyBetExecutor } from '../src/executors/MollyBetExecutor.js';
import { OrderExecutor } from '../src/executors/OrderExecutor.js';
import { SettlementService } from '../src/services/SettlementService.js';
import { CircuitBreaker } from '../src/services/CircuitBreaker.js';
import { MetricsCollector } from '../src/telemetry/MetricsCollector.js';
import { OrderWorker } from '../src/workers/OrderWorker.js';
import { loadConfig, type AppConfig } from '../src/config/index.js';
import type { MollyBetClient, MollyBetOrder } from '../src/services/MollyBetClient.js';

const silent = { info: () => {}, warn: () => {}, error: () => {} };
const EVENT_ID = '2026-11-05,64630,64456';
const PLAYER_A = 'atp:alcaraz-c';
const PLAYER_B = 'atp:sinner-j';

/** In-memory stand-in for the four Redis surfaces the pipeline touches. */
class FakeRedis {
  hashes = new Map<string, Record<string, string>>();
  lists = new Map<string, string[]>();
  sets = new Map<string, string>();
  /** Every pipeline op, so a test can assert what was (not) written. */
  ops: string[] = [];

  seedPlayer(id: string, h: Record<string, string>): void {
    this.hashes.set(`ta:player:${id}`, h);
  }

  async hgetall(key: string): Promise<Record<string, string>> {
    return this.hashes.get(key) ?? {};
  }

  async set(key: string, value: string): Promise<'OK'> {
    this.sets.set(key, value);
    return 'OK';
  }

  /** Non-blocking: returns the oldest entry or null, like BRPOP on timeout. */
  async brpop(key: string, _timeoutSec: number): Promise<[string, string] | null> {
    const l = this.lists.get(key);
    if (!l || l.length === 0) {
      await new Promise((r) => setTimeout(r, 1));
      return null;
    }
    return [key, l.pop() as string];
  }

  pipeline(): IngestorPipeline {
    const queued: (() => void)[] = [];
    const api: IngestorPipeline = {
      hset: (key, data) => {
        queued.push(() => {
          this.ops.push(`hset ${key}`);
          this.hashes.set(key, { ...(this.hashes.get(key) ?? {}), ...data });
        });
        return api;
      },
      lpush: (key, value) => {
        queued.push(() => {
          this.ops.push(`lpush ${key}`);
          const l = this.lists.get(key) ?? [];
          l.unshift(value);
          this.lists.set(key, l);
        });
        return api;
      },
      exec: async () => {
        queued.forEach((f) => f());
        return [];
      },
    };
    return api;
  }

  asIngestorRedis(): IngestorRedis {
    return { pipeline: () => this.pipeline() };
  }
}

/** Fake MollyBet websocket, driven frame by frame from the test. */
function makeSocket() {
  const sent: string[] = [];
  const ws: WebSocketLike = {
    send: (d) => void sent.push(d),
    close: vi.fn(),
    onopen: null,
    onmessage: null,
    onclose: null,
    onerror: null,
  };
  const factory = (): WebSocketLike => {
    setTimeout(() => ws.onopen?.({}), 0);
    return ws;
  };
  const frame = (...items: [string, unknown][]) =>
    ws.onmessage?.({ data: JSON.stringify({ ts: 1, data: items }) });
  return { ws, sent, factory, frame };
}

const eventFrame = (over: Record<string, unknown> = {}): [string, unknown] => [
  'event',
  {
    event_type: 'normal',
    sport: 'tennis',
    event_id: EVENT_ID,
    home: 'Carlos Alcaraz',
    away: 'Jannik Sinner',
    ir_status: 'in_running',
    ...over,
  },
];

const offerFrame = (betType: string, price: number): [string, unknown] => [
  'offer',
  {
    bookie: 'ps3838',
    market: 'm1',
    selection: betType === 'for,h' ? 's1' : 's2',
    sport: 'tennis',
    event_id: EVENT_ID,
    bet_type: betType,
    in_running: true,
    price_list: [{ ps3838: { price, min: ['EUR', 10], max: ['EUR', 5000] } }],
  },
];

/** A MollyBet client whose two order endpoints are mocked. */
function fakeMollyBetClient(over: Partial<Record<'openBetslip' | 'placeOrder', unknown>> = {}) {
  const openBetslip = vi.fn(async () => ({ betslip_id: 'slip-1', is_open: true }));
  const placeOrder = vi.fn(
    async (): Promise<MollyBetOrder> => ({
      order_id: 9001,
      status: 'open',
      closed: false,
      bets: [{ bet_id: 1, bookie: 'ps3838', got_price: 1.9, got_stake: ['EUR', 10] }],
    }),
  );
  const client = {
    login: vi.fn(async () => 'tok-int'),
    keepAlive: vi.fn(async () => {}),
    openBetslip,
    placeOrder,
    getOrder: vi.fn(),
    ...over,
  } as unknown as MollyBetClient;
  return {
    client,
    openBetslip: (client as unknown as { openBetslip: ReturnType<typeof vi.fn> }).openBetslip,
    placeOrder: (client as unknown as { placeOrder: ReturnType<typeof vi.fn> }).placeOrder,
  };
}

function testConfig(env: Record<string, string> = {}): AppConfig {
  return loadConfig({
    REDIS_URL: 'redis://127.0.0.1:6379',
    APPROVED_ORDER_QUEUE: 'ApprovedOrder',
    REDIS_BLOCK_TIMEOUT_SEC: '1',
    MAX_ALLOWED_E2E_LATENCY_MS: '100000',
    ...env,
  } as NodeJS.ProcessEnv);
}

/** Drain the queue with a worker until `done()` or the budget expires. */
async function drain(worker: OrderWorker, done: () => boolean, budgetMs = 2000): Promise<void> {
  const run = worker.run();
  const start = Date.now();
  while (!done() && Date.now() - start < budgetMs) {
    await new Promise((r) => setTimeout(r, 5));
  }
  worker.stop();
  await run;
}

/** Wire the whole pipeline over one FakeRedis. */
function buildPipeline(opts: {
  redis: FakeRedis;
  client: MollyBetClient;
  provider: BetInAsiaProvider;
  config: AppConfig;
  settlement?: SettlementService;
}) {
  const { redis, client, provider, config } = opts;
  const featureStore = new RedisFeatureStoreClient({ redis, logger: silent });
  const ingestor = new LiveFeedIngestor({
    redis: redis.asIngestorRedis(),
    connect: provider.connector,
    ratings: featureStoreRatingResolver({ client: featureStore, cacheTtlMs: 0, logger: silent }),
    minEv: config.execution.minEv,
    stakeUnit: config.execution.stakeUnit,
    logger: silent,
  });
  const breaker = new CircuitBreaker({
    failureThreshold: config.circuitBreaker.failureThreshold,
    resetTimeoutMs: config.circuitBreaker.resetTimeoutMs,
    latencyThresholdMs: config.circuitBreaker.maxE2ELatencyMs,
    logger: silent,
  });
  const executor = new MollyBetExecutor({
    client,
    sport: config.betinasia.sport,
    currency: config.betinasia.currency,
    orderDurationSec: config.betinasia.orderDurationSec,
    maxSlippageTolerance: config.execution.maxSlippageTolerance,
    shadowMode: config.shadowMode,
    breaker,
    liveOdds: (id, side) => provider.getBestPrice(id, side),
    logger: silent,
  });
  const settlement = opts.settlement ?? new SettlementService({ redis, logger: silent });
  const metrics = new MetricsCollector({ logger: silent });
  const worker = new OrderWorker({
    redis: redis as unknown as never,
    loader: new FeatureLoader({ client: featureStore, logger: silent }),
    executor,
    breaker,
    metrics,
    settlement,
    config,
    logger: silent,
  });
  return { ingestor, executor, worker, settlement, metrics, breaker };
}

describe('MollyBet end-to-end', () => {
  let redis: FakeRedis;
  let sock: ReturnType<typeof makeSocket>;
  let provider: BetInAsiaProvider;
  let mb: ReturnType<typeof fakeMollyBetClient>;

  beforeEach(async () => {
    redis = new FakeRedis();
    // Elo gap makes backing the home side clearly +EV at 1.9.
    redis.seedPlayer(PLAYER_A, { elo_blend: '2000', fatigue: '12', form_rating: '3', updated_at: '1' });
    redis.seedPlayer(PLAYER_B, { elo_blend: '1800', fatigue: '30', form_rating: '1', updated_at: '1' });
    sock = makeSocket();
    mb = fakeMollyBetClient();
    provider = new BetInAsiaProvider({
      client: mb.client,
      wsUrl: 'wss://api.mollybet.com/v1/stream/',
      wsFactory: sock.factory,
      logger: silent,
    });
  });

  afterEach(() => vi.clearAllMocks());

  /** Push the frames that make one live, two-sided tennis market. */
  const streamLiveMarket = (priceHome = 1.9, priceAway = 2.1): void => {
    sock.frame(eventFrame());
    sock.frame(offerFrame('for,h', priceHome));
    sock.frame(offerFrame('for,a', priceAway));
  };

  it('carries a live MollyBet price through to a placed order and a settled PnL', async () => {
    const config = testConfig();
    const { ingestor, worker, settlement } = buildPipeline({ redis, client: mb.client, provider, config });

    await ingestor.start();
    streamLiveMarket();
    await vi.waitFor(() => expect(redis.lists.get('ApprovedOrder')?.length).toBe(1));

    // --- The queued order carries what the executor needs. ---
    const queued = JSON.parse(redis.lists.get('ApprovedOrder')![0] as string);
    expect(queued).toMatchObject({
      marketId: EVENT_ID,
      matchId: EVENT_ID,
      selection: PLAYER_A,
      playerAId: PLAYER_A,
      playerBId: PLAYER_B,
      targetOdds: 1.9,
      stake: 10,
      market: 'match_winner',
    });
    expect(queued.ev).toBeGreaterThan(config.execution.minEv);

    // --- Worker -> executor -> official API. ---
    await drain(worker, () => mb.placeOrder.mock.calls.length > 0);

    expect(mb.openBetslip).toHaveBeenCalledWith({
      sport: 'tennis',
      event_id: EVENT_ID,
      bet_type: 'for,h',
    });
    const sent = mb.placeOrder.mock.calls[0]![0] as Record<string, unknown>;
    expect(sent).toMatchObject({
      betslip_id: 'slip-1',
      price: 1.862, // 1.9 * (1 - 0.02) floor
      stake: ['EUR', 10],
    });

    // --- Settlement: the emitted order is recorded, then settled. ---
    const settled = settlement.settleMatch(EVENT_ID, PLAYER_A, 1.75);
    expect(settled).toHaveLength(1);
    const report = settlement.getFinancialReport();
    expect(report).toMatchObject({ totalOrders: 1, settledOrders: 1, won: 1, lost: 0 });
    expect(report.totalPnl).toBeCloseTo(9, 6); // 10 * (1.9 - 1)
    expect(report.avgClvEdgePct).toBeGreaterThan(0); // took 1.9, market closed 1.75

    ingestor.stop();
  });

  it('runs the whole chain in SHADOW_MODE without posting an order', async () => {
    const config = testConfig({ SHADOW_MODE: 'true' });
    const { ingestor, worker, settlement, metrics } = buildPipeline({
      redis,
      client: mb.client,
      provider,
      config,
    });

    await ingestor.start();
    streamLiveMarket();
    await vi.waitFor(() => expect(redis.lists.get('ApprovedOrder')?.length).toBe(1));

    await drain(worker, () => metrics.exportMetricsSummary().counters.totalOrders > 0);

    expect(mb.openBetslip).not.toHaveBeenCalled();
    expect(mb.placeOrder).not.toHaveBeenCalled();
    // The order is still audited, so paper-trading PnL stays measurable.
    expect(settlement.getFinancialReport().totalOrders).toBe(1);
    expect(metrics.exportMetricsSummary().counters.shadowInvocations).toBe(1);

    ingestor.stop();
  });

  it('resolves ratings from the Feature Store and never writes them back', async () => {
    const config = testConfig();
    const { ingestor } = buildPipeline({ redis, client: mb.client, provider, config });

    await ingestor.start();
    streamLiveMarket();
    await vi.waitFor(() => expect(redis.lists.get('ApprovedOrder')?.length).toBe(1));

    // MollyBet carries no ratings, so nothing was hashed back into ta:player:*.
    expect(redis.ops.filter((o) => o.startsWith('hset'))).toHaveLength(0);
    expect(redis.ops).toEqual(['lpush ApprovedOrder']);

    ingestor.stop();
  });

  it('qualifies nothing when the Feature Store has no rating for a side', async () => {
    redis.hashes.delete(`ta:player:${PLAYER_B}`);
    const config = testConfig();
    const { ingestor } = buildPipeline({ redis, client: mb.client, provider, config });

    await ingestor.start();
    streamLiveMarket();
    await new Promise((r) => setTimeout(r, 50));

    expect(redis.lists.get('ApprovedOrder') ?? []).toHaveLength(0);

    ingestor.stop();
  });

  it('rejects on slippage when the live price falls away from the queued target', async () => {
    const config = testConfig();
    const { ingestor, worker, metrics, settlement } = buildPipeline({
      redis,
      client: mb.client,
      provider,
      config,
    });

    await ingestor.start();
    streamLiveMarket(1.9, 2.1);
    await vi.waitFor(() => expect(redis.lists.get('ApprovedOrder')?.length).toBe(1));
    const queuedAt19 = redis.lists.get('ApprovedOrder')![0] as string;

    // The market moves hard against us before the worker gets to the order.
    sock.frame(offerFrame('for,h', 1.6));
    await vi.waitFor(() => expect(provider.getBestPrice(EVENT_ID, 'home')).toBe(1.6));

    // 1.6 is still +EV on this Elo gap, so the ingestor queues a second, fresh
    // order at the new price. Stop ingesting and keep only the stale 1.9 one:
    // this test is about what the guard does with a target the market has
    // already left behind. The provider keeps its last prices after the stop,
    // so the guard still reads 1.6 as the live price.
    ingestor.stop();
    await new Promise((r) => setTimeout(r, 20));
    redis.lists.set('ApprovedOrder', [queuedAt19]);
    expect(provider.getBestPrice(EVENT_ID, 'home')).toBe(1.6);

    await drain(worker, () => metrics.exportMetricsSummary().counters.totalOrders > 0);

    expect(mb.placeOrder).not.toHaveBeenCalled();
    expect(mb.openBetslip).not.toHaveBeenCalled();
    expect(metrics.exportMetricsSummary().counters.rejectedSlippage).toBe(1);
    // A refused order is not a bet, so it never enters the PnL audit.
    expect(settlement.getFinancialReport().totalOrders).toBe(0);
  });

  it('trips the circuit breaker when the official API keeps refusing', async () => {
    const config = testConfig({ CIRCUIT_BREAKER_FAILURE_THRESHOLD: '1' });
    const failing = fakeMollyBetClient({
      placeOrder: vi.fn(async () => {
        throw Object.assign(new Error('POST /v1/orders/ failed: HTTP 403 forbidden'), {
          name: 'MollyBetError',
          httpStatus: 403,
        });
      }),
    });
    const { ingestor, worker, breaker, metrics } = buildPipeline({
      redis,
      client: failing.client,
      provider,
      config,
    });

    await ingestor.start();
    streamLiveMarket();
    await vi.waitFor(() => expect(redis.lists.get('ApprovedOrder')?.length).toBe(1));

    await drain(worker, () => metrics.exportMetricsSummary().counters.totalOrders > 0);

    expect(failing.placeOrder).toHaveBeenCalledTimes(1);
    expect(metrics.exportMetricsSummary().counters.failedDomOrWaf).toBe(1);
    expect(breaker.getState()).toBe('OPEN');

    ingestor.stop();
  });
});

describe('OrderWorker executor wiring', () => {
  it('runs an API executor with no BrowserManager at all', async () => {
    const redis = new FakeRedis();
    redis.seedPlayer(PLAYER_A, { elo_blend: '2000' });
    redis.seedPlayer(PLAYER_B, { elo_blend: '1800' });
    redis.lists.set('ApprovedOrder', [
      JSON.stringify({
        orderId: 'ord-1',
        marketId: EVENT_ID,
        playerAId: PLAYER_A,
        playerBId: PLAYER_B,
        selection: PLAYER_A,
        targetOdds: 2,
        stake: 10,
      }),
    ]);
    const mb = fakeMollyBetClient();
    const config = testConfig();
    const executor = new MollyBetExecutor({ client: mb.client, shadowMode: false, logger: silent });
    const worker = new OrderWorker({
      redis: redis as unknown as never,
      loader: new FeatureLoader({
        client: new RedisFeatureStoreClient({ redis, logger: silent }),
        logger: silent,
      }),
      executor,
      config,
      logger: silent,
    });

    await drain(worker, () => mb.placeOrder.mock.calls.length > 0);
    expect(mb.placeOrder).toHaveBeenCalledTimes(1);
  });

  it('refuses to start a DOM executor without a BrowserManager', () => {
    const redis = new FakeRedis();
    expect(
      () =>
        new OrderWorker({
          redis: redis as unknown as never,
          loader: new FeatureLoader({
            client: new RedisFeatureStoreClient({ redis, logger: silent }),
            logger: silent,
          }),
          executor: new OrderExecutor({ logger: silent }),
          config: testConfig(),
          logger: silent,
        }),
    ).toThrow(/needs a browser/);
  });
});
