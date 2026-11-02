/**
 * Health server tests.
 *
 * Each test starts the server on an ephemeral port, hits it with the global
 * fetch, and asserts the JSON response. Connections are force-closed on
 * teardown so the keep-alive agent cannot hold the server open.
 */

import { describe, expect, it } from 'vitest';
import type { AddressInfo } from 'node:net';
import type { Server } from 'node:http';
import { createHealthServer } from '../src/server.js';
import { MetricsCollector, OrderMetricSample } from '../src/telemetry/MetricsCollector.js';
import { CircuitState } from '../src/services/CircuitBreaker.js';
import { SettlementService } from '../src/services/SettlementService.js';
import type { HydratedOrderPayload, PlayerFeatures } from '../src/types/features.js';

const silent = { info: () => {}, error: () => {} };

function listen(server: Server): Promise<number> {
  return new Promise((resolve) =>
    server.listen(0, () => resolve((server.address() as AddressInfo).port)),
  );
}

function close(server: Server): Promise<void> {
  return new Promise((resolve) => {
    server.close(() => resolve());
    server.closeAllConnections(); // drop keep-alive sockets so close() resolves
  });
}

function sample(overrides: Partial<OrderMetricSample> = {}): OrderMetricSample {
  return {
    orderId: 'o1',
    outcome: 'EXECUTED',
    shadow: false,
    hydrationMs: 2,
    browserAcquisitionMs: 5,
    domCheckMs: 3,
    totalE2EMs: 12,
    circuitState: CircuitState.CLOSED,
    ...overrides,
  };
}

describe('health server', () => {
  it('GET /health returns UP with circuit state and uptime', async () => {
    const metrics = new MetricsCollector({ logger: silent });
    const server = createHealthServer({
      metrics,
      getCircuitState: () => CircuitState.CLOSED,
      getUptimeSeconds: () => 42,
      logger: silent,
    });
    const port = await listen(server);
    try {
      const res = await fetch(`http://127.0.0.1:${port}/health`);
      expect(res.status).toBe(200);
      expect(res.headers.get('content-type')).toMatch(/application\/json/);
      const body = (await res.json()) as {
        status: string;
        circuitState: string;
        uptimeSeconds: number;
      };
      expect(body.status).toBe('UP');
      expect(body.circuitState).toBe('CLOSED');
      expect(body.uptimeSeconds).toBe(42);
    } finally {
      await close(server);
    }
  });

  it('GET /metrics returns the metrics summary', async () => {
    const metrics = new MetricsCollector({ logger: silent });
    metrics.record(sample());
    metrics.record(sample({ outcome: 'REJECTED_SLIPPAGE' }));
    const server = createHealthServer({
      metrics,
      getCircuitState: () => CircuitState.OPEN,
      logger: silent,
    });
    const port = await listen(server);
    try {
      const res = await fetch(`http://127.0.0.1:${port}/metrics`);
      expect(res.status).toBe(200);
      const body = (await res.json()) as {
        counters: { totalOrders: number; executed: number; rejectedSlippage: number };
        rates: { slippagePct: number };
        latencyMs: { totalE2E: { count: number } };
      };
      expect(body.counters.totalOrders).toBe(2);
      expect(body.counters.executed).toBe(1);
      expect(body.counters.rejectedSlippage).toBe(1);
      expect(body.rates.slippagePct).toBe(50);
      expect(body.latencyMs.totalE2E.count).toBe(2);
    } finally {
      await close(server);
    }
  });

  it('returns 404 for an unknown route', async () => {
    const metrics = new MetricsCollector({ logger: silent });
    const server = createHealthServer({
      metrics,
      getCircuitState: () => CircuitState.CLOSED,
      logger: silent,
    });
    const port = await listen(server);
    try {
      const res = await fetch(`http://127.0.0.1:${port}/nope`);
      expect(res.status).toBe(404);
      const body = (await res.json()) as { error: string };
      expect(body.error).toBe('not_found');
    } finally {
      await close(server);
    }
  });

  it('GET /financial-report returns the settlement audit', async () => {
    const feat = (id: string): PlayerFeatures => ({
      id,
      elo: 1900,
      fatigueScore: 0,
      formRating: 0,
      updatedAt: 0,
    });
    const order: HydratedOrderPayload = {
      orderId: 'o1',
      playerAId: 'pa',
      playerBId: 'pb',
      matchId: 'm1',
      selection: 'home',
      targetOdds: 2.1,
      stake: 10,
      features: { playerA: feat('pa'), playerB: feat('pb') },
      hydratedAt: 0,
      hydrationSource: { playerA: 'cache', playerB: 'cache' },
      hydrationLatencyMs: 0,
    };
    const settlement = new SettlementService({ logger: { warn: () => {} } });
    settlement.recordEmittedOrder(order);
    settlement.settleMatch('m1', 'home', 2.0); // WON, +5% CLV, +11 PnL

    const metrics = new MetricsCollector({ logger: silent });
    const server = createHealthServer({
      metrics,
      getCircuitState: () => CircuitState.CLOSED,
      getFinancialReport: () => settlement.getFinancialReport(),
      logger: silent,
    });
    const port = await listen(server);
    try {
      const res = await fetch(`http://127.0.0.1:${port}/financial-report`);
      expect(res.status).toBe(200);
      const body = (await res.json()) as {
        totalOrders: number;
        won: number;
        winRatePct: number;
        totalPnl: number;
        btclRatePct: number;
      };
      expect(body.totalOrders).toBe(1);
      expect(body.won).toBe(1);
      expect(body.winRatePct).toBeCloseTo(100, 2);
      expect(body.totalPnl).toBeCloseTo(11, 2);
      expect(body.btclRatePct).toBeCloseTo(100, 2);
    } finally {
      await close(server);
    }
  });

  it('GET /financial-report returns 503 when no settlement is wired', async () => {
    const metrics = new MetricsCollector({ logger: silent });
    const server = createHealthServer({
      metrics,
      getCircuitState: () => CircuitState.CLOSED,
      logger: silent,
    });
    const port = await listen(server);
    try {
      const res = await fetch(`http://127.0.0.1:${port}/financial-report`);
      expect(res.status).toBe(503);
    } finally {
      await close(server);
    }
  });
});
