/**
 * Health & metrics HTTP server for the Execution Gateway.
 *
 * A deliberately tiny server on `node:http` -- no framework -- exposing two
 * read-only JSON endpoints for liveness probes and observability:
 *
 *   GET /health   -> 200 { status: 'UP', circuitState, uptimeSeconds }
 *   GET /metrics  -> 200 MetricsCollector.exportMetricsSummary()
 *
 * The server is created but NOT started; the caller decides the port
 * (`server.listen(...)`), which keeps it trivially unit-testable on an
 * ephemeral port.
 */

import { createServer, type Server, type ServerResponse } from 'node:http';
import { MetricsCollector } from './telemetry/MetricsCollector.js';

export interface HealthServerOptions {
  metrics: MetricsCollector;
  /** Current circuit-breaker state, read live per request. */
  getCircuitState: () => string;
  /** Seconds the process/gateway has been up. Defaults to process uptime. */
  getUptimeSeconds?: () => number;
  logger?: Pick<Console, 'info' | 'error'>;
}

function sendJson(res: ServerResponse, status: number, body: unknown): void {
  const payload = JSON.stringify(body);
  res.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'content-length': Buffer.byteLength(payload),
  });
  res.end(payload);
}

export function createHealthServer(opts: HealthServerOptions): Server {
  const { metrics, getCircuitState } = opts;
  const logger = opts.logger ?? console;
  const uptimeSeconds = opts.getUptimeSeconds ?? (() => Math.floor(process.uptime()));

  return createServer((req, res) => {
    const method = req.method ?? 'GET';
    const path = (req.url ?? '/').split('?')[0] ?? '/';

    try {
      if (method === 'GET' && path === '/health') {
        sendJson(res, 200, {
          status: 'UP',
          circuitState: getCircuitState(),
          uptimeSeconds: uptimeSeconds(),
        });
        return;
      }

      if (method === 'GET' && path === '/metrics') {
        sendJson(res, 200, metrics.exportMetricsSummary());
        return;
      }

      sendJson(res, 404, { error: 'not_found', path });
    } catch (err) {
      logger.error(`health-server: ${(err as Error).message}`);
      sendJson(res, 500, { error: 'internal_error' });
    }
  });
}
