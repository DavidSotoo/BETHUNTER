/**
 * WebSocketTransport -- a real `ws` transport for the LiveFeedIngestor.
 *
 * Each `connect()` opens one WebSocket and returns a FeedConnection the
 * ingestor observes. A per-connection heartbeat pings the server on an
 * interval and, if no pong arrives within the deadline, TERMINATES the socket
 * (within < 5s) -- that emits 'close', and the ingestor's own backoff policy
 * reconnects by calling `connect()` again. Reconnection is therefore driven by
 * the ingestor; the transport's job is to detect a hung socket fast.
 *
 * The `ws` implementation is injectable so the transport is unit-testable
 * without opening a real socket.
 */

import WebSocket from 'ws';
import type { FeedConnection } from '../LiveFeedIngestor.js';

/** Minimal surface of a `ws` socket the transport uses. */
export interface WebSocketLike {
  on(event: string, listener: (...args: unknown[]) => void): void;
  ping(): void;
  terminate(): void;
  close(): void;
}
export interface WebSocketCtor {
  new (url: string, options?: { headers?: Record<string, string> }): WebSocketLike;
}

export interface WebSocketTransportOptions {
  url: string;
  /** Sent as `Authorization: Bearer <key>` when set. */
  apiKey?: string;
  /** Ping cadence. Default 1500ms. */
  heartbeatIntervalMs?: number;
  /** Terminate if no pong lands within this window. Default 3000ms. */
  pongTimeoutMs?: number;
  /** Injectable ws implementation (defaults to the real `ws`). */
  WebSocketImpl?: WebSocketCtor;
  logger?: Pick<Console, 'info' | 'warn' | 'error'>;
}

export class WebSocketTransport {
  private readonly url: string;
  private readonly apiKey: string;
  private readonly heartbeatIntervalMs: number;
  private readonly pongTimeoutMs: number;
  private readonly Ctor: WebSocketCtor;
  private readonly logger: Pick<Console, 'info' | 'warn' | 'error'>;

  constructor(opts: WebSocketTransportOptions) {
    this.url = opts.url;
    this.apiKey = opts.apiKey ?? '';
    // Worst-case hang detection = interval + pongTimeout; defaults give ~4.5s (<5s).
    this.heartbeatIntervalMs = opts.heartbeatIntervalMs ?? 1500;
    this.pongTimeoutMs = opts.pongTimeoutMs ?? 3000;
    this.Ctor = opts.WebSocketImpl ?? (WebSocket as unknown as WebSocketCtor);
    this.logger = opts.logger ?? console;
  }

  /** Open one socket; resolves to a FeedConnection once the socket is open. */
  connect(): Promise<FeedConnection> {
    const options = this.apiKey ? { headers: { Authorization: `Bearer ${this.apiKey}` } } : undefined;
    const ws = new this.Ctor(this.url, options);

    const messageCbs: ((raw: unknown) => void)[] = [];
    const closeCbs: (() => void)[] = [];
    const errorCbs: ((err: Error) => void)[] = [];

    let heartbeat: ReturnType<typeof setInterval> | undefined;
    let pongTimer: ReturnType<typeof setTimeout> | undefined;
    let settled = false;

    const clearTimers = (): void => {
      if (heartbeat) clearInterval(heartbeat);
      if (pongTimer) clearTimeout(pongTimer);
      heartbeat = undefined;
      pongTimer = undefined;
    };

    const startHeartbeat = (): void => {
      heartbeat = setInterval(() => {
        if (pongTimer) return; // still awaiting the previous pong
        ws.ping();
        pongTimer = setTimeout(() => {
          this.logger.warn('WebSocketTransport: no pong within deadline; terminating socket');
          ws.terminate(); // -> 'close' -> ingestor reconnects
        }, this.pongTimeoutMs);
      }, this.heartbeatIntervalMs);
    };

    return new Promise<FeedConnection>((resolve, reject) => {
      const connection: FeedConnection = {
        onMessage: (cb) => messageCbs.push(cb),
        onClose: (cb) => closeCbs.push(cb),
        onError: (cb) => errorCbs.push(cb),
        close: () => {
          clearTimers();
          ws.close();
        },
      };

      ws.on('open', () => {
        startHeartbeat();
        if (!settled) {
          settled = true;
          this.logger.info(`WebSocketTransport: connected to ${this.url}`);
          resolve(connection);
        }
      });

      ws.on('message', (...args: unknown[]) => {
        try {
          const data = args[0];
          const text =
            typeof data === 'string' ? data : data instanceof Buffer ? data.toString('utf8') : String(data);
          messageCbs.forEach((cb) => cb(JSON.parse(text)));
        } catch (err) {
          this.logger.warn(`WebSocketTransport: dropping unparseable message: ${(err as Error).message}`);
        }
      });

      ws.on('pong', () => {
        if (pongTimer) {
          clearTimeout(pongTimer);
          pongTimer = undefined;
        }
      });

      ws.on('close', () => {
        clearTimers();
        closeCbs.forEach((cb) => cb());
        if (!settled) {
          settled = true;
          reject(new Error('WebSocketTransport: socket closed before open'));
        }
      });

      ws.on('error', (...args: unknown[]) => {
        const err = args[0] instanceof Error ? args[0] : new Error(String(args[0]));
        errorCbs.forEach((cb) => cb(err));
        if (!settled) {
          settled = true;
          clearTimers();
          reject(err);
        }
      });
    });
  }
}
