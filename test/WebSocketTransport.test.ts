/**
 * WebSocketTransport unit tests ('ws' mocked via an injected implementation).
 *
 * Validate: connect resolves on open, heartbeat ping, hung-socket termination
 * (the reconnect trigger), pong cancelling the deadline, message parsing,
 * close/error propagation, and the API-key header.
 */

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import {
  WebSocketTransport,
  type WebSocketCtor,
} from '../src/feed/transports/WebSocketTransport.js';

const silent = { info: () => {}, warn: () => {}, error: () => {} };

/** Controllable fake `ws` socket. */
class FakeWS {
  static instances: FakeWS[] = [];
  private handlers: Record<string, ((...args: unknown[]) => void)[]> = {};
  ping = vi.fn();
  terminate = vi.fn(() => this.emit('close'));
  close = vi.fn(() => this.emit('close'));

  constructor(
    public url: string,
    public options?: { headers?: Record<string, string> },
  ) {
    FakeWS.instances.push(this);
  }

  on(event: string, listener: (...args: unknown[]) => void): void {
    (this.handlers[event] ??= []).push(listener);
  }
  emit(event: string, ...args: unknown[]): void {
    (this.handlers[event] ?? []).forEach((f) => f(...args));
  }
}

const Impl = FakeWS as unknown as WebSocketCtor;
const last = (): FakeWS => FakeWS.instances[FakeWS.instances.length - 1] as FakeWS;

beforeEach(() => {
  FakeWS.instances = [];
});
afterEach(() => {
  vi.useRealTimers();
});

describe('WebSocketTransport', () => {
  it('resolves the connection once the socket opens', async () => {
    const t = new WebSocketTransport({ url: 'ws://x', WebSocketImpl: Impl, logger: silent });
    const p = t.connect();
    last().emit('open');
    await expect(p).resolves.toMatchObject({ onMessage: expect.any(Function) });
  });

  it('sends the API key as an Authorization header', async () => {
    const t = new WebSocketTransport({ url: 'ws://x', apiKey: 'secret', WebSocketImpl: Impl, logger: silent });
    const p = t.connect();
    expect(last().options?.headers?.Authorization).toBe('Bearer secret');
    last().emit('open');
    await p;
  });

  it('parses JSON messages and forwards them', async () => {
    const t = new WebSocketTransport({ url: 'ws://x', WebSocketImpl: Impl, logger: silent });
    const p = t.connect();
    last().emit('open');
    const conn = await p;

    const received: unknown[] = [];
    conn.onMessage((m) => received.push(m));
    last().emit('message', Buffer.from(JSON.stringify({ matchId: 'm1' })));
    expect(received).toEqual([{ matchId: 'm1' }]);
  });

  it('pings on the heartbeat interval and terminates a hung socket (< 5s)', async () => {
    vi.useFakeTimers();
    const t = new WebSocketTransport({
      url: 'ws://x',
      WebSocketImpl: Impl,
      heartbeatIntervalMs: 1500,
      pongTimeoutMs: 3000,
      logger: silent,
    });
    const p = t.connect();
    const ws = last();
    ws.emit('open');
    await p;

    await vi.advanceTimersByTimeAsync(1500);
    expect(ws.ping).toHaveBeenCalledTimes(1);

    // No pong -> terminate within the window (1500 + 3000 = 4500ms < 5s).
    await vi.advanceTimersByTimeAsync(3000);
    expect(ws.terminate).toHaveBeenCalledTimes(1);
  });

  it('does not terminate when a pong arrives in time', async () => {
    vi.useFakeTimers();
    const t = new WebSocketTransport({
      url: 'ws://x',
      WebSocketImpl: Impl,
      heartbeatIntervalMs: 1500,
      pongTimeoutMs: 3000,
      logger: silent,
    });
    const p = t.connect();
    const ws = last();
    ws.emit('open');
    await p;

    await vi.advanceTimersByTimeAsync(1500);
    ws.emit('pong'); // healthy
    await vi.advanceTimersByTimeAsync(3000);
    expect(ws.terminate).not.toHaveBeenCalled();
  });

  it('propagates close to onClose handlers (the reconnect trigger)', async () => {
    const t = new WebSocketTransport({ url: 'ws://x', WebSocketImpl: Impl, logger: silent });
    const p = t.connect();
    last().emit('open');
    const conn = await p;

    const closes: number[] = [];
    conn.onClose(() => closes.push(1));
    last().emit('close');
    expect(closes).toHaveLength(1);
  });

  it('rejects connect when the socket errors before opening', async () => {
    const t = new WebSocketTransport({ url: 'ws://x', WebSocketImpl: Impl, logger: silent });
    const p = t.connect();
    last().emit('error', new Error('ECONNREFUSED'));
    await expect(p).rejects.toThrow('ECONNREFUSED');
  });
});
