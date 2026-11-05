/**
 * MollyBetClient unit tests (fetch mocked -- no network): session login, the
 * `Session` header, transparent re-login on 401, error-envelope mapping and
 * request timeouts.
 */

import { describe, expect, it, vi } from 'vitest';
import { MollyBetClient, MollyBetError } from '../src/services/MollyBetClient.js';

const silent = { info: () => {}, warn: () => {}, error: () => {} };

const json = (status: number, body: unknown): Response =>
  new Response(JSON.stringify(body), { status, headers: { 'Content-Type': 'application/json' } });

function make(fetchImpl: typeof fetch, timeoutMs = 1000) {
  return new MollyBetClient({
    apiUrl: 'https://api.mollybet.com/',
    username: 'acct',
    password: 'secret',
    timeoutMs,
    fetch: fetchImpl,
    logger: silent,
  });
}

describe('MollyBetClient', () => {
  it('logs in with username/password and sends the token in the Session header', async () => {
    const fetchMock = vi.fn(async (url: string | URL | Request) =>
      String(url).endsWith('/v1/sessions/')
        ? json(200, { status: 'ok', data: 'tok-1' })
        : json(200, { status: 'ok', data: { betslip_id: 'slip-1', is_open: true } }),
    );
    const client = make(fetchMock as unknown as typeof fetch);

    const slip = await client.openBetslip({ sport: 'tennis', event_id: 'e1', bet_type: 'for,h' });
    expect(slip.betslip_id).toBe('slip-1');

    const [loginUrl, loginInit] = fetchMock.mock.calls[0] as unknown as [string, RequestInit];
    expect(loginUrl).toBe('https://api.mollybet.com/v1/sessions/');
    expect(JSON.parse(loginInit.body as string)).toEqual({ username: 'acct', password: 'secret' });

    const [, slipInit] = fetchMock.mock.calls[1] as unknown as [string, RequestInit];
    expect((slipInit.headers as Record<string, string>).Session).toBe('tok-1');
    expect(JSON.parse(slipInit.body as string)).toEqual({ sport: 'tennis', event_id: 'e1', bet_type: 'for,h' });
  });

  it('re-logs in once on 401 and replays the request', async () => {
    let logins = 0;
    const fetchMock = vi.fn(async (url: string | URL | Request, init?: RequestInit) => {
      if (String(url).endsWith('/v1/sessions/')) {
        logins++;
        return json(200, { status: 'ok', data: `tok-${logins}` });
      }
      const session = (init?.headers as Record<string, string>).Session;
      return session === 'tok-1'
        ? json(401, { status: 'error', code: 'auth_rejected' })
        : json(200, { status: 'ok', data: { order_id: 9 } });
    });
    const client = make(fetchMock as unknown as typeof fetch);

    const order = await client.placeOrder({ betslip_id: 's', price: 1.9, stake: ['EUR', 10] });
    expect(order.order_id).toBe(9);
    expect(logins).toBe(2);
  });

  it('shares one login between concurrent callers', async () => {
    let logins = 0;
    const fetchMock = vi.fn(async (url: string | URL | Request) => {
      if (String(url).endsWith('/v1/sessions/')) {
        logins++;
        await new Promise((r) => setTimeout(r, 5));
        return json(200, { status: 'ok', data: 'tok' });
      }
      return json(200, { status: 'ok', data: {} });
    });
    const client = make(fetchMock as unknown as typeof fetch);
    await Promise.all([client.getOrder(1), client.getOrder(2), client.getOrder(3)]);
    expect(logins).toBe(1);
  });

  it('maps an error envelope to MollyBetError with the HTTP status and code', async () => {
    const fetchMock = vi.fn(async (url: string | URL | Request) =>
      String(url).endsWith('/v1/sessions/')
        ? json(200, { status: 'ok', data: 'tok' })
        : json(400, { status: 'error', code: 'validation_error', data: { price: ['too low'] } }),
    );
    const client = make(fetchMock as unknown as typeof fetch);

    const err = await client.placeOrder({ betslip_id: 's', price: 0.5, stake: ['EUR', 1] }).catch((e) => e);
    expect(err).toBeInstanceOf(MollyBetError);
    expect(err).toMatchObject({ httpStatus: 400, code: 'validation_error', data: { price: ['too low'] } });
    expect(err.message).not.toContain('secret');
  });

  it('fails on bad credentials without looping', async () => {
    const fetchMock = vi.fn(async () => json(401, { status: 'error', code: 'invalid_credentials' }));
    const client = make(fetchMock as unknown as typeof fetch);
    await expect(client.login()).rejects.toMatchObject({ httpStatus: 401, code: 'invalid_credentials' });
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it('times out slow requests', async () => {
    const fetchMock = vi.fn(
      (_url: string | URL | Request, init?: RequestInit) =>
        new Promise<Response>((_res, rej) => {
          init?.signal?.addEventListener('abort', () => rej(Object.assign(new Error('aborted'), { name: 'AbortError' })));
        }),
    );
    const client = make(fetchMock as unknown as typeof fetch, 20);
    await expect(client.login()).rejects.toMatchObject({ code: 'timeout', httpStatus: 0 });
  });
});
