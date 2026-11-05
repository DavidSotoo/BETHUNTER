/**
 * MollyBetClient -- thin, typed REST client for the official MollyBet API
 * (BetInAsia BLACK). Shared by the live-odds provider and the order executor so
 * both reuse one authenticated session.
 *
 * Contract (official docs, https://api.mollybet.com/docs):
 *
 *   POST /v1/sessions/   {username, password} -> {status:"ok", data:"<token>"}
 *   (all later calls)    `Session: <token>` header
 *   POST /v1/betslips/   {sport, event_id, bet_type} -> data.betslip_id
 *   POST /v1/orders/     {betslip_id, price, stake:[ccy,amt], ...}
 *   GET  /v1/orders/<id>/
 *
 * Envelope: success `{status:"ok", data}`, failure `{status:"error", code, data?}`.
 * A 401 means the session expired -> re-login once and replay the request.
 *
 * Credentials and the session token are never logged.
 */

export interface MollyBetClientOptions {
  /** e.g. https://api.mollybet.com */
  apiUrl: string;
  /** BIA_ACCOUNT_ID: the MollyBet username. */
  username: string;
  /** BIA_API_KEY: the credential sent as the login password. */
  password: string;
  /** Per-request budget in ms. Default 5000. */
  timeoutMs?: number;
  /** Injectable for tests. Default: global fetch. */
  fetch?: typeof fetch;
  logger?: Pick<Console, 'info' | 'warn' | 'error'>;
}

/** An error envelope or a transport failure from the MollyBet API. */
export class MollyBetError extends Error {
  constructor(
    message: string,
    /** HTTP status, or 0 for a transport failure / timeout. */
    readonly httpStatus: number,
    /** Machine-readable `code` from the error envelope, when present. */
    readonly code?: string,
    readonly data?: unknown,
  ) {
    super(message);
    this.name = 'MollyBetError';
  }
}

/** Stake as the API expects it: [ISO-4217 currency, amount]. */
export type MollyBetStake = [string, number];

export interface BetslipRequest {
  sport: string;
  event_id: string;
  bet_type: string;
}

export interface PlaceOrderRequest {
  betslip_id: string;
  /** Minimum acceptable price (decimal odds) for a normal (back) betslip. */
  price: number;
  stake: MollyBetStake;
  /** Seconds the order stays open trying to fill. */
  duration?: number;
  /** Deduplication key: a repeated UUID within 24h is acted on once. */
  request_uuid?: string;
  /** Free-form correlation string (max 512 chars), echoed back. */
  user_data?: string;
  [key: string]: unknown;
}

export interface MollyBetBet {
  bet_id?: number;
  order_id?: number;
  bookie?: string;
  want_price?: number;
  got_price?: number | null;
  want_stake?: MollyBetStake;
  got_stake?: MollyBetStake | null;
  status?: { code?: string; reason?: string };
  [key: string]: unknown;
}

/** The order object returned on placement (the official ticket confirmation). */
export interface MollyBetOrder {
  order_id: number;
  status?: string;
  closed?: boolean;
  close_reason?: string | null;
  want_price?: number;
  want_stake?: MollyBetStake;
  bet_type?: string;
  sport?: string;
  request_uuid?: string;
  user_data?: string;
  bets?: MollyBetBet[];
  [key: string]: unknown;
}

export interface MollyBetBetslip {
  betslip_id: string;
  is_open?: boolean;
  [key: string]: unknown;
}

export class MollyBetClient {
  private readonly apiUrl: string;
  private readonly username: string;
  private readonly password: string;
  private readonly timeoutMs: number;
  private readonly fetchImpl: typeof fetch;
  private readonly logger: Pick<Console, 'info' | 'warn' | 'error'>;

  private token?: string;
  private loginInFlight?: Promise<string>;

  constructor(opts: MollyBetClientOptions) {
    this.apiUrl = opts.apiUrl.replace(/\/+$/, '');
    this.username = opts.username;
    this.password = opts.password;
    this.timeoutMs = opts.timeoutMs ?? 5_000;
    this.fetchImpl = opts.fetch ?? ((...a) => fetch(...a));
    this.logger = opts.logger ?? console;
  }

  /** Create a fresh API session and remember its token. Concurrent calls share one login. */
  login(): Promise<string> {
    if (this.loginInFlight) return this.loginInFlight;
    this.loginInFlight = this.doLogin().finally(() => {
      this.loginInFlight = undefined;
    });
    return this.loginInFlight;
  }

  /** The current session token, logging in first when there is none. */
  async getToken(): Promise<string> {
    return this.token ?? this.login();
  }

  private async doLogin(): Promise<string> {
    const data = await this.send<string>(
      'POST',
      '/v1/sessions/',
      { username: this.username, password: this.password },
      undefined,
    );
    if (typeof data !== 'string' || data === '') {
      throw new MollyBetError('login returned no session token', 200, 'bad_login_response');
    }
    this.token = data;
    this.logger.info('MollyBetClient: session established');
    return data;
  }

  /** Open a betslip for one selection; yields the `betslip_id` an order needs. */
  openBetslip(req: BetslipRequest): Promise<MollyBetBetslip> {
    return this.request<MollyBetBetslip>('POST', '/v1/betslips/', req);
  }

  /** Place an order against an open betslip. Returns the API's order confirmation. */
  placeOrder(req: PlaceOrderRequest): Promise<MollyBetOrder> {
    return this.request<MollyBetOrder>('POST', '/v1/orders/', req);
  }

  getOrder(orderId: number): Promise<MollyBetOrder> {
    return this.request<MollyBetOrder>('GET', `/v1/orders/${orderId}/`);
  }

  /**
   * Cheap authenticated call used as a keep-alive: API sessions expire after 24h
   * without HTTP activity, and websocket commands do not count as activity.
   */
  async keepAlive(): Promise<void> {
    await this.request<unknown>('GET', '/v1/orders/');
  }

  /** Authenticated request; re-logs in once on 401 and replays. */
  async request<T>(method: 'GET' | 'POST', path: string, body?: unknown): Promise<T> {
    const token = await this.getToken();
    try {
      return await this.send<T>(method, path, body, token);
    } catch (err) {
      if (err instanceof MollyBetError && err.httpStatus === 401) {
        this.token = undefined;
        const fresh = await this.login();
        return this.send<T>(method, path, body, fresh);
      }
      throw err;
    }
  }

  private async send<T>(
    method: 'GET' | 'POST',
    path: string,
    body: unknown,
    token: string | undefined,
  ): Promise<T> {
    const headers: Record<string, string> = { Accept: 'application/json' };
    if (token) headers.Session = token;

    let url = this.apiUrl + path;
    let payload: string | undefined;
    if (method === 'POST' && body !== undefined) {
      headers['Content-Type'] = 'application/json; charset=utf-8';
      payload = JSON.stringify(body);
    } else if (method === 'GET' && body && typeof body === 'object') {
      url += `?${new URLSearchParams(body as Record<string, string>).toString()}`;
    }

    const ac = new AbortController();
    const timer = setTimeout(() => ac.abort(), this.timeoutMs);
    let res: Response;
    try {
      res = await this.fetchImpl(url, { method, headers, body: payload, signal: ac.signal });
    } catch (err) {
      const aborted = (err as Error).name === 'AbortError';
      throw new MollyBetError(
        aborted
          ? `${method} ${path} timed out after ${this.timeoutMs}ms`
          : `${method} ${path} transport error: ${(err as Error).message}`,
        0,
        aborted ? 'timeout' : 'transport_error',
      );
    } finally {
      clearTimeout(timer);
    }

    let json: { status?: string; code?: string; data?: unknown } | undefined;
    try {
      json = (await res.json()) as typeof json;
    } catch {
      json = undefined;
    }

    if (!res.ok || !json || json.status !== 'ok') {
      const code = json?.code;
      // The word "403"/"429" is deliberate: OrderWorker's block heuristic keys on it.
      throw new MollyBetError(
        `${method} ${path} failed: HTTP ${res.status}${code ? ` ${code}` : ''}`,
        res.status,
        code,
        json?.data,
      );
    }
    return json.data as T;
  }
}
