/**
 * Centralised runtime configuration for the Execution Gateway.
 *
 * Everything the gateway tunes at runtime is read from the environment here,
 * once, into a typed `AppConfig`. Modules import `config` instead of touching
 * `process.env` directly, so the surface of env coupling is one file and the
 * defaults are all visible in one place.
 *
 * SHADOW_MODE is the headline flag: when true the worker runs the entire
 * pipeline -- hydrate, acquire a browser context, navigate -- but stops short
 * of the final, irreversible transaction. It is how we exercise the live path
 * (and measure its latency) without placing a real bet.
 */

function envStr(env: NodeJS.ProcessEnv, name: string, def: string): string {
  const v = env[name];
  return v === undefined || v === '' ? def : v;
}

function envBool(env: NodeJS.ProcessEnv, name: string, def: boolean): boolean {
  const v = env[name];
  if (v === undefined || v === '') return def;
  return ['1', 'true', 'yes', 'on'].includes(v.toLowerCase());
}

function envNum(env: NodeJS.ProcessEnv, name: string, def: number): number {
  const v = env[name];
  if (v === undefined || v === '') return def;
  const n = Number(v);
  return Number.isFinite(n) ? n : def;
}

export interface AppConfig {
  redis: {
    /** Node connects to 127.0.0.1, not localhost: localhost can resolve to
     *  IPv6 (::1) where the grant may not exist. */
    url: string;
    queueKey: string;
    blockTimeoutSec: number;
  };
  /** When true, run the full pipeline but skip the final transaction. */
  shadowMode: boolean;
  featureStore: {
    ttlMs: number;
    fetchTimeoutMs: number;
    maxEntries: number;
  };
  circuitBreaker: {
    /** Consecutive failures (including latency breaches) that open the circuit. */
    failureThreshold: number;
    /** How long the circuit stays OPEN before probing in HALF-OPEN. */
    resetTimeoutMs: number;
    /** E2E latency above this marks an execution as a failure. */
    maxE2ELatencyMs: number;
  };
  execution: {
    /** MIN_EV: minimum +EV for the ingestor to qualify an order. */
    minEv: number;
    /** STAKE_UNIT: stake stamped on each qualified order. */
    stakeUnit: number;
    /** Max tolerated adverse relative slippage before an order is rejected. */
    maxSlippageTolerance: number;
    /** Per-selector wait budget when extracting odds / clicking. */
    domSelectorTimeoutMs: number;
  };
  browser: {
    /** When true, model the browser without launching real Chromium -- lets
     *  the harness run in CI with no browser binaries. Set BROWSER_SIMULATE=false
     *  to drive a real Playwright browser. */
    simulate: boolean;
    headless: boolean;
    navigationTimeoutMs: number;
    /** Where the "navigation" step points when an order carries no targetUrl.
     *  about:blank keeps the shadow path offline and side-effect free. */
    defaultTargetUrl: string;
  };
  server: {
    /** Port for the health / metrics HTTP server. */
    httpPort: number;
  };
  /** Official BetInAsia BLACK (MollyBet) API. Credentials come from the env only. */
  betinasia: {
    /** BIA_API_URL: REST base URL. */
    apiUrl: string;
    /** BIA_WS_URL: websocket stream URL (the session token is appended as ?token=). */
    wsUrl: string;
    /** BIA_API_KEY: credential sent as the MollyBet login password. No default. */
    apiKey: string;
    /** BIA_ACCOUNT_ID: the MollyBet username. No default. */
    accountId: string;
    /** BIA_SPORT: sport code used in event/betslip requests. */
    sport: string;
    /** BIA_CURRENCY: ISO-4217 currency the stake is expressed in. */
    currency: string;
    /** BIA_ORDER_DURATION_SEC: how long an order stays open trying to fill. */
    orderDurationSec: number;
    /** BIA_REQUEST_TIMEOUT_MS: per-request budget for REST calls. */
    requestTimeoutMs: number;
  };
  feed: {
    /** Live feed WebSocket URL. */
    wsUrl: string;
    /** API key sent on connect (header / query), if the feed requires one. */
    apiKey: string;
    /** Polling fallback interval in ms (for a polling transport). */
    pollIntervalMs: number;
  };
}

/** Build an AppConfig from an environment (defaults to process.env). */
export function loadConfig(env: NodeJS.ProcessEnv = process.env): AppConfig {
  return {
    redis: {
      url: envStr(env, 'REDIS_URL', 'redis://127.0.0.1:6379'),
      queueKey: envStr(env, 'APPROVED_ORDER_QUEUE', 'ApprovedOrder'),
      blockTimeoutSec: envNum(env, 'REDIS_BLOCK_TIMEOUT_SEC', 5),
    },
    shadowMode: envBool(env, 'SHADOW_MODE', false),
    featureStore: {
      ttlMs: envNum(env, 'FEATURE_CACHE_TTL_MS', 45_000),
      fetchTimeoutMs: envNum(env, 'FEATURE_FETCH_TIMEOUT_MS', 50),
      maxEntries: envNum(env, 'FEATURE_CACHE_MAX', 10_000),
    },
    circuitBreaker: {
      failureThreshold: envNum(env, 'CIRCUIT_BREAKER_FAILURE_THRESHOLD', 3),
      resetTimeoutMs: envNum(env, 'CIRCUIT_BREAKER_RESET_TIMEOUT_MS', 30_000),
      maxE2ELatencyMs: envNum(env, 'MAX_ALLOWED_E2E_LATENCY_MS', 500),
    },
    execution: {
      minEv: envNum(env, 'MIN_EV', 0.02),
      stakeUnit: envNum(env, 'STAKE_UNIT', 10),
      maxSlippageTolerance: envNum(env, 'MAX_SLIPPAGE_TOLERANCE', 0.02),
      domSelectorTimeoutMs: envNum(env, 'DOM_SELECTOR_TIMEOUT_MS', 3_000),
    },
    browser: {
      simulate: envBool(env, 'BROWSER_SIMULATE', true),
      headless: envBool(env, 'BROWSER_HEADLESS', true),
      navigationTimeoutMs: envNum(env, 'BROWSER_NAV_TIMEOUT_MS', 15_000),
      defaultTargetUrl: envStr(env, 'BROWSER_TARGET_URL', 'about:blank'),
    },
    server: {
      httpPort: envNum(env, 'HTTP_PORT', 3000),
    },
    betinasia: {
      apiUrl: envStr(env, 'BIA_API_URL', 'https://api.mollybet.com'),
      wsUrl: envStr(env, 'BIA_WS_URL', 'wss://api.mollybet.com/v1/stream/'),
      apiKey: envStr(env, 'BIA_API_KEY', ''),
      accountId: envStr(env, 'BIA_ACCOUNT_ID', ''),
      sport: envStr(env, 'BIA_SPORT', 'tennis'),
      currency: envStr(env, 'BIA_CURRENCY', 'EUR'),
      orderDurationSec: envNum(env, 'BIA_ORDER_DURATION_SEC', 15),
      requestTimeoutMs: envNum(env, 'BIA_REQUEST_TIMEOUT_MS', 5_000),
    },
    feed: {
      wsUrl: envStr(env, 'FEED_WS_URL', 'wss://localhost:9443/feed'),
      apiKey: envStr(env, 'FEED_API_KEY', ''),
      pollIntervalMs: envNum(env, 'FEED_POLL_INTERVAL_MS', 2_000),
    },
  };
}

/**
 * Fail fast when the BetInAsia credentials are missing. The API entrypoints
 * call this before wiring anything, so a misconfigured deploy stops at startup
 * instead of failing on the first order. Shadow mode places nothing, so it
 * only warns.
 */
export function assertBetInAsiaCredentials(
  cfg: AppConfig = config,
  logger: Pick<Console, 'warn'> = console,
): void {
  const missing: string[] = [];
  if (cfg.betinasia.apiKey === '') missing.push('BIA_API_KEY');
  if (cfg.betinasia.accountId === '') missing.push('BIA_ACCOUNT_ID');
  if (missing.length === 0) return;
  const msg = `BetInAsia credentials missing: ${missing.join(', ')}`;
  if (cfg.shadowMode) {
    logger.warn(`config: ${msg} (tolerated: SHADOW_MODE places no orders)`);
    return;
  }
  throw new Error(`${msg}. Set them, or run with SHADOW_MODE=true.`);
}

/** Process-wide singleton, resolved once at import. */
export const config: AppConfig = loadConfig();
