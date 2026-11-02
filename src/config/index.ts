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
      maxSlippageTolerance: envNum(env, 'MAX_SLIPPAGE_TOLERANCE', 0.02),
      domSelectorTimeoutMs: envNum(env, 'DOM_SELECTOR_TIMEOUT_MS', 3_000),
    },
    browser: {
      simulate: envBool(env, 'BROWSER_SIMULATE', true),
      headless: envBool(env, 'BROWSER_HEADLESS', true),
      navigationTimeoutMs: envNum(env, 'BROWSER_NAV_TIMEOUT_MS', 15_000),
      defaultTargetUrl: envStr(env, 'BROWSER_TARGET_URL', 'about:blank'),
    },
  };
}

/** Process-wide singleton, resolved once at import. */
export const config: AppConfig = loadConfig();
