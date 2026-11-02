/**
 * BrowserManager -- owns the Playwright browser lifecycle for order execution.
 *
 * The OrderWorker does not talk to Playwright directly; it asks the
 * BrowserManager to `acquire()` a session (a fresh context + page) and hands
 * back the timing of that acquisition. Keeping the browser behind this class
 * means the worker's hot path is transport-agnostic and testable.
 *
 * TWO BACKENDS
 *
 *   - real:      one shared Chromium launched lazily; each acquire() opens an
 *                isolated context so concurrent orders never share cookies.
 *   - simulated: no binaries launched; acquisition and navigation are modelled
 *                with small jittered delays. This is what lets the integration
 *                harness run in CI (and in SHADOW_MODE demos) without installing
 *                browsers, while still producing realistic latency telemetry.
 */

import type { Browser, Page } from 'playwright';

export interface BrowserSession {
  /** Correlates log lines for this context. */
  readonly id: string;
  /**
   * The Playwright page for this context, handed to the OrderExecutor for DOM
   * work. In the simulated backend this is a stub that throws on any DOM call
   * (SHADOW_MODE never touches it); drive real DOM with BROWSER_SIMULATE=false.
   */
  readonly page: Page;
  /** Load a URL. In the simulated backend this is modelled, not fetched. */
  navigate(url: string): Promise<void>;
  /** Tear down the context (not the shared browser). */
  close(): Promise<void>;
}

export interface BrowserManagerOptions {
  /** Model the browser instead of launching Chromium. Default false. */
  simulate?: boolean;
  headless?: boolean;
  navigationTimeoutMs?: number;
  logger?: Pick<Console, 'info' | 'warn' | 'error'>;
}

const delay = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));
/** A small jittered cost so simulated telemetry is not suspiciously flat. */
const jitter = (min: number, max: number): number => min + Math.random() * (max - min);

export class BrowserManager {
  private readonly simulate: boolean;
  private readonly headless: boolean;
  private readonly navigationTimeoutMs: number;
  private readonly logger: Pick<Console, 'info' | 'warn' | 'error'>;

  /** Lazily-launched shared browser for the real backend. */
  private browser?: Browser;
  /** Coalesces concurrent first-acquires so Chromium launches exactly once. */
  private launching?: Promise<Browser>;
  private seq = 0;

  constructor(opts: BrowserManagerOptions = {}) {
    this.simulate = opts.simulate ?? false;
    this.headless = opts.headless ?? true;
    this.navigationTimeoutMs = opts.navigationTimeoutMs ?? 15_000;
    this.logger = opts.logger ?? console;
  }

  /**
   * Acquire an isolated session. The caller times this call to obtain the
   * "Browser Acquisition Latency": for the real backend it includes the
   * one-off Chromium launch on the very first acquire.
   */
  async acquire(): Promise<BrowserSession> {
    const id = `ctx-${++this.seq}`;
    return this.simulate ? this.acquireSimulated(id) : this.acquireReal(id);
  }

  private async acquireSimulated(id: string): Promise<BrowserSession> {
    await delay(jitter(2, 8)); // model launch + context creation
    const navTimeout = this.navigationTimeoutMs;
    // Stub page: any DOM access throws, so a misconfigured LIVE+simulate run
    // surfaces loudly instead of silently "succeeding".
    const noDom = (): never => {
      throw new Error(
        'BrowserManager(simulate): no real DOM; use SHADOW_MODE or BROWSER_SIMULATE=false',
      );
    };
    const page = {
      waitForSelector: async () => noDom(),
      click: async () => noDom(),
    } as unknown as Page;
    return {
      id,
      page,
      async navigate(_url: string): Promise<void> {
        void navTimeout;
        await delay(jitter(1, 6)); // model a page load
      },
      async close(): Promise<void> {
        /* nothing to release */
      },
    };
  }

  private async acquireReal(id: string): Promise<BrowserSession> {
    const browser = await this.ensureBrowser();
    const context = await browser.newContext();
    const page = await context.newPage();
    const navTimeout = this.navigationTimeoutMs;
    return {
      id,
      page,
      async navigate(url: string): Promise<void> {
        await page.goto(url, { timeout: navTimeout });
      },
      async close(): Promise<void> {
        await context.close();
      },
    };
  }

  private ensureBrowser(): Promise<Browser> {
    if (this.browser) return Promise.resolve(this.browser);
    if (this.launching) return this.launching;

    this.launching = (async () => {
      // Dynamic import: the real backend pulls Playwright only when actually
      // used, so the simulated path (and CI) need no browser binaries present.
      const { chromium } = await import('playwright');
      const browser = await chromium.launch({ headless: this.headless });
      this.browser = browser;
      this.logger.info('BrowserManager: launched Chromium');
      return browser;
    })();

    return this.launching;
  }

  /** Shut down the shared browser, if any was launched. */
  async close(): Promise<void> {
    if (this.browser) {
      await this.browser.close();
      this.browser = undefined;
    }
    this.launching = undefined;
  }
}
