/**
 * OrderExecutor -- places an order through Playwright with a Slippage Guard.
 *
 * FLOW (live)
 *
 *   1. Read the requested odds from the order (order.targetOdds).
 *   2. Extract the live odds from the target DOM node (bounded by
 *      DOM_SELECTOR_TIMEOUT_MS). A timeout / missing node -> FAILED_DOM.
 *   3. SLIPPAGE GUARD: slippage = (requested - executed) / requested.
 *      If the odds have dropped against us beyond MAX_SLIPPAGE_TOLERANCE,
 *      abort BEFORE clicking -> REJECTED_SLIPPAGE.
 *   4. Otherwise click the place-bet control -> EXECUTED.
 *
 * SHADOW MODE
 *
 *   When enabled, the executor simulates the odds verification, logs what it
 *   WOULD do (including whether slippage would have rejected the order) and
 *   returns SHADOW_SIMULATED WITHOUT ever touching the DOM or clicking.
 *
 * The executor owns no transport details beyond the Playwright `Page` it is
 * handed, which keeps it unit-testable with a mocked page.
 */

import type { Page } from 'playwright';
import { performance } from 'node:perf_hooks';
import { HydratedOrderPayload } from '../types/features.js';
import { OrderExecutionResult, OrderExecutorPort } from '../types/execution.js';

export interface OrderExecutorOptions {
  /** Max tolerated adverse relative slippage. Default 0.02 (2%). */
  maxSlippageTolerance?: number;
  /** Per-selector wait budget in ms. Default 3000. */
  domSelectorTimeoutMs?: number;
  /** When true, simulate the verification and never click. Default false. */
  shadowMode?: boolean;
  /** DOM node holding the live odds. */
  oddsSelector?: string;
  /** Control that commits the bet. */
  placeBetSelector?: string;
  logger?: Pick<Console, 'info' | 'warn' | 'error'>;
  /**
   * Odds source used in shadow mode (no DOM). Defaults to the requested odds
   * (zero simulated slippage) so shadow runs are reproducible; override to
   * model adverse moves.
   */
  simulateOdds?: (requestedOdds: number) => number;
}

export class OrderExecutor implements OrderExecutorPort {
  /** DOM executor: the worker must hand it a browser page. */
  readonly needsBrowser = true;

  private readonly maxSlippage: number;
  private readonly domTimeout: number;
  private readonly shadowMode: boolean;
  private readonly oddsSelector: string;
  private readonly placeBetSelector: string;
  private readonly logger: Pick<Console, 'info' | 'warn' | 'error'>;
  private readonly simulateOdds: (requestedOdds: number) => number;

  constructor(opts: OrderExecutorOptions = {}) {
    this.maxSlippage = opts.maxSlippageTolerance ?? 0.02;
    this.domTimeout = opts.domSelectorTimeoutMs ?? 3000;
    this.shadowMode = opts.shadowMode ?? false;
    this.oddsSelector = opts.oddsSelector ?? '[data-testid="odds-value"]';
    this.placeBetSelector = opts.placeBetSelector ?? '[data-testid="place-bet"]';
    this.logger = opts.logger ?? console;
    this.simulateOdds = opts.simulateOdds ?? ((r) => r);
  }

  async executeOrder(
    page: Page | undefined,
    order: HydratedOrderPayload,
  ): Promise<OrderExecutionResult> {
    const start = performance.now();
    const elapsed = (): number => performance.now() - start;
    const requestedOdds = this.readRequestedOdds(order);

    if (!Number.isFinite(requestedOdds) || requestedOdds <= 0) {
      return {
        success: false,
        requestedOdds: Number.isFinite(requestedOdds) ? requestedOdds : 0,
        slippage: 0,
        status: 'FAILED_DOM',
        latencyMs: elapsed(),
        error: 'order.targetOdds is missing or not a positive number',
      };
    }

    // --- SHADOW: simulate the verification, never touch the DOM. ---
    if (this.shadowMode) {
      const executedOdds = this.simulateOdds(requestedOdds);
      const slippage = this.computeSlippage(requestedOdds, executedOdds);
      const wouldReject = slippage > this.maxSlippage;
      this.logger.info(
        `OrderExecutor[SHADOW]: order=${order.orderId} requested=${requestedOdds} ` +
          `simulated=${executedOdds.toFixed(3)} slippage=${(slippage * 100).toFixed(2)}% ` +
          `wouldReject=${wouldReject} (no click)`,
      );
      return {
        success: true,
        executedOdds,
        requestedOdds,
        slippage,
        status: 'SHADOW_SIMULATED',
        latencyMs: elapsed(),
      };
    }

    // --- LIVE: extract the real odds from the DOM. ---
    if (!page) {
      return {
        success: false,
        requestedOdds,
        slippage: 0,
        status: 'FAILED_DOM',
        latencyMs: elapsed(),
        error: 'OrderExecutor needs a browser page: none was supplied',
      };
    }

    let executedOdds: number;
    try {
      const handle = await page.waitForSelector(this.oddsSelectorFor(order), {
        timeout: this.domTimeout,
      });
      const text = handle ? await handle.textContent() : null;
      executedOdds = this.parseOdds(text);
    } catch (err) {
      return {
        success: false,
        requestedOdds,
        slippage: 0,
        status: 'FAILED_DOM',
        latencyMs: elapsed(),
        error: `odds extraction failed: ${(err as Error).message}`,
      };
    }

    if (!Number.isFinite(executedOdds) || executedOdds <= 0) {
      return {
        success: false,
        requestedOdds,
        slippage: 0,
        status: 'FAILED_DOM',
        latencyMs: elapsed(),
        error: 'odds node present but value was unparseable',
      };
    }

    // --- SLIPPAGE GUARD (before any irreversible click). ---
    const slippage = this.computeSlippage(requestedOdds, executedOdds);
    if (slippage > this.maxSlippage) {
      this.logger.warn(
        `OrderExecutor: REJECTED_SLIPPAGE order=${order.orderId} ` +
          `requested=${requestedOdds} actual=${executedOdds} ` +
          `slippage=${(slippage * 100).toFixed(2)}% > tol=${(this.maxSlippage * 100).toFixed(2)}%`,
      );
      return {
        success: false,
        executedOdds,
        requestedOdds,
        slippage,
        status: 'REJECTED_SLIPPAGE',
        latencyMs: elapsed(),
      };
    }

    // --- Commit the bet. ---
    try {
      await page.click(this.placeBetSelectorFor(order), { timeout: this.domTimeout });
    } catch (err) {
      return {
        success: false,
        executedOdds,
        requestedOdds,
        slippage,
        status: 'FAILED_DOM',
        latencyMs: elapsed(),
        error: `place-bet click failed: ${(err as Error).message}`,
      };
    }

    this.logger.info(
      `OrderExecutor: EXECUTED order=${order.orderId} at odds=${executedOdds} ` +
        `(slippage=${(slippage * 100).toFixed(2)}%)`,
    );
    return {
      success: true,
      executedOdds,
      requestedOdds,
      slippage,
      status: 'EXECUTED',
      latencyMs: elapsed(),
    };
  }

  // --- helpers ---

  private computeSlippage(requested: number, executed: number): number {
    return (requested - executed) / requested;
  }

  private readRequestedOdds(order: HydratedOrderPayload): number {
    const v = order.targetOdds;
    return typeof v === 'number' ? v : Number(v);
  }

  /** Per-order selector override, else the executor default. */
  private oddsSelectorFor(order: HydratedOrderPayload): string {
    return typeof order.oddsSelector === 'string' ? order.oddsSelector : this.oddsSelector;
  }

  private placeBetSelectorFor(order: HydratedOrderPayload): string {
    return typeof order.placeBetSelector === 'string'
      ? order.placeBetSelector
      : this.placeBetSelector;
  }

  /** Pull the first numeric token out of odds text ("@ 1,98" -> 1.98). */
  private parseOdds(text: string | null): number {
    if (!text) return NaN;
    const m = text.replace(',', '.').match(/\d+(?:\.\d+)?/);
    return m ? parseFloat(m[0]) : NaN;
  }
}
