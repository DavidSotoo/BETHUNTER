/**
 * Execution-layer contracts for the Execution Gateway.
 *
 * `OrderExecutionResult` is what the OrderExecutor returns for every attempt,
 * whether it placed the bet, refused it on slippage, failed on the DOM, or
 * only simulated the check in shadow mode. The OrderWorker both logs this and
 * maps it onto the CircuitBreaker:
 *
 *   EXECUTED / SHADOW_SIMULATED / REJECTED_SLIPPAGE -> healthy (no trip)
 *   FAILED_DOM                                       -> a downstream fault
 *
 * REJECTED_SLIPPAGE is a deliberate business decision, not a degradation, so
 * it must NOT count against the breaker.
 */

import type { Page } from 'playwright';
import type { HydratedOrderPayload } from './features.js';

export type OrderExecutionStatus =
  | 'EXECUTED'
  | 'REJECTED_SLIPPAGE'
  | 'FAILED_DOM'
  | 'SHADOW_SIMULATED';

export interface OrderExecutionResult {
  /** True only when the attempt ran to completion: EXECUTED or SHADOW_SIMULATED. */
  success: boolean;
  /** Odds actually read from the DOM (or simulated in shadow mode). */
  executedOdds?: number;
  /** Odds the order was placed against (order.targetOdds). */
  requestedOdds: number;
  /**
   * Relative adverse move: (requestedOdds - executedOdds) / requestedOdds.
   * Positive = odds dropped against us; negative = odds improved.
   */
  slippage: number;
  status: OrderExecutionStatus;
  /** Wall-clock cost of the execution leg in milliseconds. */
  latencyMs: number;
  error?: string;
  /**
   * The venue's own confirmation of the order (API-based executors). For
   * MollyBet this is the order object returned by `POST /v1/orders/`.
   */
  confirmation?: Record<string, unknown>;
}

/**
 * What the OrderWorker needs from any executor.
 *
 * `page` is a Playwright page for DOM-based executors (`OrderExecutor`) and
 * `undefined` for API-based ones (`MollyBetExecutor`), which the worker runs
 * without acquiring a browser at all. An executor that needs the page must
 * fail cleanly when it is absent rather than dereferencing it.
 */
export interface OrderExecutorPort {
  executeOrder(
    page: Page | undefined,
    order: HydratedOrderPayload,
  ): Promise<OrderExecutionResult>;
}

/** Does this executor need a browser session? API executors do not. */
export function executorNeedsBrowser(e: OrderExecutorPort): boolean {
  return (e as { readonly needsBrowser?: boolean }).needsBrowser !== false;
}
