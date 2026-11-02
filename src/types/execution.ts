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
}
