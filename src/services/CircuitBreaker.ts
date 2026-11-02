/**
 * CircuitBreaker -- protects the execution path from a degrading downstream
 * (slow network, WAF/Captcha walls, 403 blocks) by failing fast instead of
 * hammering a surface that is already in trouble.
 *
 * STATES
 *
 *   CLOSED     Normal operation. Outcomes are recorded; consecutive failures
 *              (a thrown error, an E2E latency over the threshold, or an
 *              explicit block) move the breaker toward OPEN.
 *   OPEN       Tripped. Requests are rejected immediately (the worker pauses
 *              consumption) for `resetTimeoutMs`, after which the next probe is
 *              allowed through as HALF-OPEN.
 *   HALF-OPEN  Cooling probe. A bounded number of trial requests are let
 *              through: a success closes the circuit, any failure re-opens it.
 *
 * TRIGGERS (what counts as a failure)
 *
 *   1. `failureThreshold` consecutive failures.
 *   2. An E2E latency above `latencyThresholdMs` (counts as one failure).
 *   3. An explicit block (WAF / Captcha / HTTP 403) -- trips IMMEDIATELY,
 *      regardless of the consecutive-failure count, from any state.
 *
 * Time is read through an injectable `clock` so the OPEN -> HALF-OPEN cooldown
 * is deterministic under test without fake timers.
 */

export enum CircuitState {
  CLOSED = 'CLOSED',
  OPEN = 'OPEN',
  HALF_OPEN = 'HALF_OPEN',
}

/** Result of one protected execution, fed back to the breaker. */
export interface ExecutionOutcome {
  /** True when the execution completed without throwing. */
  success: boolean;
  /** Measured end-to-end latency, if known. Over the threshold = failure. */
  latencyMs?: number;
  /** Explicit downstream block (WAF / Captcha / 403). Trips immediately. */
  blocked?: boolean;
}

export interface CircuitBreakerOptions {
  /** Consecutive failures that open the circuit. Default 3. */
  failureThreshold?: number;
  /** OPEN duration before a HALF-OPEN probe is allowed. Default 30000ms. */
  resetTimeoutMs?: number;
  /** E2E latency above this counts as a failure. Default 500ms. */
  latencyThresholdMs?: number;
  /** Trial requests allowed at once in HALF-OPEN. Default 1. */
  halfOpenMaxCalls?: number;
  /** Successful probes needed in HALF-OPEN to close. Default 1. */
  successesToClose?: number;
  /** Injectable time source (ms). Default Date.now. */
  clock?: () => number;
  /** Optional name for log lines. */
  name?: string;
  logger?: Pick<Console, 'info' | 'warn'>;
  /** Observes every state change. */
  onStateChange?: (from: CircuitState, to: CircuitState) => void;
}

export interface CircuitBreakerStats {
  state: CircuitState;
  consecutiveFailures: number;
  lastReason?: string;
  openedAt?: number;
}

export class CircuitBreaker {
  private readonly failureThreshold: number;
  private readonly resetTimeoutMs: number;
  private readonly latencyThresholdMs: number;
  private readonly halfOpenMaxCalls: number;
  private readonly successesToClose: number;
  private readonly clock: () => number;
  private readonly name: string;
  private readonly logger: Pick<Console, 'info' | 'warn'>;
  private readonly onStateChange?: (from: CircuitState, to: CircuitState) => void;

  private state: CircuitState = CircuitState.CLOSED;
  private consecutiveFailures = 0;
  private openedAt = 0;
  private halfOpenInFlight = 0;
  private halfOpenSuccesses = 0;
  private lastReason?: string;

  constructor(opts: CircuitBreakerOptions = {}) {
    this.failureThreshold = opts.failureThreshold ?? 3;
    this.resetTimeoutMs = opts.resetTimeoutMs ?? 30_000;
    this.latencyThresholdMs = opts.latencyThresholdMs ?? 500;
    this.halfOpenMaxCalls = opts.halfOpenMaxCalls ?? 1;
    this.successesToClose = opts.successesToClose ?? 1;
    this.clock = opts.clock ?? Date.now;
    this.name = opts.name ?? 'circuit';
    this.logger = opts.logger ?? console;
    this.onStateChange = opts.onStateChange;
  }

  getState(): CircuitState {
    // Reading the state also realises a due OPEN -> HALF-OPEN transition, so
    // callers never observe a stale OPEN past its cooldown.
    this.maybeHalfOpen();
    return this.state;
  }

  getStats(): CircuitBreakerStats {
    return {
      state: this.state,
      consecutiveFailures: this.consecutiveFailures,
      lastReason: this.lastReason,
      openedAt: this.state === CircuitState.OPEN ? this.openedAt : undefined,
    };
  }

  /**
   * May a request pass right now? Performs the time-based OPEN -> HALF-OPEN
   * transition. Does NOT reserve a probe slot -- call `onExecuteStart` only
   * once you actually have work to run.
   */
  allowsRequest(): boolean {
    this.maybeHalfOpen();
    switch (this.state) {
      case CircuitState.CLOSED:
        return true;
      case CircuitState.OPEN:
        return false;
      case CircuitState.HALF_OPEN:
        return this.halfOpenInFlight < this.halfOpenMaxCalls;
    }
  }

  /** Reserve a HALF-OPEN probe slot for a request that is about to run. */
  onExecuteStart(): void {
    if (this.state === CircuitState.HALF_OPEN) {
      this.halfOpenInFlight++;
    }
  }

  /** Feed back the result of a protected execution and update the state. */
  recordOutcome(outcome: ExecutionOutcome): void {
    if (this.state === CircuitState.HALF_OPEN) {
      this.halfOpenInFlight = Math.max(0, this.halfOpenInFlight - 1);
    }

    // An explicit block is the strongest signal: trip now, from any state.
    if (outcome.blocked) {
      this.trip('block (WAF/Captcha/403)');
      return;
    }

    const slow =
      outcome.latencyMs !== undefined && outcome.latencyMs > this.latencyThresholdMs;

    if (outcome.success && !slow) {
      this.onSuccess();
    } else {
      this.onFailure(slow ? `latency>${this.latencyThresholdMs}ms` : 'execution-failure');
    }
  }

  /** Force the circuit back to CLOSED (ops/test hook). */
  reset(): void {
    this.consecutiveFailures = 0;
    this.halfOpenInFlight = 0;
    this.halfOpenSuccesses = 0;
    this.lastReason = undefined;
    this.setState(CircuitState.CLOSED);
  }

  // --- internals ---

  private onSuccess(): void {
    if (this.state === CircuitState.HALF_OPEN) {
      this.halfOpenSuccesses++;
      if (this.halfOpenSuccesses >= this.successesToClose) {
        this.consecutiveFailures = 0;
        this.setState(CircuitState.CLOSED);
      }
      return;
    }
    // CLOSED: a good call clears the streak.
    this.consecutiveFailures = 0;
  }

  private onFailure(reason: string): void {
    this.lastReason = reason;
    if (this.state === CircuitState.HALF_OPEN) {
      // A failed probe means the downstream is still unhealthy: re-open.
      this.trip(reason);
      return;
    }
    // CLOSED: count toward the threshold.
    this.consecutiveFailures++;
    if (this.consecutiveFailures >= this.failureThreshold) {
      this.trip(reason);
    }
  }

  private trip(reason: string): void {
    this.lastReason = reason;
    this.openedAt = this.clock();
    this.setState(CircuitState.OPEN);
  }

  /** If OPEN and the cooldown has elapsed, move to HALF-OPEN. */
  private maybeHalfOpen(): void {
    if (
      this.state === CircuitState.OPEN &&
      this.clock() - this.openedAt >= this.resetTimeoutMs
    ) {
      this.setState(CircuitState.HALF_OPEN);
    }
  }

  private setState(to: CircuitState): void {
    if (to === this.state) return;
    const from = this.state;
    this.state = to;
    if (to === CircuitState.HALF_OPEN || to === CircuitState.CLOSED) {
      // Fresh accounting for the probe window / normal operation.
      this.halfOpenInFlight = 0;
      this.halfOpenSuccesses = 0;
    }
    this.logger.warn(
      `CircuitBreaker[${this.name}]: ${from} -> ${to}` +
        (this.lastReason ? ` (${this.lastReason})` : ''),
    );
    this.onStateChange?.(from, to);
  }
}
