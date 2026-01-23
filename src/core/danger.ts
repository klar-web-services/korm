const dangerOps = new WeakMap<
  BaseNeedsDanger<unknown, unknown[]>,
  () => unknown
>();

/**
 * A wrapper that seals a dangerous operation until `danger(...)` is called.
 * @template T Result type produced by the wrapped operation.
 * @template Args Argument tuple forwarded to the wrapped function.
 * @example
 * const op = new BaseNeedsDanger(dropAllTables, pool);
 * // Next: pass `op` to `danger(op)` when you're ready to execute.
 * Next: pass instances to `danger(...)` when you intend to execute them.
 */
export class BaseNeedsDanger<T, Args extends unknown[] = unknown[]> {
  /**
   * Wrap a function so it can only be executed via `danger(...)`.
   * @param wants Function that performs the dangerous operation.
   * @param args Arguments forwarded to `wants`.
   * Next: return this wrapper and require callers to use `danger(...)`.
   */
  constructor(wants: (...args: Args) => T, ...args: Args) {
    const run = () => wants(...args);
    dangerOps.set(this, run);
  }
}

/**
 * Execute a wrapped dangerous operation.
 * @template T Result type produced by the wrapped operation.
 * @param op Wrapper created by `new BaseNeedsDanger(...)`.
 * @returns The result of the wrapped operation.
 * @throws If the wrapper was not created by this module.
 * Next: handle the returned value or await it if it is a promise.
 */
export function danger<T, Args extends unknown[]>(
  op: BaseNeedsDanger<T, Args>,
): T {
  const run = dangerOps.get(op as BaseNeedsDanger<unknown, unknown[]>);
  if (!run) {
    throw new Error(
      "danger() can only execute values created by BaseNeedsDanger.",
    );
  }
  return run() as T;
}

/**
 * Helper to wrap a dangerous operation without calling `new`.
 * @template Args Argument tuple forwarded to the wrapped function.
 * @template T Result type produced by the wrapped operation.
 * @param wants Function that performs the dangerous operation.
 * @param args Arguments forwarded to `wants`.
 * @returns A wrapper that requires `danger(...)` to execute.
 * Next: pass the returned wrapper to `danger(...)`.
 */
export function needsDanger<Args extends unknown[], T>(
  wants: (...args: Args) => T,
  ...args: Args
): BaseNeedsDanger<T, Args> {
  return new BaseNeedsDanger<T, Args>(wants, ...args);
}
