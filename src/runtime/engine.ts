/**
 * Runtime engine discriminator used for adapter selection.
 * Next: call `isBunRuntime()` when choosing Bun vs Node implementations.
 */
export type RuntimeEngine = "bun" | "node";

/**
 * Read-only shape for the Bun global used by runtime adapters.
 * Next: call `getBunGlobal()` and feature-check specific APIs.
 */
export type BunGlobal = {
  SQL?: new (connection: unknown) => unknown;
  S3Client?: new (options: unknown) => unknown;
  password?: {
    hash: (
      plainText: string,
      options?: {
        algorithm?: string;
        memoryCost?: number;
        timeCost?: number;
      },
    ) => Promise<string> | string;
    verify: (plainText: string, hash: string) => Promise<boolean> | boolean;
  };
  argv?: string[];
};

/**
 * Return the Bun global when present.
 * Next: call `isBunRuntime()` or inspect Bun-specific APIs defensively.
 */
export function getBunGlobal(): BunGlobal | undefined {
  const value = (globalThis as { Bun?: BunGlobal }).Bun;
  if (!value || typeof value !== "object") {
    return undefined;
  }
  return value;
}

/**
 * Detect whether code is executing under Bun.
 * Next: branch runtime bindings in adapters (`pg`, `sqlite`, `s3`).
 */
export function isBunRuntime(): boolean {
  return Boolean(getBunGlobal());
}

/**
 * Resolve the active runtime engine.
 * Next: pass the result into runtime-specific diagnostics/logging if needed.
 */
export function getRuntimeEngine(): RuntimeEngine {
  return isBunRuntime() ? "bun" : "node";
}
