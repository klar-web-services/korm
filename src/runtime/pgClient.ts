import { createRequire } from "node:module";
import { getBunGlobal, isBunRuntime } from "./engine";

const runtimeRequire = createRequire(import.meta.url);

/**
 * Postgres connection options accepted by runtime adapters.
 * Use a connection URL for portability, or pass engine-native options.
 * Next: pass to `korm.layers.pg(...)`.
 */
export type PgConnectionOptions = Record<string, unknown>;

/**
 * Input accepted by the Postgres adapter.
 * Next: construct a client with `createPgClient(...)`.
 */
export type PgConnectionInput = string | PgConnectionOptions;

/**
 * Runtime-neutral Postgres client contract used by `PgLayer`.
 * Next: call `unsafe(...)` for SQL execution and `end(...)` on shutdown.
 */
export interface PgClient {
  /**
   * Execute an unsafe SQL query with optional positional parameters.
   * Next: decode rows or propagate the returned error.
   */
  unsafe<T = unknown>(query: string, values?: unknown[]): Promise<T>;
  /**
   * Close the underlying Postgres client.
   * Next: release pool resources by calling layer `close()`.
   */
  end(options?: { timeout?: number }): Promise<void>;
}

type PgUnsafe = <T = unknown>(query: string, values?: unknown[]) => Promise<T>;
type BunSqlLike = { unsafe: PgUnsafe; end: (opts?: unknown) => Promise<void> };
type PostgresSqlLike = {
  unsafe: PgUnsafe;
  end: (opts?: { timeout?: number }) => Promise<void>;
};
type PostgresFactory = (
  input?: string | Record<string, unknown>,
) => PostgresSqlLike;

function loadPostgresFactory(): PostgresFactory {
  const loaded = runtimeRequire("postgres") as
    | PostgresFactory
    | { default?: PostgresFactory };
  const factory =
    typeof loaded === "function" ? loaded : (loaded.default as PostgresFactory);
  if (typeof factory !== "function") {
    throw new Error('Failed to load "postgres" runtime adapter.');
  }
  return factory;
}

function createNodePgClient(input: PgConnectionInput): PgClient {
  const postgres = loadPostgresFactory();
  const sql =
    typeof input === "string" ? postgres(input) : postgres(input as any);
  return {
    unsafe: <T = unknown>(query: string, values?: unknown[]) =>
      sql.unsafe<T>(query, values),
    end: async (options?: { timeout?: number }) =>
      sql.end({ timeout: options?.timeout }),
  };
}

function createBunPgClient(input: PgConnectionInput): PgClient {
  const bun = getBunGlobal();
  if (!bun?.SQL) {
    throw new Error("Bun runtime detected but Bun.SQL is unavailable.");
  }
  const SQLCtor = bun.SQL as unknown as new (connection: unknown) => BunSqlLike;
  const sql = new SQLCtor(input as any);
  return {
    unsafe: <T = unknown>(query: string, values?: unknown[]) =>
      values === undefined
        ? sql.unsafe<T>(query)
        : sql.unsafe<T>(query, values),
    end: async (options?: { timeout?: number }) => sql.end(options),
  };
}

/**
 * Create a runtime-aware Postgres client for Node or Bun.
 * Next: inject the client into `PgLayer`.
 */
export function createPgClient(input: PgConnectionInput): PgClient {
  if (isBunRuntime()) {
    return createBunPgClient(input);
  }
  return createNodePgClient(input);
}
