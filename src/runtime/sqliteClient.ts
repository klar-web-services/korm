import { createRequire } from "node:module";
import { isBunRuntime } from "./engine";

const runtimeRequire = createRequire(import.meta.url);

type SqliteParamInput = unknown[] | Record<string, unknown> | undefined;

/**
 * Runtime-neutral SQLite prepared statement interface.
 * Next: call `.run(...)`, `.get(...)`, `.all(...)`, or `.iterate(...)`.
 */
export interface SqliteStatement {
  /**
   * Execute a write statement.
   * Next: inspect the driver result or continue transaction flow.
   */
  run(...params: unknown[]): unknown;
  /**
   * Read one row.
   * Next: decode or validate the returned row object.
   */
  get<T = unknown>(...params: unknown[]): T | undefined;
  /**
   * Read all rows.
   * Next: map rows into typed item payloads.
   */
  all<T = unknown>(...params: unknown[]): T[];
  /**
   * Iterate rows lazily.
   * Next: stream through the iterator in backup/query code paths.
   */
  iterate<T = unknown>(...params: unknown[]): IterableIterator<T>;
}

/**
 * Runtime-neutral SQLite client interface used by `SqliteLayer`.
 * Next: create with `createSqliteClient(...)`.
 */
export interface SqliteClient {
  /**
   * Execute a non-prepared SQL statement.
   * Next: call `prepare(...)` for repeated statements.
   */
  run(sql: string, params?: SqliteParamInput): unknown;
  /**
   * Build a prepared statement wrapper.
   * Next: call statement methods with optional parameters.
   */
  prepare(sql: string): SqliteStatement;
  /**
   * Close the underlying SQLite database handle.
   * Next: release the owning layer via `pool.close()`.
   */
  close(): void;
}

type StatementLike = {
  run: (...params: unknown[]) => unknown;
  get: (...params: unknown[]) => unknown;
  all: (...params: unknown[]) => unknown;
  iterate: (...params: unknown[]) => IterableIterator<unknown>;
};

type BunSqliteDatabaseLike = {
  run: (sql: string, params?: SqliteParamInput) => unknown;
  prepare: (sql: string) => StatementLike;
  close: () => void;
};

type BetterSqliteStatementLike = {
  run: (...params: unknown[]) => unknown;
  get: (...params: unknown[]) => unknown;
  all: (...params: unknown[]) => unknown;
  iterate: (...params: unknown[]) => IterableIterator<unknown>;
};

type BetterSqliteDatabaseLike = {
  prepare: (sql: string) => BetterSqliteStatementLike;
  close: () => void;
};

type BetterSqliteCtor = new (path: string) => BetterSqliteDatabaseLike;
type BunSqliteCtor = new (path: string) => BunSqliteDatabaseLike;

function normalizeParams(args: unknown[]): unknown[] {
  if (args.length === 1 && Array.isArray(args[0])) {
    return args[0] as unknown[];
  }
  return args;
}

function adaptStatement(statement: StatementLike): SqliteStatement {
  return {
    run(...params: unknown[]): unknown {
      const normalized = normalizeParams(params);
      return statement.run(...normalized);
    },
    get<T = unknown>(...params: unknown[]): T | undefined {
      const normalized = normalizeParams(params);
      return statement.get(...normalized) as T | undefined;
    },
    all<T = unknown>(...params: unknown[]): T[] {
      const normalized = normalizeParams(params);
      return statement.all(...normalized) as T[];
    },
    iterate<T = unknown>(...params: unknown[]): IterableIterator<T> {
      const normalized = normalizeParams(params);
      return statement.iterate(...normalized) as IterableIterator<T>;
    },
  };
}

function loadBunSqliteCtor(): BunSqliteCtor {
  const loaded = runtimeRequire("bun:sqlite") as
    | { Database?: BunSqliteCtor }
    | BunSqliteCtor;
  const ctor =
    typeof loaded === "function"
      ? loaded
      : (loaded.Database as BunSqliteCtor | undefined);
  if (typeof ctor !== "function") {
    throw new Error('Failed to load Bun SQLite adapter from "bun:sqlite".');
  }
  return ctor;
}

function loadBetterSqliteCtor(): BetterSqliteCtor {
  const loaded = runtimeRequire("better-sqlite3") as
    | BetterSqliteCtor
    | { default?: BetterSqliteCtor };
  const ctor =
    typeof loaded === "function"
      ? loaded
      : (loaded.default as BetterSqliteCtor | undefined);
  if (typeof ctor !== "function") {
    throw new Error(
      'Failed to load "better-sqlite3". Install it to use SQLite under Node.',
    );
  }
  return ctor;
}

function createBunSqliteClient(path: string): SqliteClient {
  const DatabaseCtor = loadBunSqliteCtor();
  const db = new DatabaseCtor(path);
  return {
    run(sql: string, params?: SqliteParamInput): unknown {
      if (params === undefined) {
        return db.run(sql);
      }
      return db.run(sql, params);
    },
    prepare(sql: string): SqliteStatement {
      return adaptStatement(db.prepare(sql));
    },
    close(): void {
      db.close();
    },
  };
}

function createNodeSqliteClient(path: string): SqliteClient {
  const DatabaseCtor = loadBetterSqliteCtor();
  const db = new DatabaseCtor(path);
  return {
    run(sql: string, params?: SqliteParamInput): unknown {
      const statement = db.prepare(sql);
      if (params === undefined) {
        return statement.run();
      }
      if (Array.isArray(params)) {
        return statement.run(...params);
      }
      return statement.run(params);
    },
    prepare(sql: string): SqliteStatement {
      return adaptStatement(db.prepare(sql));
    },
    close(): void {
      db.close();
    },
  };
}

/**
 * Create a runtime-aware SQLite client for Node or Bun.
 * Next: pass into `SqliteLayer` and use layer CRUD methods.
 */
export function createSqliteClient(path: string): SqliteClient {
  if (isBunRuntime()) {
    return createBunSqliteClient(path);
  }
  return createNodeSqliteClient(path);
}
