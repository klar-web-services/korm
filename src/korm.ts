import { UninitializedItem } from "./core/item";
import { RN } from "./core/rn";
import { FloatingDepotFile, type DepotBlob } from "./depot/depotFile";
import type { Depot } from "./depot/depot";
import type { SourceLayer } from "./sources/sourceLayer";
import { LayerPool } from "./sources/layerPool";
import { layers } from "./sources";
import {
  BackMan,
  type IntervalFactory,
  type IntervalSpec,
} from "./sources/backMan";
import {
  eq,
  and,
  gt,
  gte,
  inList,
  like,
  lt,
  lte,
  not,
  or,
} from "./core/queryFns";
import { tx } from "./core/txBuilder";
import { Encrypt } from "./security/encryption";
import { Unique } from "./core/unique";
import type { WalMode } from "./wal/wal";
import type { LockMode } from "./sources/lockStore";
import {
  dropPoolMetaConfig,
  hydratePoolMetaConfig,
  readPoolMetaConfig,
  type PoolMetaBackups,
  type PoolMetaMode,
} from "./sources/poolMeta";
import { LocalDepot, S3Depot } from ".";
import type { S3DepotOptions } from "./depot/depots/s3Depot";
import type { PgConnectionOptions } from "./runtime/pgClient";
import type { PoolOptions as MysqlPoolOptions } from "mysql2/promise";
import type {
  DisallowMissingReferencesGetOption,
  FirstGetOption,
  ResolveGetOption,
  SortByGetOption,
  SortDirection,
} from "./core/query";
import { danger, needsDanger } from "./core/danger";

type JsonPrimitive = string | number | boolean | null;

type JsonSerializable =
  | JsonPrimitive
  | JsonSerializable[]
  | { [k: string]: JsonSerializable }
  | { toJSON(): JsonSerializable };

/**
 * JSON-compatible values accepted by korm item data.
 * Use this as the bound for item generics when you build models.
 */
export type JSONable = JsonSerializable;

const layerUseBrand: unique symbol = Symbol("korm.use.layer");
const depotUseBrand: unique symbol = Symbol("korm.use.depot");
const layerTargetBrand: unique symbol = Symbol("korm.target.layer");

type LayerUseEntry<Ident extends string = string> = {
  layer: SourceLayer;
  ident: Ident;
  readonly [layerUseBrand]: "korm.use.layer";
};

type LayerUseBuilder = {
  as<const Ident extends string>(ident: Ident): LayerUseEntry<Ident>;
};

type DepotUseEntry<Ident extends string = string> = {
  depot: Depot;
  ident: Ident;
  readonly [depotUseBrand]: "korm.use.depot";
};

type DepotUseBuilder = {
  as<const Ident extends string>(ident: Ident): DepotUseEntry<Ident>;
};

type LayerTarget<Ident extends string = string> = {
  layerIdent: Ident;
  readonly [layerTargetBrand]: "korm.target.layer";
};

type LockOptions = Omit<LockMode, "layerIdent">;

type PoolLayerEntry = {
  /** Layer instance to include in the pool. */
  layer: SourceLayer;
  /** Identifier used by `from` RN mods; defaults to `layer.identifier`. */
  ident?: string;
};

type PoolDepotEntry = {
  /** Depot instance to include in the pool. */
  depot: Depot;
  /** Identifier used by `depot` RN mods; defaults to `depot.identifier`. */
  ident?: string;
};

type PoolEntry = PoolLayerEntry | PoolDepotEntry;

type LayerInput = SourceLayer | LayerUseEntry;
type DepotInput = Depot | DepotUseEntry;

type LayerIdentOf<T> = T extends { ident: infer I extends string }
  ? I
  : T extends SourceLayer
    ? string
    : never;

type DepotIdentOf<T> = T extends { ident: infer I extends string }
  ? I
  : T extends Depot
    ? string
    : never;

type LayerIdentFrom<Entries extends readonly unknown[]> = LayerIdentOf<
  Entries[number]
>;
type DepotIdentFrom<Entries extends readonly unknown[]> = DepotIdentOf<
  Entries[number]
>;
type LayerIdentOrWildcard<LIds extends string> = LIds | "*";
type RetainCallable<LIds extends string, DIds extends string> = BackupsBuilder<
  LIds,
  DIds
> &
  (() => BackupsBuilder<LIds, DIds>);
type RetainSelector<LIds extends string, DIds extends string> = {
  readonly backups: RetainCallable<LIds, DIds>;
  readonly days: RetainCallable<LIds, DIds>;
};

type WalModeFor<D extends string> = Omit<WalMode, "depotIdent"> & {
  depotIdent: D;
};
type PoolMetaModeFor<L extends string> = LayerTarget<L>;

type PoolBuilderStart = {
  /**
   * Provide the source layers for the pool.
   * Use `korm.use.layer(layer).as("ident")` to name them.
   * Next: optionally call `setDepots(...)`, `withWal(...)`, `backups(...)`, or `open()`.
   */
  setLayers<const Entries extends readonly LayerInput[]>(
    ...entries: Entries
  ): PoolBuilderWithLayers<LayerIdentFrom<Entries>>;
};

/**
 * Name a layer for pool configuration.
 * Next: call `.as("ident")`, then pass it to `setLayers(...)`.
 */
function useLayer<const Ident extends string>(
  layer: SourceLayer,
  ident: Ident,
): LayerUseEntry<Ident>;
function useLayer(layer: SourceLayer): LayerUseBuilder;
function useLayer<const Ident extends string>(
  layer: SourceLayer,
  ident?: Ident,
): LayerUseEntry<Ident> | LayerUseBuilder {
  if (ident !== undefined) {
    return {
      layer,
      ident,
      [layerUseBrand]: "korm.use.layer",
    };
  }
  return {
    as<const Name extends string>(name: Name) {
      return {
        layer,
        ident: name,
        [layerUseBrand]: "korm.use.layer",
      };
    },
  };
}

/**
 * Name a depot for pool configuration.
 * Next: call `.as("ident")`, then pass it to `setDepots(...)`.
 */
function useDepot<const Ident extends string>(
  depot: Depot,
  ident: Ident,
): DepotUseEntry<Ident>;
function useDepot(depot: Depot): DepotUseBuilder;
function useDepot<const Ident extends string>(
  depot: Depot,
  ident?: Ident,
): DepotUseEntry<Ident> | DepotUseBuilder {
  if (ident !== undefined) {
    return {
      depot,
      ident,
      [depotUseBrand]: "korm.use.depot",
    };
  }
  return {
    as<const Name extends string>(name: Name) {
      return {
        depot,
        ident: name,
        [depotUseBrand]: "korm.use.depot",
      };
    },
  };
}

type PoolBuilderWithLayers<LIds extends string> = {
  /**
   * Attach depots to enable WAL or backups.
   * Use `korm.use.depot(depot).as("ident")` to name them.
   * Next: call `withWal(...)`, `backups(...)`, or stop.
   */
  setDepots<const Entries extends readonly DepotInput[]>(
    ...entries: Entries
  ): PoolBuilderWithDepots<LIds, DepotIdentFrom<Entries>>;
  /**
   * Enable pool metadata tracking using a layer in this pool.
   * Use `korm.target.layer(...)` to select the layer.
   * Next: call `setDepots(...)`, `withWal(...)`, `withLocks(...)`, or stop.
   */
  withMeta: (opts: PoolMetaModeFor<LIds>) => PoolBuilderWithLayers<LIds>;
  /**
   * Enable shared locking using a layer in this pool.
   * Use `korm.target.layer(...)` to select the layer.
   * Next: call `setDepots(...)`, `withWal(...)`, or stop.
   */
  withLocks: (
    target: LayerTarget<LIds>,
    options?: LockOptions,
  ) => PoolBuilderWithLayers<LIds>;
  /** Finalize the builder and return a ready LayerPool. */
  open: () => LayerPool;
};

type PoolBuilderWithDepots<LIds extends string, DIds extends string> = {
  /**
   * Attach additional depots to the pool.
   * Use `korm.use.depot(depot).as("ident")` to name them.
   * Next: call `withWal(...)`, `backups(...)`, or stop.
   */
  setDepots<const Entries extends readonly DepotInput[]>(
    ...entries: Entries
  ): PoolBuilderWithDepots<LIds, DIds | DepotIdentFrom<Entries>>;
  /**
   * Enable pool metadata tracking using a layer in this pool.
   * Use `korm.target.layer(...)` to select the layer.
   * Next: call `withWal(...)`, `withLocks(...)`, `backups(...)`, or stop.
   */
  withMeta: (opts: PoolMetaModeFor<LIds>) => PoolBuilderWithDepots<LIds, DIds>;
  /**
   * Enable shared locking using a layer in this pool.
   * Use `korm.target.layer(...)` to select the layer.
   * Next: call `withWal(...)`, `backups(...)`, or stop.
   */
  withLocks: (
    target: LayerTarget<LIds>,
    options?: LockOptions,
  ) => PoolBuilderWithDepots<LIds, DIds>;
  /**
   * Enable WAL for the pool using a depot in this pool.
   * Next: call `backups(...)` or stop.
   */
  withWal: (opts: WalModeFor<DIds>) => PoolBuilderWithDepots<LIds, DIds>;
  /**
   * Enable backups for this pool using a depot in this pool.
   * Requires pool metadata via `withMeta(...)`.
   * Next: call `addInterval(...)` to register schedules.
   */
  backups: (depotIdent?: DIds) => BackupsBuilder<LIds, DIds>;
  /** Finalize the builder and return a ready LayerPool. */
  open: () => LayerPool;
};

type BackupsBuilder<
  LIds extends string,
  DIds extends string,
> = PoolBuilderWithDepots<LIds, DIds> & {
  /**
   * Register a backup interval for a specific layer ident.
   * Next: call `addInterval(...)` again or stop.
   */
  addInterval: (
    layerIdent: LayerIdentOrWildcard<LIds>,
    interval: IntervalSpec,
  ) => BackupsBuilder<LIds, DIds>;
  /**
   * Configure backup retention policy.
   * Next: call `addInterval(...)` or stop.
   */
  retain: {
    (policy: "all" | "none"): BackupsBuilder<LIds, DIds>;
    (count: number): RetainSelector<LIds, DIds>;
  };
};

type PoolOptions = {
  /** Optional WAL configuration for create/commit/tx operations. */
  walMode?: WalMode;
  /** Optional shared lock configuration for cross-process coordination. */
  lockMode?: LockMode;
  /** Optional pool metadata configuration. */
  metaMode?: PoolMetaMode;
};

type FileParams<T extends JSONable = JSONable> = {
  /** Depot file RN or RN string; must point to a depot file. */
  rn: RN<T> | string;
  /** File payload to upload (Blob or readable stream). */
  file: DepotBlob;
};

function isPoolOptions(value: unknown): value is PoolOptions {
  if (!value || typeof value !== "object") return false;
  return !("layer" in (value as object)) && !("depot" in (value as object));
}

function isPoolEntry(value: unknown): value is PoolEntry {
  if (!value || typeof value !== "object") return false;
  return "layer" in (value as object) || "depot" in (value as object);
}

function item<T extends JSONable>(sourcePool: LayerPool): UninitializedItem<T>;
function item<T extends JSONable>(
  sourcePool: "Did you forget to call .open()?",
): UninitializedItem<T>;
function item<T extends JSONable>(
  sourcePool: LayerPool | "Did you forget to call .open()?",
): UninitializedItem<T> {
  return new UninitializedItem<T>(sourcePool as LayerPool);
}

function buildBackupsManager(
  config: PoolMetaBackups,
  layers: Map<string, SourceLayer>,
  depots: Map<string, Depot>,
): { depotIdent: string; manager: BackMan } {
  const depot = depots.get(config.depotIdent);
  if (!depot) {
    throw new Error(
      `Backups depot "${config.depotIdent}" is not present in the discovered pool.`,
    );
  }
  const manager = new BackMan();
  manager.conveyLayers(layers).conveyDepot(depot, config.depotIdent);
  for (const entry of config.intervals) {
    manager.addInterval(entry.layerIdent, entry.interval);
  }
  for (const entry of config.retention) {
    manager.setRetentionForLayer(entry.layerIdent, entry.retention);
  }
  return { depotIdent: config.depotIdent, manager };
}

async function discover(sourceLayer: SourceLayer): Promise<LayerPool> {
  const meta = await readPoolMetaConfig(sourceLayer);
  if (!meta) {
    throw new Error(
      `Pool metadata not found in layer "${sourceLayer.identifier}".`,
    );
  }
  const hydrated = await hydratePoolMetaConfig(meta);

  const layerMap = new Map<string, SourceLayer>();
  for (const layer of hydrated.layers) {
    if (layer.type === "sqlite") {
      if (layer.mode !== "path" || typeof layer.value !== "string") {
        throw new Error(
          `Pool metadata has invalid sqlite config for "${layer.ident}".`,
        );
      }
      layerMap.set(layer.ident, layers.sqlite(layer.value));
      continue;
    }
    if (layer.type === "pg") {
      if (layer.mode === "url") {
        if (typeof layer.value !== "string") {
          throw new Error(
            `Pool metadata has invalid pg connection string for "${layer.ident}".`,
          );
        }
        layerMap.set(layer.ident, layers.pg(layer.value));
        continue;
      }
      if (typeof layer.value !== "object" || layer.value === null) {
        throw new Error(
          `Pool metadata has invalid pg connection options for "${layer.ident}".`,
        );
      }
      layerMap.set(layer.ident, layers.pg(layer.value as PgConnectionOptions));
      continue;
    }
    if (layer.type === "mysql") {
      if (layer.mode === "url") {
        if (typeof layer.value !== "string") {
          throw new Error(
            `Pool metadata has invalid mysql connection string for "${layer.ident}".`,
          );
        }
        layerMap.set(layer.ident, layers.mysql(layer.value));
        continue;
      }
      if (typeof layer.value !== "object" || layer.value === null) {
        throw new Error(
          `Pool metadata has invalid mysql connection options for "${layer.ident}".`,
        );
      }
      layerMap.set(layer.ident, layers.mysql(layer.value as MysqlPoolOptions));
      continue;
    }
  }

  const depotMap = new Map<string, Depot>();
  for (const depot of hydrated.depots) {
    if (depot.type === "local") {
      if (depot.mode !== "local" || typeof depot.value !== "string") {
        throw new Error(
          `Pool metadata has invalid local depot config for "${depot.ident}".`,
        );
      }
      depotMap.set(depot.ident, new LocalDepot(depot.value));
      continue;
    }
    if (depot.type === "s3") {
      if (
        depot.mode !== "s3" ||
        typeof depot.value !== "object" ||
        depot.value === null
      ) {
        throw new Error(
          `Pool metadata has invalid s3 depot config for "${depot.ident}".`,
        );
      }
      depotMap.set(depot.ident, new S3Depot(depot.value as S3DepotOptions));
      continue;
    }
  }

  const options: PoolOptions & {
    backups?: { depotIdent: string; manager: BackMan };
  } = {};
  if (hydrated.wal) {
    options.walMode = hydrated.wal;
  }
  if (hydrated.locks) {
    options.lockMode = hydrated.locks;
  }
  if (hydrated.metaLayerIdent) {
    options.metaMode = { layerIdent: hydrated.metaLayerIdent };
  }
  if (hydrated.backups) {
    const backups = buildBackupsManager(hydrated.backups, layerMap, depotMap);
    options.backups = backups;
  }

  return new LayerPool(layerMap, depotMap, options);
}

type DangerResetMode = "all" | "layers" | "depots" | "meta" | "meta only";
type DangerResetOptions = {
  /**
   * Scope of deletion. Defaults to "all".
   * - "all": drop layer data, depot data, and pool metadata.
   * - "layers": drop layer data (including pool metadata tables).
   * - "depots": delete all depot files (including WAL).
   * - "meta": delete pool metadata only.
   */
  mode?: DangerResetMode;
};

function normalizeResetMode(
  mode?: DangerResetMode,
): "all" | "layers" | "depots" | "meta" {
  if (
    !mode ||
    mode === "all" ||
    mode === "layers" ||
    mode === "depots" ||
    mode === "meta"
  ) {
    return mode ?? "all";
  }
  if (mode === "meta only") {
    return "meta";
  }
  return "all";
}

async function reset(
  pool: LayerPool,
  options: DangerResetOptions = {},
): Promise<void> {
  const mode = normalizeResetMode(options.mode);
  const dropLayers = mode === "all" || mode === "layers";
  const dropDepots = mode === "all" || mode === "depots";
  const dropMetaOnly = mode === "meta";

  const layers = pool.getLayers();
  if (dropLayers) {
    for (const layer of layers.values()) {
      if (layer.type === "sqlite") {
        const sqlite = layer as unknown as {
          _db: {
            prepare: (sql: string) => { all: () => Array<{ name: string }> };
            run: (sql: string) => void;
          };
        };
        const tables = sqlite._db
          .prepare(
            `SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '__items__%'`,
          )
          .all();
        for (const row of tables) {
          sqlite._db.run(`DROP TABLE IF EXISTS "${row.name}"`);
        }
        sqlite._db.run(`DROP TABLE IF EXISTS "__korm_meta__"`);
        sqlite._db.run(`DROP TABLE IF EXISTS "__korm_locks__"`);
        sqlite._db.run(`DROP TABLE IF EXISTS "__korm_backups__"`);
        sqlite._db.run(`DROP TABLE IF EXISTS "__korm_pool__"`);
        continue;
      }
      if (layer.type === "pg") {
        const pg = layer as unknown as {
          _db: {
            unsafe: (sql: string, params?: unknown[]) => Promise<unknown>;
          };
        };
        const rows = (await pg._db.unsafe(
          `SELECT table_name
                     FROM information_schema.tables
                     WHERE table_schema = 'public' AND table_name LIKE '__items__%'`,
        )) as Array<{ table_name: string }>;
        for (const row of rows) {
          await pg._db.unsafe(
            `DROP TABLE IF EXISTS "${row.table_name}" CASCADE`,
          );
        }
        await pg._db.unsafe(`DROP TABLE IF EXISTS "__korm_meta__"`);
        await pg._db.unsafe(`DROP TABLE IF EXISTS "__korm_locks__"`);
        await pg._db.unsafe(`DROP TABLE IF EXISTS "__korm_backups__"`);
        await pg._db.unsafe(`DROP TABLE IF EXISTS "__korm_pool__"`);
        await pg._db.unsafe(`DROP DOMAIN IF EXISTS "korm_rn_ref_text"`);
        await pg._db.unsafe(`DROP DOMAIN IF EXISTS "korm_encrypted_json"`);
        continue;
      }
      if (layer.type === "mysql") {
        const mysql = layer as unknown as {
          _pool: {
            query: (sql: string, params?: unknown[]) => Promise<unknown>;
          };
        };
        const [rows] = (await mysql._pool.query(
          `SELECT table_name
                     FROM information_schema.tables
                     WHERE table_schema = DATABASE() AND table_name LIKE '__items__%'`,
        )) as [Array<{ table_name: string }>, unknown];
        for (const row of rows) {
          await mysql._pool.query(`DROP TABLE IF EXISTS \`${row.table_name}\``);
        }
        await mysql._pool.query(`DROP TABLE IF EXISTS \`__korm_meta__\``);
        await mysql._pool.query(`DROP TABLE IF EXISTS \`__korm_locks__\``);
        await mysql._pool.query(`DROP TABLE IF EXISTS \`__korm_backups__\``);
        await mysql._pool.query(`DROP TABLE IF EXISTS \`__korm_pool__\``);
      }
    }
  }

  if (dropMetaOnly) {
    for (const layer of layers.values()) {
      await dropPoolMetaConfig(layer);
    }
  }

  if (dropDepots) {
    for (const [ident, depot] of pool.getDepots()) {
      const prefix = RN.create(`[rn][depot::${ident}]:*`);
      if (prefix.isErr()) {
        throw prefix.error;
      }
      const files = await depot.listFiles(prefix.unwrap());
      if (files.length === 0) {
        continue;
      }
      await Promise.all(files.map((file) => depot.deleteFile(file.rn)));
    }
  }
}

/**
 * Main korm entry point.
 * Start here to build items, parse RNs, create pools/layers/depots, and run queries.
 */
const korm = {
  /**
   * Begin an item builder for a pool.
   * Next: call `.empty()` for a placeholder, `from.data(...)` to create, `from.query(...)` to query, or `from.rn(...)` to fetch a single RN.
   */
  item: item,

  /**
   * Parse or validate a resource name string into an RN.
   * Use RNs in `from.query`, `from.rn`, depot operations, and references inside item data.
   */
  rn<T extends JSONable = JSONable>(rn: string): RN<T> {
    // Will throw if invalid (via Result.unwrap())
    return RN.create<T>(rn).unwrap();
  },

  /**
   * Wrap a depot file RN plus a file or stream as a floating depot file.
   * Next: call `.create(pool)` or include it in item data to upload on create/commit.
   */
  file<T extends JSONable = JSONable>(
    params: FileParams<T>,
  ): FloatingDepotFile {
    const rn =
      typeof params.rn === "string" ? korm.rn<T>(params.rn) : params.rn;
    if (!rn.isDepot() || rn.pointsTo() !== "depotFile") {
      throw new Error(`Expected depot file RN but got "${rn.value()}".`);
    }
    return new FloatingDepotFile(rn as RN<JSONable>, params.file);
  },

  /** Source layer factories (SQLite, Postgres, MySQL). Use with `korm.pool(...)`. */
  layers: layers,

  /**
   * Interval builder helpers for BackMan scheduling.
   * Next: call `korm.interval.every(...)`.
   */
  interval: new BackMan() as IntervalFactory,

  /** Query helper functions for `QueryBuilder.where(...)`. */
  qfns: { eq, and, gt, gte, inList, like, lt, lte, not, or },

  /** Depot factory helpers for local and S3 storage. */
  depots: {
    /** Create a local filesystem depot rooted at `path`. */
    local: (path: string) => new LocalDepot(path),
    /** Create an S3-compatible depot. */
    s3: (params: S3DepotOptions) => new S3Depot(params),
  },

  /**
   * Discover a pool configuration from a layer that already stores pool metadata.
   * Next: use the returned pool or close it when done.
   */
  discover: discover,

  /**
   * Execute a wrapped dangerous operation.
   * Next: await the returned result or handle it if it is synchronous.
   */
  danger: danger,

  /**
   * Wrap a pool reset behind `korm.danger(...)`.
   * Pass `{ mode: "all" | "layers" | "depots" | "meta" | "meta only" }` to scope deletion.
   * Next: call `korm.danger(korm.reset(pool, options))` to execute.
   */
  reset: (pool: LayerPool, options?: DangerResetOptions) =>
    needsDanger(reset, pool, options ?? {}),

  /**
   * Encrypt a value symmetrically (AES-256-GCM) for storage.
   * Use the returned `Encrypt<T>` directly in item data, or call `reveal()` in memory.
   */
  async encrypt<T extends JSONable>(value: T): Promise<Encrypt<T>> {
    const encrypt = new Encrypt(value, "symmetric");
    await encrypt.lock();
    return encrypt;
  },

  /**
   * Hash a password with argon2id for storage.
   * Use the returned `Encrypt<T>` in item data, then call `verifyPassword(...)` when checking credentials.
   */
  async password<T extends JSONable>(value: T): Promise<Encrypt<T>> {
    const encrypt = new Encrypt(value, "password");
    await encrypt.lock();
    return encrypt;
  },

  /**
   * Mark a value as unique within its namespace/kind column.
   * For nested objects, korm canonicalizes key order before uniqueness fingerprinting.
   * Next: use this in `from.data({ data: { ... } })` values for fields typed as `korm.types.Unique<T>`.
   */
  unique<T extends JSONable>(value: T): Unique<T> {
    return new Unique(value);
  },

  /** Create a transaction builder for FloatingItem and UncommittedItem values. */
  tx: tx,
  /** Create a LayerPool from layers/depots and optional WAL settings. */
  pool: pool,

  /**
   * Build resolve options from path strings.
   * Next: pass this to `from.rn(...)` or `QueryBuilder.get(...)`.
   */
  resolve<const Paths extends readonly string[]>(
    ...paths: Paths
  ): ResolveGetOption<Paths> {
    return { type: "resolve", paths };
  },

  /**
   * Keep only the first `n` query results after filtering/sorting.
   * Next: pass this to `QueryBuilder.get(...)`.
   */
  first<const N extends number = 1>(n?: N): FirstGetOption<N> {
    return { type: "first", n: (n ?? 1) as N };
  },

  /**
   * Sort query results by a data path.
   * Next: pass this to `QueryBuilder.get(...)`.
   */
  sortBy(
    key: string,
    directionOrOptions?: SortDirection | { allowStringify?: boolean },
    options?: { allowStringify?: boolean },
  ): SortByGetOption {
    const direction: SortDirection =
      directionOrOptions === "asc" || directionOrOptions === "desc"
        ? directionOrOptions
        : "asc";
    const config =
      typeof directionOrOptions === "object" &&
      directionOrOptions !== null &&
      !Array.isArray(directionOrOptions)
        ? directionOrOptions
        : options;
    return {
      type: "sortBy",
      key,
      direction,
      allowStringify: config?.allowStringify,
    };
  },

  /**
   * Fail when RN resolution hits a missing reference.
   * Next: pass this to `from.rn(...)` or `QueryBuilder.get(...)`.
   */
  disallowMissingReferences(): DisallowMissingReferencesGetOption {
    return { type: "disallowMissingReferences" };
  },

  /**
   * Pool entry helpers for naming layers and depots.
   * Next: pass `korm.use.layer(...).as("ident")` to `setLayers(...)` or
   * `korm.use.depot(...).as("ident")` to `setDepots(...)`.
   */
  use: {
    layer: useLayer,
    depot: useDepot,
  },

  mods: {
    /**
     * Create a `from` RN modifier for item creation.
     * Next: pass this in `from.data({ mods: [...] })` when you target a specific layer.
     */
    from(v: string): {
      key: "from";
      value: string;
    } {
      return {
        key: "from",
        value: v,
      };
    },
  },

  /**
   * Pool target helpers for choosing where metadata/locks live.
   * Next: pick a layer with `korm.target.layer(...)`, then pass it to `withMeta(...)` or `withLocks(...)`.
   */
  target: {
    /**
     * Target a pool layer for metadata or shared locks.
     * Next: pass this to `withMeta(...)` or `withLocks(...)`.
     */
    layer<const Ident extends string>(ident: Ident): LayerTarget<Ident> {
      return {
        layerIdent: ident,
        [layerTargetBrand]: "korm.target.layer",
      };
    },
  },
};

export { korm };

/**
 * Type-only namespace for public korm types.
 * Next: reference these as `korm.types.*` in type positions.
 */
export namespace korm {
  /**
   * Public type aliases exposed under `korm.types`.
   * Next: pick a type and use it in your model or helper signatures.
   */
  export namespace types {
    /**
     * Canonical JSON payload shape supported by korm item `data`.
     * Represents the serializable primitives/arrays/objects persisted to layers.
     * Next: use as a generic bound for model types and reusable helpers.
     */
    export type JSONable = JsonSerializable;
    /**
     * Time unit literals accepted by backup interval schedules.
     * Represents the frequency granularity passed into the interval builder.
     * Next: call `korm.interval.every(unit)` when defining backups.
     */
    export type IntervalUnit = import("./sources/backMan").IntervalUnit;
    /**
     * Factory surface exposed by `korm.interval`.
     * Represents the entry point that creates typed backup interval builders.
     * Next: call `.every(...)` to start building an interval rule.
     */
    export type IntervalFactory = import("./sources/backMan").IntervalFactory;
    /**
     * Concrete, validated backup interval specification.
     * Represents a fully resolved schedule consumed by BackMan.
     * Next: pass to `backups(...).addInterval(...)`.
     */
    export type IntervalSpec = import("./sources/backMan").IntervalSpec;
    /**
     * Interval specification input accepted by backup scheduling APIs.
     * Represents the serializable schedule payload stored in backup metadata.
     * Next: pass to `backups(...).addInterval(...)`.
     */
    export type IntervalPartial = import("./sources/backMan").IntervalPartial;
    /**
     * Fluent builder type returned from `korm.interval.every(...)`.
     * Represents the stateful schedule-construction chain with compile-time step constraints.
     * Next: chain `.on(...)` and/or `.at(...)`, then register via `addInterval(...)`.
     */
    export type IntervalBuilder<
      U extends import("./sources/backMan").IntervalUnit,
      S extends
        | "start"
        | "yearSelected"
        | "dateSelected"
        | "timeSelected"
        | "secondSelected",
    > = import("./sources/backMan").IntervalBuilder<U, S>;
    /**
     * Weekday selector accepted by weekly interval schedules.
     * Represents either a weekday name token or weekday index.
     * Next: pass to `korm.interval.every("week").on(...)`.
     */
    export type Weekday = import("./sources/backMan").Weekday;
    /**
     * String weekday names supported by interval schedules.
     * Represents symbolic day selectors used in backup rules.
     * Next: use with weekly `.on(...)` schedule builders.
     */
    export type WeekdayName = import("./sources/backMan").WeekdayName;
    /**
     * Numeric weekday indices used by interval schedules.
     * Represents `0..6` day positions for weekly backup rules.
     * Next: pass to `korm.interval.every("week").on(...)`.
     */
    export type WeekdayIndex = import("./sources/backMan").WeekdayIndex;
    /**
     * Persisted item wrapper with staged but uncommitted edits.
     * Represents the result of `Item.update(...)` before the write is finalized.
     * Next: call `.commit()` or include it in `korm.tx(...).persist()`.
     */
    export type UncommittedItem<T extends JSONable> =
      import("./core/item").UncommittedItem<T>;
    /**
     * In-memory item wrapper that does not yet exist in storage.
     * Represents the result of `from.data(...)` / `.empty()` before insertion.
     * Next: call `.create()` or include it in `korm.tx(...).persist()`.
     */
    export type FloatingItem<T extends JSONable> =
      import("./core/item").FloatingItem<T>;
    /**
     * Persisted item wrapper synchronized with layer state.
     * Represents a committed row read from or written to a layer.
     * Next: call `.update(...)`, `.delete()`, or read `.data`.
     */
    export type Item<T extends JSONable> = import("./core/item").Item<T>;
    /**
     * Typed item factory API returned by `korm.item<T>(pool)`.
     * Represents the namespace/kind-scoped constructor for CRUD/query entry points.
     * Next: call `.from.data(...)`, `.from.query(...)`, `.from.rn(...)`, or `.empty()`.
     */
    export type UninitializedItem<T extends JSONable> =
      import("./core/item").UninitializedItem<T>;
    /**
     * Resource Name type used for cross-item and depot references.
     * Represents parsed RN tokens with optional layer/depot mods and typed payload links.
     * Next: build with `korm.rn(...)` and store in relation/depot fields.
     */
    export type RN<T extends JSONable = JSONable> = import("./core/rn").RN<T>;
    /**
     * Query `get(...)` option that resolves RN references along selected paths.
     * Represents a path list plus resolve behavior configuration.
     * Next: create via `korm.resolve(...)` and pass to query/RN reads.
     */
    export type ResolveGetOption<
      Paths extends readonly string[] = readonly string[],
    > = import("./core/query").ResolveGetOption<Paths>;
    /**
     * Query `get(...)` option limiting result count.
     * Represents single-record mode or fixed-size first-`n` slicing.
     * Next: create via `korm.first()` or `korm.first(n)`.
     */
    export type FirstGetOption<N extends number = number> =
      import("./core/query").FirstGetOption<N>;
    /**
     * Query `get(...)` option defining sort path/direction behavior.
     * Represents a typed sort instruction generated by query helpers.
     * Next: create via `korm.sortBy(path, direction?)`.
     */
    export type SortByGetOption = import("./core/query").SortByGetOption;
    /**
     * Direction literals accepted by `korm.sortBy(...)`.
     * Represents ascending/descending ordering for query result sets.
     * Next: pass when constructing `SortByGetOption` instances.
     */
    export type SortDirection = import("./core/query").SortDirection;
    /**
     * Query `get(...)` option enabling strict missing-reference handling.
     * Represents a guard that turns unresolved RN paths into runtime errors.
     * Next: create via `korm.disallowMissingReferences()`.
     */
    export type DisallowMissingReferencesGetOption =
      import("./core/query").DisallowMissingReferencesGetOption;
    /**
     * Union of all options accepted by `QueryBuilder.get(...)`.
     * Represents the complete option contract for query result shaping.
     * Next: use in helper APIs that forward query options.
     */
    export type QueryGetOption = import("./core/query").QueryGetOption;
    /**
     * Option subset accepted by `from.rn(...)` reads.
     * Represents only RN-read-compatible query options.
     * Next: use when typing wrappers around RN fetch calls.
     */
    export type RnGetOption = import("./core/query").RnGetOption;
    /**
     * Core storage-layer interface implemented by sqlite/pg/mysql adapters.
     * Represents the CRUD/query/schema contract consumed by `LayerPool`.
     * Next: implement this when building custom/wrapped layers for `setLayers(...)`.
     */
    export type SourceLayer = import("./sources/sourceLayer").SourceLayer;
    /**
     * Write-operation options passed into layer insert/update/delete calls.
     * Represents mutation behavior flags such as destructive schema allowance.
     * Next: use when forwarding options inside `SourceLayer` wrappers.
     */
    export type PersistOptions = import("./sources/sourceLayer").PersistOptions;
    /**
     * Discriminated result union returned by layer insert/update methods.
     * Represents success/failure payloads including revert handlers and affected item wrappers.
     * Next: return this from `SourceLayer.insertItem(...)` or `SourceLayer.updateItem(...)`.
     */
    export type DbChangeResult<T extends JSONable> =
      import("./sources/sourceLayer").DbChangeResult<T>;
    /**
     * Result union returned by `SourceLayer.deleteItem(...)`.
     * Represents either a successful delete or an error payload.
     * Next: use in layer wrappers and transaction plumbing.
     */
    export type DbDeleteResult = import("./sources/sourceLayer").DbDeleteResult;
    /**
     * Normalized column-kind metadata used by layers/query planners.
     * Represents decoded storage types for table columns in a namespace/kind.
     * Next: return from `SourceLayer.getColumnKinds(...)`.
     */
    export type ColumnKind = import("./core/columnKind").ColumnKind;
    /**
     * Concrete SQLite `SourceLayer` implementation class.
     * Represents a file-backed layer adapter with korm schema/query behavior.
     * Next: create via `korm.layers.sqlite(path)` and register in a pool.
     */
    export type SqliteLayer = import("./sources").SqliteLayer;
    /**
     * Concrete Postgres `SourceLayer` implementation class.
     * Represents the pg-backed adapter used by pools and backups.
     * Next: create via `korm.layers.pg(...)` and register in a pool.
     */
    export type PgLayer = import("./sources").PgLayer;
    /**
     * Concrete MySQL `SourceLayer` implementation class.
     * Represents the mysql2-backed adapter used by pools and backups.
     * Next: create via `korm.layers.mysql(...)` and register in a pool.
     */
    export type MysqlLayer = import("./sources").MysqlLayer;
    /**
     * Input union accepted by `korm.layers.pg(...)`.
     * Represents either a connection URL string or a typed connection-options object.
     * Next: use for helper signatures that construct pg layers.
     */
    export type PgConnectionInput = import("./runtime/pgClient").PgConnectionInput;
    /**
     * Object-form Postgres connection options accepted by `korm.layers.pg(...)`.
     * Represents engine-native pg/bun SQL configuration fields for object literals.
     * Next: use when you want IntelliSense for pg config keys.
     */
    export type PgConnectionOptions = import("./runtime/pgClient").PgConnectionOptions;
    /**
     * Input union accepted by `korm.layers.mysql(...)`.
     * Represents either a MySQL connection URL or mysql2 pool options.
     * Next: use for helper signatures that construct mysql layers.
     */
    export type MysqlConnectionInput = string | import("mysql2/promise").PoolOptions;
    /**
     * Object-form mysql2 pool options accepted by `korm.layers.mysql(...)`.
     * Represents the typed config shape for MySQL object-literal connection setup.
     * Next: use when you want IntelliSense for mysql config keys.
     */
    export type MysqlConnectionOptions = import("mysql2/promise").PoolOptions;
    /**
     * Wrapper type for values that should be encrypted at rest.
     * Represents encrypted-field placeholders carried through item data until persistence.
     * Next: create values via `korm.encrypt(...)`.
     */
    export type Encrypt<T extends JSONable> =
      import("./security/encryption").Encrypt<T>;
    /**
     * Wrapper type for password-hash semantics.
     * Represents one-way secret fields intended for verify/check flows, not decryption.
     * Next: create values via `korm.password(...)`.
     */
    export type Password<T extends JSONable> =
      import("./security/encryption").Password<T>;
    /**
     * Wrapper type for values that must be unique per namespace/kind column.
     * Represents canonicalized values tracked with deterministic uniqueness fingerprints.
     * Next: create values via `korm.unique(...)`.
     */
    export type Unique<T extends JSONable> = import("./core/unique").Unique<T>;
    /**
     * File-storage backend interface used by depots.
     * Represents the read/write/list/delete contract consumed by depot workflows.
     * Next: implement custom depots or use `korm.depots.*` factories.
     */
    export type Depot = import("./depot/depot").Depot;
    /**
     * Binary payload union accepted for depot file writes.
     * Represents Blob/Buffer/stream-like content sources depots can upload.
     * Next: pass to `korm.file({ rn, file })`.
     */
    export type DepotBlob = import("./depot/depotFile").DepotBlob;
    /**
     * Union of value shapes accepted for depot-backed item fields.
     * Represents floating/uncommitted/committed depot file wrappers and compatible inputs.
     * Next: use in model field typing for file attachments.
     */
    export type DepotFileLike = import("./depot/depotFile").DepotFileLike;
    /**
     * State discriminants for depot file lifecycle wrappers.
     * Represents where a file currently sits in its create/update/commit flow.
     * Next: narrow on this in advanced depot helper logic.
     */
    export type DepotFileState = import("./depot/depotFile").DepotFileState;
    /**
     * Configuration object for the built-in S3 depot adapter.
     * Represents bucket/path/credential/runtime settings for S3 file storage.
     * Next: pass to `korm.depots.s3(...)`.
     */
    export type S3DepotOptions =
      import("./depot/depots/s3Depot").S3DepotOptions;
    /**
     * Write-ahead-log mode configuration accepted by pool builders.
     * Represents how WAL capture/recovery behavior is enabled for a pool.
     * Next: pass to `.withWal(...)`.
     */
    export type WalMode = import("./wal/wal").WalMode;
    /**
     * Retention-policy shape for WAL cleanup.
     * Represents pruning configuration for historical WAL records.
     * Next: set as `retention` inside `.withWal(...)`.
     */
    export type WalRetention = import("./wal/wal").WalRetention;
    /**
     * Item-level operation record stored inside WAL entries.
     * Represents create/update/delete intents used during replay/recovery.
     * Next: use when inspecting or validating WAL record contents.
     */
    export type WalOp = import("./wal/wal").WalOp;
    /**
     * Depot-file operation record stored inside WAL entries.
     * Represents file upload/delete intents associated with transactional writes.
     * Next: use when inspecting WAL depot operation streams.
     */
    export type WalDepotOp = import("./wal/wal").WalDepotOp;
    /**
     * Top-level WAL record envelope persisted to the WAL depot.
     * Represents a transaction batch plus metadata needed for crash recovery.
     * Next: use when reading/parsing WAL files generated by `.withWal(...)`.
     */
    export type WalRecord = import("./wal/wal").WalRecord;
    /**
     * Live pool instance coordinating layers, depots, locks, metadata, and WAL.
     * Represents the runtime object returned by `korm.pool(...).open()`.
     * Next: pass to `korm.item<T>(pool)` and call `.close()` on shutdown.
     */
    export type LayerPool = import("./sources/layerPool").LayerPool;
    /**
     * Shared-lock configuration accepted by pool builders.
     * Represents lock storage mode used for cross-process/item concurrency control.
     * Next: pass to `.withLocks(...)`.
     */
    export type LockMode = import("./sources/lockStore").LockMode;
    /**
     * Pool metadata configuration accepted by pool builders.
     * Represents where/how layer and depot metadata is stored for discovery.
     * Next: pass to `.withMeta(...)`.
     */
    export type PoolMetaMode = import("./sources/poolMeta").PoolMetaMode;
  }
}

/**
 * Create a LayerPool from layers/depots and optional WAL configuration.
 * Pass each entry as `korm.use.layer(...).as("ident")` or `korm.use.depot(...).as("ident")` (legacy object entries still work),
 * then optional `{ walMode }`.
 * `ident` is the value used in `from` or `depot` RN mods for disambiguation.
 * Alternatively, call with no args to start a progressive builder chain.
 * Next: call `.open()` to get a pool, then `pool.close()` when you are done.
 */
function pool(): PoolBuilderStart;
function pool(...entries: PoolEntry[]): LayerPool;
function pool(...entries: [...PoolEntry[], PoolOptions]): LayerPool;
function pool(
  ...entries: (PoolEntry | PoolOptions)[]
): LayerPool | PoolBuilderStart {
  if (entries.length === 0) {
    return createPoolBuilder();
  }
  const input = [...entries];
  let options: PoolOptions | undefined;
  if (input.length > 0 && isPoolOptions(input[input.length - 1])) {
    options = input.pop() as PoolOptions;
  }
  for (const entry of input) {
    if (!isPoolEntry(entry)) {
      throw new Error(
        "Invalid pool entry. Entries must be layers, depots, or a final options object.",
      );
    }
  }
  const poolEntries = input as PoolEntry[];
  const layerMap: Map<string, SourceLayer> = new Map<string, SourceLayer>();
  const depotMap: Map<string, Depot> = new Map<string, Depot>();
  poolEntries.forEach((entry) => {
    if ("layer" in entry) {
      let ident = entry.ident;
      if (!ident) {
        ident = entry.layer.identifier;
      }
      layerMap.set(ident, entry.layer);
      return;
    }
    let ident = entry.ident;
    if (!ident) {
      ident = entry.depot.identifier;
    }
    depotMap.set(ident, entry.depot);
  });
  return new LayerPool(layerMap, depotMap, options);
}

type PoolBuilderContext = {
  layers: Map<string, SourceLayer>;
  depots: Map<string, Depot>;
  walMode?: WalMode;
  lockMode?: LockMode;
  metaMode?: PoolMetaMode;
  backupsManager?: BackMan;
  backupsDepotIdent?: string;
  lastBackupLayer?: string;
};

function isLayerInputEntry(value: LayerInput): value is LayerUseEntry {
  return Boolean(
    value &&
    typeof value === "object" &&
    layerUseBrand in value &&
    value[layerUseBrand] === "korm.use.layer",
  );
}

function isDepotInputEntry(value: DepotInput): value is DepotUseEntry {
  return Boolean(
    value &&
    typeof value === "object" &&
    depotUseBrand in value &&
    value[depotUseBrand] === "korm.use.depot",
  );
}

function normalizeLayerInputs(
  entries: readonly LayerInput[],
): Map<string, SourceLayer> {
  const layerMap = new Map<string, SourceLayer>();
  for (const entry of entries) {
    if (isLayerInputEntry(entry)) {
      layerMap.set(entry.ident, entry.layer);
      continue;
    }
    if (!entry || typeof entry !== "object" || !("identifier" in entry)) {
      throw new Error(
        'Invalid layer entry. Use `korm.use.layer(layer).as("ident")` or pass a SourceLayer.',
      );
    }
    layerMap.set(entry.identifier, entry);
  }
  return layerMap;
}

function normalizeDepotInputs(
  entries: readonly DepotInput[],
): Map<string, Depot> {
  const depotMap = new Map<string, Depot>();
  for (const entry of entries) {
    if (isDepotInputEntry(entry)) {
      depotMap.set(entry.ident, entry.depot);
      continue;
    }
    if (!entry || typeof entry !== "object" || !("identifier" in entry)) {
      throw new Error(
        'Invalid depot entry. Use `korm.use.depot(depot).as("ident")` or pass a Depot.',
      );
    }
    depotMap.set(entry.identifier, entry);
  }
  return depotMap;
}

function createPoolBuilder(): PoolBuilderStart {
  return {
    setLayers: <const Entries extends readonly LayerInput[]>(
      ...entries: Entries
    ) => {
      if (entries.length === 0) {
        throw new Error("setLayers(...) requires at least one layer.");
      }
      const layerMap = normalizeLayerInputs(entries);
      const context: PoolBuilderContext = {
        layers: layerMap,
        depots: new Map(),
      };
      return attachPoolBuilderMethods<LayerIdentFrom<Entries>, never>(
        context,
      ) as PoolBuilderWithLayers<LayerIdentFrom<Entries>>;
    },
  };
}

function attachPoolBuilderMethods<LIds extends string, DIds extends string>(
  context: PoolBuilderContext,
): PoolBuilderWithLayers<LIds> | PoolBuilderWithDepots<LIds, DIds> {
  const builder = {} as Partial<BackupsBuilder<LIds, DIds>>;
  const mutable = builder as Partial<BackupsBuilder<LIds, DIds>>;
  const createCallableBuilder = (): RetainCallable<LIds, DIds> => {
    const callable = () => builder;
    return new Proxy(callable, {
      get(_target, prop) {
        const value = (builder as unknown as Record<PropertyKey, unknown>)[
          prop
        ];
        if (typeof value === "function") {
          return (value as (...args: unknown[]) => unknown).bind(builder);
        }
        return value;
      },
      apply() {
        return builder;
      },
    }) as RetainCallable<LIds, DIds>;
  };

  if (!mutable.open) {
    mutable.open = (() => {
      const options: PoolOptions & {
        backups?: { depotIdent: string; manager: BackMan };
      } = {};
      if (context.walMode) {
        options.walMode = context.walMode;
      }
      if (context.lockMode) {
        options.lockMode = context.lockMode;
      }
      if (context.metaMode) {
        options.metaMode = context.metaMode;
      }
      if (context.backupsManager && context.backupsDepotIdent) {
        options.backups = {
          depotIdent: context.backupsDepotIdent,
          manager: context.backupsManager,
        };
      }
      return new LayerPool(context.layers, context.depots, options);
    }) as PoolBuilderWithLayers<LIds>["open"];
  }

  if (!mutable.setDepots) {
    mutable.setDepots = ((...entries: DepotInput[]) => {
      if (entries.length === 0) {
        throw new Error("setDepots(...) requires at least one depot.");
      }
      const depotMap = normalizeDepotInputs(entries);
      for (const [ident, depot] of depotMap) {
        context.depots.set(ident, depot);
      }
      return builder as PoolBuilderWithDepots<LIds, DIds>;
    }) as PoolBuilderWithDepots<LIds, DIds>["setDepots"];
  }

  if (!mutable.withMeta) {
    mutable.withMeta = ((opts: PoolMetaModeFor<LIds>) => {
      if (context.metaMode) {
        throw new Error("Pool metadata is already configured for this pool.");
      }
      if (!context.layers.has(opts.layerIdent)) {
        throw new Error(
          `Pool meta layer "${opts.layerIdent}" is not present in the pool.`,
        );
      }
      context.metaMode = opts;
      return builder as PoolBuilderWithLayers<LIds> &
        PoolBuilderWithDepots<LIds, DIds>;
    }) as unknown as PoolBuilderWithDepots<LIds, DIds>["withMeta"];
  }

  if (!mutable.withLocks) {
    mutable.withLocks = ((
      target: LayerTarget<LIds>,
      options: LockOptions = {},
    ) => {
      if (context.lockMode) {
        throw new Error("Shared locks are already configured for this pool.");
      }
      if (!context.layers.has(target.layerIdent)) {
        throw new Error(
          `Lock layer "${target.layerIdent}" is not present in the pool.`,
        );
      }
      context.lockMode = { layerIdent: target.layerIdent, ...options };
      return builder as PoolBuilderWithLayers<LIds> &
        PoolBuilderWithDepots<LIds, DIds>;
    }) as unknown as PoolBuilderWithDepots<LIds, DIds>["withLocks"];
  }

  if (!mutable.withWal) {
    mutable.withWal = ((opts: WalModeFor<DIds>) => {
      if (context.depots.size === 0) {
        throw new Error(
          "withWal(...) requires at least one depot. Call setDepots(...) first.",
        );
      }
      if (context.walMode) {
        throw new Error("WAL is already configured for this pool.");
      }
      if (!context.depots.has(opts.depotIdent)) {
        throw new Error(
          `WAL depot "${opts.depotIdent}" is not present in the pool.`,
        );
      }
      context.walMode = opts;
      return builder as PoolBuilderWithDepots<LIds, DIds>;
    }) as PoolBuilderWithDepots<LIds, DIds>["withWal"];
  }

  if (!mutable.backups) {
    mutable.backups = ((depotIdent?: DIds) => {
      if (context.depots.size === 0) {
        throw new Error(
          "backups(...) requires at least one depot. Call setDepots(...) first.",
        );
      }
      if (context.backupsManager) {
        if (depotIdent && depotIdent !== context.backupsDepotIdent) {
          throw new Error(
            `Backups are already configured for depot "${context.backupsDepotIdent}".`,
          );
        }
      } else {
        const chosenIdent =
          depotIdent ??
          (context.depots.size === 1
            ? [...context.depots.keys()][0]
            : undefined);
        if (!chosenIdent) {
          throw new Error(
            "backups(...) requires a depot ident when multiple depots are present.",
          );
        }
        if (!context.depots.has(chosenIdent)) {
          throw new Error(
            `Backups depot "${chosenIdent}" is not present in the pool.`,
          );
        }
        const manager = new BackMan();
        manager
          .conveyLayers(context.layers)
          .conveyDepot(context.depots.get(chosenIdent), chosenIdent);
        context.backupsManager = manager;
        context.backupsDepotIdent = chosenIdent;
      }

      const backupsBuilder = builder as BackupsBuilder<LIds, DIds>;
      const backupsMutable = backupsBuilder as Partial<
        BackupsBuilder<LIds, DIds>
      >;
      if (!backupsMutable.addInterval) {
        backupsMutable.addInterval = ((
          layerIdent: LayerIdentOrWildcard<LIds>,
          interval: IntervalSpec,
        ) => {
          if (!context.backupsManager) {
            throw new Error(
              "addInterval(...) requires backups(...) to be configured first.",
            );
          }
          if (layerIdent !== "*" && !context.layers.has(layerIdent)) {
            throw new Error(
              `Layer "${layerIdent}" is not present in the pool.`,
            );
          }
          context.backupsManager.addInterval(layerIdent, interval);
          context.lastBackupLayer = layerIdent;
          return backupsBuilder;
        }) as BackupsBuilder<LIds, DIds>["addInterval"];
      }

      if (!backupsMutable.retain) {
        backupsMutable.retain = ((policy: "all" | "none" | number) => {
          if (!context.backupsManager) {
            throw new Error(
              "retain(...) requires backups(...) to be configured first.",
            );
          }
          if (!context.lastBackupLayer) {
            throw new Error(
              "retain(...) must follow addInterval(...) to know which layer to target.",
            );
          }
          const layerIdent = context.lastBackupLayer;
          if (policy === "all" || policy === "none") {
            context.backupsManager.setRetentionForLayer(layerIdent, {
              mode: policy,
            });
            return backupsBuilder;
          }
          const selector = {
            get backups() {
              context.backupsManager!.setRetentionForLayer(layerIdent, {
                mode: "count",
                count: policy,
              });
              return createCallableBuilder();
            },
            get days() {
              context.backupsManager!.setRetentionForLayer(layerIdent, {
                mode: "days",
                days: policy,
              });
              return createCallableBuilder();
            },
          };
          return selector as RetainSelector<LIds, DIds>;
        }) as BackupsBuilder<LIds, DIds>["retain"];
      }

      return backupsBuilder;
    }) as PoolBuilderWithDepots<LIds, DIds>["backups"];
  }

  return builder as PoolBuilderWithLayers<LIds>;
}
