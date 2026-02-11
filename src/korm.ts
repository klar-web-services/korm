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
import type { GetOpts } from "./core/query";
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
  /** File payload to upload (Blob, Bun file, or readable stream). */
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
      layerMap.set(layer.ident, layers.pg(layer.value as any));
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
      layerMap.set(layer.ident, layers.mysql(layer.value as any));
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
  ): GetOpts<Paths> {
    return { resolvePaths: paths };
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
     * JSON-compatible values accepted by korm item data.
     * Next: use as the bound for `korm.item<T>(pool)` model types.
     */
    export type JSONable = JsonSerializable;
    /**
     * Time units supported by backup interval schedules.
     * Next: call `korm.interval.every(...)`.
     */
    export type IntervalUnit = import("./sources/backMan").IntervalUnit;
    /**
     * Interval builder factory used by `korm.interval`.
     * Next: call `korm.interval.every(...)`.
     */
    export type IntervalFactory = import("./sources/backMan").IntervalFactory;
    /**
     * Fully specified interval definition for backups.
     * Next: pass to `backups(...).addInterval(...)`.
     */
    export type IntervalSpec = import("./sources/backMan").IntervalSpec;
    /**
     * Alias for interval specifications.
     * Next: pass to `backups(...).addInterval(...)`.
     */
    export type IntervalPartial = import("./sources/backMan").IntervalPartial;
    /**
     * Fluent interval builder for schedules.
     * Next: chain `.at(...)` or `.on(...)`, then pass to `backups(...).addInterval(...)`.
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
     * Weekday selector for interval schedules.
     * Next: pass to `korm.interval.every("week").on(...)`.
     */
    export type Weekday = import("./sources/backMan").Weekday;
    /**
     * Weekday name for interval schedules.
     * Next: pass to `korm.interval.every("week").on(...)`.
     */
    export type WeekdayName = import("./sources/backMan").WeekdayName;
    /**
     * Weekday index for interval schedules (0=Monday..6=Sunday).
     * Next: pass to `korm.interval.every("week").on(...)`.
     */
    export type WeekdayIndex = import("./sources/backMan").WeekdayIndex;
    /**
     * Persisted item with pending changes.
     * Next: call `.commit()` or include it in `korm.tx(...).persist()`.
     */
    export type UncommittedItem<T extends JSONable> =
      import("./core/item").UncommittedItem<T>;
    /**
     * In-memory item awaiting `.create()`.
     * Next: call `.create()` or include it in `korm.tx(...).persist()`.
     */
    export type FloatingItem<T extends JSONable> =
      import("./core/item").FloatingItem<T>;
    /**
     * Persisted item in sync with the database.
     * Next: call `.update(...)` or `.delete()`.
     */
    export type Item<T extends JSONable> = import("./core/item").Item<T>;
    /**
     * Item builder returned by `korm.item<T>(pool)`.
     * Next: call `.from.data(...)`, `.from.query(...)`, `.from.rn(...)`, or `.empty()`.
     */
    export type UninitializedItem<T extends JSONable> =
      import("./core/item").UninitializedItem<T>;
    /**
     * Resource Name (RN) type for typed references.
     * Next: build with `korm.rn(...)` or store in item fields.
     */
    export type RN<T extends JSONable = JSONable> = import("./core/rn").RN<T>;
    /**
     * SQLite layer implementation.
     * Next: pass `korm.layers.sqlite(...)` to `korm.pool().setLayers(...)`.
     */
    export type SqliteLayer = import("./sources").SqliteLayer;
    /**
     * Postgres layer implementation.
     * Next: pass `korm.layers.pg(...)` to `korm.pool().setLayers(...)`.
     */
    export type PgLayer = import("./sources").PgLayer;
    /**
     * MySQL layer implementation.
     * Next: pass `korm.layers.mysql(...)` to `korm.pool().setLayers(...)`.
     */
    export type MysqlLayer = import("./sources").MysqlLayer;
    /**
     * In-memory wrapper for encrypted values.
     * Next: create via `korm.encrypt(...)`.
     */
    export type Encrypt<T extends JSONable> =
      import("./security/encryption").Encrypt<T>;
    /**
     * Alias for password-hashed values.
     * Next: create via `korm.password(...)`.
     */
    export type Password<T extends JSONable> =
      import("./security/encryption").Password<T>;
    /**
     * Depot interface for file storage.
     * Next: create with `korm.depots.*` and pass to `setDepots(...)`.
     */
    export type Depot = import("./depot/depot").Depot;
    /**
     * Blob-like payload accepted by depots, including readable streams.
     * Next: pass to `korm.file(...)` when creating depot files.
     */
    export type DepotBlob = import("./depot/depotFile").DepotBlob;
    /**
     * Value accepted for depot file fields in item data.
     * Next: use `korm.file(...)` or include it in item data.
     */
    export type DepotFileLike = import("./depot/depotFile").DepotFileLike;
    /**
     * Depot file state machine variants.
     * Next: use with `DepotFile` helpers after resolving depot RNs.
     */
    export type DepotFileState = import("./depot/depotFile").DepotFileState;
    /**
     * S3 depot configuration options.
     * Next: pass to `korm.depots.s3(...)`.
     */
    export type S3DepotOptions =
      import("./depot/depots/s3Depot").S3DepotOptions;
    /**
     * WAL configuration options.
     * Next: pass to `withWal(...)`.
     */
    export type WalMode = import("./wal/wal").WalMode;
    /**
     * WAL retention policy.
     * Next: set the `retention` field in `withWal(...)`.
     */
    export type WalRetention = import("./wal/wal").WalRetention;
    /**
     * WAL operation record.
     * Next: use when inspecting WAL data produced by `withWal(...)`.
     */
    export type WalOp = import("./wal/wal").WalOp;
    /**
     * WAL depot file operation record.
     * Next: use when inspecting WAL depot operations.
     */
    export type WalDepotOp = import("./wal/wal").WalDepotOp;
    /**
     * WAL record envelope stored in the WAL depot.
     * Next: use when reading WAL records generated by `withWal(...)`.
     */
    export type WalRecord = import("./wal/wal").WalRecord;
    /**
     * Opened layer pool instance.
     * Next: call `.close()` when you are done.
     */
    export type LayerPool = import("./sources/layerPool").LayerPool;
    /**
     * Shared lock configuration options.
     * Next: pass to `withLocks(...)`.
     */
    export type LockMode = import("./sources/lockStore").LockMode;
    /**
     * Pool metadata configuration.
     * Next: pass to `withMeta(...)`.
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
