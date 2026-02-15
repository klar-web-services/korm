import { Result } from "@fkws/klonk-result";
import {
  QueryBuilder,
  normalizeGetOptions,
  type GetOptionResolvePaths,
  type QueryGetOption,
  type ResolvePaths,
  type RnGetOption,
} from "./query";
import { RN } from "./rn";
import util from "util";
import type { LayerPool } from "../sources/layerPool";
import type { JSONable } from "../korm";
import { eq } from "./queryFns";
import {
  deepEqualJson,
  getResolvedMeta,
  materializeRaw,
  setResolvedMeta,
  getValueAtPath,
  setValueAtPath,
  type PathKey,
  type ResolvedMeta,
} from "./resolveMeta";
import {
  applyEncryptionMeta,
  cloneEncryptionMeta,
  getEncryptionMeta,
  setEncryptionMeta,
  type EncryptionMeta,
} from "./encryptionMeta";
import { isDepotFile, type DepotFileBase } from "../depot/depotFile";
import { uploadDepotFilesFromRefs } from "../depot/depotPersist";
import { prepareWriteData } from "./persist";
import { isUnsafeObjectKey, safeAssign } from "./safeObject";
import { needsDanger, type BaseNeedsDanger } from "./danger";

type DeepPartial<T> = T extends (infer U)[]
  ? DeepPartial<U>[]
  : T extends object
    ? { [P in keyof T]?: DeepPartial<T[P]> }
    : T;

type RnModInput = {
  /** Modifier key, e.g. `from` to select a layer in a pool. */
  key: string;
  /** Modifier value for the key (layer identifier for `from`, or a custom tag). */
  value: string;
};

type FromDataParams<T extends JSONable> = {
  /** Item data payload; must be JSONable. */
  data: T;
  /** Namespace segment for the RN (pattern: [a-z][a-z0-9]*). */
  namespace: string;
  /** Kind segment for the RN (pattern: [a-z][a-z0-9]*). */
  kind: string;
  /**
   * Optional RN modifiers applied to the item RN.
   * Use `{ key: "from", value: "<layerIdent>" }` when the pool has multiple layers.
   * Item RNs cannot use the `depot` mod; use `korm.rn(...)` for depot RNs instead.
   */
  mods?: RnModInput[];
};

type ItemShowOptions = {
  /** Whether to colorize output (currently ignored; output is always colored). */
  color?: boolean;
  /** Output mode; only `full` is currently implemented. */
  what?: "data" | "rn" | "full";
};

function isPlainObject(item: unknown): item is Record<string, any> {
  if (!item || typeof item !== "object") return false;
  const proto = Object.getPrototypeOf(item);
  return proto === Object.prototype || proto === null;
}

function deepMerge<T>(target: T, source: DeepPartial<T>): T {
  if (!source) return target;
  if (!target) return source as T;

  if (!isPlainObject(target) || !isPlainObject(source)) {
    return source as T;
  }

  const targetObj = target as Record<string, any>;
  const sourceObj = source as Record<string, any>;
  const output: Record<string, any> = {};
  for (const [key, value] of Object.entries(targetObj)) {
    safeAssign(output, key, value);
  }
  Object.keys(sourceObj).forEach((key) => {
    const sVal = sourceObj[key];
    if (isUnsafeObjectKey(key)) {
      safeAssign(output, key, sVal);
      return;
    }
    if (isPlainObject(sVal)) {
      if (!(key in targetObj)) {
        safeAssign(output, key, sVal);
      } else {
        safeAssign(output, key, deepMerge(targetObj[key], sVal));
      }
    } else {
      safeAssign(output, key, sVal);
    }
  });
  return output as T;
}

function mergeInto<T>(target: T, source: DeepPartial<T>): T {
  if (!source) return target;
  if (!isPlainObject(target) || !isPlainObject(source)) {
    return source as T;
  }
  const targetObj = target as Record<string, any>;
  const sourceObj = source as Record<string, any>;
  Object.keys(sourceObj).forEach((key) => {
    const sVal = sourceObj[key];
    if (isUnsafeObjectKey(key)) {
      safeAssign(targetObj, key, sVal);
      return;
    }
    const tVal = targetObj[key];
    if (isPlainObject(sVal) && isPlainObject(tVal)) {
      safeAssign(targetObj, key, mergeInto(tVal, sVal));
    } else {
      safeAssign(targetObj, key, sVal);
    }
  });
  return target;
}

function resolvedDataFor<T extends JSONable>(
  target: object,
  fallback?: T,
): T | undefined {
  const meta = getResolvedMeta(target);
  if (meta) {
    return meta.resolvedData as T;
  }
  return fallback;
}

function isRnValue(value: unknown): value is RN<JSONable> | string {
  if (value === null || value === undefined) return false;
  if (typeof value === "string") return value.startsWith("[rn]");
  if (
    typeof value === "object" &&
    "__RN__" in (value as Record<string, unknown>)
  )
    return true;
  return false;
}

function isRefOverride(value: unknown): boolean {
  if (value === undefined) return false;
  if (isRnValue(value)) return true;
  if (isDepotFile(value)) return true;
  if (value === null) return true;
  return typeof value !== "object";
}

function normalizeRefOverride(value: unknown): {
  rawValue: unknown;
  resolvedValue: unknown;
} {
  if (isDepotFile(value)) {
    return { rawValue: value.rn.value(), resolvedValue: value };
  }
  if (
    value &&
    typeof value === "object" &&
    "__RN__" in (value as Record<string, unknown>)
  ) {
    const rnValue = (value as { value?: () => string }).value?.();
    if (rnValue) {
      return { rawValue: rnValue, resolvedValue: value };
    }
  }
  return { rawValue: value, resolvedValue: value };
}

/**
 * A committed item that is in sync with the database.
 * Use `.update(...)` to create an UncommittedItem and then `.commit()` to persist changes.
 */
export class Item<T extends JSONable> {
  protected _data?: T;
  protected _rn?: RN<T>;
  protected _layerPool: LayerPool;
  private __EMPTY__ = false;

  constructor(
    layerPool: LayerPool,
    data?: T,
    rn?: RN<T>,
    options?: { empty?: boolean },
  ) {
    this._data = data;
    this._rn = rn;
    this._layerPool = layerPool;
    this.__EMPTY__ = options?.empty ?? false;
  }

  /** Current data snapshot; may include resolved RN references if `korm.resolve(...)` was used. */
  get data(): T | undefined {
    return resolvedDataFor<T>(this, this._data);
  }

  /** Resource name for this item, if known. */
  get rn(): RN<T> | undefined {
    return this._rn;
  }

  /** Pool used to resolve layers and depots for this item. */
  public get pool(): LayerPool {
    return this._layerPool;
  }

  /**
   * Format the item for debugging output.
   * Note: only `what: "full"` is currently implemented.
   */
  show({ color = false, what = "full" }: ItemShowOptions = {}): string {
    if (what === "full") {
      let s = `┌─ ${this.rn?.value()}\n`;
      const os = util.inspect(this.data, {
        depth: null,
        colors: true,
        compact: true,
        sorted: false,
        getters: true,
      });
      const split = os.split("\n");
      split.forEach((line) => {
        s += `│ ${line}\n`;
      });
      s += "└─";
      return s;
    }
    return "Unknown";
  }

  /**
   * True if the item has no RN or data, or was created as an explicit empty placeholder.
   * Next: call `.update(...)` to prepare changes or `.show(...)` to inspect it.
   */
  isEmpty(): boolean {
    const predicates = [
      !this.data || !this.rn,
      this.data === ({} as T),
      this.rn?.value() === "",
    ];
    // Maybe future empty predicates here
    return this.__EMPTY__ || predicates.every(Boolean);
  }

  /**
   * Merge a partial update into this item and return an UncommittedItem.
   * Next: call `.commit()` or include it in `korm.tx(...).persist()`.
   */
  update(delta: DeepPartial<T>): Result<UncommittedItem<T>> {
    try {
      const encryptionMeta = getEncryptionMeta(this);
      const nextEncryptionMeta = encryptionMeta
        ? cloneEncryptionMeta(encryptionMeta)
        : undefined;
      const meta = getResolvedMeta(this);
      if (!meta) {
        const newData = deepMerge(this._data as T, delta);
        const uncommitted = new UncommittedItem<T>(
          this._layerPool,
          this._rn,
          newData,
        );
        if (nextEncryptionMeta) {
          setEncryptionMeta(uncommitted, nextEncryptionMeta);
        }
        return new Result({
          success: true,
          data: uncommitted,
        });
      }

      const baseResolved = meta.resolvedData as T;
      const nextResolved = mergeInto(baseResolved, delta);

      const overrides: Array<{
        path: PathKey[];
        rawValue: unknown;
        resolvedValue: unknown;
      }> = [];
      const nextRefs: ResolvedMeta["refs"] = [];

      const isContainerOverride = (value: unknown): boolean => {
        if (value === undefined) return false;
        if (isRefOverride(value)) return true;
        if (Array.isArray(value)) return true;
        if (!isPlainObject(value)) return true;
        return false;
      };

      const isOverriddenByParent = (path: PathKey[]): boolean => {
        for (let i = path.length - 1; i > 0; i--) {
          const parentValue = getValueAtPath(
            delta as unknown,
            path.slice(0, i),
          );
          if (isContainerOverride(parentValue)) return true;
        }
        return false;
      };

      for (const ref of meta.refs) {
        const deltaValue = getValueAtPath(delta as unknown, ref.path);
        if (isRefOverride(deltaValue)) {
          const normalized = normalizeRefOverride(deltaValue);
          overrides.push({
            path: ref.path,
            rawValue: normalized.rawValue,
            resolvedValue: normalized.resolvedValue,
          });
          continue;
        }
        if (isOverriddenByParent(ref.path)) {
          continue;
        }
        nextRefs.push(ref);
      }

      const nextMeta: ResolvedMeta = {
        ...meta,
        rawData: meta.rawData,
        resolvedData: nextResolved as JSONable,
        refs: nextRefs,
      };
      const nextRaw = materializeRaw(nextMeta);
      for (const override of overrides) {
        setValueAtPath(nextRaw, override.path, override.rawValue);
        setValueAtPath(
          nextResolved as unknown,
          override.path,
          override.resolvedValue,
        );
      }
      nextMeta.rawData = nextRaw;
      const uncommitted = new UncommittedItem<T>(
        this._layerPool,
        this._rn,
        nextRaw as T,
      );
      setResolvedMeta(uncommitted, nextMeta);
      if (nextEncryptionMeta) {
        setEncryptionMeta(uncommitted, nextEncryptionMeta);
      }
      return new Result({
        success: true,
        data: uncommitted,
      });
    } catch (error) {
      return new Result({
        success: false,
        error: new Error("Failed to update item: " + error),
      });
    }
  }

  /**
   * Delete this item from its source layer and keep a restore snapshot.
   * Next: call `.restore()` to recreate it or pass `.destroy()` to `korm.danger(...)` to drop the snapshot.
   */
  async delete(): Promise<Result<DeletedItem<T>>> {
    if (this.isEmpty() || !this._rn) {
      return new Result({
        success: false,
        error: new Error("Cannot delete an empty item."),
      });
    }

    await this._layerPool.ensureWalReady();
    const release = await this._layerPool.locker.acquire(this._rn);
    try {
      return await this._deleteLocked();
    } finally {
      release();
    }
  }

  private async _deleteLocked(): Promise<Result<DeletedItem<T>>> {
    const layer = this._layerPool.findLayerForRn(this._rn!);
    const snapshot = new Item<T>(this._layerPool, this._data, this._rn);
    const encryptionMeta = getEncryptionMeta(this);
    if (encryptionMeta) {
      setEncryptionMeta(snapshot, cloneEncryptionMeta(encryptionMeta));
    }
    const deleted = new DeletedItem(snapshot);

    const wal = this._layerPool.wal;
    let walHandle:
      | Awaited<ReturnType<NonNullable<typeof wal>["stage"]>>
      | undefined;
    try {
      if (wal) {
        walHandle = await wal.stage([
          {
            type: "delete",
            rn: this._rn!.value(),
            data: null,
          },
        ]);
      }

      const result = await layer.deleteItem(this._rn!);
      if (result.success) {
        if (wal && walHandle) {
          await wal.markDone(walHandle);
        }
        return new Result({ success: true, data: deleted });
      }
      if (wal && walHandle) {
        await wal.abort(walHandle);
      }
      return new Result({
        success: false,
        error: result.error ?? new Error("Failed to delete item."),
      });
    } catch (error) {
      if (wal && walHandle) {
        await wal.abort(walHandle);
      }
      return new Result({
        success: false,
        error: error instanceof Error ? error : new Error(String(error)),
      });
    }
  }
}

/**
 * Starting point for item creation, queries, and RN lookups.
 * Use `.empty()` for a placeholder, or `.from` helpers for create/query/fetch flows.
 */
export class UninitializedItem<T extends JSONable> {
  // This class may seem unnecessary but it's absolutely needed
  // to maintain korm's autocomplete commitment to the user.
  // The korm API cannot be achieved with discro-unions in any
  // sort of clean or acceptable way.
  protected _layerPool: LayerPool;
  protected _query?: QueryBuilder<T>;

  constructor(layerPool: LayerPool) {
    this._layerPool = layerPool;
  }

  /** Pool used by this item builder. */
  public get pool(): LayerPool {
    return this._layerPool;
  }

  /**
   * Create an empty item placeholder with no RN or data.
   * Next: call `.isEmpty()` to check it or replace it with a real item via `from.data(...)` or `from.rn(...)`.
   */
  empty(): Item<T> {
    return new Item<T>(this._layerPool, undefined, undefined, { empty: true });
  }

  /**
   * Entry point for item workflows.
   * - `from.data(...)` -> FloatingItem -> `.create()`
   * - `from.query(...)` -> QueryBuilder -> `.where()` / `.get()`
   * - `from.rn(...)` -> fetch a single item by RN
   */
  public readonly from = {
    /**
     * Fetch a single item by item RN.
     * Use `korm.resolve(...)` to materialize RN references in the returned data.
     */
    rn: async <const Options extends readonly RnGetOption[] = []>(
      rn: RN<T>,
      ...options: Options
    ): Promise<
      Result<Item<ResolvePaths<T, GetOptionResolvePaths<Options>>>>
    > => {
      try {
        if (rn.pointsTo() !== "item") {
          throw new Error(
            `Trying to fetch a collection RN "${rn.value()}" with from.rn`,
          );
        }
        if (!rn.namespace || !rn.kind || !rn.id) {
          throw new Error(`Invalid RN "${rn.value()}"`);
        }
        const collectionRes = RN.create<T>(
          rn.namespace,
          rn.kind,
          "*",
          new Map(rn.mods ?? []),
        );
        if (collectionRes.isErr()) {
          return new Result({ success: false, error: collectionRes.error });
        }
        const collectionRn = collectionRes.unwrap();
        const query = new QueryBuilder<T>(collectionRn, this);
        query.where(eq("rnId", rn.id));
        const normalized = normalizeGetOptions(options, "rn");
        if (normalized.isErr()) {
          return new Result({ success: false, error: normalized.error });
        }
        const parsed = normalized.unwrap();
        const queryOptions: QueryGetOption[] = [];
        if (parsed.resolvePaths.length > 0) {
          queryOptions.push({ type: "resolve", paths: parsed.resolvePaths });
        }
        if (!parsed.allowMissing) {
          queryOptions.push({ type: "disallowMissingReferences" });
        }
        const result = await query.get(...queryOptions);
        if (result.isErr()) {
          return new Result({ success: false, error: result.error });
        }
        const items = result.unwrap();
        if (items.length !== 1) {
          return new Result({
            success: false,
            error: new Error(
              `Expected 1 item for RN "${rn.value()}" but got ${items.length}.`,
            ),
          });
        }
        return new Result({
          success: true,
          data: items[0] as Item<ResolvePaths<T, GetOptionResolvePaths<Options>>>,
        });
      } catch (error) {
        return new Result({
          success: false,
          error: error instanceof Error ? error : new Error(String(error)),
        });
      }
    },

    /**
     * Start a query on a collection RN.
     * Next: call `.where(...)` and `.get(...)`.
     */
    query: (rn: RN<T>): QueryBuilder<T> => {
      if (rn.pointsTo() !== "collection") {
        throw new Error(
          `Trying to run query on item-specific RN "${rn.value()}"`,
        );
      }
      return new QueryBuilder(rn, this);
    },

    /**
     * Create a FloatingItem from data and metadata (namespace/kind/mods).
     * Next: call `.create()` to persist it.
     */
    data: (params: FromDataParams<T>): Result<FloatingItem<T>> => {
      const rnRes = RN.create<T>(
        params.namespace,
        params.kind,
        crypto.randomUUID(),
      );
      if (rnRes.isErr()) {
        return new Result({ success: false, error: rnRes.error });
      }
      if (params.mods) {
        for (const { key, value } of params.mods) {
          rnRes.mod(key, value);
        }
      }
      return new Result({
        success: true,
        data: new FloatingItem<T>(this._layerPool, rnRes.unwrap(), params.data),
      });
    },
  };
}

/**
 * An in-memory item that is not yet persisted.
 * Call `.create()` to insert it into a layer.
 */
export class FloatingItem<T extends JSONable> {
  // An item that does not yet exist in the database at all
  protected _rn?: RN<T>;
  protected _data?: T;
  protected _layerPool: LayerPool;

  constructor(layerPool: LayerPool, rn?: RN<T>, data?: T) {
    this._rn = rn;
    this._data = data;
    this._layerPool = layerPool;
  }

  /** Resource name for this item, if assigned. */
  get rn(): RN<T> | undefined {
    return this._rn;
  }

  /** Current data snapshot; may include resolved RN references if any were set. */
  get data(): T | undefined {
    return resolvedDataFor<T>(this, this._data);
  }

  /** Pool used to persist this item. */
  get pool(): LayerPool {
    return this._layerPool;
  }

  /**
   * Persist this item, creating tables if needed.
   * Next: use `.update(...)` on the returned Item to make changes.
   */
  async create(): Promise<Result<Item<T>>> {
    await this._layerPool.ensureWalReady();

    // Acquire lock for this RN to prevent concurrent operations
    const release = await this._layerPool.locker.acquire(this._rn!);
    try {
      return await this._createLocked();
    } finally {
      release();
    }
  }

  private async _createLocked(): Promise<Result<Item<T>>> {
    const layer = this._layerPool.findLayerForRn(this._rn!);
    const encryptionMeta = getEncryptionMeta(this);
    let persistItem: FloatingItem<T> = this;
    let nextEncryptionMeta = encryptionMeta;

    const wal = this._layerPool.wal;
    const captureDepotOps = wal?.depotOpsEnabled ?? false;
    const depotOps: DepotFileBase[] | undefined = captureDepotOps
      ? []
      : undefined;
    const prepared = await prepareWriteData(
      this._data,
      encryptionMeta,
      this._layerPool,
      {
        depotOps,
        skipDepotUpload: captureDepotOps,
      },
    );
    if (prepared.data !== undefined) {
      persistItem = new FloatingItem<T>(
        this._layerPool,
        this._rn,
        prepared.data as T,
      );
      nextEncryptionMeta = prepared.meta;
      if (nextEncryptionMeta) {
        setEncryptionMeta(persistItem, nextEncryptionMeta);
      }
    }
    let walHandle:
      | Awaited<ReturnType<NonNullable<typeof wal>["stage"]>>
      | undefined;
    try {
      if (wal && prepared.data !== undefined) {
        walHandle = await wal.stage(
          [
            {
              type: "insert",
              rn: this._rn!.value(),
              data: prepared.data as JSONable,
            },
          ],
          depotOps,
        );
        if (captureDepotOps) {
          await wal.applyDepotOps(walHandle);
        }
      }
      const result = await layer.insertItem(persistItem);
      if (result.success) {
        if (wal && walHandle) {
          await wal.markDone(walHandle);
        }
        const committed = new Item<T>(this._layerPool, this._data, this._rn);
        if (nextEncryptionMeta) {
          setEncryptionMeta(committed, nextEncryptionMeta);
        }
        return new Result({ success: true, data: committed });
      }
      if (wal && walHandle) {
        if (captureDepotOps) {
          try {
            await wal.undoDepotOps(walHandle);
          } catch {}
        }
        await wal.abort(walHandle);
      }
      return new Result({
        success: false,
        error: new Error("Failed to create item " + this._rn?.value()),
      });
    } catch (error) {
      if (wal && walHandle) {
        if (captureDepotOps) {
          try {
            await wal.undoDepotOps(walHandle);
          } catch {}
        }
        await wal.abort(walHandle);
      }
      return new Result({ success: false, error: error as Error });
    }
  }

  /**
   * Apply a partial update and return a new FloatingItem.
   * This does not persist; call `.create()` when ready.
   */
  update(delta: DeepPartial<T>): Result<FloatingItem<T>> {
    try {
      const encryptionMeta = getEncryptionMeta(this);
      const nextEncryptionMeta = encryptionMeta
        ? cloneEncryptionMeta(encryptionMeta)
        : undefined;
      const newData = deepMerge(this._data as T, delta);
      const nextItem = new FloatingItem<T>(this._layerPool, this._rn, newData);
      if (nextEncryptionMeta) {
        setEncryptionMeta(nextItem, nextEncryptionMeta);
      }
      return new Result({
        success: true,
        data: nextItem,
      });
    } catch (error) {
      return new Result({
        success: false,
        error: new Error("Failed to update item: " + error),
      });
    }
  }
}

/**
 * An item snapshot after deletion, used for optional restore.
 * Next: call `.restore()` to recreate it, or pass `.destroy()` to `korm.danger(...)` to drop the snapshot.
 */
export class DeletedItem<T extends JSONable> {
  public __DELETED__: true = true;
  private __WAS__?: Item<T>;
  private __WAS_RN_VAL__: string;

  constructor(was: Item<T>) {
    this.__WAS__ = was;
    this.__WAS_RN_VAL__ = "[DELETED]" + this.__WAS__.rn?.value();
  }

  /**
   * Restore the deleted item by re-inserting its last known data.
   * Next: call `.update(...)` on the returned Item to make changes.
   */
  async restore(): Promise<Result<Item<T>>> {
    if (!this.__WAS__) {
      return new Result({
        success: false,
        error: new Error(
          "Tried restoring Item " +
            this.__WAS_RN_VAL__ +
            " which was already destroyed.",
        ),
      });
    }

    const newFloating = new FloatingItem<T>(
      this.__WAS__.pool,
      this.__WAS__.rn,
      this.__WAS__.data,
    );
    const encryptionMeta = getEncryptionMeta(this.__WAS__);
    if (encryptionMeta) {
      setEncryptionMeta(newFloating, cloneEncryptionMeta(encryptionMeta));
    }

    const tryCreate = await newFloating.create();
    if (tryCreate.isErr()) {
      return new Result({ success: false, error: tryCreate.error });
    }

    return tryCreate;
  }

  /**
   * Destroy the snapshot to prevent any future restore.
   * Next: execute it with `korm.danger(...)` and discard the DeletedItem reference.
   */
  destroy(): BaseNeedsDanger<boolean> {
    return needsDanger(() => delete this.__WAS__);
  }
}

/**
 * A persisted item with pending in-memory changes.
 * Call `.commit()` to save or include it in `korm.tx(...).persist()`.
 */
export class UncommittedItem<T extends JSONable> {
  // An item that exists in the database but whose data has been changed but not yet saved
  protected _rn?: RN<T>;
  protected _data?: T;
  protected _layerPool: LayerPool;

  constructor(layerPool: LayerPool, rn?: RN<T>, data?: T) {
    this._rn = rn;
    this._data = data;
    this._layerPool = layerPool;
  }

  /** Resource name for this item, if known. */
  get rn(): RN<T> | undefined {
    return this._rn;
  }

  /** Current data snapshot; may include resolved RN references. */
  get data(): T | undefined {
    return resolvedDataFor<T>(this, this._data);
  }

  /** Pool used to persist this item. */
  public get pool(): LayerPool {
    return this._layerPool;
  }

  /**
   * Persist pending changes.
   * If resolved RN references were mutated, those changes are written to their own layers.
   */
  async commit(): Promise<Result<Item<T>>> {
    await this._layerPool.ensureWalReady();
    const meta = getResolvedMeta(this);

    if (!meta) {
      // Simple case: no resolved references, just lock this RN
      const release = await this._layerPool.locker.acquire(this._rn!);
      try {
        return await this._commitSimpleLocked();
      } finally {
        release();
      }
    }

    // Complex case: collect all RNs that will be updated (including resolved references)
    const rnsToLock: RN<JSONable>[] = [this._rn!];
    const visited = new Set<object>();

    const collectRns = (currentMeta: ResolvedMeta) => {
      const resolvedObj = currentMeta.resolvedData;
      if (resolvedObj && typeof resolvedObj === "object") {
        if (visited.has(resolvedObj as object)) return;
        visited.add(resolvedObj as object);
      }
      for (const ref of currentMeta.refs) {
        const childValue = getValueAtPath(currentMeta.resolvedData, ref.path);
        if (!childValue || typeof childValue !== "object") continue;
        const childMeta = getResolvedMeta(childValue as object);
        if (!childMeta || !childMeta.rn) continue;
        if (childMeta.rn.isDepot()) continue;
        rnsToLock.push(childMeta.rn);
        collectRns(childMeta);
      }
    };
    collectRns(meta);

    // Acquire all locks in sorted order to prevent deadlocks
    const release = await this._layerPool.locker.acquireMultiple(rnsToLock);
    try {
      return await this._commitWithResolvedLocked(meta);
    } finally {
      release();
    }
  }

  private async _commitSimpleLocked(): Promise<Result<Item<T>>> {
    const wal = this._layerPool.wal;
    const captureDepotOps = wal?.depotOpsEnabled ?? false;
    const layer = this._layerPool.findLayerForRn(this._rn!);
    const encryptionMeta = getEncryptionMeta(this);
    let persistItem: UncommittedItem<T> = this;
    let nextEncryptionMeta = encryptionMeta;

    const depotOps: DepotFileBase[] | undefined = captureDepotOps
      ? []
      : undefined;
    const prepared = await prepareWriteData(
      this._data,
      encryptionMeta,
      this._layerPool,
      {
        depotOps,
        skipDepotUpload: captureDepotOps,
      },
    );
    if (prepared.data !== undefined) {
      persistItem = new UncommittedItem<T>(
        this._layerPool,
        this._rn,
        prepared.data as T,
      );
      nextEncryptionMeta = prepared.meta;
      if (nextEncryptionMeta) {
        setEncryptionMeta(persistItem, nextEncryptionMeta);
      }
    }

    let walHandle:
      | Awaited<ReturnType<NonNullable<typeof wal>["stage"]>>
      | undefined;
    try {
      if (wal && prepared.data !== undefined) {
        walHandle = await wal.stage(
          [
            {
              type: "update",
              rn: this._rn!.value(),
              data: prepared.data as JSONable,
            },
          ],
          depotOps,
        );
        if (captureDepotOps) {
          await wal.applyDepotOps(walHandle);
        }
      }
      const result = await layer.updateItem(persistItem);
      if (result.success) {
        if (wal && walHandle) {
          await wal.markDone(walHandle);
        }
        const committed = new Item<T>(this._layerPool, this._data, this._rn);
        if (nextEncryptionMeta) {
          setEncryptionMeta(committed, nextEncryptionMeta);
        }
        return new Result({ success: true, data: committed });
      }
      if (wal && walHandle) {
        if (captureDepotOps) {
          try {
            await wal.undoDepotOps(walHandle);
          } catch {}
        }
        await wal.abort(walHandle);
      }
      return new Result({
        success: false,
        error: new Error("Failed to commit item " + this._rn?.value()),
      });
    } catch (error) {
      if (wal && walHandle) {
        if (captureDepotOps) {
          try {
            await wal.undoDepotOps(walHandle);
          } catch {}
        }
        await wal.abort(walHandle);
      }
      return new Result({ success: false, error: error as Error });
    }
  }

  private async _commitWithResolvedLocked(
    meta: ResolvedMeta,
  ): Promise<Result<Item<T>>> {
    const wal = this._layerPool.wal;
    const captureDepotOps = wal?.depotOpsEnabled ?? false;

    const depotOps: DepotFileBase[] | undefined = captureDepotOps
      ? []
      : undefined;
    const updatesByRn = new Map<
      string,
      {
        meta: ResolvedMeta;
        nextRaw: JSONable;
        encryptionMeta?: EncryptionMeta;
        target?: object;
      }
    >();
    const visited = new Set<object>();

    const collectUpdates = async (currentMeta: ResolvedMeta) => {
      const resolvedObj = currentMeta.resolvedData;
      if (resolvedObj && typeof resolvedObj === "object") {
        if (visited.has(resolvedObj as object)) return;
        visited.add(resolvedObj as object);
      }
      await uploadDepotFilesFromRefs(currentMeta, this._layerPool, {
        depotOps,
        skipUpload: captureDepotOps,
      });
      for (const ref of currentMeta.refs) {
        const childValue = getValueAtPath(currentMeta.resolvedData, ref.path);
        if (!childValue || typeof childValue !== "object") continue;
        const childMeta = getResolvedMeta(childValue as object);
        if (!childMeta || !childMeta.rn) continue;
        if (childMeta.rn.isDepot()) continue;
        let childRaw = materializeRaw(childMeta);
        const childEncryptionMeta = getEncryptionMeta(childValue as object);
        if (childEncryptionMeta) {
          const applied = await applyEncryptionMeta(
            childRaw as JSONable,
            childEncryptionMeta,
          );
          childRaw = applied.data as JSONable;
          if (!deepEqualJson(childRaw, childMeta.rawData)) {
            updatesByRn.set(childMeta.rn.value(), {
              meta: childMeta,
              nextRaw: childRaw,
              encryptionMeta: applied.meta,
              target: childValue as object,
            });
          }
        } else if (!deepEqualJson(childRaw, childMeta.rawData)) {
          updatesByRn.set(childMeta.rn.value(), {
            meta: childMeta,
            nextRaw: childRaw,
          });
        }
        await collectUpdates(childMeta);
      }
    };

    await collectUpdates(meta);

    const pendingUpdates: {
      update: {
        meta: ResolvedMeta;
        nextRaw: JSONable;
        encryptionMeta?: EncryptionMeta;
        target?: object;
      };
      item: UncommittedItem<JSONable>;
    }[] = [];
    const walOps: { type: "update"; rn: string; data: JSONable }[] = [];

    for (const update of updatesByRn.values()) {
      const prepared = await prepareWriteData(
        update.nextRaw as JSONable,
        update.encryptionMeta,
        this._layerPool,
        {
          depotOps,
          skipDepotUpload: captureDepotOps,
        },
      );
      if (prepared.data === undefined) {
        continue;
      }
      update.nextRaw = prepared.data as JSONable;
      update.encryptionMeta = prepared.meta;
      const item = new UncommittedItem<JSONable>(
        this._layerPool,
        update.meta.rn,
        prepared.data,
      );
      if (update.encryptionMeta) {
        setEncryptionMeta(item, update.encryptionMeta);
      }
      pendingUpdates.push({ update, item });
      walOps.push({
        type: "update",
        rn: update.meta.rn!.value(),
        data: prepared.data as JSONable,
      });
    }

    const rootRaw = materializeRaw(meta) as T;
    const rootEncryptionMeta = getEncryptionMeta(this);
    const rootPrepared = await prepareWriteData(
      rootRaw as JSONable,
      rootEncryptionMeta,
      this._layerPool,
      {
        depotOps,
        skipDepotUpload: captureDepotOps,
      },
    );
    const rootPersistRaw = rootPrepared.data as JSONable;
    const nextRootEncryptionMeta = rootPrepared.meta;

    const rootItem = new UncommittedItem<T>(
      this._layerPool,
      this._rn,
      rootPersistRaw as T,
    );
    if (nextRootEncryptionMeta) {
      setEncryptionMeta(rootItem, nextRootEncryptionMeta);
    }
    walOps.push({
      type: "update",
      rn: this._rn!.value(),
      data: rootPersistRaw as JSONable,
    });

    let walHandle:
      | Awaited<ReturnType<NonNullable<typeof wal>["stage"]>>
      | undefined;
    const applied: { revert: () => void; success: boolean }[] = [];
    try {
      if (wal && (walOps.length > 0 || (depotOps && depotOps.length > 0))) {
        walHandle = await wal.stage(walOps, depotOps);
        if (captureDepotOps) {
          await wal.applyDepotOps(walHandle);
        }
      }

      for (const pending of pendingUpdates) {
        const layer = this._layerPool.findLayerForRn(pending.update.meta.rn!);
        const result = await layer.updateItem(pending.item);
        if (!result.success) {
          for (const op of applied) {
            if (op.success) op.revert();
          }
          if (wal && walHandle) {
            if (captureDepotOps) {
              try {
                await wal.undoDepotOps(walHandle);
              } catch {}
            }
            await wal.abort(walHandle);
          }
          return new Result({
            success: false,
            error:
              result.error ??
              new Error("Failed to update resolved references."),
          });
        }
        applied.push(result);
      }

      const rootLayer = this._layerPool.findLayerForRn(this._rn!);
      const rootResult = await rootLayer.updateItem(rootItem);
      if (!rootResult.success) {
        for (const op of applied) {
          if (op.success) op.revert();
        }
        if (wal && walHandle) {
          if (captureDepotOps) {
            try {
              await wal.undoDepotOps(walHandle);
            } catch {}
          }
          await wal.abort(walHandle);
        }
        return new Result({
          success: false,
          error:
            rootResult.error ??
            new Error("Failed to commit item " + this._rn?.value()),
        });
      }
      if (wal && walHandle) {
        await wal.markDone(walHandle);
      }
    } catch (error) {
      if (wal && walHandle) {
        if (captureDepotOps) {
          try {
            await wal.undoDepotOps(walHandle);
          } catch {}
        }
        await wal.abort(walHandle);
      }
      return new Result({ success: false, error: error as Error });
    }

    for (const update of updatesByRn.values()) {
      update.meta.rawData = update.nextRaw;
      if (update.encryptionMeta && update.target) {
        setEncryptionMeta(update.target, update.encryptionMeta);
      }
    }
    meta.rawData = rootPersistRaw as JSONable;

    const committed = new Item<T>(this._layerPool, rootRaw, this._rn);
    setResolvedMeta(committed, meta);
    if (nextRootEncryptionMeta) {
      setEncryptionMeta(committed, nextRootEncryptionMeta);
    }
    return new Result({ success: true, data: committed });
  }

  /**
   * Apply another partial update and keep the item uncommitted.
   * Next: call `.commit()` to persist.
   */
  update(delta: DeepPartial<T>): Result<UncommittedItem<T>> {
    try {
      const encryptionMeta = getEncryptionMeta(this);
      const nextEncryptionMeta = encryptionMeta
        ? cloneEncryptionMeta(encryptionMeta)
        : undefined;
      const meta = getResolvedMeta(this);
      if (!meta) {
        const newData = deepMerge(this._data as T, delta);
        const uncommitted = new UncommittedItem<T>(
          this._layerPool,
          this._rn,
          newData,
        );
        if (nextEncryptionMeta) {
          setEncryptionMeta(uncommitted, nextEncryptionMeta);
        }
        return new Result({
          success: true,
          data: uncommitted,
        });
      }

      const baseResolved = meta.resolvedData as T;
      const nextResolved = mergeInto(baseResolved, delta);

      const overrides: Array<{
        path: PathKey[];
        rawValue: unknown;
        resolvedValue: unknown;
      }> = [];
      const nextRefs: ResolvedMeta["refs"] = [];

      const isContainerOverride = (value: unknown): boolean => {
        if (value === undefined) return false;
        if (isRefOverride(value)) return true;
        if (Array.isArray(value)) return true;
        if (!isPlainObject(value)) return true;
        return false;
      };

      const isOverriddenByParent = (path: PathKey[]): boolean => {
        for (let i = path.length - 1; i > 0; i--) {
          const parentValue = getValueAtPath(
            delta as unknown,
            path.slice(0, i),
          );
          if (isContainerOverride(parentValue)) return true;
        }
        return false;
      };

      for (const ref of meta.refs) {
        const deltaValue = getValueAtPath(delta as unknown, ref.path);
        if (isRefOverride(deltaValue)) {
          const normalized = normalizeRefOverride(deltaValue);
          overrides.push({
            path: ref.path,
            rawValue: normalized.rawValue,
            resolvedValue: normalized.resolvedValue,
          });
          continue;
        }
        if (isOverriddenByParent(ref.path)) {
          continue;
        }
        nextRefs.push(ref);
      }

      const nextMeta: ResolvedMeta = {
        ...meta,
        rawData: meta.rawData,
        resolvedData: nextResolved as JSONable,
        refs: nextRefs,
      };
      const nextRaw = materializeRaw(nextMeta);
      for (const override of overrides) {
        setValueAtPath(nextRaw, override.path, override.rawValue);
        setValueAtPath(
          nextResolved as unknown,
          override.path,
          override.resolvedValue,
        );
      }
      nextMeta.rawData = nextRaw;
      const uncommitted = new UncommittedItem<T>(
        this._layerPool,
        this._rn,
        nextRaw as T,
      );
      setResolvedMeta(uncommitted, nextMeta);
      if (nextEncryptionMeta) {
        setEncryptionMeta(uncommitted, nextEncryptionMeta);
      }
      return new Result({
        success: true,
        data: uncommitted,
      });
    } catch (error) {
      return new Result({
        success: false,
        error: new Error("Failed to update item: " + error),
      });
    }
  }
}
