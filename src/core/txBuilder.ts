import { FloatingItem, Item, UncommittedItem } from "./item";
import type { JSONable } from "../korm";
import type { DbChangeResult, PersistOptions } from "../sources/sourceLayer";
import {
    getEncryptionMeta,
    setEncryptionMeta,
    type EncryptionMeta,
} from "./encryptionMeta";
import { getResolvedMeta, getValueAtPath, materializeRaw } from "./resolveMeta";
import { uploadDepotFilesFromRefs } from "../depot/depotPersist";
import { prepareWriteData } from "./persist";
import type { DepotFileBase } from "../depot/depotFile";
import type { RN } from "./rn";

type TxItem = FloatingItem<JSONable> | UncommittedItem<JSONable>;
type TxItemData<I> = I extends FloatingItem<infer T> ? T : I extends UncommittedItem<infer T> ? T : never;
type PersistableItem<T extends JSONable> = FloatingItem<T> | UncommittedItem<T>;

type ItemResult<T extends JSONable> = {
    reverted: false,
    item: Item<T>,
    error?: string
} | {
    reverted: true,
    item: FloatingItem<T> | UncommittedItem<T>,
    error?: string
}

/**
 * Result of a transaction persist.
 * Use `isErr()` / `isOk()` and inspect `items` for committed or reverted items.
 */
class TxResult<T extends JSONable> {
    private _success: boolean;
    private _items: ItemResult<T>[];
    private _error?: string;

    constructor({ success, items, error }: { success: boolean; items: ItemResult<T>[]; error?: string }) {
        this._success = success;
        this._items = items;
        this._error = error;
    }

    /** True when the transaction failed. */
    isErr() {
        return !this._success;
    }

    /** True when the transaction succeeded. */
    isOk() {
        return this._success;
    }

    /** Items touched by the transaction, each with its reverted/committed state. */
    get items() {
        return this._items;
    }

    /** Error message for failed transactions, if any. */
    get error() {
        return this._error;
    }
}

/**
 * Transaction builder for FloatingItem and UncommittedItem objects.
 * Call `persist()` to apply all operations in order with rollback on failure.
 */
class Tx<Items extends readonly TxItem[]> {
    private _itemsToPersist: Items;
    private _opResults: DbChangeResult<TxItemData<Items[number]>>[] = [];
    private _spent: boolean = false;

    constructor(itemsToPersist: Items) {
        this._itemsToPersist = itemsToPersist;
    }

    private _revertAll(): ItemResult<TxItemData<Items[number]>>[] {
        if (this._spent) {
            throw new Error("This Tx is spent. Please start a new Tx.")
        }
        const results: ItemResult<TxItemData<Items[number]>>[] = []
        for (const op of this._opResults) {
            if (op.success) {
                op.revert();
                if (op.type === "insert") {
                    results.push({
                        reverted: true,
                        item: new FloatingItem(op.item.pool, op.item.rn, op.item.data as TxItemData<Items[number]>)
                    })
                } else {
                    results.push({
                        reverted: true,
                        item: new UncommittedItem(op.item.pool, op.item.rn, op.oldData)
                    })
                }
            } else {
                results.push({
                    reverted: true,
                    item: op.item,
                    error: op.error?.message
                })
            }
        }

        if (results.length < this._itemsToPersist.length) {
            for (let i = results.length; i < this._itemsToPersist.length; i++) {
                results.push({
                    reverted: true,
                    item: this._itemsToPersist[i]! as PersistableItem<TxItemData<Items[number]>>
                })
            }
        }

        this._spent = true;

        return results;
    }

    /**
     * Collect all RNs that will be affected by this transaction,
     * including resolved references.
     */
    private _collectAllRns(): { rns: RN<JSONable>[]; poolForRn: Map<string, TxItem["pool"]> } {
        const rns: RN<JSONable>[] = [];
        const poolForRn = new Map<string, TxItem["pool"]>();
        const visited = new Set<object>();

        const collectFromMeta = (meta: ReturnType<typeof getResolvedMeta>, pool: TxItem["pool"]) => {
            if (!meta) return;
            const resolvedObj = meta.resolvedData;
            if (resolvedObj && typeof resolvedObj === "object") {
                if (visited.has(resolvedObj as object)) return;
                visited.add(resolvedObj as object);
            }
            for (const ref of meta.refs) {
                const childValue = getValueAtPath(meta.resolvedData, ref.path);
                if (!childValue || typeof childValue !== "object") continue;
                const childMeta = getResolvedMeta(childValue as object);
                if (!childMeta || !childMeta.rn) continue;
                if (childMeta.rn.isDepot()) continue;
                rns.push(childMeta.rn);
                poolForRn.set(childMeta.rn.value(), pool);
                collectFromMeta(childMeta, pool);
            }
        };

        for (const item of this._itemsToPersist) {
            rns.push(item.rn!);
            poolForRn.set(item.rn!.value(), item.pool);
            const resolvedMeta = getResolvedMeta(item as object);
            if (resolvedMeta) {
                collectFromMeta(resolvedMeta, item.pool);
            }
        }

        return { rns, poolForRn };
    }

    /**
     * Persist all items in this transaction.
     * Use `{ destructive: true }` to allow column recreation when shapes change.
     */
    public async persist(options?: PersistOptions): Promise<TxResult<TxItemData<Items[number]>>> {
        if (this._spent) {
            throw new Error("This Tx is spent. Please start a new Tx.")
        }
        const persistOptions: PersistOptions = options ?? {};
        const pools = new Set(this._itemsToPersist.map((item) => item.pool));
        await Promise.all(Array.from(pools, (pool) => pool.ensureWalReady()));

        // Collect all RNs and acquire locks in sorted order to prevent deadlocks
        const { rns, poolForRn } = this._collectAllRns();

        // Group RNs by pool for acquireMultiple (each pool has its own locker)
        const rnsByPool = new Map<TxItem["pool"], RN<JSONable>[]>();
        for (const rn of rns) {
            const pool = poolForRn.get(rn.value())!;
            const existing = rnsByPool.get(pool) ?? [];
            existing.push(rn);
            rnsByPool.set(pool, existing);
        }

        // Acquire all locks (sorted by pool identifier, then by RN within each pool)
        const sortedPools = Array.from(rnsByPool.keys()).sort((a, b) => {
            const aIdent = Array.from(a.getLayers().keys()).sort().join(",");
            const bIdent = Array.from(b.getLayers().keys()).sort().join(",");
            return aIdent.localeCompare(bIdent);
        });

        const releases: (() => void)[] = [];
        try {
            for (const pool of sortedPools) {
                const poolRns = rnsByPool.get(pool)!;
                const release = await pool.locker.acquireMultiple(poolRns);
                releases.push(release);
            }
            return await this._persistLocked(persistOptions);
        } finally {
            // Release in reverse order
            for (let i = releases.length - 1; i >= 0; i--) {
                releases[i]!();
            }
        }
    }

    private async _persistLocked(persistOptions: PersistOptions): Promise<TxResult<TxItemData<Items[number]>>> {
        const updatedMetas: (EncryptionMeta | undefined)[] = [];
        const prepared: Array<{
            item: PersistableItem<TxItemData<Items[number]>>;
            layer: ReturnType<TxItem["pool"]["findLayerForRn"]>;
            isFloating: boolean;
            pool: TxItem["pool"];
        }> = [];
        const walOpsByPool = new Map<TxItem["pool"], { wal: NonNullable<TxItem["pool"]["wal"]>; ops: { type: "insert" | "update"; rn: string; data: JSONable; destructive?: boolean }[] }>();
        const walDepotOpsByPool = new Map<TxItem["pool"], { wal: NonNullable<TxItem["pool"]["wal"]>; ops: DepotFileBase[] }>();

        for (const item of this._itemsToPersist) {
            const isFloating = "create" in item;
            const layer = item.pool.findLayerForRn(item.rn!);
            const encryptionMeta = getEncryptionMeta(item);
            let persistItem: PersistableItem<TxItemData<Items[number]>> = item as PersistableItem<TxItemData<Items[number]>>;

            const wal = item.pool.wal;
            const captureDepotOps = wal?.depotOpsEnabled ?? false;
            const depotOps: DepotFileBase[] | undefined = captureDepotOps ? [] : undefined;
            const resolvedMeta = getResolvedMeta(item as object);
            if (resolvedMeta) {
                await uploadDepotFilesFromRefs(resolvedMeta, item.pool, captureDepotOps ? {
                    depotOps,
                    skipUpload: true,
                } : undefined);
            }
            const baseData = resolvedMeta
                ? (materializeRaw(resolvedMeta) as JSONable)
                : (item.data as JSONable | undefined);

            const preparedData = await prepareWriteData(
                baseData as JSONable | undefined,
                encryptionMeta,
                item.pool,
                captureDepotOps ? { depotOps, skipDepotUpload: true } : undefined
            );
            if (preparedData.data !== undefined) {
                persistItem = isFloating
                    ? new FloatingItem<TxItemData<Items[number]>>(item.pool, item.rn, preparedData.data as TxItemData<Items[number]>)
                    : new UncommittedItem<TxItemData<Items[number]>>(item.pool, item.rn, preparedData.data as TxItemData<Items[number]>);
                if (preparedData.meta) {
                    setEncryptionMeta(persistItem, preparedData.meta);
                }
            }

            prepared.push({ item: persistItem, layer, isFloating, pool: item.pool });
            updatedMetas.push(preparedData.meta);

            if (wal && preparedData.data !== undefined) {
                const existing = walOpsByPool.get(item.pool) ?? { wal, ops: [] };
                existing.ops.push({
                    type: isFloating ? "insert" : "update",
                    rn: item.rn!.value(),
                    data: preparedData.data as JSONable,
                    destructive: persistOptions.destructive,
                });
                walOpsByPool.set(item.pool, existing);
            }
            if (wal && captureDepotOps && depotOps && depotOps.length > 0) {
                const existing = walDepotOpsByPool.get(item.pool) ?? { wal, ops: [] };
                existing.ops.push(...depotOps);
                walDepotOpsByPool.set(item.pool, existing);
            }
        }

        const walHandles = new Map<TxItem["pool"], Awaited<ReturnType<NonNullable<TxItem["pool"]["wal"]>["stage"]>>>();
        try {
            const poolsWithWal = new Set<TxItem["pool"]>([
                ...walOpsByPool.keys(),
                ...walDepotOpsByPool.keys(),
            ]);
            for (const pool of poolsWithWal) {
                const wal = pool.wal;
                if (!wal) continue;
                const ops = walOpsByPool.get(pool)?.ops ?? [];
                const depotOps = walDepotOpsByPool.get(pool)?.ops;
                walHandles.set(pool, await wal.stage(ops, depotOps));
            }
            for (const [pool, handle] of walHandles.entries()) {
                const wal = pool.wal;
                if (wal?.depotOpsEnabled) {
                    await wal.applyDepotOps(handle);
                }
            }
        } catch (error) {
            for (const [pool, handle] of walHandles.entries()) {
                const wal = pool.wal;
                if (wal) {
                    if (wal.depotOpsEnabled) {
                        try {
                            await wal.undoDepotOps(handle);
                        } catch { }
                    }
                    await wal.abort(handle);
                }
            }
            return new TxResult({
                success: false,
                items: this._revertAll(),
                error: (error as Error).message
            });
        }

        for (const preparedItem of prepared) {
            let r: DbChangeResult<TxItemData<Items[number]>>;
            if (preparedItem.isFloating) {
                r = await preparedItem.layer.insertItem(preparedItem.item as FloatingItem<TxItemData<Items[number]>>, persistOptions);
            } else {
                r = await preparedItem.layer.updateItem(preparedItem.item as UncommittedItem<TxItemData<Items[number]>>, persistOptions);
            }
            this._opResults.push(r);
            if (!r.success) {
                for (const [pool, handle] of walHandles.entries()) {
                    const wal = pool.wal;
                    if (wal) {
                        if (wal.depotOpsEnabled) {
                            try {
                                await wal.undoDepotOps(handle);
                            } catch { }
                        }
                        await wal.abort(handle);
                    }
                }
                const itemResults = this._revertAll();
                return new TxResult({
                    success: false,
                    items: itemResults,
                    error: r.error?.message
                });
            }
        }

        for (const [pool, handle] of walHandles.entries()) {
            const wal = pool.wal;
            if (wal) {
                await wal.markDone(handle);
            }
        }

        this._spent = true;

        return new TxResult<TxItemData<Items[number]>>({
            success: true,
            items: this._itemsToPersist.map(i => {
                const meta = updatedMetas.shift();
                const committed = new Item<TxItemData<Items[number]>>(i.pool, i.data as TxItemData<Items[number]>, i.rn);
                if (meta) {
                    setEncryptionMeta(committed, meta);
                }
                return {
                    reverted: false,
                    item: committed
                }
            })
        });
    }
}

/**
 * Build a transaction from FloatingItem and UncommittedItem values.
 * Next: call `.persist()` on the returned Tx.
 */
export function tx<Items extends readonly TxItem[]>(...items: Items) {
    return new Tx(items)
}
