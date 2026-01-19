import type { JSONable } from "../korm";
import type { LayerPool } from "../sources/layerPool";
import type { ResolvedMeta } from "../core/resolveMeta";
import { getValueAtPath, setValueAtPath } from "../core/resolveMeta";
import type { Depot } from "./depot";
import {
    DepotFileBase,
    FloatingDepotFile,
    UncommittedDepotFile,
    isDepotFile,
} from "./depotFile";
import { safeAssign } from "../core/safeObject";

type DepotCache = Map<string, Depot>;

function isPlainObject(value: unknown): value is Record<string, JSONable> {
    if (!value || typeof value !== "object") return false;
    const proto = Object.getPrototypeOf(value);
    return proto === Object.prototype || proto === null;
}

function getDepotForFile(pool: LayerPool, file: DepotFileBase, cache: DepotCache): Depot {
    const rn = file.rn;
    if (!rn.isDepot()) {
        throw new Error(`RN "${rn.value()}" does not refer to a depot.`);
    }
    if (rn.pointsTo() !== "depotFile") {
        throw new Error(`Expected depot file RN but got "${rn.value()}".`);
    }
    const key = rn.depotIdent() ?? rn.value();
    const cached = cache.get(key);
    if (cached) return cached;
    const depot = pool.findDepotForRn(rn);
    cache.set(key, depot);
    return depot;
}

async function uploadDepotFile(pool: LayerPool, file: DepotFileBase, cache: DepotCache): Promise<void> {
    if (file.state === "committed") return;
    const depot = getDepotForFile(pool, file as DepotFileBase, cache);
    await depot.createFile(file);
}

export async function persistDepotFilesInData<T extends JSONable>(
    data: T,
    pool: LayerPool,
    options?: { depotOps?: DepotFileBase[]; skipUpload?: boolean }
): Promise<T> {
    const cache: DepotCache = new Map();
    const uploads = new Map<DepotFileBase, Promise<void>>();
    const skipUpload = options?.skipUpload ?? false;

    const queueUpload = (file: DepotFileBase) => {
        if (file.state === "committed" || uploads.has(file)) return;
        if (options?.depotOps) options.depotOps.push(file);
        if (skipUpload) {
            uploads.set(file, Promise.resolve());
            return;
        }
        if (file instanceof FloatingDepotFile) {
            uploads.set(file, file.create(pool).then(() => undefined));
            return;
        }
        if (file instanceof UncommittedDepotFile) {
            uploads.set(file, file.commit(pool).then(() => undefined));
            return;
        }
        uploads.set(file, uploadDepotFile(pool, file, cache));
    };

    const walk = (value: JSONable): JSONable => {
        if (isDepotFile(value)) {
            queueUpload(value);
            return value.rn.value();
        }
        if (Array.isArray(value)) {
            return value.map((item) => walk(item as JSONable)) as JSONable;
        }
        if (isPlainObject(value)) {
            const out: Record<string, JSONable> = {};
            for (const [key, val] of Object.entries(value)) {
                safeAssign(out, key, walk(val as JSONable));
            }
            return out as JSONable;
        }
        return value;
    };

    const next = walk(data as JSONable) as T;
    if (uploads.size > 0) {
        await Promise.all(uploads.values());
    }
    return next;
}

export async function uploadDepotFilesFromRefs(
    meta: ResolvedMeta,
    pool: LayerPool,
    options?: { depotOps?: DepotFileBase[]; skipUpload?: boolean }
): Promise<void> {
    if (meta.refs.length === 0) return;
    const cache: DepotCache = new Map();
    const uploads = new Map<DepotFileBase, Promise<void>>();
    const skipUpload = options?.skipUpload ?? false;

    const queueUpload = (file: DepotFileBase) => {
        if (file.state === "committed" || uploads.has(file)) return;
        if (options?.depotOps) options.depotOps.push(file);
        if (skipUpload) {
            uploads.set(file, Promise.resolve());
            return;
        }
        if (file instanceof FloatingDepotFile) {
            uploads.set(file, file.create(pool).then(() => undefined));
            return;
        }
        if (file instanceof UncommittedDepotFile) {
            uploads.set(file, file.commit(pool).then(() => undefined));
            return;
        }
        uploads.set(file, uploadDepotFile(pool, file, cache));
    };

    for (const ref of meta.refs) {
        const value = getValueAtPath(meta.resolvedData, ref.path);
        if (isDepotFile(value)) {
            queueUpload(value);
            setValueAtPath(meta.rawData, ref.path, value.rn.value());
            continue;
        }
        if (Array.isArray(value)) {
            for (const entry of value) {
                if (isDepotFile(entry)) {
                    queueUpload(entry);
                }
            }
        }
    }

    if (uploads.size > 0) {
        await Promise.all(uploads.values());
    }
}
