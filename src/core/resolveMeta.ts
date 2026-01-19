import type { JSONable } from "../korm";
import type { RN } from "./rn";
import { isDepotFile } from "../depot/depotFile";
import { isUnsafeObjectKey, safeAssign } from "./safeObject";

/** Path segment key used for JSON path traversal. */
export type PathKey = string | number;

/** Reference path tracked during `resolvePaths`. */
export type ResolvedRef = {
    /** JSON path to the reference. */
    path: PathKey[];
};

/** Metadata used to track resolved RN references and raw data. */
export type ResolvedMeta = {
    /** RN for the resolved item, if any. */
    rn?: RN<JSONable>;
    /** Raw data snapshot with RN strings. */
    rawData: JSONable;
    /** Resolved data snapshot with RN objects. */
    resolvedData: JSONable;
    /** Reference paths tracked for write-back. */
    refs: ResolvedRef[];
};

const resolvedMetaMap = new WeakMap<object, ResolvedMeta>();
type RnLike = { __RN__: true; value: () => string };

/** Attach resolved metadata to a target object. */
export function setResolvedMeta(target: object, meta: ResolvedMeta): void {
    resolvedMetaMap.set(target, meta);
}

/** Read resolved metadata from a target object, if any. */
export function getResolvedMeta(target: object): ResolvedMeta | undefined {
    return resolvedMetaMap.get(target);
}

/** Remove resolved metadata from a target object. */
export function clearResolvedMeta(target: object): void {
    resolvedMetaMap.delete(target);
}

function isPlainObject(value: unknown): value is Record<string, JSONable> {
    if (!value || typeof value !== "object") return false;
    const proto = Object.getPrototypeOf(value);
    return proto === Object.prototype || proto === null;
}

function isRnObject(value: unknown): value is RnLike {
    return Boolean(
        value
        && typeof value === "object"
        && "__RN__" in (value as Record<string, unknown>)
        && typeof (value as RnLike).value === "function"
    );
}

/** Clone JSONable data while preserving RN string values. */
export function cloneJson<T extends JSONable>(value: T): T {
    if (value === null || typeof value !== "object") {
        return value;
    }
    if (isRnObject(value)) {
        return value.value() as T;
    }
    if (Array.isArray(value)) {
        return value.map((item) => cloneJson(item as JSONable)) as T;
    }
    if (!isPlainObject(value)) {
        return value;
    }
    const out: Record<string, JSONable> = {};
    for (const [key, val] of Object.entries(value as Record<string, JSONable>)) {
        safeAssign(out, key, cloneJson(val));
    }
    return out as T;
}

/** Deep equality check for JSONable values with RN and depot file handling. */
export function deepEqualJson(a: JSONable, b: JSONable): boolean {
    if (isDepotFile(a) || isDepotFile(b)) {
        const aValue = isDepotFile(a) ? a.rn.value() : a;
        const bValue = isDepotFile(b) ? b.rn.value() : b;
        return deepEqualJson(aValue as JSONable, bValue as JSONable);
    }
    if (isRnObject(a) || isRnObject(b)) {
        const aValue = isRnObject(a) ? a.value() : a;
        const bValue = isRnObject(b) ? b.value() : b;
        return deepEqualJson(aValue as JSONable, bValue as JSONable);
    }
    if (a === b) return true;
    if (a === null || b === null) return a === b;
    if (typeof a !== typeof b) return false;

    if (Array.isArray(a)) {
        if (!Array.isArray(b)) return false;
        if (a.length !== b.length) return false;
        for (let i = 0; i < a.length; i++) {
            if (!deepEqualJson(a[i] as JSONable, b[i] as JSONable)) return false;
        }
        return true;
    }

    if (typeof a === "object") {
        if (Array.isArray(b)) return false;
        const aObj = a as Record<string, JSONable>;
        const bObj = b as Record<string, JSONable>;
        const aKeys = Object.keys(aObj);
        const bKeys = Object.keys(bObj);
        if (aKeys.length !== bKeys.length) return false;
        for (const key of aKeys) {
            if (!(key in bObj)) return false;
            if (!deepEqualJson(aObj[key] as JSONable, bObj[key] as JSONable)) return false;
        }
        return true;
    }

    return false;
}

/** Read a nested value by path from a JSON-like object. */
export function getValueAtPath(obj: unknown, path: PathKey[]): unknown {
    let cur: unknown = obj;
    for (const key of path) {
        if (cur === null || cur === undefined) return undefined;
        if (typeof key === "number") {
            if (!Array.isArray(cur)) return undefined;
            cur = cur[key];
            continue;
        }
        if (isUnsafeObjectKey(key)) return undefined;
        if (typeof cur !== "object") return undefined;
        cur = (cur as Record<string, unknown>)[key];
    }
    return cur;
}

/** Set a nested value by path on a JSON-like object (no-op if path is invalid). */
export function setValueAtPath(obj: unknown, path: PathKey[], value: unknown): void {
    if (path.length === 0) return;
    let cur: unknown = obj;
    for (let i = 0; i < path.length; i++) {
        const key = path[i]!;
        const isLast = i === path.length - 1;
        if (typeof key === "number") {
            if (!Array.isArray(cur)) return;
            if (isLast) {
                cur[key] = value;
                return;
            }
            cur = cur[key];
            continue;
        }
        if (isUnsafeObjectKey(key)) return;
        if (typeof cur !== "object" || cur === null) return;
        const record = cur as Record<string, unknown>;
        if (isLast) {
            safeAssign(record, key, value);
            return;
        }
        cur = record[key];
    }
}

/** Materialize raw data by replacing resolved references with their RN values. */
export function materializeRaw(meta: ResolvedMeta): JSONable {
    const raw = cloneJson(meta.resolvedData as JSONable);
    for (const ref of meta.refs) {
        const rawValue = getValueAtPath(meta.rawData, ref.path);
        if (rawValue !== undefined) {
            const nextValue = isRnObject(rawValue) ? rawValue.value() : rawValue;
            setValueAtPath(raw, ref.path, nextValue);
        }
    }
    return raw;
}
