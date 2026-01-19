import type { JSONable } from "../korm";
import { Encrypt } from "../security/encryption";
import type { PathKey } from "./resolveMeta";
import { cloneJson, deepEqualJson, getValueAtPath, setValueAtPath } from "./resolveMeta";
import { safeAssign } from "./safeObject";

/** Serialized encrypted payload stored in data. */
export type EncryptedPayload = {
    /** Marker to recognize encrypted payloads. */
    __ENCRYPTED__: true;
    /** Encryption kind. */
    type: "password" | "symmetric";
    /** Encrypted payload value. */
    value: string;
    /** Original data type (optional). */
    dataType?: string;
    /** Initialization vector for symmetric encryption. */
    iv?: string;
    /** Auth tag for symmetric encryption. */
    authTag?: string;
};

/** Tracked encryption entry for a JSON path. */
export type EncryptionEntry = {
    /** Path to the encrypted field. */
    path: PathKey[];
    /** Encrypted payload stored at that path. */
    payload: EncryptedPayload;
    /** Optional plaintext snapshot for change detection. */
    plainValue?: JSONable;
};

/** Metadata used to retain encrypted payloads alongside plaintext data. */
export type EncryptionMeta = {
    /** Tracked encryption entries. */
    entries: EncryptionEntry[];
};

const encryptionMetaMap = new WeakMap<object, EncryptionMeta>();

/** Attach encryption metadata to a target object. */
export function setEncryptionMeta(target: object, meta: EncryptionMeta): void {
    encryptionMetaMap.set(target, meta);
}

/** Read encryption metadata from a target object, if any. */
export function getEncryptionMeta(target: object): EncryptionMeta | undefined {
    return encryptionMetaMap.get(target);
}

/** Remove encryption metadata from a target object. */
export function clearEncryptionMeta(target: object): void {
    encryptionMetaMap.delete(target);
}

/** Deep-clone encryption metadata for safe mutation. */
export function cloneEncryptionMeta(meta: EncryptionMeta): EncryptionMeta {
    return {
        entries: meta.entries.map((entry) => ({
            path: [...entry.path],
            payload: { ...entry.payload },
            plainValue: entry.plainValue === undefined
                ? undefined
                : cloneJson(entry.plainValue as JSONable),
        })),
    };
}

/** Type guard for encrypted payload objects. */
export function isEncryptedPayload(value: unknown): value is EncryptedPayload {
    return Boolean(
        value &&
        typeof value === "object" &&
        "__ENCRYPTED__" in (value as Record<string, unknown>) &&
        (value as EncryptedPayload).__ENCRYPTED__ === true &&
        typeof (value as EncryptedPayload).type === "string" &&
        typeof (value as EncryptedPayload).value === "string"
    );
}

function isEncryptValue(value: unknown): value is Encrypt<JSONable> & { __ENCRYPT__: true } {
    return Boolean(
        value &&
        typeof value === "object" &&
        (value as { __ENCRYPT__?: boolean }).__ENCRYPT__ === true &&
        typeof (value as { safeValue?: () => unknown }).safeValue === "function"
    );
}

function toEncryptedPayload(encrypted: {
    value: string;
    type: "password" | "symmetric";
    dataType?: string;
    iv?: string;
    authTag?: string;
}): EncryptedPayload {
    const payload: EncryptedPayload = {
        __ENCRYPTED__: true,
        type: encrypted.type,
        value: encrypted.value,
    };
    if (encrypted.dataType) payload.dataType = encrypted.dataType;
    if (encrypted.type === "symmetric") {
        payload.iv = encrypted.iv;
        payload.authTag = encrypted.authTag;
    }
    return payload;
}

async function encryptPlainValue(value: JSONable, type: "password" | "symmetric"): Promise<EncryptedPayload> {
    const encrypt = new Encrypt(value, type);
    await encrypt.lock();
    const encrypted = encrypt.safeValue() as {
        value: string;
        type: "password" | "symmetric";
        dataType?: string;
        iv?: string;
        authTag?: string;
    };
    return toEncryptedPayload(encrypted);
}

async function toPayloadFromEncrypt(value: Encrypt<JSONable>): Promise<EncryptedPayload> {
    await value.lock();
    const encrypted = value.safeValue() as {
        value: string;
        type: "password" | "symmetric";
        dataType?: string;
        iv?: string;
        authTag?: string;
    };
    return toEncryptedPayload(encrypted);
}

type ApplyResult = {
    nextValue: unknown;
    payload?: EncryptedPayload;
    plainValue?: JSONable;
};

async function applyEntry(value: unknown, entry: EncryptionEntry): Promise<ApplyResult> {
    if (value === null || value === undefined) {
        return { nextValue: value };
    }

    if (isEncryptedPayload(value)) {
        return { nextValue: value, payload: value };
    }

    if (isEncryptValue(value)) {
        const payload = await toPayloadFromEncrypt(value);
        return { nextValue: payload, payload };
    }

    if (entry.payload.type === "password") {
        if (entry.plainValue !== undefined && deepEqualJson(value as JSONable, entry.plainValue)) {
            return { nextValue: entry.payload, payload: entry.payload, plainValue: entry.plainValue };
        }
        if (typeof value === "string" && value === entry.payload.value) {
            return { nextValue: entry.payload, payload: entry.payload, plainValue: value };
        }
        const payload = await encryptPlainValue(value as JSONable, "password");
        return { nextValue: payload, payload, plainValue: cloneJson(value as JSONable) };
    }

    if (entry.plainValue !== undefined && deepEqualJson(value as JSONable, entry.plainValue)) {
        return { nextValue: entry.payload, payload: entry.payload, plainValue: entry.plainValue };
    }

    const payload = await encryptPlainValue(value as JSONable, "symmetric");
    return { nextValue: payload, payload, plainValue: cloneJson(value as JSONable) };
}

/**
 * Apply encryption metadata to data, replacing plaintext values with payloads as needed.
 */
export async function applyEncryptionMeta<T extends JSONable>(
    data: T,
    meta?: EncryptionMeta
): Promise<{ data: T; meta?: EncryptionMeta }> {
    if (!meta || meta.entries.length === 0) {
        return { data, meta };
    }

    const nextData = cloneJson(data) as T;
    const nextMeta = cloneEncryptionMeta(meta);

    for (const entry of nextMeta.entries) {
        const currentValue = getValueAtPath(data as unknown, entry.path);
        if (currentValue === undefined) {
            continue;
        }
        const applied = await applyEntry(currentValue, entry);
        setValueAtPath(nextData as unknown, entry.path, applied.nextValue);
        if (applied.payload) {
            entry.payload = applied.payload;
        }
        if (applied.plainValue !== undefined) {
            entry.plainValue = applied.plainValue;
        }
    }

    return { data: nextData, meta: nextMeta };
}

/**
 * Create a snapshot where tracked paths are replaced by encrypted payloads.
 */
export function materializeEncryptedSnapshot<T extends JSONable>(
    data: T,
    meta?: EncryptionMeta
): T {
    if (!meta || meta.entries.length === 0) {
        return data;
    }
    const nextData = cloneJson(data) as T;
    for (const entry of meta.entries) {
        setValueAtPath(nextData as unknown, entry.path, entry.payload);
    }
    return nextData;
}

function isPlainObject(value: unknown): value is Record<string, JSONable> {
    if (!value || typeof value !== "object") return false;
    const proto = Object.getPrototypeOf(value);
    return proto === Object.prototype || proto === null;
}

/**
 * Normalize Encrypt wrappers or payloads into serialized payloads.
 */
export async function normalizeEncryptedValues<T extends JSONable>(value: T): Promise<T> {
    if (value === null || value === undefined) return value;
    if (isEncryptedPayload(value)) return value;
    if (isEncryptValue(value)) {
        await value.lock();
        const encrypted = value.safeValue() as {
            value: string;
            type: "password" | "symmetric";
            dataType?: string;
            iv?: string;
            authTag?: string;
        };
        return toEncryptedPayload(encrypted) as unknown as T;
    }
    if (Array.isArray(value)) {
        const next = await Promise.all(value.map((entry) => normalizeEncryptedValues(entry as JSONable)));
        return next as T;
    }
    if (!isPlainObject(value)) {
        return value;
    }
    const out: Record<string, JSONable> = {};
    for (const [key, entry] of Object.entries(value)) {
        safeAssign(out, key, await normalizeEncryptedValues(entry as JSONable));
    }
    return out as T;
}
