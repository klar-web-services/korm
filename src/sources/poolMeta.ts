import crypto from "node:crypto";
import type { JSONable } from "../korm";
import type { Depot } from "../depot/depot";
import type { SourceLayer } from "./sourceLayer";
import { Encrypt, decrypt } from "../security/encryption";
import type { WalMode } from "../wal/wal";
import type { LockMode } from "./lockStore";
import type { BackupRetention, IntervalSpec } from "./backMan";
import { safeAssign } from "../core/safeObject";

const POOL_META_TABLE = "__korm_pool__";
const POOL_META_ROW_ID = "pool";
const POOL_META_VERSION = 1;

const DEFAULT_LOCK_TTL_MS = 60_000;
const DEFAULT_LOCK_RETRY_MS = 50;
const MIN_LOCK_REFRESH_MS = 1_000;

/**
 * Pool metadata configuration for `korm.pool().withMeta(...)`.
 * Enables pool discovery and config mismatch detection.
 */
export type PoolMetaMode = {
  /** Layer identifier used to persist pool metadata. */
  layerIdent: string;
};

/** Backup settings stored in pool metadata. */
export type PoolMetaBackups = {
  depotIdent: string;
  intervals: { layerIdent: string; interval: IntervalSpec }[];
  retention: { layerIdent: string; retention: BackupRetention }[];
};

/** WAL settings stored in pool metadata. */
export type PoolMetaWal = {
  depotIdent: string;
  walNamespace: string;
  retention: "keep" | "delete";
  depotOps: "off" | "record";
};

/** Shared lock settings stored in pool metadata. */
export type PoolMetaLocks = {
  layerIdent: string;
  ttlMs: number;
  retryMs: number;
  refreshMs: number;
};

/** Encrypted payload stored inside pool metadata. */
export type PoolEncryptedPayload = {
  __ENCRYPTED__: true;
  type: "symmetric";
  value: string;
  iv: string;
  authTag: string;
  dataType?: string;
};

/** Serialized layer configuration stored in pool metadata. */
export type PoolMetaLayer = {
  ident: string;
  type: "sqlite" | "pg" | "mysql";
  fingerprint: string;
  config:
    | { mode: "path"; path: string }
    | { mode: "url"; encrypted: PoolEncryptedPayload }
    | { mode: "options"; encrypted: PoolEncryptedPayload };
};

/** Serialized depot configuration stored in pool metadata. */
export type PoolMetaDepot = {
  ident: string;
  type: "local" | "s3";
  fingerprint: string;
  config:
    | { mode: "local"; root: string }
    | { mode: "s3"; encrypted: PoolEncryptedPayload };
};

/** Full pool metadata payload stored in __korm_pool__. */
export type PoolMetaConfig = {
  version: number;
  poolId: string;
  metaLayerIdent?: string;
  layers: PoolMetaLayer[];
  depots: PoolMetaDepot[];
  wal?: PoolMetaWal;
  locks?: PoolMetaLocks;
  backups?: PoolMetaBackups;
};

/** Pool metadata config with decrypted values, used by `korm.discover(...)`. */
export type PoolMetaHydratedConfig = {
  metaLayerIdent?: string;
  layers: {
    ident: string;
    type: "sqlite" | "pg" | "mysql";
    mode: "path" | "url" | "options";
    value: unknown;
  }[];
  depots: {
    ident: string;
    type: "local" | "s3";
    mode: "local" | "s3";
    value: unknown;
  }[];
  wal?: PoolMetaWal;
  locks?: PoolMetaLocks;
  backups?: PoolMetaBackups;
};

type LayerConfigInput =
  | { type: "sqlite"; path: string }
  | { type: "pg"; mode: "url" | "options"; value: unknown }
  | { type: "mysql"; mode: "url" | "options"; value: unknown };

type DepotConfigInput =
  | { type: "local"; root: string }
  | { type: "s3"; options: unknown };

type LayerConfigProvider = SourceLayer & {
  getPoolConfig?: () => LayerConfigInput;
};
type DepotConfigProvider = Depot & { getPoolConfig?: () => DepotConfigInput };

type StoredMeta = {
  config: PoolMetaConfig;
  createdAt: number;
  updatedAt: number;
};

function stableStringify(value: JSONable): string {
  if (value === null || typeof value !== "object") {
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) {
    return `[${value.map((entry) => stableStringify(entry)).join(",")}]`;
  }
  const obj = value as Record<string, JSONable>;
  const keys = Object.keys(obj).sort();
  const entries = keys.map(
    (key) => `${JSON.stringify(key)}:${stableStringify(obj[key]!)}`,
  );
  return `{${entries.join(",")}}`;
}

function hashValue(value: JSONable): string {
  const serialized = stableStringify(value);
  return crypto.createHash("sha256").update(serialized).digest("hex");
}

const META_KEY = "__korm_meta__";
const TYPED_ARRAY_NAMES = new Set<string>([
  "Int8Array",
  "Uint8Array",
  "Uint8ClampedArray",
  "Int16Array",
  "Uint16Array",
  "Int32Array",
  "Uint32Array",
  "Float32Array",
  "Float64Array",
]);
if (typeof BigInt64Array !== "undefined") {
  TYPED_ARRAY_NAMES.add("BigInt64Array");
}
if (typeof BigUint64Array !== "undefined") {
  TYPED_ARRAY_NAMES.add("BigUint64Array");
}

const TYPED_ARRAY_CTORS: Record<
  string,
  { new (buffer: ArrayBuffer): ArrayBufferView }
> = {
  Int8Array,
  Uint8Array,
  Uint8ClampedArray,
  Int16Array,
  Uint16Array,
  Int32Array,
  Uint32Array,
  Float32Array,
  Float64Array,
};
if (typeof BigInt64Array !== "undefined") {
  TYPED_ARRAY_CTORS.BigInt64Array = BigInt64Array;
}
if (typeof BigUint64Array !== "undefined") {
  TYPED_ARRAY_CTORS.BigUint64Array = BigUint64Array;
}

type MetaEnvelope = { [META_KEY]: { type: string; value: JSONable } };

function wrapMeta(type: string, value: JSONable): MetaEnvelope {
  return { [META_KEY]: { type, value } };
}

function isMetaEnvelope(value: unknown): value is MetaEnvelope {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  if (!(META_KEY in (value as Record<string, unknown>))) return false;
  const keys = Object.keys(value as Record<string, unknown>);
  if (keys.length !== 1 || keys[0] !== META_KEY) return false;
  const inner = (value as Record<string, unknown>)[META_KEY];
  return Boolean(
    inner &&
    typeof inner === "object" &&
    "type" in (inner as Record<string, unknown>),
  );
}

function encodeBinary(value: ArrayBufferView | ArrayBuffer): string {
  if (value instanceof ArrayBuffer) {
    return Buffer.from(value).toString("base64");
  }
  const buffer = Buffer.from(value.buffer, value.byteOffset, value.byteLength);
  return buffer.toString("base64");
}

function encodeMetaValue(value: unknown, path: string[] = []): JSONable {
  const label = path.join(".") || "root";
  if (value === null) return null;
  if (value === undefined) {
    throw new Error(`Pool metadata cannot serialize undefined at ${label}.`);
  }
  if (typeof value === "string" || typeof value === "boolean") {
    return value;
  }
  if (typeof value === "number") {
    if (Number.isNaN(value)) {
      return wrapMeta("NaN", "NaN");
    }
    if (!Number.isFinite(value)) {
      return wrapMeta(value > 0 ? "Infinity" : "-Infinity", String(value));
    }
    return value;
  }
  if (typeof value === "bigint") {
    return wrapMeta("BigInt", value.toString());
  }
  if (typeof value === "function") {
    throw new Error(`Pool metadata cannot serialize function at ${label}.`);
  }
  if (typeof value === "symbol") {
    throw new Error(`Pool metadata cannot serialize symbol at ${label}.`);
  }

  if (typeof Buffer !== "undefined" && Buffer.isBuffer(value)) {
    return wrapMeta("Buffer", encodeBinary(value));
  }
  if (value instanceof ArrayBuffer) {
    return wrapMeta("ArrayBuffer", encodeBinary(value));
  }
  if (typeof URL !== "undefined" && value instanceof URL) {
    return wrapMeta("URL", value.toString());
  }
  if (value instanceof Date) {
    return wrapMeta("Date", value.toISOString());
  }
  if (value instanceof RegExp) {
    return wrapMeta("RegExp", {
      source: value.source,
      flags: value.flags,
    } as JSONable);
  }
  if (value instanceof Map) {
    const entries: JSONable[] = [];
    let index = 0;
    for (const [key, entry] of value.entries()) {
      const encodedKey = encodeMetaValue(key, [...path, `map[${index}].key`]);
      const encodedValue = encodeMetaValue(entry, [
        ...path,
        `map[${index}].value`,
      ]);
      entries.push([encodedKey, encodedValue] as JSONable);
      index += 1;
    }
    return wrapMeta("Map", entries);
  }
  if (value instanceof Set) {
    const entries: JSONable[] = [];
    let index = 0;
    for (const entry of value.values()) {
      entries.push(encodeMetaValue(entry, [...path, `set[${index}]`]));
      index += 1;
    }
    return wrapMeta("Set", entries);
  }
  if (ArrayBuffer.isView(value)) {
    if (value instanceof DataView) {
      return wrapMeta("DataView", encodeBinary(value));
    }
    const typeName = value.constructor?.name;
    if (!typeName || !TYPED_ARRAY_NAMES.has(typeName)) {
      throw new Error(
        `Pool metadata cannot serialize typed array "${typeName ?? "unknown"}" at ${label}.`,
      );
    }
    return wrapMeta(typeName, encodeBinary(value));
  }
  if (Array.isArray(value)) {
    return value.map((entry, index) =>
      encodeMetaValue(entry, [...path, String(index)]),
    );
  }
  if (typeof value === "object") {
    const proto = Object.getPrototypeOf(value);
    if (proto && proto !== Object.prototype) {
      const name =
        (value as { constructor?: { name?: string } }).constructor?.name ??
        "Object";
      throw new Error(`Pool metadata cannot serialize ${name} at ${label}.`);
    }
    const out: Record<string, JSONable> = {};
    for (const [key, entry] of Object.entries(
      value as Record<string, unknown>,
    )) {
      if (entry === undefined) continue;
      safeAssign(out, key, encodeMetaValue(entry, [...path, key]));
    }
    return out;
  }
  throw new Error(`Pool metadata cannot serialize value at ${label}.`);
}

function decodeBinary(value: JSONable): ArrayBuffer {
  if (typeof value !== "string") {
    throw new Error("Pool metadata binary payload must be a base64 string.");
  }
  const buffer = Buffer.from(value, "base64");
  return buffer.buffer.slice(
    buffer.byteOffset,
    buffer.byteOffset + buffer.byteLength,
  );
}

function decodeMetaValue(value: JSONable): unknown {
  if (value === null) return null;
  if (typeof value !== "object") return value;
  if (Array.isArray(value)) {
    return value.map((entry) => decodeMetaValue(entry));
  }
  if (isMetaEnvelope(value)) {
    const payload = (value as MetaEnvelope)[META_KEY];
    const type = payload.type;
    if (type === "Buffer") {
      return Buffer.from(payload.value as string, "base64");
    }
    if (type === "ArrayBuffer") {
      return decodeBinary(payload.value);
    }
    if (type === "DataView") {
      return new DataView(decodeBinary(payload.value));
    }
    if (type === "Date") {
      return new Date(payload.value as string);
    }
    if (type === "URL") {
      return new URL(payload.value as string);
    }
    if (type === "RegExp") {
      const regex = payload.value as { source: string; flags: string };
      return new RegExp(regex.source, regex.flags);
    }
    if (type === "BigInt") {
      return BigInt(payload.value as string);
    }
    if (type === "NaN") {
      return Number.NaN;
    }
    if (type === "Infinity") {
      return Number.POSITIVE_INFINITY;
    }
    if (type === "-Infinity") {
      return Number.NEGATIVE_INFINITY;
    }
    if (type === "Map") {
      const entries = payload.value as JSONable[];
      const map = new Map<unknown, unknown>();
      for (const entry of entries) {
        if (!Array.isArray(entry) || entry.length !== 2) {
          throw new Error(
            "Pool metadata map entry must be a [key, value] tuple.",
          );
        }
        const key = decodeMetaValue(entry[0] as JSONable);
        const val = decodeMetaValue(entry[1] as JSONable);
        map.set(key, val);
      }
      return map;
    }
    if (type === "Set") {
      const entries = payload.value as JSONable[];
      const set = new Set<unknown>();
      for (const entry of entries) {
        set.add(decodeMetaValue(entry));
      }
      return set;
    }
    if (TYPED_ARRAY_NAMES.has(type)) {
      const buffer = decodeBinary(payload.value);
      const ctor = TYPED_ARRAY_CTORS[type];
      if (!ctor) {
        throw new Error(`Pool metadata cannot hydrate typed array "${type}".`);
      }
      return new ctor(buffer);
    }
  }
  const out: Record<string, unknown> = {};
  for (const [key, entry] of Object.entries(
    value as Record<string, JSONable>,
  )) {
    safeAssign(out, key, decodeMetaValue(entry));
  }
  return out;
}

async function encryptPayload(value: JSONable): Promise<PoolEncryptedPayload> {
  const encrypt = new Encrypt(value, "symmetric");
  const encrypted = await encrypt.lock();
  const json = encrypted.toJSON() as Record<string, unknown>;
  if (!json || json.__ENCRYPTED__ !== true) {
    throw new Error("Pool metadata encryption produced an unexpected payload.");
  }
  return json as PoolEncryptedPayload;
}

async function decryptPayload<T extends JSONable>(
  payload: PoolEncryptedPayload,
): Promise<T> {
  return await decrypt(
    payload as unknown as {
      value: string;
      type: "symmetric";
      iv: string;
      authTag: string;
    },
  );
}

function computePoolId(
  layers: Map<string, SourceLayer>,
  depots: Map<string, Depot>,
): string {
  const layerKeys = Array.from(layers.keys()).sort();
  const depotKeys = Array.from(depots.keys()).sort();
  const seed = JSON.stringify({ layers: layerKeys, depots: depotKeys });
  return crypto.createHash("sha256").update(seed).digest("hex").slice(0, 16);
}

function normalizeWalMode(mode: WalMode, poolId: string): PoolMetaWal {
  return {
    depotIdent: mode.depotIdent,
    walNamespace: mode.walNamespace ?? poolId,
    retention: mode.retention ?? "keep",
    depotOps: mode.depotOps ?? "off",
  };
}

function normalizeLockMode(mode: LockMode): PoolMetaLocks {
  const ttlMs = mode.ttlMs ?? DEFAULT_LOCK_TTL_MS;
  const retryMs = mode.retryMs ?? DEFAULT_LOCK_RETRY_MS;
  const refreshMs =
    mode.refreshMs ?? Math.max(MIN_LOCK_REFRESH_MS, Math.floor(ttlMs / 2));
  return {
    layerIdent: mode.layerIdent,
    ttlMs,
    retryMs,
    refreshMs,
  };
}

function normalizeBackups(backups?: PoolMetaBackups): string {
  if (!backups) return "";
  const intervals = [...backups.intervals].sort((a, b) => {
    const layerCmp = a.layerIdent.localeCompare(b.layerIdent);
    if (layerCmp !== 0) return layerCmp;
    return stableStringify(a.interval as JSONable).localeCompare(
      stableStringify(b.interval as JSONable),
    );
  });
  const retention = [...backups.retention].sort((a, b) =>
    a.layerIdent.localeCompare(b.layerIdent),
  );
  const normalized = {
    depotIdent: backups.depotIdent,
    intervals,
    retention,
  };
  return stableStringify(normalized as unknown as JSONable);
}

function getLayerConfig(layer: SourceLayer): LayerConfigInput {
  const provider = layer as LayerConfigProvider;
  if (typeof provider.getPoolConfig !== "function") {
    throw new Error("Pool metadata requires layers created via korm.layers.*");
  }
  const config = provider.getPoolConfig();
  if (!config || typeof config !== "object") {
    throw new Error("Pool metadata received an invalid layer config.");
  }
  return config;
}

function getDepotConfig(depot: Depot): DepotConfigInput {
  const provider = depot as DepotConfigProvider;
  if (typeof provider.getPoolConfig !== "function") {
    throw new Error("Pool metadata requires depots created via korm.depots.*");
  }
  const config = provider.getPoolConfig();
  if (!config || typeof config !== "object") {
    throw new Error("Pool metadata received an invalid depot config.");
  }
  return config;
}

async function buildLayerMeta(
  ident: string,
  layer: SourceLayer,
): Promise<PoolMetaLayer> {
  const config = getLayerConfig(layer);
  if (config.type === "sqlite") {
    if (!config.path || typeof config.path !== "string") {
      throw new Error(
        `Pool metadata requires a sqlite path for layer "${ident}".`,
      );
    }
    return {
      ident,
      type: "sqlite",
      fingerprint: hashValue(config.path),
      config: { mode: "path", path: config.path },
    };
  }
  if (config.type === "pg" || config.type === "mysql") {
    const mode = config.mode;
    if (mode === "url") {
      if (typeof config.value !== "string" || !config.value) {
        throw new Error(
          `Pool metadata requires a connection string for layer "${ident}".`,
        );
      }
      const fingerprint = hashValue(config.value);
      const encrypted = await encryptPayload(config.value);
      return {
        ident,
        type: config.type,
        fingerprint,
        config: { mode, encrypted },
      };
    }
    if (mode === "options") {
      const encoded = encodeMetaValue(config.value, [
        "layers",
        ident,
        "connection",
      ]);
      const fingerprint = hashValue(encoded);
      const encrypted = await encryptPayload(encoded);
      return {
        ident,
        type: config.type,
        fingerprint,
        config: { mode, encrypted },
      };
    }
  }
  throw new Error(
    `Pool metadata received an unsupported layer config for "${ident}".`,
  );
}

async function buildDepotMeta(
  ident: string,
  depot: Depot,
): Promise<PoolMetaDepot> {
  const config = getDepotConfig(depot);
  if (config.type === "local") {
    if (!config.root || typeof config.root !== "string") {
      throw new Error(
        `Pool metadata requires a local depot root for "${ident}".`,
      );
    }
    return {
      ident,
      type: "local",
      fingerprint: hashValue(config.root),
      config: { mode: "local", root: config.root },
    };
  }
  if (config.type === "s3") {
    const encoded = encodeMetaValue(config.options, [
      "depots",
      ident,
      "options",
    ]);
    const fingerprint = hashValue(encoded);
    const encrypted = await encryptPayload(encoded);
    return {
      ident,
      type: "s3",
      fingerprint,
      config: { mode: "s3", encrypted },
    };
  }
  throw new Error(
    `Pool metadata received an unsupported depot config for "${ident}".`,
  );
}

export async function buildPoolMetaConfig(opts: {
  layers: Map<string, SourceLayer>;
  depots: Map<string, Depot>;
  walMode?: WalMode;
  lockMode?: LockMode;
  metaMode?: PoolMetaMode;
  backups?: PoolMetaBackups;
}): Promise<PoolMetaConfig> {
  const poolId = computePoolId(opts.layers, opts.depots);
  const layers = await Promise.all(
    Array.from(opts.layers.entries())
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([ident, layer]) => buildLayerMeta(ident, layer)),
  );
  const depots = await Promise.all(
    Array.from(opts.depots.entries())
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([ident, depot]) => buildDepotMeta(ident, depot)),
  );
  return {
    version: POOL_META_VERSION,
    poolId,
    metaLayerIdent: opts.metaMode?.layerIdent,
    layers,
    depots,
    wal: opts.walMode ? normalizeWalMode(opts.walMode, poolId) : undefined,
    locks: opts.lockMode ? normalizeLockMode(opts.lockMode) : undefined,
    backups: opts.backups,
  };
}

export function comparePoolMetaConfig(
  current: PoolMetaConfig,
  stored: PoolMetaConfig,
): string[] {
  const discrepancies: string[] = [];
  if (stored.version !== current.version) {
    discrepancies.push(
      `pool version mismatch: expected ${stored.version}, got ${current.version}`,
    );
  }
  if (stored.poolId !== current.poolId) {
    discrepancies.push(
      `pool id mismatch: expected ${stored.poolId}, got ${current.poolId}`,
    );
  }
  if ((stored.metaLayerIdent ?? null) !== (current.metaLayerIdent ?? null)) {
    discrepancies.push(
      `meta layer mismatch: expected ${stored.metaLayerIdent ?? "none"}, got ${current.metaLayerIdent ?? "none"}`,
    );
  }

  const storedLayers = new Map(
    stored.layers.map((layer) => [layer.ident, layer]),
  );
  const currentLayers = new Map(
    current.layers.map((layer) => [layer.ident, layer]),
  );
  const layerIdents = new Set([
    ...storedLayers.keys(),
    ...currentLayers.keys(),
  ]);
  for (const ident of layerIdents) {
    const storedLayer = storedLayers.get(ident);
    const currentLayer = currentLayers.get(ident);
    if (!storedLayer) {
      discrepancies.push(`layer missing in stored meta: ${ident}`);
      continue;
    }
    if (!currentLayer) {
      discrepancies.push(`layer missing in current config: ${ident}`);
      continue;
    }
    if (storedLayer.type !== currentLayer.type) {
      discrepancies.push(
        `layer type mismatch for ${ident}: expected ${storedLayer.type}, got ${currentLayer.type}`,
      );
      continue;
    }
    if (storedLayer.fingerprint !== currentLayer.fingerprint) {
      if (
        storedLayer.config.mode === "path" &&
        currentLayer.config.mode === "path"
      ) {
        discrepancies.push(
          `layer path mismatch for ${ident}: expected ${storedLayer.config.path}, got ${currentLayer.config.path}`,
        );
      } else {
        discrepancies.push(`layer config mismatch for ${ident}`);
      }
    }
  }

  const storedDepots = new Map(
    stored.depots.map((depot) => [depot.ident, depot]),
  );
  const currentDepots = new Map(
    current.depots.map((depot) => [depot.ident, depot]),
  );
  const depotIdents = new Set([
    ...storedDepots.keys(),
    ...currentDepots.keys(),
  ]);
  for (const ident of depotIdents) {
    const storedDepot = storedDepots.get(ident);
    const currentDepot = currentDepots.get(ident);
    if (!storedDepot) {
      discrepancies.push(`depot missing in stored meta: ${ident}`);
      continue;
    }
    if (!currentDepot) {
      discrepancies.push(`depot missing in current config: ${ident}`);
      continue;
    }
    if (storedDepot.type !== currentDepot.type) {
      discrepancies.push(
        `depot type mismatch for ${ident}: expected ${storedDepot.type}, got ${currentDepot.type}`,
      );
      continue;
    }
    if (storedDepot.fingerprint !== currentDepot.fingerprint) {
      if (
        storedDepot.config.mode === "local" &&
        currentDepot.config.mode === "local"
      ) {
        discrepancies.push(
          `depot root mismatch for ${ident}: expected ${storedDepot.config.root}, got ${currentDepot.config.root}`,
        );
      } else {
        discrepancies.push(`depot config mismatch for ${ident}`);
      }
    }
  }

  const storedWal = stored.wal;
  const currentWal = current.wal;
  if (!storedWal && currentWal) {
    discrepancies.push("wal config missing in stored meta");
  } else if (storedWal && !currentWal) {
    discrepancies.push("wal config missing in current config");
  } else if (storedWal && currentWal) {
    if (storedWal.depotIdent !== currentWal.depotIdent) {
      discrepancies.push(
        `wal depot mismatch: expected ${storedWal.depotIdent}, got ${currentWal.depotIdent}`,
      );
    }
    if (storedWal.walNamespace !== currentWal.walNamespace) {
      discrepancies.push(
        `wal namespace mismatch: expected ${storedWal.walNamespace}, got ${currentWal.walNamespace}`,
      );
    }
    if (storedWal.retention !== currentWal.retention) {
      discrepancies.push(
        `wal retention mismatch: expected ${storedWal.retention}, got ${currentWal.retention}`,
      );
    }
    if (storedWal.depotOps !== currentWal.depotOps) {
      discrepancies.push(
        `wal depotOps mismatch: expected ${storedWal.depotOps}, got ${currentWal.depotOps}`,
      );
    }
  }

  const storedLocks = stored.locks;
  const currentLocks = current.locks;
  if (!storedLocks && currentLocks) {
    discrepancies.push("lock config missing in stored meta");
  } else if (storedLocks && !currentLocks) {
    discrepancies.push("lock config missing in current config");
  } else if (storedLocks && currentLocks) {
    if (storedLocks.layerIdent !== currentLocks.layerIdent) {
      discrepancies.push(
        `lock layer mismatch: expected ${storedLocks.layerIdent}, got ${currentLocks.layerIdent}`,
      );
    }
    if (storedLocks.ttlMs !== currentLocks.ttlMs) {
      discrepancies.push(
        `lock ttl mismatch: expected ${storedLocks.ttlMs}, got ${currentLocks.ttlMs}`,
      );
    }
    if (storedLocks.retryMs !== currentLocks.retryMs) {
      discrepancies.push(
        `lock retry mismatch: expected ${storedLocks.retryMs}, got ${currentLocks.retryMs}`,
      );
    }
    if (storedLocks.refreshMs !== currentLocks.refreshMs) {
      discrepancies.push(
        `lock refresh mismatch: expected ${storedLocks.refreshMs}, got ${currentLocks.refreshMs}`,
      );
    }
  }

  const storedBackups = normalizeBackups(stored.backups);
  const currentBackups = normalizeBackups(current.backups);
  if (storedBackups !== currentBackups) {
    if (stored.backups || current.backups) {
      discrepancies.push("backups config mismatch");
    }
  }

  return discrepancies;
}

export async function hydratePoolMetaConfig(
  config: PoolMetaConfig,
): Promise<PoolMetaHydratedConfig> {
  const layers = await Promise.all(
    config.layers.map(async (layer) => {
      if (layer.config.mode === "path") {
        return {
          ident: layer.ident,
          type: layer.type,
          mode: "path" as const,
          value: layer.config.path,
        };
      }
      if (layer.config.mode === "url") {
        const value = await decryptPayload<string>(layer.config.encrypted);
        return {
          ident: layer.ident,
          type: layer.type,
          mode: "url" as const,
          value,
        };
      }
      const decrypted = await decryptPayload<JSONable>(layer.config.encrypted);
      const value = decodeMetaValue(decrypted);
      return {
        ident: layer.ident,
        type: layer.type,
        mode: "options" as const,
        value,
      };
    }),
  );

  const depots = await Promise.all(
    config.depots.map(async (depot) => {
      if (depot.config.mode === "local") {
        return {
          ident: depot.ident,
          type: depot.type,
          mode: "local" as const,
          value: depot.config.root,
        };
      }
      const decrypted = await decryptPayload<JSONable>(depot.config.encrypted);
      const value = decodeMetaValue(decrypted);
      return {
        ident: depot.ident,
        type: depot.type,
        mode: "s3" as const,
        value,
      };
    }),
  );

  return {
    metaLayerIdent: config.metaLayerIdent,
    layers,
    depots,
    wal: config.wal,
    locks: config.locks,
    backups: config.backups,
  };
}

class LayerPoolMetaStore {
  private _layer: SourceLayer;
  private _ensured?: Promise<void>;

  constructor(layer: SourceLayer) {
    this._layer = layer;
  }

  private _sqliteDb():
    | {
        run: (sql: string, params?: unknown[]) => void;
        prepare: <T>(sql: string) => { get: (arg: string) => T | undefined };
      }
    | undefined {
    const db = (this._layer as { _db?: unknown })._db;
    if (
      !db ||
      typeof (db as { run?: unknown }).run !== "function" ||
      typeof (db as { prepare?: unknown }).prepare !== "function"
    ) {
      return undefined;
    }
    return db as {
      run: (sql: string, params?: unknown[]) => void;
      prepare: <T>(sql: string) => { get: (arg: string) => T | undefined };
    };
  }

  private _pgDb():
    | { unsafe: (sql: string, params?: unknown[]) => Promise<unknown> }
    | undefined {
    const db = (this._layer as { _db?: unknown })._db;
    if (!db || typeof (db as { unsafe?: unknown }).unsafe !== "function") {
      return undefined;
    }
    return db as {
      unsafe: (sql: string, params?: unknown[]) => Promise<unknown>;
    };
  }

  private _mysqlPool():
    | { query: (sql: string, params?: unknown[]) => Promise<unknown> }
    | undefined {
    const pool = (this._layer as { _pool?: unknown })._pool;
    if (!pool || typeof (pool as { query?: unknown }).query !== "function") {
      return undefined;
    }
    return pool as {
      query: (sql: string, params?: unknown[]) => Promise<unknown>;
    };
  }

  private async _ensureTable(): Promise<void> {
    if (this._ensured) {
      return this._ensured;
    }
    this._ensured = this._createTable();
    return this._ensured;
  }

  private async _createTable(): Promise<void> {
    if (this._layer.type === "sqlite") {
      const db = this._sqliteDb();
      if (!db) {
        throw new Error(
          `Pool metadata requires a sqlite layer with a live connection for "${this._layer.identifier}".`,
        );
      }
      db.run(
        `CREATE TABLE IF NOT EXISTS "${POOL_META_TABLE}" (
                    "id" TEXT PRIMARY KEY,
                    "config" TEXT NOT NULL,
                    "created_at" INTEGER NOT NULL,
                    "updated_at" INTEGER NOT NULL
                )`,
      );
      return;
    }
    if (this._layer.type === "pg") {
      const db = this._pgDb();
      if (!db) {
        throw new Error(
          `Pool metadata requires a pg layer with a live connection for "${this._layer.identifier}".`,
        );
      }
      await db.unsafe(
        `CREATE TABLE IF NOT EXISTS "${POOL_META_TABLE}" (
                    "id" TEXT PRIMARY KEY,
                    "config" JSONB NOT NULL,
                    "created_at" BIGINT NOT NULL,
                    "updated_at" BIGINT NOT NULL
                )`,
      );
      return;
    }
    if (this._layer.type === "mysql") {
      const pool = this._mysqlPool();
      if (!pool) {
        throw new Error(
          `Pool metadata requires a mysql layer with a live connection for "${this._layer.identifier}".`,
        );
      }
      await pool.query(
        `CREATE TABLE IF NOT EXISTS \`${POOL_META_TABLE}\` (
                    \`id\` VARCHAR(64) PRIMARY KEY,
                    \`config\` JSON NOT NULL,
                    \`created_at\` BIGINT NOT NULL,
                    \`updated_at\` BIGINT NOT NULL
                )`,
      );
      return;
    }
    throw new Error(
      `Pool metadata does not support layer type "${this._layer.type}".`,
    );
  }

  private async _hasTable(): Promise<boolean> {
    if (this._layer.type === "sqlite") {
      const db = this._sqliteDb();
      if (!db) {
        return false;
      }
      const row = db
        .prepare<{
          name: string;
        }>(`SELECT name FROM sqlite_master WHERE type='table' AND name=?`)
        .get(POOL_META_TABLE);
      return Boolean(row?.name);
    }
    if (this._layer.type === "pg") {
      const db = this._pgDb();
      if (!db) {
        return false;
      }
      const rows = (await db.unsafe(
        `SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema = 'public' AND table_name = $1
                ) AS exists`,
        [POOL_META_TABLE],
      )) as Array<{ exists: boolean }>;
      return Boolean(rows[0]?.exists);
    }
    if (this._layer.type === "mysql") {
      const pool = this._mysqlPool();
      if (!pool) {
        return false;
      }
      const [rows] = (await pool.query(
        `SELECT 1 AS present
                 FROM information_schema.tables
                 WHERE table_schema = DATABASE() AND table_name = ?
                 LIMIT 1`,
        [POOL_META_TABLE],
      )) as [{ present?: number }[]];
      return Boolean(rows?.length);
    }
    return false;
  }

  async read(): Promise<StoredMeta | undefined> {
    const hasTable = await this._hasTable();
    if (!hasTable) return undefined;
    if (this._layer.type === "sqlite") {
      const db = this._sqliteDb();
      if (!db) return undefined;
      const row = db
        .prepare<{
          config: string;
          created_at: number;
          updated_at: number;
        }>(
          `SELECT config, created_at, updated_at FROM "${POOL_META_TABLE}" WHERE id = ?`,
        )
        .get(POOL_META_ROW_ID);
      if (!row?.config) return undefined;
      const parsed = JSON.parse(row.config) as PoolMetaConfig;
      return {
        config: parsed,
        createdAt: row.created_at,
        updatedAt: row.updated_at,
      };
    }
    if (this._layer.type === "pg") {
      const db = this._pgDb();
      if (!db) return undefined;
      const rows = (await db.unsafe(
        `SELECT config, created_at, updated_at FROM "${POOL_META_TABLE}" WHERE id = $1`,
        [POOL_META_ROW_ID],
      )) as Array<{ config: unknown; created_at: number; updated_at: number }>;
      const row = rows[0];
      if (!row?.config) return undefined;
      const config =
        typeof row.config === "string" ? JSON.parse(row.config) : row.config;
      return {
        config: config as PoolMetaConfig,
        createdAt: row.created_at,
        updatedAt: row.updated_at,
      };
    }
    if (this._layer.type === "mysql") {
      const pool = this._mysqlPool();
      if (!pool) return undefined;
      const [rows] = (await pool.query(
        `SELECT config, created_at, updated_at FROM \`${POOL_META_TABLE}\` WHERE id = ? LIMIT 1`,
        [POOL_META_ROW_ID],
      )) as [{ config?: unknown; created_at?: number; updated_at?: number }[]];
      const row = rows?.[0];
      if (!row?.config) return undefined;
      const config =
        typeof row.config === "string" ? JSON.parse(row.config) : row.config;
      return {
        config: config as PoolMetaConfig,
        createdAt: row.created_at ?? 0,
        updatedAt: row.updated_at ?? 0,
      };
    }
    return undefined;
  }

  async write(config: PoolMetaConfig): Promise<void> {
    await this._ensureTable();
    const now = Date.now();
    const json = JSON.stringify(config);
    if (this._layer.type === "sqlite") {
      const db = this._sqliteDb();
      if (!db) {
        throw new Error(
          `Pool metadata requires a sqlite layer with a live connection for "${this._layer.identifier}".`,
        );
      }
      db.run(
        `INSERT INTO "${POOL_META_TABLE}" (id, config, created_at, updated_at)
                 VALUES (?, ?, ?, ?)
                 ON CONFLICT(id) DO UPDATE SET config = excluded.config, updated_at = excluded.updated_at`,
        [POOL_META_ROW_ID, json, now, now],
      );
      return;
    }
    if (this._layer.type === "pg") {
      const db = this._pgDb();
      if (!db) {
        throw new Error(
          `Pool metadata requires a pg layer with a live connection for "${this._layer.identifier}".`,
        );
      }
      await db.unsafe(
        `INSERT INTO "${POOL_META_TABLE}" (id, config, created_at, updated_at)
                 VALUES ($1, $2::jsonb, $3, $4)
                 ON CONFLICT (id) DO UPDATE SET config = EXCLUDED.config, updated_at = EXCLUDED.updated_at`,
        [POOL_META_ROW_ID, json, now, now],
      );
      return;
    }
    if (this._layer.type === "mysql") {
      const pool = this._mysqlPool();
      if (!pool) {
        throw new Error(
          `Pool metadata requires a mysql layer with a live connection for "${this._layer.identifier}".`,
        );
      }
      await pool.query(
        `INSERT INTO \`${POOL_META_TABLE}\` (id, config, created_at, updated_at)
                 VALUES (?, ?, ?, ?)
                 ON DUPLICATE KEY UPDATE config = VALUES(config), updated_at = VALUES(updated_at)`,
        [POOL_META_ROW_ID, json, now, now],
      );
      return;
    }
    throw new Error(
      `Pool metadata does not support layer type "${this._layer.type}".`,
    );
  }

  async drop(): Promise<void> {
    if (this._layer.type === "sqlite") {
      const db = this._sqliteDb();
      if (!db) {
        throw new Error(
          `Pool metadata requires a sqlite layer with a live connection for "${this._layer.identifier}".`,
        );
      }
      db.run(`DROP TABLE IF EXISTS "${POOL_META_TABLE}"`);
      return;
    }
    if (this._layer.type === "pg") {
      const db = this._pgDb();
      if (!db) {
        throw new Error(
          `Pool metadata requires a pg layer with a live connection for "${this._layer.identifier}".`,
        );
      }
      await db.unsafe(`DROP TABLE IF EXISTS "${POOL_META_TABLE}"`);
      return;
    }
    if (this._layer.type === "mysql") {
      const pool = this._mysqlPool();
      if (!pool) {
        throw new Error(
          `Pool metadata requires a mysql layer with a live connection for "${this._layer.identifier}".`,
        );
      }
      await pool.query(`DROP TABLE IF EXISTS \`${POOL_META_TABLE}\``);
      return;
    }
  }
}

export async function readPoolMetaConfig(
  layer: SourceLayer,
): Promise<PoolMetaConfig | undefined> {
  const store = new LayerPoolMetaStore(layer);
  const row = await store.read();
  if (!row) return undefined;
  if (row.config.version !== POOL_META_VERSION) {
    throw new Error(
      `Pool metadata version ${row.config.version} is not supported.`,
    );
  }
  return row.config;
}

export async function writePoolMetaConfig(
  layer: SourceLayer,
  config: PoolMetaConfig,
): Promise<void> {
  const store = new LayerPoolMetaStore(layer);
  await store.write(config);
}

export async function dropPoolMetaConfig(layer: SourceLayer): Promise<void> {
  const store = new LayerPoolMetaStore(layer);
  await store.drop();
}

export async function findExistingPoolMeta(
  layers: Map<string, SourceLayer>,
): Promise<Array<{ ident: string; config: PoolMetaConfig }>> {
  const entries = await Promise.all(
    Array.from(layers.entries()).map(async ([ident, layer]) => {
      const config = await readPoolMetaConfig(layer);
      return config ? { ident, config } : undefined;
    }),
  );
  return entries.filter(Boolean) as Array<{
    ident: string;
    config: PoolMetaConfig;
  }>;
}

export function formatPoolMetaMismatch(params: {
  discrepancies: string[];
  layerIdents: string[];
}): Error {
  const discrepancyText = JSON.stringify(params.discrepancies);
  const layerText = params.layerIdents.join(", ");
  const message = [
    `ATTENTION: parts of this pool have been used by another instance of korm. Your configuration does not match. Discrepancies: ${discrepancyText}`,
    "",
    `Pool meta is present in ${layerText}. To continue, either match this pool's configuration, allow korm to discover this pool (const myPool = korm.discover(sourceLayer)) or force a pool reset explicitly by calling korm.danger(korm.reset(pool)) once. This will destroy most of the data in the layers configured in your pool. Do not use this on any production assets.`,
    "",
    "Note that pool discovery depends on sharing the same KORM_ENCRYPTION_KEY environment variable, as pool credentials are encrypted.",
  ].join("\n");
  return new Error(message);
}
