import { createHash } from "node:crypto";
import type { JSONable } from "../korm";

function canonicalizeInner(value: unknown): JSONable {
  if (
    value === null ||
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "boolean"
  ) {
    return value;
  }

  if (Array.isArray(value)) {
    return value.map((entry) =>
      entry === undefined ? null : canonicalizeInner(entry),
    );
  }

  if (value && typeof value === "object") {
    const maybeToJson = value as { toJSON?: () => unknown };
    if (typeof maybeToJson.toJSON === "function") {
      return canonicalizeInner(maybeToJson.toJSON());
    }
    const out: Record<string, JSONable> = {};
    const source = value as Record<string, unknown>;
    const keys = Object.keys(source).sort();
    for (const key of keys) {
      const child = source[key];
      if (child === undefined) continue;
      out[key] = canonicalizeInner(child);
    }
    return out;
  }

  throw new Error(
    `Unsupported unique value type: ${Object.prototype.toString.call(value)}`,
  );
}

/**
 * Canonicalize a JSONable value for deterministic uniqueness checks.
 * Next: pass the result into `fingerprintUniqueValue(...)` or `new Unique(...)`.
 */
export function canonicalizeUniqueValue<T extends JSONable>(value: T): T {
  return canonicalizeInner(value) as T;
}

/**
 * Build a stable fingerprint for a JSONable value.
 * Next: store the fingerprint in a unique shadow column to enforce uniqueness.
 */
export function fingerprintUniqueValue(value: JSONable): string {
  const canonical = canonicalizeInner(value);
  const payload = JSON.stringify(canonical);
  return createHash("sha512").update(payload).digest("hex");
}

/**
 * Wrapper that marks a field as uniqueness-constrained.
 * Next: create instances with `korm.unique(value)` and use as `korm.types.Unique<T>`.
 */
export class Unique<T extends JSONable> {
  readonly __UNIQUE__: true = true;
  private readonly _value: T;
  private readonly _fingerprint: string;

  /** Create a unique wrapper around a JSONable value. */
  constructor(value: T) {
    const canonical = canonicalizeUniqueValue(value);
    this._value = canonical;
    this._fingerprint = fingerprintUniqueValue(canonical);
  }

  /** Read the canonicalized value carried by this wrapper. */
  value(): T {
    return this._value;
  }

  /** Read the stable fingerprint used for uniqueness enforcement. */
  fingerprint(): string {
    return this._fingerprint;
  }

  toJSON(): JSONable {
    return this._value;
  }
}
