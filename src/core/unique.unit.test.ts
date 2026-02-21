import { describe, expect, test } from "bun:test";
import {
  Unique,
  canonicalizeUniqueValue,
  fingerprintUniqueValue,
} from "./unique";

describe("Unique helper", () => {
  test("canonicalizes nested objects deterministically", () => {
    const input = {
      z: 1,
      a: {
        y: 2,
        x: 3,
      },
      list: [{ b: 2, a: 1 }, { b: 4, a: 3 }],
    };

    const canonical = canonicalizeUniqueValue(input);
    expect(canonical).toEqual({
      a: {
        x: 3,
        y: 2,
      },
      list: [{ a: 1, b: 2 }, { a: 3, b: 4 }],
      z: 1,
    });
  });

  test("produces identical fingerprints for semantically equal objects", () => {
    const left = {
      id: "car-1",
      meta: {
        model: "Yaris",
        make: "Toyota",
      },
    };
    const right = {
      meta: {
        make: "Toyota",
        model: "Yaris",
      },
      id: "car-1",
    };

    expect(fingerprintUniqueValue(left)).toBe(fingerprintUniqueValue(right));
  });

  test("stores canonical value and stable fingerprint on wrapper", () => {
    const unique = new Unique({
      b: 2,
      a: 1,
    });

    expect(unique.value()).toEqual({ a: 1, b: 2 });
    expect(unique.toJSON()).toEqual({ a: 1, b: 2 });
    expect(unique.fingerprint()).toBe(
      fingerprintUniqueValue({ a: 1, b: 2 }),
    );
  });
});
