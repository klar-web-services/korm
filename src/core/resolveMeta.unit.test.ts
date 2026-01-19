import { describe, expect, test } from "bun:test";
import type { JSONable } from "../korm";
import { RN } from "./rn";
import { DepotFile } from "../depot/depotFile";
import {
    clearResolvedMeta,
    cloneJson,
    deepEqualJson,
    getResolvedMeta,
    getValueAtPath,
    materializeRaw,
    setResolvedMeta,
    setValueAtPath,
} from "./resolveMeta";

const UUID = "3dd91ede-37a4-4c25-a86a-6f1a9e132186";

describe("resolveMeta helpers", () => {
    test("stores and clears resolved metadata", () => {
        const target = {};
        const meta = {
            rawData: {} as JSONable,
            resolvedData: {} as JSONable,
            refs: [],
        };
        setResolvedMeta(target, meta);
        expect(getResolvedMeta(target)).toBe(meta);
        clearResolvedMeta(target);
        expect(getResolvedMeta(target)).toBeUndefined();
    });

    test("cloneJson preserves RN values and non-plain objects", () => {
        const rn = RN.create("cars", "suv", UUID).unwrap();
        const date = new Date("2024-01-01T00:00:00Z");
        const input = {
            owner: rn,
            list: [rn],
            when: date,
            nested: { ok: true },
        };
        const cloned = cloneJson(input as JSONable) as Record<string, JSONable>;
        expect(cloned).toEqual({
            owner: rn.value(),
            list: [rn.value()],
            when: date,
            nested: { ok: true },
        });
        expect(cloned.when).toBe(date);
    });

    test("deepEqualJson handles RN and depot files", () => {
        const rn = RN.create("cars", "suv", UUID).unwrap();
        const depotRn = RN.create("[rn][depot::files]:invoices:2024:receipt.txt").unwrap();
        const file = new DepotFile(depotRn, new Blob(["x"]));
        expect(deepEqualJson(rn as JSONable, rn.value() as JSONable)).toBe(true);
        expect(deepEqualJson(file as JSONable, depotRn.value() as JSONable)).toBe(true);
        expect(deepEqualJson([1, 2] as JSONable, [1, 2] as JSONable)).toBe(true);
        expect(deepEqualJson([1] as JSONable, [1, 2] as JSONable)).toBe(false);
        expect(deepEqualJson({ a: 1 } as JSONable, { a: 1 } as JSONable)).toBe(true);
        expect(deepEqualJson({ a: 1 } as JSONable, { a: 2 } as JSONable)).toBe(false);
        expect(deepEqualJson({ a: 1 } as JSONable, { a: 1, b: 2 } as JSONable)).toBe(false);
        expect(deepEqualJson("1" as JSONable, 1 as JSONable)).toBe(false);
    });

    test("getValueAtPath and setValueAtPath walk nested paths", () => {
        const data = { user: { addresses: [{ city: "A" }] } };
        expect(getValueAtPath(data, ["user", "addresses", 0, "city"])).toBe("A");
        expect(getValueAtPath(data, ["user", "addresses", 1, "city"])).toBeUndefined();
        expect(getValueAtPath(data, ["user", 0])).toBeUndefined();

        setValueAtPath(data, ["user", "addresses", 0, "city"], "B");
        expect(data.user.addresses[0]!.city).toBe("B");
        setValueAtPath(data, [], "X");
        expect(data.user.addresses[0]!.city).toBe("B");
        const snapshot = JSON.stringify(data);
        setValueAtPath(data, ["user", 0], "X");
        expect(JSON.stringify(data)).toBe(snapshot);

        const list = ["x", "y"];
        setValueAtPath(list, [1], "z");
        expect(list[1]).toBe("z");
    });

    test("materializeRaw restores RN values at tracked refs", () => {
        const rn = RN.create("cars", "suv", UUID).unwrap();
        const meta = {
            rn,
            rawData: { owner: rn },
            resolvedData: { owner: { name: "Mia" }, nested: { ok: true } },
            refs: [{ path: ["owner"] }, { path: ["missing"] }],
        };
        const materialized = materializeRaw(meta);
        expect(materialized).toEqual({ owner: rn.value(), nested: { ok: true } });
        (materialized as Record<string, JSONable>).nested = { ok: false };
        expect((meta.resolvedData as Record<string, JSONable>).nested).toEqual({ ok: true });
    });
});
