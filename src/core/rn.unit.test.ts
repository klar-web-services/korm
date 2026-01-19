import { describe, expect, test } from "bun:test";
import { RN } from "./rn";

const UUID = "3dd91ede-37a4-4c25-a86a-6f1a9e132186";

describe("RN", () => {
    test("creates item RN from parts with mods", () => {
        const mods = new Map([["from", "pg"]]);
        const res = RN.create("cars", "suv", UUID, mods);
        expect(res.isOk()).toBe(true);
        const rn = res.unwrap();
        expect(rn.namespace).toBe("cars");
        expect(rn.kind).toBe("suv");
        expect(rn.id).toBe(UUID);
        expect(rn.mods.get("from")).toBe("pg");
        expect(rn.isDepot()).toBe(false);
        expect(rn.pointsTo()).toBe("item");
        expect(rn.value()).toBe(`[rn][from::pg]:cars:suv:${UUID}`);
        expect(rn.toJSON()).toBe(rn.value());
    });

    test("creates collection RN", () => {
        const res = RN.create("cars", "suv", "*");
        expect(res.isOk()).toBe(true);
        const rn = res.unwrap();
        expect(rn.pointsTo()).toBe("collection");
    });

    test("parses item RN string with mods", () => {
        const res = RN.create(`[rn][from::pg]:cars:suv:${UUID}`);
        expect(res.isOk()).toBe(true);
        const rn = res.unwrap();
        expect(rn.namespace).toBe("cars");
        expect(rn.kind).toBe("suv");
        expect(rn.id).toBe(UUID);
        expect(rn.mods.get("from")).toBe("pg");
        expect(rn.value()).toBe(`[rn][from::pg]:cars:suv:${UUID}`);
    });

    test("rejects invalid RN parts", () => {
        expect(RN.create("Cars", "suv", UUID).isErr()).toBe(true);
        expect(RN.create("cars", "SUV", UUID).isErr()).toBe(true);
        expect(RN.create("undefined", "suv", UUID).isErr()).toBe(true);
        expect(RN.create("cars", "undefined", UUID).isErr()).toBe(true);
        expect(RN.create("cars", "suv", "not-a-uuid").isErr()).toBe(true);
    });

    test("rejects invalid RN strings", () => {
        expect(RN.create(`[rn][From::pg]:cars:suv:${UUID}`).isErr()).toBe(true);
        expect(RN.create(`[rn][from::bad[]:cars:suv:${UUID}`).isErr()).toBe(true);
        expect(RN.create(`[rn][from::pg]cars:suv:${UUID}`).isErr()).toBe(true);
        expect(RN.create(`[rn]:cars:suv:not-a-uuid`).isErr()).toBe(true);
    });

    test("parses depot RN strings and paths", () => {
        const res = RN.create("[rn][depot::files]:invoices:2024:receipt.txt");
        expect(res.isOk()).toBe(true);
        const rn = res.unwrap();
        expect(rn.isDepot()).toBe(true);
        expect(rn.pointsTo()).toBe("depotFile");
        expect(rn.depotIdent()).toBe("files");
        expect(rn.depotParts()).toEqual(["invoices", "2024", "receipt.txt"]);
        rn.mod("tag", "blue");
        expect(rn.mods.get("tag")).toBe("blue");
        expect(rn.value()).toBe("[rn][depot::files][tag::blue]:invoices:2024:receipt.txt");
    });

    test("rejects depot wildcard in non-final segment", () => {
        const res = RN.create("[rn][depot::files]:a:*:b");
        expect(res.isErr()).toBe(true);
    });

    test("disallows depot mod on item RN", () => {
        const rn = RN.create("cars", "suv", UUID).unwrap();
        expect(() => rn.mod("depot", "files")).toThrow();
    });

    test("rejects depot mod in parts constructor", () => {
        const mods = new Map([["depot", "files"]]);
        expect(RN.create("cars", "suv", UUID, mods).isErr()).toBe(true);
    });

    test("rejects malformed mod segments", () => {
        expect(RN.create(`[rn][from]:cars:suv:${UUID}`).isErr()).toBe(true);
        expect(RN.create(`[rn][from::bad[]]:cars:suv:${UUID}`).isErr()).toBe(true);
        expect(RN.create(`[rn][from::pg:cars:suv:${UUID}`).isErr()).toBe(true);
    });

    test("rejects malformed RN bodies", () => {
        expect(RN.create("[rn]:").isErr()).toBe(true);
        expect(RN.create(`[rn][from::pg]cars:suv:${UUID}`).isErr()).toBe(true);
        expect(RN.create(`[rn]:cars:suv:${UUID}:extra`).isErr()).toBe(true);
        expect(RN.create(`[rn]:undefined:suv:${UUID}`).isErr()).toBe(true);
        expect(RN.create(`[rn]:cars:undefined:${UUID}`).isErr()).toBe(true);
    });

    test("rejects invalid mods in parts constructor", () => {
        const badKey = new Map([["From", "pg"]]);
        expect(RN.create("cars", "suv", UUID, badKey).isErr()).toBe(true);
        const badValue = new Map([["from", "bad[]"]]);
        expect(RN.create("cars", "suv", UUID, badValue).isErr()).toBe(true);
    });

    test("parses depot prefix and returns stable parts", () => {
        const rn = RN.create("[rn][depot::files]:invoices:*").unwrap();
        expect(rn.pointsTo()).toBe("depotPrefix");
        const parts = rn.depotParts() ?? [];
        parts.push("extra");
        expect(rn.depotParts()).toEqual(["invoices", "*"]);
    });

    test("renders item RN without mods", () => {
        const rn = RN.create("cars", "suv", UUID).unwrap();
        expect(rn.value()).toBe(`[rn]:cars:suv:${UUID}`);
    });
});
