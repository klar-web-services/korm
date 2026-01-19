import { describe, expect, test } from "bun:test";
import crypto from "node:crypto";
import { createDepotIdentifier } from "./depotIdent";

describe("createDepotIdentifier", () => {
    test("trims parts and hashes deterministically", () => {
        const ident = createDepotIdentifier("local", [" /tmp ", " invoices "]);
        const seed = ["local", "/tmp", "invoices"].join("|");
        const hash = crypto.createHash("sha256").update(seed).digest("hex").slice(0, 12);
        expect(ident).toBe(`korm_local_${hash}`);
    });

    test("changes when inputs change", () => {
        const first = createDepotIdentifier("local", ["a"]);
        const second = createDepotIdentifier("local", ["b"]);
        expect(first).not.toBe(second);
    });
});
