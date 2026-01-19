import { describe, expect, test } from "bun:test";
import { buildBackupPayload, buildBackupPrefixRn, buildBackupRn, parseBackupTimestamp, serializeBackupPayload } from "./backups";

describe("backups helpers", () => {
    test("buildBackupRn uses depot path segments and timestamp", () => {
        const now = new Date("2024-01-02T03:04:05Z");
        const rn = buildBackupRn("archiveDepot", "layerOne", now, "json");
        expect(rn.isDepot()).toBe(true);
        expect(rn.pointsTo()).toBe("depotFile");
        expect(rn.depotIdent()).toBe("archiveDepot");
        const parts = rn.depotParts() ?? [];
        expect(parts[0]).toBe("__korm_backups__");
        expect(parts[1]).toBe("layerOne");
        expect(parts[2]).toBe("20240102T030405Z");
        expect(rn.value().endsWith(".json")).toBe(true);
    });

    test("buildBackupRn hashes invalid layer identifiers", () => {
        const now = new Date("2024-01-02T03:04:05Z");
        const rn = buildBackupRn("archiveDepot", "bad:seg/ment", now, "json");
        const parts = rn.depotParts() ?? [];
        expect(parts[0]).toBe("__korm_backups__");
        expect(parts[1]?.startsWith("layer-")).toBe(true);
        expect(parts[1]).not.toBe("bad:seg/ment");
    });

    test("buildBackupPrefixRn and parseBackupTimestamp locate backups", () => {
        const now = new Date("2024-01-02T03:04:05Z");
        const rn = buildBackupRn("archiveDepot", "layerOne", now, "json");
        const prefix = buildBackupPrefixRn("archiveDepot", "layerOne");
        expect(prefix.pointsTo()).toBe("depotPrefix");
        const parts = prefix.depotParts() ?? [];
        expect(parts[0]).toBe("__korm_backups__");
        expect(parts[1]).toBe("layerOne");
        expect(parts[2]).toBe("*");
        const stamp = parseBackupTimestamp(rn);
        expect(stamp).toBe(now.getTime());
    });

    test("serializeBackupPayload stringifies bigint values", async () => {
        const now = new Date("2024-01-02T03:04:05Z");
        const payload = buildBackupPayload("sqlite", "layerOne", [
            { name: "__items__cars__suv", rows: [{ id: 1, count: BigInt(2) }] },
        ], now);
        const blob = serializeBackupPayload(payload);
        const text = await blob.text();
        const parsed = JSON.parse(text) as { tables: Array<{ rows: Array<{ count: string }> }> };
        expect(parsed.tables[0]?.rows[0]?.count).toBe("2");
    });
});
