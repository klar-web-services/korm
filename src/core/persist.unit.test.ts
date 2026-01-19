import { describe, expect, test } from "bun:test";
import type { JSONable } from "../korm";
import { RN } from "./rn";
import type { LayerPool } from "../sources/layerPool";
import { Encrypt } from "../security/encryption";
import { isEncryptedPayload, type EncryptionMeta } from "./encryptionMeta";
import { DepotFileBase, FloatingDepotFile } from "../depot/depotFile";
import { prepareWriteData } from "./persist";

const UUID = "3dd91ede-37a4-4c25-a86a-6f1a9e132186";

describe("prepareWriteData", () => {
    test("returns early for undefined data", async () => {
        const pool = {} as LayerPool;
        const meta: EncryptionMeta = { entries: [] };
        const depotOps: DepotFileBase[] = [];
        const result = await prepareWriteData(undefined, meta, pool, { depotOps });
        expect(result.data).toBeUndefined();
        expect(result.meta).toBe(meta);
        expect(result.depotOps).toBe(depotOps);
    });

    test("prepares encryption and depot payloads", async () => {
        const rn = RN.create("[rn][depot::files]:docs:2024:note.txt").unwrap();
        const file = new FloatingDepotFile(rn, new Blob(["note"]));
        const encrypt = new Encrypt("secret", "password");
        const meta: EncryptionMeta = { entries: [] };
        const pool = {} as LayerPool;

        const result = await prepareWriteData(
            { file, secret: encrypt } as JSONable,
            meta,
            pool,
            { skipDepotUpload: true }
        );

        expect(result.meta).toBe(meta);
        expect(result.depotOps).toEqual([file]);
        const output = result.data as Record<string, JSONable>;
        expect(output.file).toBe(rn.value());
        expect(isEncryptedPayload(output.secret)).toBe(true);
    });
});
