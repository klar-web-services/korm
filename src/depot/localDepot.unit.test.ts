import { describe, expect, test } from "bun:test";
import path from "node:path";
import os from "node:os";
import fs from "node:fs/promises";
import { RN } from "../core/rn";
import { LocalDepot } from "./depots/localDepot";
import { FloatingDepotFile } from "./depotFile";

describe("LocalDepot", () => {
    test("creates, edits, lists, and deletes files", async () => {
        const root = await fs.mkdtemp(path.join(os.tmpdir(), "korm-local-"));
        const depot = new LocalDepot(root);
        const rn = RN.create(`[rn][depot::${depot.identifier}]:alpha:bravo.txt`).unwrap();
        const prefix = RN.create(`[rn][depot::${depot.identifier}]:alpha:*`).unwrap();
        const rootPrefix = RN.create(`[rn][depot::${depot.identifier}]:*`).unwrap();

        try {
            await depot.createFile(new FloatingDepotFile(rn, new Blob(["hello"])));
            const files = await depot.listFiles(prefix);
            expect(files.length).toBe(1);
            expect(await files[0]!.text()).toBe("hello");

            const dirs = await depot.listDirs(rootPrefix);
            expect(dirs).toContain("alpha");

            await depot.editFile(rn, async () => new FloatingDepotFile(rn, new Blob(["updated"])));
            const updated = await depot.getFile(rn);
            expect(await updated.text()).toBe("updated");

            expect(await depot.deleteFile(rn)).toBe(true);
            const afterDelete = await depot.listFiles(prefix);
            expect(afterDelete.length).toBe(0);
        } finally {
            await fs.rm(root, { recursive: true, force: true });
        }
    });
});
