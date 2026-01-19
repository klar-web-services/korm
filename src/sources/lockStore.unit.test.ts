import { describe, expect, test } from "bun:test";
import path from "node:path";
import os from "node:os";
import fs from "node:fs/promises";
import { LayerLockStore } from "./lockStore";
import { SqliteLayer } from "./layers/sqlite";

describe("LayerLockStore (sqlite)", () => {
    test("acquires, refreshes, and releases locks", async () => {
        const root = await fs.mkdtemp(path.join(os.tmpdir(), "korm-locks-"));
        const dbPath = path.join(root, "locks.sqlite");
        const layer = new SqliteLayer(dbPath);
        try {
            const store = new LayerLockStore(layer);
            const lockId = "lock:one";
            const ownerA = "owner-a";
            const ownerB = "owner-b";

            expect(await store.tryAcquire(lockId, ownerA, 1_000)).toBe(true);
            expect(await store.tryAcquire(lockId, ownerB, 1_000)).toBe(false);
            expect(await store.refresh(lockId, ownerA, 1_000)).toBe(true);

            await store.release(lockId, ownerA);
            expect(await store.tryAcquire(lockId, ownerB, 1_000)).toBe(true);
        } finally {
            await layer.close();
            await fs.rm(root, { recursive: true, force: true });
        }
    });
});
