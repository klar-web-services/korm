import { describe, expect, test } from "bun:test";
import path from "node:path";
import os from "node:os";
import fs from "node:fs/promises";
import { korm } from "../korm";
import type { Password } from "../security/encryption";
import type { DepotFileLike } from "../depot/depotFile";
import type { RN } from "../core/rn";

type User = {
  name: string;
  password: Password<string>;
  avatar?: DepotFileLike;
};

type Group = {
  name: string;
  owner: RN<User>;
};

async function createTempPool() {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "korm-flow-"));
  const dbPath = path.join(root, "db.sqlite");
  const depotRoot = path.join(root, "depot");
  const walRoot = path.join(root, "wal");
  await fs.mkdir(depotRoot, { recursive: true });
  await fs.mkdir(walRoot, { recursive: true });

  const pool = korm
    .pool()
    .setLayers(korm.use.layer(korm.layers.sqlite(dbPath), "main"))
    .withMeta(korm.target.layer("main"))
    .withLocks(korm.target.layer("main"))
    .setDepots(
      korm.use.depot(korm.depots.local(depotRoot), "files"),
      korm.use.depot(korm.depots.local(walRoot), "wal"),
    )
    .withWal({
      depotIdent: "wal",
      walNamespace: "unit",
      retention: "keep",
      depotOps: "record",
    })
    .backups("files")
    .addInterval("*", korm.interval.every("day").runNow())
    .retain("all")
    .open();

  return { pool, root };
}

describe("korm core flow", () => {
  test("creates, queries, resolves, and runs WAL/backups", async () => {
    const { pool, root } = await createTempPool();
    try {
      const avatar = korm.file({
        rn: "[rn][depot::files]:avatars:ada.txt",
        file: new Blob(["hello"]),
      });
      const created = (
        await korm
          .item<User>(pool)
          .from.data({
            namespace: "users",
            kind: "basic",
            data: {
              name: "Ada",
              password: await korm.password("secret"),
              avatar,
            },
          })
          .create()
      ).unwrap();
      const updated = (
        await created.update({ name: "Ada2" }).commit()
      ).unwrap();

      const groupResult = await korm
        .item<Group>(pool)
        .from.data({
          namespace: "groups",
          kind: "team",
          data: { name: "Ops", owner: updated.rn! },
        })
        .create();
      expect(groupResult.isOk()).toBe(true);

      const pending = updated.update({ name: "Ada3" }).unwrap();
      const floating = korm
        .item<User>(pool)
        .from.data({
          namespace: "users",
          kind: "basic",
          data: { name: "Bob", password: await korm.password("pw") },
        })
        .unwrap();
      const txResult = await korm.tx(floating, pending).persist();
      expect(txResult.isOk()).toBe(true);

      const { and, eq, like } = korm.qfns;
      const users = (
        await korm
          .item<User>(pool)
          .from.query(korm.rn("[rn][from::main]:users:basic:*"))
          .where(and(eq("name", "Ada3"), like("name", "Ada%")))
          .get()
      ).unwrap();
      expect(users.length).toBe(1);

      const groups = (
        await korm
          .item<Group>(pool)
          .from.query(korm.rn("[rn][from::main]:groups:team:*"))
          .get(korm.resolve("owner"))
      ).unwrap();
      expect((groups[0]!.data!.owner as User).name).toBe("Ada3");

      const lockRelease = await pool.locker.acquire(updated.rn!);
      lockRelease();
      expect(pool.locker.tryAcquire(updated.rn!)).toBeUndefined();

      await new Promise((resolve) => setTimeout(resolve, 50));
      const backupDepot = pool.getDepot("files")!;
      const backupPrefix = korm.rn("[rn][depot::files]:__korm_backups__:*");
      const backups = await backupDepot.listFiles(backupPrefix);
      expect(backups.length).toBeGreaterThan(0);

      const walDepot = pool.getDepot("wal")!;
      const walPrefix = korm.rn("[rn][depot::wal]:*");
      const walFiles = await walDepot.listFiles(walPrefix);
      expect(walFiles.length).toBeGreaterThan(0);
    } finally {
      await pool.close();
      await fs.rm(root, { recursive: true, force: true });
    }
  });

  test("query rehydrates RN references as RN objects", async () => {
    type Agent = { nickname: string };
    type A2AMessage = {
      from: RN<Agent>;
      to: RN<Agent>;
      message: string;
    };

    const { pool, root } = await createTempPool();
    try {
      const a = (
        await korm
          .item<Agent>(pool)
          .from.data({
            namespace: "agents",
            kind: "registered",
            data: { nickname: "a" },
          })
          .create()
      ).unwrap();

      const b = (
        await korm
          .item<Agent>(pool)
          .from.data({
            namespace: "agents",
            kind: "registered",
            data: { nickname: "b" },
          })
          .create()
      ).unwrap();

      (
        await korm
          .item<A2AMessage>(pool)
          .from.data({
            namespace: "messages",
            kind: "a2a",
            data: {
              from: a.rn!,
              to: b.rn!,
              message: "hello",
            },
          })
          .create()
      ).unwrap();

      const rows = (
        await korm
          .item<A2AMessage>(pool)
          .from.query(korm.rn("[rn][from::main]:messages:a2a:*"))
          .get()
      ).unwrap();
      expect(rows.length).toBe(1);

      const fromRef = rows[0]!.data!.from;
      expect(typeof (fromRef as any)?.value).toBe("function");
      expect(fromRef.value()).toBe(a.rn!.value());
    } finally {
      await pool.close();
      await fs.rm(root, { recursive: true, force: true });
    }
  });

  test("deleteItem reports missing rows", async () => {
    const { pool, root } = await createTempPool();
    try {
      const created = (
        await korm
          .item<User>(pool)
          .from.data({
            namespace: "users",
            kind: "basic",
            data: { name: "DeleteMe", password: await korm.password("pw") },
          })
          .create()
      ).unwrap();

      const layer = pool.getLayer("main")!;
      const firstDelete = await layer.deleteItem(created.rn!);
      expect(firstDelete.success).toBe(true);

      const secondDelete = await layer.deleteItem(created.rn!);
      expect(secondDelete.success).toBe(false);
      if (!secondDelete.success) {
        expect(secondDelete.error.message).toContain("which does not exist");
      }

      const missingRn = korm.rn(
        "[rn][from::main]:ghosts:phantom:00000000-0000-4000-8000-000000000000",
      );
      const missingDelete = await layer.deleteItem(missingRn);
      expect(missingDelete.success).toBe(false);
      if (!missingDelete.success) {
        expect(missingDelete.error.message).toContain("which does not exist");
      }
    } finally {
      await pool.close();
      await fs.rm(root, { recursive: true, force: true });
    }
  });

  test("Item.delete persists and can restore", async () => {
    const { pool, root } = await createTempPool();
    try {
      const created = (
        await korm
          .item<User>(pool)
          .from.data({
            namespace: "users",
            kind: "basic",
            data: { name: "DeleteFlow", password: await korm.password("pw") },
          })
          .create()
      ).unwrap();

      const deleteResult = await created.delete();
      expect(deleteResult.isOk()).toBe(true);
      const deleted = deleteResult.unwrap();

      const layer = pool.getLayer("main")!;
      const missing = await layer.readItemRaw<User>(created.rn!);
      expect(missing).toBeUndefined();

      const restoreResult = await deleted.restore();
      expect(restoreResult.isOk()).toBe(true);
      const restored = restoreResult.unwrap();
      const restoredRaw = await layer.readItemRaw<User>(restored.rn!);
      expect(restoredRaw?.name).toBe("DeleteFlow");

      const wal = pool.wal!;
      const walDepot = pool.getDepot(wal.depotIdent)!;
      const donePrefix = korm.rn(
        `[rn][depot::${wal.depotIdent}]:__korm_wal__:${wal.namespace}:${wal.poolId}:done:*`,
      );
      const doneFiles = await walDepot.listFiles(donePrefix);
      const records = await Promise.all(
        doneFiles.map(async (file) => JSON.parse(await file.text())),
      );
      const hasDelete = records.some(
        (record: { ops?: { type: string; rn?: string }[] }) =>
          record.ops?.some(
            (op) => op.type === "delete" && op.rn === created.rn!.value(),
          ),
      );
      expect(hasDelete).toBe(true);
    } finally {
      await pool.close();
      await fs.rm(root, { recursive: true, force: true });
    }
  });

  test("discovers a pool from metadata and can reset meta", async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "korm-discover-"));
    const dbPath = path.join(root, "db.sqlite");
    const depotRoot = path.join(root, "depot");
    await fs.mkdir(depotRoot, { recursive: true });

    const pool = korm
      .pool()
      .setLayers(korm.use.layer(korm.layers.sqlite(dbPath), "main"))
      .withMeta(korm.target.layer("main"))
      .setDepots(korm.use.depot(korm.depots.local(depotRoot), "files"))
      .open();
    await pool.ensureMetaReady();
    await pool.close();

    const sourceLayer = korm.layers.sqlite(dbPath);
    const discovered = await korm.discover(sourceLayer);
    expect(discovered.getLayers().size).toBe(1);
    expect(discovered.getDepots().size).toBe(1);
    await korm.danger(korm.reset(discovered, { mode: "meta" }));
    await discovered.close();
    sourceLayer.close();
    await fs.rm(root, { recursive: true, force: true });
  });
});
