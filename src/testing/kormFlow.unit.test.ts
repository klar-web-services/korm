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
    await korm.danger.reset(discovered, { mode: "meta" });
    await discovered.close();
    sourceLayer.close();
    await fs.rm(root, { recursive: true, force: true });
  });
});
