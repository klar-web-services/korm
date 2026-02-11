import { describe, expect, test } from "bun:test";
import os from "node:os";
import path from "node:path";
import { mkdtempSync, rmSync } from "node:fs";
import { BackMan } from "./backMan";
import { korm } from "../korm";
import { FloatingDepotFile } from "../depot/depotFile";
import {
  BACKUP_EXTENSION,
  buildBackupPrefixRn,
  buildBackupRn,
  parseBackupTimestamp,
} from "./backups";

describe("BackMan interval builder", () => {
  test("captures precision steps and runNow", () => {
    const spec = new BackMan()
      .every("week")
      .runNow()
      .on("tuesday")
      .at(9, 30)
      .onSecond(15);
    expect(spec.unit).toBe("week");
    expect(spec.weekday).toBe("tuesday");
    expect(spec.hour).toBe(9);
    expect(spec.minute).toBe(30);
    expect(spec.second).toBe(15);
    expect(spec.startsNow).toBe(true);
  });

  test("validates required steps for day/hour/year units", () => {
    expect(() => {
      (
        new BackMan().every("day") as unknown as {
          onSecond: (value: number) => void;
        }
      ).onSecond(5);
    }).toThrow("at(hour, minute)");
    expect(() => {
      (
        new BackMan().every("hour") as unknown as {
          onSecond: (value: number) => void;
        }
      ).onSecond(10);
    }).toThrow("at(minute)");
    expect(() => {
      (
        new BackMan().every("year") as unknown as {
          at: (hour: number, minute: number) => void;
        }
      ).at(1, 2);
    }).toThrow("onDate");
  });

  test("validates multi-year scheduling requirements", () => {
    expect(() => {
      (
        new BackMan().every("decade") as unknown as {
          onDate: (month: number, day: number) => void;
        }
      ).onDate(1, 1);
    }).toThrow("inYear");
    expect(() => {
      (
        new BackMan().every("century") as unknown as {
          at: (hour: number, minute: number) => void;
        }
      ).at(0, 0);
    }).toThrow("inYear");
    const spec = new BackMan()
      .every("millennium")
      .inYear(2)
      .onDate(1, 1)
      .at(0, 0);
    expect(spec.yearInUnit).toBe(2);
  });

  test("rejects invalid weekday values", () => {
    expect(() => {
      new BackMan().every("week").on("noday" as unknown as "monday");
    }).toThrow("weekday");
  });

  test("registers intervals and retention for known layers", () => {
    const manager = new BackMan().conveyLayers(
      new Map([["main", {} as never]]),
    );
    manager.addInterval("main", new BackMan().every("minute").onSecond(10));
    manager.setRetentionForLayer("main", { mode: "count", count: 1 });
    const config = manager.getPoolConfig();
    expect(config.intervals.length).toBe(1);
    expect(config.retention.length).toBe(1);
  });

  test("applies retention policy to backup files", async () => {
    const root = mkdtempSync(path.join(os.tmpdir(), "korm-retention-"));
    const depotRoot = path.join(root, "depot");
    const depot = korm.depots.local(depotRoot);
    const manager = new BackMan().conveyDepot(depot, "backups");
    manager.setRetentionForLayer("main", { mode: "count", count: 1 });

    try {
      const stamps = [
        new Date("2024-01-01T00:00:00Z"),
        new Date("2024-01-02T00:00:00Z"),
        new Date("2024-01-03T00:00:00Z"),
      ];
      for (const stamp of stamps) {
        const rn = buildBackupRn("backups", "main", stamp, BACKUP_EXTENSION);
        const file = new FloatingDepotFile(
          rn,
          new Blob(["{}"], { type: "application/json" }),
        );
        await depot.createFile(file);
      }

      await (manager as any)._applyRetention("main");
      const remaining = await depot.listFiles(
        buildBackupPrefixRn("backups", "main"),
      );
      expect(remaining.length).toBe(1);
      const remainingStamp = parseBackupTimestamp(remaining[0]!.rn);
      expect(remainingStamp).toBe(stamps[2]!.getTime());
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });

  test("play restores backups into a fresh layer", async () => {
    const root = mkdtempSync(path.join(os.tmpdir(), "korm-play-"));
    const depotRoot = path.join(root, "depot");
    const sourcePath = path.join(root, "source.sqlite");
    const restorePath = path.join(root, "restore.sqlite");
    const depot = korm.depots.local(depotRoot);
    const sourceLayer = korm.layers.sqlite(sourcePath);
    const pool = korm
      .pool()
      .setLayers(korm.use.layer(sourceLayer).as("main"))
      .setDepots(korm.use.depot(depot).as("backups"))
      .withMeta(korm.target.layer("main"))
      .open();

    let restoreLayer = korm.layers.sqlite(restorePath);

    try {
      await pool.ensureMetaReady();
      const created = await korm
        .item<{ name: string }>(pool)
        .from.data({
          namespace: "users",
          kind: "basic",
          data: { name: "Ada" },
        })
        .create();
      expect(created.isOk()).toBe(true);

      const now = new Date("2024-01-02T03:04:05Z");
      await sourceLayer.backup({
        depot,
        depotIdent: "backups",
        layerIdent: "main",
        now,
      });
      const backups = await depot.listFiles(
        buildBackupPrefixRn("backups", "main"),
      );
      expect(backups.length).toBe(1);

      const manager = new BackMan()
        .conveyLayers(new Map([["main", restoreLayer]]))
        .conveyDepot(depot, "backups");
      await manager.play(backups[0]!.rn);

      const rows = restoreLayer._db
        .prepare(`SELECT * FROM "__items__users__basic"`)
        .all() as Array<{ name?: string }>;
      expect(rows.length).toBe(1);
      expect(rows[0]?.name).toBe("Ada");

      const poolRows = restoreLayer._db
        .prepare(`SELECT id FROM "__korm_pool__"`)
        .all() as Array<{ id?: string }>;
      expect(poolRows[0]?.id).toBe("pool");
    } finally {
      await pool.close();
      restoreLayer.close();
      rmSync(root, { recursive: true, force: true });
    }
  });
});
