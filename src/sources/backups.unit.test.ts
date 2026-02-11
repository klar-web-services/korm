import { describe, expect, test } from "bun:test";
import {
  BACKUP_EXTENSION,
  BackupEventReader,
  buildBackupHeaderEvent,
  buildBackupPrefixRn,
  buildBackupRn,
  encodeBackupEvent,
  parseBackupTimestamp,
  streamBackupEvents,
  type BackupEvent,
} from "./backups";

describe("backups helpers", () => {
  test("buildBackupRn uses depot path segments and timestamp", () => {
    const now = new Date("2024-01-02T03:04:05Z");
    const rn = buildBackupRn("archiveDepot", "layerOne", now, BACKUP_EXTENSION);
    expect(rn.isDepot()).toBe(true);
    expect(rn.pointsTo()).toBe("depotFile");
    expect(rn.depotIdent()).toBe("archiveDepot");
    const parts = rn.depotParts() ?? [];
    expect(parts[0]).toBe("__korm_backups__");
    expect(parts[1]).toBe("layerOne");
    expect(parts[2]).toBe("20240102T030405Z");
    expect(rn.value().endsWith(`.${BACKUP_EXTENSION}`)).toBe(true);
  });

  test("buildBackupRn hashes invalid layer identifiers", () => {
    const now = new Date("2024-01-02T03:04:05Z");
    const rn = buildBackupRn(
      "archiveDepot",
      "bad:seg/ment",
      now,
      BACKUP_EXTENSION,
    );
    const parts = rn.depotParts() ?? [];
    expect(parts[0]).toBe("__korm_backups__");
    expect(parts[1]?.startsWith("layer-")).toBe(true);
    expect(parts[1]).not.toBe("bad:seg/ment");
  });

  test("buildBackupPrefixRn and parseBackupTimestamp locate backups", () => {
    const now = new Date("2024-01-02T03:04:05Z");
    const rn = buildBackupRn("archiveDepot", "layerOne", now, BACKUP_EXTENSION);
    const prefix = buildBackupPrefixRn("archiveDepot", "layerOne");
    expect(prefix.pointsTo()).toBe("depotPrefix");
    const parts = prefix.depotParts() ?? [];
    expect(parts[0]).toBe("__korm_backups__");
    expect(parts[1]).toBe("layerOne");
    expect(parts[2]).toBe("*");
    const stamp = parseBackupTimestamp(rn);
    expect(stamp).toBe(now.getTime());
  });

  test("encodeBackupEvent stringifies bigint values", () => {
    const line = encodeBackupEvent({
      t: "row",
      table: "__items__cars__suv",
      row: { id: 1, count: BigInt(2) },
    });
    const parsed = JSON.parse(line.trim()) as { row?: { count?: string } };
    expect(parsed.row?.count).toBe("2");
  });

  test("streamBackupEvents round-trips events", async () => {
    const now = new Date("2024-01-02T03:04:05Z");
    const events = async function* (): AsyncGenerator<BackupEvent> {
      yield buildBackupHeaderEvent("sqlite", "layerOne", now);
      yield { t: "table", name: "__items__cars__suv" } as const;
      yield {
        t: "row",
        table: "__items__cars__suv",
        row: { id: 1, count: BigInt(2) },
      } as const;
      yield { t: "end" } as const;
    };
    const stream = streamBackupEvents(events());
    const reader = new BackupEventReader(stream);
    const header = await reader.next();
    const table = await reader.next();
    const row = await reader.next();
    expect(header?.t).toBe("header");
    expect(table?.t).toBe("table");
    expect(row?.t).toBe("row");
    if (row?.t === "row") {
      expect((row.row as any)?.count).toBe("2");
    }
  });

  test("streamBackupEvents handles large streams", async () => {
    const totalRows = 10000;
    const now = new Date("2024-01-02T03:04:05Z");
    const events = async function* (): AsyncGenerator<BackupEvent> {
      yield buildBackupHeaderEvent("sqlite", "layerOne", now);
      yield { t: "table", name: "__items__cars__suv" } as const;
      for (let i = 0; i < totalRows; i += 1) {
        yield {
          t: "row",
          table: "__items__cars__suv",
          row: { id: i, count: i },
        } as const;
      }
      yield { t: "end" } as const;
    };
    const stream = streamBackupEvents(events());
    const reader = new BackupEventReader(stream);
    const header = await reader.next();
    expect(header?.t).toBe("header");
    let rows = 0;
    while (true) {
      const event = await reader.next();
      if (!event) break;
      if (event.t === "row") rows += 1;
    }
    expect(rows).toBe(totalRows);
  });
});
