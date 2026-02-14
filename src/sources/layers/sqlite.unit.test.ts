import { describe, expect, mock, test } from "bun:test";
import { Database } from "bun:sqlite";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { RN } from "../../core/rn";
import { SqliteLayer } from "./sqlite";

const UUID = "3dd91ede-37a4-4c25-a86a-6f1a9e132186";

function makeSqliteLayer(
  overrides: Record<string, unknown> = {},
): SqliteLayer & Record<string, unknown> {
  const layer = Object.create(SqliteLayer.prototype) as any;
  layer._db = {
    run: () => {},
    prepare: () => ({ all: () => [] }),
    close: () => {},
  };
  layer.type = "sqlite";
  layer.identifier = ":memory:";
  layer._path = ":memory:";
  layer._columnKindsCache = new Map();
  layer._tableInfoCache = new Map();
  Object.assign(layer, overrides);
  return layer as SqliteLayer & Record<string, unknown>;
}

describe("SqliteLayer helpers", () => {
  test("constructor avoids lock-sensitive startup pragmas", async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "korm-sqlite-lock-"));
    const dbPath = path.join(root, "locked.sqlite");
    const blocker = new Database(dbPath);
    let layer: { close(): void } | undefined;
    mock.module("../../runtime/engine", () => ({
      getBunGlobal: () => ({}),
      isBunRuntime: () => true,
      getRuntimeEngine: () => "bun",
    }));

    try {
      const modPath =
        `./sqlite.ts?ctor=${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`;
      const { SqliteLayer: SqliteLayerCtor } =
        (await import(modPath)) as typeof import("./sqlite");
      blocker.run(`CREATE TABLE IF NOT EXISTS "t" ("x" INTEGER)`);
      blocker.run("BEGIN EXCLUSIVE");

      expect(() => {
        layer = new SqliteLayerCtor(dbPath);
      }).not.toThrow();
    } finally {
      try {
        layer?.close();
      } catch {}
      try {
        blocker.run("ROLLBACK");
      } catch {}
      blocker.close();
      await fs.rm(root, { recursive: true, force: true });
      mock.restore();
    }
  });

  test("decode rehydrates RN column values", async () => {
    const layer = makeSqliteLayer() as any;
    const ownerRn = `[rn]:users:basic:${UUID}`;
    const malformed = "[rn]:users:basic:not-a-uuid";
    const decoded = await layer._decodeRowUsingTableInfo(
      {
        owner: ownerRn,
        invalidRef: malformed,
        note: "ok",
      },
      [
        { name: "owner", type: "RN_REF_TEXT" },
        { name: "invalidRef", type: "RN_REF_TEXT" },
        { name: "note", type: "TEXT" },
      ],
    );

    expect(decoded.owner).toBeInstanceOf(RN);
    expect(decoded.owner.value()).toBe(ownerRn);
    expect(decoded.invalidRef).toBe(malformed);
    expect(decoded.note).toBe("ok");
  });
});
