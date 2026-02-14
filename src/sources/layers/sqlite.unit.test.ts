import { describe, expect, test } from "bun:test";
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
