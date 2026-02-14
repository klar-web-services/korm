import { describe, expect, test } from "bun:test";
import { and, eq, inList, not } from "../../core/queryFns";
import { RN } from "../../core/rn";
import { MysqlLayer } from "./mysql";

const UUID = "3dd91ede-37a4-4c25-a86a-6f1a9e132186";

function makeRn(): RN {
  return RN.create("users", "basic", UUID).unwrap();
}

function makeMysqlLayer(
  overrides: Record<string, unknown> = {},
): MysqlLayer & Record<string, unknown> {
  const layer = Object.create(MysqlLayer.prototype) as any;
  layer._pool = {
    query: async () => [[], {}],
    end: async () => {},
  } as any;
  layer.type = "mysql";
  layer.identifier = "app@localhost";
  layer._connectionInput = "mysql://localhost/app";
  layer._metaEnsured = true;
  layer._columnKindsCache = new Map();
  layer._tableInfoCache = new Map();
  Object.assign(layer, overrides);
  return layer as MysqlLayer & Record<string, unknown>;
}

describe("MysqlLayer helpers", () => {
  test("derives identifiers and resolves long table names", () => {
    const layer = makeMysqlLayer() as any;
    expect(layer._deriveIdentifier("mysql://db.local:3306/korm")).toBe(
      "korm@db.local",
    );
    expect(layer._deriveIdentifier("not-a-url")).toBe("mysql@localhost");
    expect(
      layer._deriveIdentifier({ database: "main", host: "example.internal" }),
    ).toBe("main@example.internal");

    expect(layer._quoteIdent("safe_name")).toBe("`safe_name`");
    expect(() => layer._quoteIdent("unsafe-name;DROP")).toThrow(
      "Unsafe identifier",
    );

    const longRawName = `__items__namespace__${"x".repeat(140)}`;
    const resolved = layer._resolveTableName(longRawName);
    expect(resolved.length).toBeLessThanOrEqual(64);
    expect(resolved.startsWith("__items__")).toBe(true);
  });

  test("builds query SQL for JSON paths, IN clauses, and NOT groups", () => {
    const layer = makeMysqlLayer() as any;
    const root = and(
      inList("meta.count", [1, 2, null]),
      eq("status", "ok"),
      not(eq("deletedAt", null)),
    );
    const built = layer._buildQueryString(root, "__items__users__basic", [
      "status",
      "status",
    ]);
    expect(built.sql).toContain("SELECT `rnId`, `status`");
    expect(built.sql).toContain("JSON_EXTRACT");
    expect(built.sql).toContain("IS NULL");
    expect(built.params).toContain("ok");

    expect(() =>
      layer._buildQueryString(
        {
          type: "group",
          method: "NOT",
          components: [eq("a", 1), eq("b", 2)],
        },
        "__items__users__basic",
      ),
    ).toThrow("NOT groups must have exactly 1 component");
  });

  test("ensureTables creates and evolves schemas via pool queries", async () => {
    const queries: string[] = [];
    const layer = makeMysqlLayer({
      _pool: {
        query: async (sql: string) => {
          queries.push(sql);
          if (sql.includes("COUNT(*) as cnt")) {
            return [[{ cnt: 0 }], {}];
          }
          return [[], {}];
        },
      },
      _setColumnKind: async () => {},
    }) as any;

    const result = await layer.ensureTables(
      {
        rn: makeRn(),
        data: {
          name: "Ada",
          age: 37,
          active: true,
          profile: { premium: true },
        },
      },
      false,
    );

    expect(result).toContain("__items__users__basic");
    expect(queries.some((q) => q.includes("CREATE TABLE IF NOT EXISTS"))).toBe(
      true,
    );
  });

  test("ensureTables adds columns and rejects mismatches unless destructive", async () => {
    const addQueries: string[] = [];
    const layer = makeMysqlLayer({
      _pool: {
        query: async (sql: string) => {
          addQueries.push(sql);
          if (sql.includes("COUNT(*) as cnt")) return [[{ cnt: 1 }], {}];
          return [[], {}];
        },
      },
      _setColumnKind: async () => {},
      _getTableInfo: async () => [{ name: "name", type: "text" }],
    }) as any;

    await expect(
      layer.ensureTables(
        { rn: makeRn(), data: { name: "Ada", age: 37 } },
        false,
      ),
    ).resolves.toContain("__items__users__basic");
    expect(addQueries.some((q) => q.includes("ADD COLUMN `age` INTEGER"))).toBe(
      true,
    );

    const mismatchLayer = makeMysqlLayer({
      _pool: {
        query: async (sql: string) => {
          if (sql.includes("COUNT(*) as cnt")) return [[{ cnt: 1 }], {}];
          return [[], {}];
        },
      },
      _setColumnKind: async () => {},
      _getTableInfo: async () => [{ name: "age", type: "int(11)" }],
    }) as any;

    await expect(
      mismatchLayer.ensureTables(
        { rn: makeRn(), data: { age: "thirty-seven" } },
        false,
      ),
    ).rejects.toThrow("Rejecting change: Column age");
  });

  test("friendly messages include identifiers and RN context", () => {
    const layer = makeMysqlLayer({ identifier: "users@localhost" }) as any;
    const item = { rn: makeRn() };
    expect(layer._friendlyMessage("create", item, "Duplicate entry")).toContain(
      "already exists",
    );
    expect(layer._friendlyMessage("update", item, "boom")).toContain(
      "Failed to update item",
    );
    expect(layer._friendlyDeleteMessage(makeRn())).toContain(
      "does not exist in mysql source layer",
    );
  });

  test("decode rehydrates RN columns via metadata kinds", async () => {
    const layer = makeMysqlLayer() as any;
    const ownerRn = `[rn]:users:basic:${UUID}`;
    const malformed = "[rn]:users:basic:not-a-uuid";
    const decoded = await layer._decodeRowUsingTableInfo(
      {
        owner: ownerRn,
        invalidRef: malformed,
        note: "ok",
      },
      [
        { name: "owner", type: "text", column_type: "text" },
        { name: "invalidRef", type: "text", column_type: "text" },
        { name: "note", type: "text", column_type: "text" },
      ],
      {
        columnKinds: new Map([
          ["owner", "rn"],
          ["invalidRef", "rn"],
        ]),
      },
    );

    expect(decoded.owner).toBeInstanceOf(RN);
    expect(decoded.owner.value()).toBe(ownerRn);
    expect(decoded.invalidRef).toBe(malformed);
    expect(decoded.note).toBe("ok");
  });
});
