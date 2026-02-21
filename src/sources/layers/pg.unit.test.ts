import { describe, expect, test } from "bun:test";
import { and, eq, inList, not } from "../../core/queryFns";
import { RN } from "../../core/rn";
import { Unique } from "../../core/unique";
import { PgLayer } from "./pg";

const UUID = "3dd91ede-37a4-4c25-a86a-6f1a9e132186";

function makeRn(): RN {
  return RN.create("users", "basic", UUID).unwrap();
}

function makePgLayer(overrides: Record<string, unknown> = {}): PgLayer {
  const layer = Object.create(PgLayer.prototype) as any;
  layer._db = {
    unsafe: async () => [] as unknown,
    end: async () => {},
  };
  layer.type = "pg";
  layer.identifier = "app@localhost";
  layer._connectionInput = "postgres://localhost/app";
  layer._domainsEnsured = true;
  layer._domainsAvailable = true;
  layer._columnKindsCache = new Map();
  layer._tableInfoCache = new Map();
  layer._metaEnsured = true;
  layer._schemaVersion = 0;
  Object.assign(layer, overrides);
  return layer as PgLayer;
}

describe("PgLayer helpers", () => {
  test("derives identifiers from urls/options and validates identifiers", () => {
    const layer = makePgLayer() as any;
    expect(layer._deriveIdentifier("postgres://db.local:5432/korm")).toBe(
      "korm@db.local",
    );
    expect(layer._deriveIdentifier("not-a-url")).toBe("pg@localhost");
    expect(
      layer._deriveIdentifier({ database: "main", host: "example.internal" }),
    ).toBe("main@example.internal");
    expect(() => layer._quoteIdent("unsafe-name;DROP")).toThrow(
      "Unsafe identifier",
    );
    expect(layer._quoteIdent("safe_name")).toBe('"safe_name"');
  });

  test("stamps schema version and bumps it", async () => {
    const queries: Array<{ sql: string; values: unknown[] | undefined }> = [];
    const layer = makePgLayer({
      _schemaVersion: 7,
      _db: {
        unsafe: async (sql: string, values?: unknown[]) => {
          queries.push({ sql, values });
          return [{ ok: true }];
        },
        end: async () => {},
      },
    }) as any;

    await layer._unsafe("SELECT 1");
    await layer._unsafe("SELECT $1", [99]);
    expect(queries[0]?.sql).toContain("korm_schema:7");
    expect(queries[1]?.values).toEqual([99]);
    layer._bumpSchemaVersion();
    expect(layer._schemaVersion).toBe(8);
  });

  test("builds query SQL for JSON paths, IN clauses, and NOT groups", () => {
    const layer = makePgLayer() as any;
    const root = and(
      inList("meta.count", [1, 2, null]),
      eq("status", "ok"),
      not(eq("deletedAt", null)),
    );
    const built = layer._buildQueryString(root, "__items__users__basic", [
      "status",
      "status",
    ]);
    expect(built.sql).toContain('SELECT "rnId", "status"');
    expect(built.sql).toContain("double precision");
    expect(built.sql).toContain("IS NULL");
    expect(built.params).toEqual([1, 2, "ok"]);

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

  test("ensureTables creates new tables and bumps schema version", async () => {
    const queries: string[] = [];
    const layer = makePgLayer({
      _ensureDomains: async () => {},
      _unsafe: async (sql: string) => {
        queries.push(sql);
        if (sql.includes("SELECT EXISTS")) return [{ e: false }];
        return [];
      },
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

    expect(result).toBe("__items__users__basic");
    expect(queries.some((q) => q.includes("CREATE TABLE IF NOT EXISTS"))).toBe(
      true,
    );
    expect(layer._schemaVersion).toBe(1);
  });

  test("ensureTables creates unique shadow columns and indexes", async () => {
    const queries: string[] = [];
    const layer = makePgLayer({
      _ensureDomains: async () => {},
      _unsafe: async (sql: string) => {
        queries.push(sql);
        if (sql.includes("SELECT EXISTS")) return [{ e: false }];
        return [];
      },
    }) as any;

    const result = await layer.ensureTables(
      {
        rn: makeRn(),
        data: {
          make: "Toyota",
          vin: new Unique("vin-1"),
        },
      },
      false,
    );

    expect(result).toBe("__items__users__basic");
    expect(queries.some((sql) => sql.includes("__korm_unique__vin"))).toBe(
      true,
    );
    expect(queries.some((sql) => sql.includes("CREATE UNIQUE INDEX"))).toBe(
      true,
    );
  });

  test("ensureTables adds columns and rejects mismatches unless destructive", async () => {
    const addQueries: string[] = [];
    const layer = makePgLayer({
      _ensureDomains: async () => {},
      _unsafe: async (sql: string) => {
        addQueries.push(sql);
        if (sql.includes("SELECT EXISTS")) return [{ e: true }];
        return [];
      },
      _getTableInfo: async () => [{ name: "name", dataType: "TEXT" }],
      _clearPlanCache: async () => {},
    }) as any;

    await expect(
      layer.ensureTables(
        { rn: makeRn(), data: { name: "Ada", age: 37 } },
        false,
      ),
    ).resolves.toBe("__items__users__basic");
    expect(addQueries.some((q) => q.includes('ADD COLUMN "age" INTEGER'))).toBe(
      true,
    );

    const mismatchLayer = makePgLayer({
      _ensureDomains: async () => {},
      _unsafe: async (sql: string) => {
        if (sql.includes("SELECT EXISTS")) return [{ e: true }];
        return [];
      },
      _getTableInfo: async () => [{ name: "age", dataType: "INTEGER" }],
      _clearPlanCache: async () => {},
    }) as any;

    await expect(
      mismatchLayer.ensureTables(
        { rn: makeRn(), data: { age: "thirty-seven" } },
        false,
      ),
    ).rejects.toThrow("Rejecting change: Column age");
  });

  test("friendly messages include identifiers and RN context", () => {
    const layer = makePgLayer({ identifier: "users@localhost" }) as any;
    const item = { rn: makeRn() };
    expect(layer._friendlyMessage("create", item, "duplicate key")).toContain(
      "unique field constraint violated",
    );
    expect(layer._friendlyMessage("update", item, "boom")).toContain(
      "Failed to update item",
    );
    expect(layer._friendlyDeleteMessage(makeRn())).toContain(
      "does not exist in pg source layer",
    );
  });

  test("decode rehydrates RN domain values", async () => {
    const layer = makePgLayer() as any;
    const ownerRn = `[rn]:users:basic:${UUID}`;
    const malformed = "[rn]:users:basic:not-a-uuid";
    const decoded = await layer._decodeRowUsingTableInfo(
      {
        owner: ownerRn,
        invalidRef: malformed,
        note: "ok",
      },
      [
        {
          name: "owner",
          dataType: "text",
          domainName: "korm_rn_ref_text",
          udtName: "korm_rn_ref_text",
        },
        {
          name: "invalidRef",
          dataType: "text",
          domainName: "korm_rn_ref_text",
          udtName: "korm_rn_ref_text",
        },
        { name: "note", dataType: "text" },
      ],
    );

    expect(decoded.owner).toBeInstanceOf(RN);
    expect(decoded.owner.value()).toBe(ownerRn);
    expect(decoded.invalidRef).toBe(malformed);
    expect(decoded.note).toBe("ok");
  });

  test("does not keep empty table info cache entries", async () => {
    let unsafeCalls = 0;
    const layer = makePgLayer({
      _unsafe: async () => {
        unsafeCalls += 1;
        if (unsafeCalls === 1) return [];
        return [{ name: "rnId", data_type: "text", udt_name: "text" }];
      },
    }) as any;

    const first = await layer._getTableInfo("__items__users__basic");
    expect(first).toEqual([]);

    const second = await layer._getTableInfo("__items__users__basic");
    expect(second).toEqual([
      {
        name: "rnId",
        dataType: "text",
        udtName: "text",
        domainName: undefined,
      },
    ]);

    const third = await layer._getTableInfo("__items__users__basic");
    expect(third).toEqual([
      {
        name: "rnId",
        dataType: "text",
        udtName: "text",
        domainName: undefined,
      },
    ]);
    expect(unsafeCalls).toBe(2);
  });
});
