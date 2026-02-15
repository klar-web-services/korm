import { describe, expect, setDefaultTimeout, test } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { korm } from "../korm";
import { FloatingItem, UncommittedItem } from "../core/item";
import { BackMan, LockTimeoutError } from "..";
import { BackupEventReader } from "../sources/backups";
import type { QueryBuilder, _QueryComponent } from "../core/query";
import type { ColumnKind } from "../core/columnKind";
import type {
  DbChangeResult,
  DbDeleteResult,
  PersistOptions,
  SourceLayer,
} from "../sources/sourceLayer";
import type { Depot } from "../depot/depot";
import type { FloatingDepotFile } from "../depot/depotFile";
import type { RN } from "../core/rn";

type Car = {
  name?: string;
  profile?: {
    role?: string;
    name?: string;
  };
};

type LockRecord = {
  name: string;
  seq: number;
};

type SecretRecord = {
  name: string;
  secret: Encrypt<string> | string | number;
};

type DepotRecord = {
  name: string;
  attachment: FloatingDepotFile | string;
};

type ResultLike = { isErr(): boolean };

const { eq, inList, like } = korm.qfns;

type JSONable = korm.types.JSONable;
type Encrypt<T extends JSONable> = korm.types.Encrypt<T>;
type Item<T extends JSONable> = korm.types.Item<T>;
type LayerPool = korm.types.LayerPool;
type SqliteLayer = korm.types.SqliteLayer;

setDefaultTimeout(20_000);

const clearedLayers = new Set<string>();
const clearedDepots = new Set<string>();

const quoteIdent = (name: string, quote: string): string => {
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(name)) {
    throw new Error(`Unsafe table name: ${name}`);
  }
  if (quote === "`") return `\`${name.replace(/`/g, "``")}\``;
  return `"${name.replace(/"/g, '""')}"`;
};

const clearSqliteLayer = (layer: SqliteLayer): void => {
  const tables = layer._db
    .prepare(`SELECT name FROM sqlite_master WHERE type='table'`)
    .all() as { name?: string }[];
  for (const row of tables) {
    const name = row.name;
    if (!name || name.startsWith("sqlite_")) continue;
    const safe = quoteIdent(name, '"');
    layer._db.run(`DROP TABLE IF EXISTS ${safe}`);
  }
};

const ensureSqliteLayer = async (pathValue: string): Promise<SqliteLayer> => {
  const key = `sqlite:${pathValue}`;
  const layer = korm.layers.sqlite(pathValue);
  if (!clearedLayers.has(key)) {
    clearSqliteLayer(layer);
    clearedLayers.add(key);
  }
  return layer;
};

const ensurePgLayer = async (configValue: string): Promise<SourceLayer> => {
  return korm.layers.pg(configValue);
};

const ensureLocalDepot = async (root: string): Promise<Depot> => {
  const depot = korm.depots.local(root);
  if (!clearedDepots.has(root)) {
    const rn = korm.rn(`[rn][depot::${depot.identifier}]:*`);
    const files = await depot.listFiles(rn);
    await Promise.all(files.map((file) => depot.deleteFile(file.rn)));
    clearedDepots.add(root);
  }
  return depot;
};

class ThrowOnceLayer implements SourceLayer {
  public readonly type: "sqlite" = "sqlite";
  public readonly identifier: string;
  private _throwOnInsert: boolean;

  constructor(
    private inner: SqliteLayer,
    opts?: { throwOnInsert?: boolean },
  ) {
    this.identifier = inner.identifier;
    this._throwOnInsert = opts?.throwOnInsert ?? false;
  }

  async insertItem<T extends JSONable>(
    item: FloatingItem<T>,
    options?: PersistOptions,
  ): Promise<DbChangeResult<T>> {
    if (this._throwOnInsert) {
      this._throwOnInsert = false;
      throw new Error("Simulated crash during insert");
    }
    return await this.inner.insertItem(item, options);
  }

  async updateItem<T extends JSONable>(
    item: UncommittedItem<T>,
    options?: PersistOptions,
  ): Promise<DbChangeResult<T>> {
    return await this.inner.updateItem(item, options);
  }

  async readItemRaw<T extends JSONable>(rn: RN): Promise<T | undefined> {
    return await this.inner.readItemRaw(rn);
  }

  async deleteItem(rn: RN, options?: PersistOptions): Promise<DbDeleteResult> {
    return await this.inner.deleteItem(rn);
  }

  async executeQuery<T extends JSONable>(query: QueryBuilder<T>) {
    return await this.inner.executeQuery<T>(query as QueryBuilder<JSONable>);
  }

  async ensureTables(
    item: Item<any> | FloatingItem<any> | UncommittedItem<any>,
    destructive: boolean = false,
  ): Promise<string> {
    return await this.inner.ensureTables(item, destructive);
  }

  async getColumnKinds(
    namespace: string,
    kind: string,
  ): Promise<Map<string, ColumnKind>> {
    return await this.inner.getColumnKinds(namespace, kind);
  }

  async close(): Promise<void> {
    await this.inner.close();
  }
}

const makeTempDir = (): string => {
  return fs.mkdtempSync(path.join(os.tmpdir(), "korm-hostile-"));
};

const withSqlitePool = async <T>(
  fn: (pool: LayerPool, layer: SqliteLayer) => Promise<T>,
): Promise<T> => {
  const layer = await ensureSqliteLayer(":memory:");
  const pool = korm.pool().setLayers(korm.use.layer(layer).as("sqlite")).open();
  try {
    return await fn(pool, layer);
  } finally {
    await pool.close();
  }
};

const withPool = async <T>(fn: (pool: LayerPool) => Promise<T>): Promise<T> => {
  return await withSqlitePool((pool) => fn(pool));
};

const createCar = async (pool: LayerPool, data: Car): Promise<Item<Car>> => {
  const result = await korm
    .item<Car>(pool)
    .from.data({
      namespace: "cars",
      kind: "suv",
      data,
    })
    .create();
  if (result.isErr()) {
    throw result.error;
  }
  return result.unwrap();
};

const getCars = (pool: LayerPool, component: _QueryComponent) => {
  return korm
    .item<Car>(pool)
    .from.query(korm.rn("[rn]:cars:suv:*"))
    .where(component)
    .get();
};

const expectQueryFailure = async (
  promise: Promise<ResultLike>,
): Promise<void> => {
  try {
    const result = await promise;
    expect(result.isErr()).toBe(true);
  } catch (error) {
    expect(error).toBeDefined();
  }
};

const withEncryptionKey = async <T>(
  key: string,
  fn: () => Promise<T>,
): Promise<T> => {
  const prior = process.env.KORM_ENCRYPTION_KEY;
  process.env.KORM_ENCRYPTION_KEY = key;
  try {
    return await fn();
  } finally {
    if (prior === undefined) {
      delete process.env.KORM_ENCRYPTION_KEY;
    } else {
      process.env.KORM_ENCRYPTION_KEY = prior;
    }
  }
};

const waitForBackupFiles = async (
  depot: Depot,
  rn: RN,
  minCount: number,
  timeoutMs: number = 2_000,
) => {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    const files = await depot.listFiles(rn);
    if (files.length >= minCount) return files;
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
  return await depot.listFiles(rn);
};

describe("hostile API probes", () => {
  test("treats SQL injection payloads as literal values", async () => {
    await withPool(async (pool) => {
      await createCar(pool, { name: "alice", profile: { role: "user" } });
      await createCar(pool, { name: "bob", profile: { role: "admin" } });

      const eqResult = await getCars(pool, eq("name", "' OR 1=1 --"));
      expect(eqResult.isOk()).toBe(true);
      expect(eqResult.unwrap()).toHaveLength(0);

      const likeResult = await getCars(pool, like("name", "%' OR 1=1 --"));
      expect(likeResult.isOk()).toBe(true);
      expect(likeResult.unwrap()).toHaveLength(0);

      const inResult = await getCars(
        pool,
        inList("name", [
          "alice",
          "bob",
          "'); DROP TABLE __items__cars__suv;--",
        ]),
      );
      expect(inResult.isOk()).toBe(true);
      const names = inResult
        .unwrap()
        .map((item) => item.data?.name ?? "")
        .sort();
      expect(names).toEqual(["alice", "bob"]);

      const jsonResult = await getCars(
        pool,
        eq("profile.role", "admin' OR 1=1 --"),
      );
      expect(jsonResult.isOk()).toBe(true);
      expect(jsonResult.unwrap()).toHaveLength(0);

      const sanity = await getCars(pool, eq("name", "alice"));
      expect(sanity.isOk()).toBe(true);
      expect(sanity.unwrap()).toHaveLength(1);
    });
  });

  test("rejects identifier injection in query properties", async () => {
    await withPool(async (pool) => {
      await createCar(pool, { name: "alice" });

      await expectQueryFailure(
        getCars(pool, eq('name"; DROP TABLE __items__cars__suv;--', "alice")),
      );

      const sanity = await getCars(pool, eq("name", "alice"));
      expect(sanity.isOk()).toBe(true);
      expect(sanity.unwrap()).toHaveLength(1);
    });
  });

  test("rejects JSON path injection attempts", async () => {
    await withPool(async (pool) => {
      await createCar(pool, { profile: { name: "alice" } });

      await expectQueryFailure(
        getCars(
          pool,
          eq("profile.name); DROP TABLE __items__cars__suv;--", "alice"),
        ),
      );
    });
  });

  test("rejects hostile column names on insert", async () => {
    await withPool(async (pool) => {
      const badResult = await korm
        .item(pool)
        .from.data({
          namespace: "cars",
          kind: "suv",
          data: {
            'name"; DROP TABLE __items__cars__suv;--': "evil",
          },
        })
        .create();
      expect(badResult.isErr()).toBe(true);

      const goodResult = await korm
        .item(pool)
        .from.data({
          namespace: "cars",
          kind: "suv",
          data: { name: "good" },
        })
        .create();
      expect(goodResult.isOk()).toBe(true);
    });
  });

  test("rejects malicious namespace/kind inputs", async () => {
    await withPool(async (pool) => {
      const badNamespace = korm.item(pool).from.data({
        namespace: "cars;drop",
        kind: "suv",
        data: { name: "evil" },
      });
      expect(badNamespace.isErr()).toBe(true);

      const badKind = korm.item(pool).from.data({
        namespace: "cars",
        kind: "suv;drop",
        data: { name: "evil" },
      });
      expect(badKind.isErr()).toBe(true);
    });
  });

  test("rejects RN strings with injected segments", () => {
    expect(() => korm.rn("[rn]:cars:suv:*;DROP TABLE")).toThrow();
    expect(() => korm.rn("[rn][from::pg]]:cars:suv:*")).toThrow();
  });

  test("rejects depot RN path traversal segments", () => {
    expect(() => korm.rn("[rn][depot::files]:..:secrets.txt")).toThrow();
    expect(() => korm.rn("[rn][depot::files]:safe:..:secrets.txt")).toThrow();
    expect(() => korm.rn("[rn][depot::files]:safe:../secrets.txt")).toThrow();
    expect(() =>
      korm.rn("[rn][depot::files]:safe:..\\\\secrets.txt"),
    ).toThrow();
  });

  test("rejects resolvePaths with unsafe keys", async () => {
    await withPool(async (pool) => {
      const created = await korm
        .item<any>(pool)
        .from.data({
          namespace: "proto",
          kind: "case",
          data: { name: "alice" },
        })
        .create();
      expect(created.isOk()).toBe(true);
      const item = created.unwrap();

      const readProto = await korm
        .item<any>(pool)
        .from.rn(item.rn!, korm.resolve("__proto__.polluted"));
      expect(readProto.isErr()).toBe(true);

      const readCtor = await korm
        .item<any>(pool)
        .from.rn(item.rn!, korm.resolve("constructor.prototype.polluted"));
      expect(readCtor.isErr()).toBe(true);
    });
  });

  test("wildcard resolvePaths skips unsafe keys in data", async () => {
    await withPool(async (pool) => {
      const ownerResult = await korm
        .item<any>(pool)
        .from.data({
          namespace: "refs",
          kind: "target",
          data: { name: "owner" },
        })
        .create();
      const protoResult = await korm
        .item<any>(pool)
        .from.data({
          namespace: "refs",
          kind: "target",
          data: { polluted: "yes" },
        })
        .create();
      expect(ownerResult.isOk()).toBe(true);
      expect(protoResult.isOk()).toBe(true);

      const owner = ownerResult.unwrap();
      const proto = protoResult.unwrap();

      const refs = Object.create(null) as Record<string, any>;
      refs.owner = owner.rn!;
      Object.defineProperty(refs, "__proto__", {
        value: proto.rn!.value(),
        enumerable: true,
        writable: true,
        configurable: true,
      });

      const holder = await korm
        .item<any>(pool)
        .from.data({
          namespace: "refs",
          kind: "holder",
          data: { refs },
        })
        .create();
      expect(holder.isOk()).toBe(true);

      const read = await korm
        .item<any>(pool)
        .from.rn(holder.unwrap().rn!, korm.resolve("refs.*"));
      expect(read.isOk()).toBe(true);
      const data = read.unwrap().data as any;

      expect(data.refs.owner?.name).toBe("owner");
      expect(Object.prototype.hasOwnProperty.call(data.refs, "__proto__")).toBe(
        true,
      );
      expect(data.refs["__proto__"]).toBe(proto.rn!.value());
      expect((data.refs as any).polluted).toBeUndefined();
      expect(({} as any).polluted).toBeUndefined();
    });
  });

  test("rejects duplicate get option kinds", async () => {
    await withPool(async (pool) => {
      const created = await korm
        .item<any>(pool)
        .from.data({
          namespace: "dupes",
          kind: "opts",
          data: { name: "alice", score: 1 },
        })
        .create();
      expect(created.isOk()).toBe(true);

      const rn = korm.rn("[rn]:dupes:opts:*");
      const read = await korm
        .item<any>(pool)
        .from.query(rn)
        .get(korm.first(), korm.first(2));
      expect(read.isErr()).toBe(true);
    });
  });

  test("rejects sortBy wildcard paths", async () => {
    await withPool(async (pool) => {
      const created = await korm
        .item<any>(pool)
        .from.data({
          namespace: "sort",
          kind: "wild",
          data: { refs: [{ name: "alice" }] },
        })
        .create();
      expect(created.isOk()).toBe(true);

      const rn = korm.rn("[rn]:sort:wild:*");
      const read = await korm
        .item<any>(pool)
        .from.query(rn)
        .get(korm.sortBy("refs[*].name", "asc"));
      expect(read.isErr()).toBe(true);
    });
  });

  test("from.rn rejects unsupported query-only options at runtime", async () => {
    await withPool(async (pool) => {
      const created = await korm
        .item<any>(pool)
        .from.data({
          namespace: "strict",
          kind: "rnopts",
          data: { name: "alice" },
        })
        .create();
      expect(created.isOk()).toBe(true);

      const read = await (korm.item<any>(pool).from.rn as any)(
        created.unwrap().rn!,
        korm.first(),
      );
      expect(read.isErr()).toBe(true);
    });
  });

  test("serializes concurrent tx persists on shared RNs without deadlock", async () => {
    await withPool(async (pool) => {
      const createdA = await korm
        .item<LockRecord>(pool)
        .from.data({
          namespace: "locks",
          kind: "pair",
          data: { name: "A", seq: 0 },
        })
        .create();
      const createdB = await korm
        .item<LockRecord>(pool)
        .from.data({
          namespace: "locks",
          kind: "pair",
          data: { name: "B", seq: 0 },
        })
        .create();

      expect(createdA.isOk()).toBe(true);
      expect(createdB.isOk()).toBe(true);

      const itemA = createdA.unwrap();
      const itemB = createdB.unwrap();

      const tasks = Array.from({ length: 50 }, (_, index) => {
        const updateA = new UncommittedItem(pool, itemA.rn!, {
          name: "A",
          seq: index,
        });
        const updateB = new UncommittedItem(pool, itemB.rn!, {
          name: "B",
          seq: index,
        });
        const tx =
          index % 2 === 0
            ? korm.tx(updateA, updateB)
            : korm.tx(updateB, updateA);
        return tx.persist();
      });

      const results = await Promise.all(tasks);
      for (const result of results) {
        expect(result.isErr()).toBe(false);
      }

      const readA = await korm.item<LockRecord>(pool).from.rn(itemA.rn!);
      const readB = await korm.item<LockRecord>(pool).from.rn(itemB.rn!);
      expect(readA.isOk()).toBe(true);
      expect(readB.isOk()).toBe(true);
    });
  });

  test("prevents double-create with identical RN under concurrency", async () => {
    await withPool(async (pool) => {
      const id = randomUUID();
      const rn = korm.rn(`[rn]:cars:suv:${id}`);

      const attempts = Array.from({ length: 10 }, (_, index) => {
        const item = new FloatingItem(pool, rn, { name: "dup", seq: index });
        return korm.tx(item).persist();
      });

      const results = await Promise.all(attempts);
      const successCount = results.filter((result) => result.isOk()).length;
      expect(successCount).toBe(1);

      const read = await korm.item<LockRecord>(pool).from.rn(rn);
      expect(read.isOk()).toBe(true);
    });
  });

  test("shared lock layer blocks cross-pool contention", async () => {
    const root = makeTempDir();
    const dbPath = path.join(root, "locks.sqlite");
    const layerA = await ensureSqliteLayer(dbPath);
    const layerB = await ensureSqliteLayer(dbPath);
    const poolA = korm
      .pool()
      .setLayers(korm.use.layer(layerA).as("sqlite"))
      .withLocks(korm.target.layer("sqlite"), { ttlMs: 2_000, retryMs: 10 })
      .open();
    const poolB = korm
      .pool()
      .setLayers(korm.use.layer(layerB).as("sqlite"))
      .withLocks(korm.target.layer("sqlite"), { ttlMs: 2_000, retryMs: 10 })
      .open();

    const rn = korm.rn(`[rn]:locks:shared:${randomUUID()}`);
    const release = await poolA.locker.acquire(rn, 500);

    let timedOut = false;
    try {
      await poolB.locker.acquire(rn, 100);
    } catch (error) {
      timedOut = error instanceof LockTimeoutError;
    }
    expect(timedOut).toBe(true);

    release();
    const release2 = await poolB.locker.acquire(rn, 500);
    release2();

    await poolA.close();
    await poolB.close();
  });

  test("encryption payload tampering does not leak plaintext", async () => {
    await withSqlitePool(async (pool, layer) => {
      await withEncryptionKey(
        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        async () => {
          const created = await korm
            .item<SecretRecord>(pool)
            .from.data({
              namespace: "vault",
              kind: "record",
              data: {
                name: "alice",
                secret: await korm.encrypt("topsecret"),
              },
            })
            .create();
          expect(created.isOk()).toBe(true);
          const item = created.unwrap();

          const rawTable = "__items__vault__record";
          const tampered = JSON.stringify({
            __ENCRYPTED__: true,
            type: "password",
            value: "not-a-hash",
          });

          layer._db.run(
            `UPDATE "${rawTable}" SET "secret" = ? WHERE "rnId" = ?`,
            [tampered, item.rn!.id!],
          );

          const read = await korm.item<SecretRecord>(pool).from.rn(item.rn!);
          expect(read.isOk()).toBe(true);
          const readItem = read.unwrap();
          expect(readItem.data).toBeDefined();
          const secret = readItem.data!.secret as Encrypt<string>;
          expect(secret.reveal()).not.toBe("topsecret");
        },
      );
    });
  });

  test("tampered symmetric payload fails closed", async () => {
    await withSqlitePool(async (pool, layer) => {
      await withEncryptionKey(
        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        async () => {
          const created = await korm
            .item<SecretRecord>(pool)
            .from.data({
              namespace: "vault",
              kind: "record",
              data: {
                name: "bob",
                secret: await korm.encrypt("supersecret"),
              },
            })
            .create();
          expect(created.isOk()).toBe(true);
          const item = created.unwrap();

          const rawTable = "__items__vault__record";
          const tampered = JSON.stringify({
            __ENCRYPTED__: true,
            type: "symmetric",
            value: "deadbeef",
          });

          layer._db.run(
            `UPDATE "${rawTable}" SET "secret" = ? WHERE "rnId" = ?`,
            [tampered, item.rn!.id!],
          );

          await expectQueryFailure(
            korm.item<SecretRecord>(pool).from.rn(item.rn!),
          );
        },
      );
    });
  });

  test("recovers cross-layer tx after simulated crash with WAL", async () => {
    const root = makeTempDir();
    const walRoot = path.join(root, "wal");
    const dbAPath = path.join(root, "layer-a.sqlite");
    const dbBPath = path.join(root, "layer-b.sqlite");

    const layerA = await ensureSqliteLayer(dbAPath);
    const layerB = await ensureSqliteLayer(dbBPath);
    const flakyLayer = new ThrowOnceLayer(layerB, { throwOnInsert: true });
    const walDepot = await ensureLocalDepot(walRoot);

    const pool = korm
      .pool()
      .setLayers(
        korm.use.layer(layerA).as("a"),
        korm.use.layer(flakyLayer).as("b"),
      )
      .setDepots(korm.use.depot(walDepot).as("wal"))
      .withWal({
        depotIdent: "wal",
        walNamespace: "hostile-wal",
        retention: "keep",
      })
      .open();

    const itemA = korm
      .item<LockRecord>(pool)
      .from.data({
        namespace: "atoms",
        kind: "a",
        mods: [{ key: "from", value: "a" }],
        data: { name: "A", seq: 1 },
      })
      .unwrap();
    const itemB = korm
      .item<LockRecord>(pool)
      .from.data({
        namespace: "atoms",
        kind: "b",
        mods: [{ key: "from", value: "b" }],
        data: { name: "B", seq: 1 },
      })
      .unwrap();

    let crashed = false;
    try {
      await korm.tx(itemA, itemB).persist();
    } catch {
      crashed = true;
    }
    expect(crashed).toBe(true);

    const partialA = await korm.item<LockRecord>(pool).from.rn(itemA.rn!);
    expect(partialA.isOk()).toBe(true);

    const wal = pool.wal!;
    const pendingPrefix = korm.rn(
      `[rn][depot::wal]:__korm_wal__:${wal.namespace}:${wal.poolId}:pending:*`,
    );
    const pendingFiles = await walDepot.listFiles(pendingPrefix);
    expect(pendingFiles.length).toBeGreaterThan(0);

    await pool.close();

    const layerA2 = await ensureSqliteLayer(dbAPath);
    const layerB2 = await ensureSqliteLayer(dbBPath);
    const pool2 = korm
      .pool()
      .setLayers(
        korm.use.layer(layerA2).as("a"),
        korm.use.layer(layerB2).as("b"),
      )
      .setDepots(korm.use.depot(walDepot).as("wal"))
      .withWal({
        depotIdent: "wal",
        walNamespace: wal.namespace,
        retention: "keep",
      })
      .open();

    const recoveredA = await korm.item<LockRecord>(pool2).from.rn(itemA.rn!);
    const recoveredB = await korm.item<LockRecord>(pool2).from.rn(itemB.rn!);
    expect(recoveredA.isOk()).toBe(true);
    expect(recoveredB.isOk()).toBe(true);

    const pendingAfter = await walDepot.listFiles(pendingPrefix);
    expect(pendingAfter.length).toBe(0);

    await pool2.close();
  });

  const pgTest = process.env.TESTING_PG_URL ? test : test.skip;

  pgTest(
    "destructive schema change keeps __korm_meta__ in sync (pg)",
    async () => {
      await withEncryptionKey(
        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        async () => {
          const namespace = "meta";
          const kind = `probe${randomUUID().replace(/-/g, "").slice(0, 8)}`;
          const pgLayer = await ensurePgLayer(process.env.TESTING_PG_URL!);
          const pgPrivate = pgLayer as unknown as {
            _db: {
              unsafe: (query: string, values?: unknown[]) => Promise<unknown[]>;
            };
            _domainsAvailable: boolean;
            _domainsEnsured: boolean;
          };

          pgPrivate._domainsAvailable = false;
          pgPrivate._domainsEnsured = true;

          const pool = korm
            .pool()
            .setLayers(korm.use.layer(pgLayer).as("pg"))
            .open();

          try {
            const created = await korm
              .item<SecretRecord>(pool)
              .from.data({
                namespace,
                kind,
                mods: [{ key: "from", value: "pg" }],
                data: {
                  name: "row",
                  secret: await korm.encrypt("alpha"),
                },
              })
              .create();
            expect(created.isOk()).toBe(true);
            const item = created.unwrap();

            const update = item.update({ secret: 42 }).unwrap();
            const result = await korm.tx(update).persist({ destructive: true });
            expect(result.isOk()).toBe(true);

            const rawTable = `__items__${namespace}__${kind}`;
            const metaRows = await pgPrivate._db.unsafe(
              `SELECT kind FROM "__korm_meta__" WHERE table_name = $1 AND column_name = $2`,
              [rawTable, "secret"],
            );
            expect(metaRows.length).toBe(0);

            const columns = await pgPrivate._db.unsafe(
              `SELECT data_type FROM information_schema.columns WHERE table_name = $1 AND column_name = $2`,
              [rawTable, "secret"],
            );
            const column = columns[0] as { data_type?: unknown } | undefined;
            const dataType = String(column?.data_type ?? "").toLowerCase();
            expect(dataType).toBe("integer");
          } finally {
            await pool.close();
          }
        },
      );
    },
  );

  test("WAL depotOps replays pending file writes (shadowing probe)", async () => {
    const root = makeTempDir();
    const walRoot = path.join(root, "wal");
    const dataRoot = path.join(root, "data");
    const dbPath = path.join(root, "files.sqlite");

    const dataDepot = await ensureLocalDepot(dataRoot);
    const walDepot = await ensureLocalDepot(walRoot);
    const layer = await ensureSqliteLayer(dbPath);
    const flakyLayer = new ThrowOnceLayer(layer, { throwOnInsert: true });

    const pool = korm
      .pool()
      .setLayers(korm.use.layer(flakyLayer).as("sqlite"))
      .setDepots(
        korm.use.depot(dataDepot).as("data"),
        korm.use.depot(walDepot).as("wal"),
      )
      .withWal({
        depotIdent: "wal",
        walNamespace: "hostile-depot",
        retention: "keep",
        depotOps: "record",
      })
      .open();

    const rn = korm.rn("[rn][depot::data]:files:shared.txt");
    const fileA = korm.file({ rn, file: new Blob(["alpha"]) });
    const fileB = korm.file({ rn, file: new Blob(["beta"]) });

    const itemA = korm
      .item<DepotRecord>(pool)
      .from.data({
        namespace: "files",
        kind: "blob",
        data: { name: "one", attachment: fileA },
      })
      .unwrap();
    const itemB = korm
      .item<DepotRecord>(pool)
      .from.data({
        namespace: "files",
        kind: "blob",
        data: { name: "two", attachment: fileB },
      })
      .unwrap();

    let crashed = false;
    try {
      await korm.tx(itemA).persist();
    } catch {
      crashed = true;
    }
    expect(crashed).toBe(true);

    const second = await korm.tx(itemB).persist();
    expect(second.isErr()).toBe(false);

    const preRecovery = await dataDepot.getFile(rn);
    const preText = await preRecovery.text();
    expect(["alpha", "beta"]).toContain(preText);

    await pool.close();

    const layer2 = await ensureSqliteLayer(dbPath);
    const pool2 = korm
      .pool()
      .setLayers(korm.use.layer(layer2).as("sqlite"))
      .setDepots(
        korm.use.depot(dataDepot).as("data"),
        korm.use.depot(walDepot).as("wal"),
      )
      .withWal({
        depotIdent: "wal",
        walNamespace: "hostile-depot",
        retention: "keep",
        depotOps: "record",
      })
      .open();

    await pool2.ensureWalReady();

    const postRecovery = await dataDepot.getFile(rn);
    const postText = await postRecovery.text();
    expect(postText).toBe("alpha");

    await pool2.close();
  });

  test("backups sanitize layer identifiers to block path traversal", async () => {
    const root = makeTempDir();
    const depotRoot = path.join(root, "backups");
    const dbPath = path.join(root, "backup.sqlite");

    const depot = await ensureLocalDepot(depotRoot);
    const layer = await ensureSqliteLayer(dbPath);
    const evilIdent = "../evil:layer[*]";

    const pool = korm
      .pool()
      .setLayers(korm.use.layer(layer).as(evilIdent))
      .setDepots(korm.use.depot(depot).as("backup"))
      .withMeta(korm.target.layer(evilIdent))
      .open();

    try {
      const manager = new BackMan();
      manager.addInterval(evilIdent, korm.interval.every("minute").runNow());
      pool.configureBackups("backup", manager);

      const prefix = korm.rn("[rn][depot::backup]:__korm_backups__:*");
      const files = await waitForBackupFiles(depot, prefix, 1);
      expect(files.length).toBe(1);

      const parts = files[0]!.rn.depotParts() ?? [];
      expect(parts[0]).toBe("__korm_backups__");
      expect(parts[1]).toMatch(/^layer-[0-9a-f]{12}$/);
      expect(parts[1]).not.toContain("..");
      expect(files[0]!.rn.value()).not.toContain(evilIdent);
    } finally {
      await pool.close();
    }
  });

  test("backups avoid leaking encrypted plaintext", async () => {
    const root = makeTempDir();
    const depotRoot = path.join(root, "backups");
    const dbPath = path.join(root, "backup.sqlite");

    const depot = await ensureLocalDepot(depotRoot);
    const layer = await ensureSqliteLayer(dbPath);

    const pool = korm
      .pool()
      .setLayers(korm.use.layer(layer).as("sqlite"))
      .setDepots(korm.use.depot(depot).as("backup"))
      .withMeta(korm.target.layer("sqlite"))
      .open();

    const secretValue = `backup-secret-${randomUUID()}`;

    try {
      await withEncryptionKey(
        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        async () => {
          const created = await korm
            .item<SecretRecord>(pool)
            .from.data({
              namespace: "vault",
              kind: "backup",
              data: {
                name: "alpha",
                secret: await korm.encrypt(secretValue),
              },
            })
            .create();
          expect(created.isOk()).toBe(true);
        },
      );

      const manager = new BackMan();
      manager.addInterval("sqlite", korm.interval.every("minute").runNow());
      pool.configureBackups("backup", manager);

      const prefix = korm.rn("[rn][depot::backup]:__korm_backups__:*");
      const files = await waitForBackupFiles(depot, prefix, 1);
      expect(files.length).toBe(1);

      const text = await files[0]!.text();
      expect(text).not.toContain(secretValue);
      expect(text).not.toContain('"__ENCRYPT__"');
      const events = text
        .split("\n")
        .map((line) => line.trim())
        .filter(Boolean)
        .map(
          (line) =>
            JSON.parse(line) as {
              t?: string;
              table?: string;
              row?: { secret?: string };
            },
        );
      const rowEvent = events.find(
        (event) =>
          event.t === "row" && event.table === "__items__vault__backup",
      );
      const secretRaw = rowEvent?.row?.secret;
      expect(typeof secretRaw).toBe("string");
      const secretPayload = JSON.parse(secretRaw ?? "{}") as {
        __ENCRYPTED__?: boolean;
      };
      expect(secretPayload.__ENCRYPTED__).toBe(true);
    } finally {
      await pool.close();
    }
  });

  test("backups stream large tables without buffering", async () => {
    const root = makeTempDir();
    const depotRoot = path.join(root, "backups");
    const dbPath = path.join(root, "backup.sqlite");

    const depot = await ensureLocalDepot(depotRoot);
    const layer = await ensureSqliteLayer(dbPath);

    const pool = korm
      .pool()
      .setLayers(korm.use.layer(layer).as("sqlite"))
      .setDepots(korm.use.depot(depot).as("backup"))
      .withMeta(korm.target.layer("sqlite"))
      .open();

    const suffix = randomUUID().replace(/-/g, "");
    const namespace = `bulk${suffix}`;
    const kind = `rows${suffix}`;
    const tableName = `__items__${namespace}__${kind}`;
    const hugeCount = 3000;

    try {
      const created = await korm
        .item<{ label: string; score: number }>(pool)
        .from.data({
          namespace,
          kind,
          data: { label: "seed", score: 0 },
        })
        .create();
      expect(created.isOk()).toBe(true);

      const stmt = layer._db.prepare(
        `INSERT INTO "${tableName}" ("rnId", "label", "score") VALUES (?, ?, ?)`,
      );
      for (let i = 0; i < hugeCount; i += 1) {
        stmt.run(randomUUID(), `bulk-${i}`, i + 1);
      }

      const manager = new BackMan();
      manager.addInterval("sqlite", korm.interval.every("minute").runNow());
      pool.configureBackups("backup", manager);

      const prefix = korm.rn("[rn][depot::backup]:__korm_backups__:*");
      const files = await waitForBackupFiles(depot, prefix, 1, 10_000);
      expect(files.length).toBeGreaterThan(0);

      const reader = new BackupEventReader(files[0]!.stream());
      const header = await reader.next();
      expect(header && header.t === "header").toBe(true);

      let rowCount = 0;
      while (true) {
        const event = await reader.next();
        if (!event) break;
        if (event.t === "row" && event.table === tableName) {
          rowCount += 1;
        }
      }
      expect(rowCount).toBe(hugeCount + 1);
    } finally {
      await pool.close();
    }
  }, 30_000);

  test("backups use shared locks to prevent duplicate runs", async () => {
    const root = makeTempDir();
    const depotRoot = path.join(root, "backups");
    const dbPath = path.join(root, "backup.sqlite");

    const depot = await ensureLocalDepot(depotRoot);
    const layerA = await ensureSqliteLayer(dbPath);
    const layerB = await ensureSqliteLayer(dbPath);

    const poolA = korm
      .pool()
      .setLayers(korm.use.layer(layerA).as("sqlite"))
      .setDepots(korm.use.depot(depot).as("backup"))
      .withMeta(korm.target.layer("sqlite"))
      .open();

    const poolB = korm
      .pool()
      .setLayers(korm.use.layer(layerB).as("sqlite"))
      .setDepots(korm.use.depot(depot).as("backup"))
      .withMeta(korm.target.layer("sqlite"))
      .open();

    try {
      const managerA = new BackMan();
      managerA.addInterval("sqlite", korm.interval.every("minute").runNow());
      poolA.configureBackups("backup", managerA);

      const managerB = new BackMan();
      managerB.addInterval("sqlite", korm.interval.every("minute").runNow());
      poolB.configureBackups("backup", managerB);

      const prefix = korm.rn("[rn][depot::backup]:__korm_backups__:*");
      const files = await waitForBackupFiles(depot, prefix, 1);
      expect(files.length).toBe(1);
    } finally {
      await poolA.close();
      await poolB.close();
    }
  });
});
