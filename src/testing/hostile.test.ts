import { afterAll, beforeAll, describe, expect, setDefaultTimeout, test } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { korm } from "../korm";
import { FloatingItem, UncommittedItem } from "../core/item";
import { BackMan, LockTimeoutError } from "..";
import type { Encrypt, Item, LayerPool, SqliteLayer } from "..";
import type { JSONable } from "../korm";
import type { QueryBuilder, _QueryComponent } from "../core/query";
import type { ColumnKind } from "../core/columnKind";
import type { DbChangeResult, DbDeleteResult, PersistOptions, SourceLayer } from "../sources/sourceLayer";
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

setDefaultTimeout(20_000);

type LayerCleanup =
    | { type: "sqlite"; path: string }
    | { type: "pg"; config: string }
    | { type: "mysql"; config: string };

const trackedLayers = new Map<string, LayerCleanup>();
const trackedDepots = new Map<string, string>();
const clearedLayers = new Set<string>();
const clearedDepots = new Set<string>();

const layerKey = (config: LayerCleanup): string => {
    switch (config.type) {
        case "sqlite":
            return `sqlite:${config.path}`;
        case "pg":
            return `pg:${config.config}`;
        case "mysql":
            return `mysql:${config.config}`;
    }
};

const trackLayer = (config: LayerCleanup): void => {
    trackedLayers.set(layerKey(config), config);
};

const trackDepot = (root: string): void => {
    trackedDepots.set(root, root);
};

const quoteIdent = (name: string, quote: string): string => {
    if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(name)) {
        throw new Error(`Unsafe table name: ${name}`);
    }
    if (quote === "`") return `\`${name.replace(/`/g, "``")}\``;
    return `"${name.replace(/"/g, '""')}"`;
};

const clearSqliteLayer = (layer: SqliteLayer): void => {
    const tables = layer._db.prepare(`SELECT name FROM sqlite_master WHERE type='table'`).all() as { name?: string }[];
    for (const row of tables) {
        const name = row.name;
        if (!name || name.startsWith("sqlite_")) continue;
        const safe = quoteIdent(name, "\"");
        layer._db.run(`DROP TABLE IF EXISTS ${safe}`);
    }
};

const clearPgLayer = async (layer: SourceLayer): Promise<void> => {
    const pg = layer as unknown as { _db: { unsafe: (query: string, values?: unknown[]) => Promise<unknown[]> } };
    const tables = await pg._db.unsafe(
        `SELECT tablename FROM pg_tables WHERE schemaname = 'public'`
    );
    for (const row of tables as Array<{ tablename?: string; table_name?: string; name?: string }>) {
        const table = row.tablename ?? row.table_name ?? row.name;
        if (!table) continue;
        const safe = quoteIdent(String(table), "\"");
        await pg._db.unsafe(`DROP TABLE IF EXISTS ${safe} CASCADE`);
    }
    await pg._db.unsafe(`DROP DOMAIN IF EXISTS ${quoteIdent("korm_rn_ref_text", "\"")} CASCADE`);
    await pg._db.unsafe(`DROP DOMAIN IF EXISTS ${quoteIdent("korm_encrypted_json", "\"")} CASCADE`);
};

const clearMysqlLayer = async (layer: SourceLayer): Promise<void> => {
    const mysql = layer as unknown as { _pool: { query: (query: string, values?: unknown[]) => Promise<unknown> } };
    const [rows] = await mysql._pool.query(
        `SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE'`
    ) as [{ table_name?: string; TABLE_NAME?: string; name?: string }[]];
    for (const row of rows) {
        const table = row.table_name ?? row.TABLE_NAME ?? row.name;
        if (!table) continue;
        const safe = quoteIdent(String(table), "`");
        await mysql._pool.query(`DROP TABLE IF EXISTS ${safe}`);
    }
};

const clearLayerConfig = async (config: LayerCleanup): Promise<void> => {
    if (config.type === "sqlite") {
        const layer = korm.layers.sqlite(config.path);
        try {
            clearSqliteLayer(layer);
        } finally {
            layer.close();
        }
        return;
    }
    if (config.type === "pg") {
        const layer = korm.layers.pg(config.config);
        try {
            await clearPgLayer(layer);
        } finally {
            await layer.close();
        }
        return;
    }
    if (config.type === "mysql") {
        const layer = korm.layers.mysql(config.config);
        try {
            await clearMysqlLayer(layer);
        } finally {
            await layer.close();
        }
    }
};

const clearDepotRoot = async (root: string): Promise<void> => {
    const depot = korm.depot.local(root);
    const rn = korm.rn(`[rn][depot::${depot.identifier}]:*`);
    const files = await depot.listFiles(rn);
    await Promise.all(files.map((file) => depot.deleteFile(file.rn)));
};

const clearTrackedResources = async (): Promise<void> => {
    for (const config of trackedLayers.values()) {
        await clearLayerConfig(config);
    }
    for (const root of trackedDepots.values()) {
        await clearDepotRoot(root);
    }
};

const ensureSqliteLayer = async (pathValue: string): Promise<SqliteLayer> => {
    const config: LayerCleanup = { type: "sqlite", path: pathValue };
    const key = layerKey(config);
    trackLayer(config);
    const layer = korm.layers.sqlite(pathValue);
    if (!clearedLayers.has(key)) {
        clearSqliteLayer(layer);
        clearedLayers.add(key);
    }
    return layer;
};

const ensurePgLayer = async (configValue: string): Promise<SourceLayer> => {
    const config: LayerCleanup = { type: "pg", config: configValue };
    const key = layerKey(config);
    trackLayer(config);
    const layer = korm.layers.pg(configValue);
    if (!clearedLayers.has(key)) {
        await clearPgLayer(layer);
        clearedLayers.add(key);
    }
    return layer;
};

const ensureLocalDepot = async (root: string): Promise<Depot> => {
    trackDepot(root);
    const depot = korm.depot.local(root);
    if (!clearedDepots.has(root)) {
        const rn = korm.rn(`[rn][depot::${depot.identifier}]:*`);
        const files = await depot.listFiles(rn);
        await Promise.all(files.map((file) => depot.deleteFile(file.rn)));
        clearedDepots.add(root);
    }
    return depot;
};

if (process.env.TESTING_PG_URL) {
    trackLayer({ type: "pg", config: process.env.TESTING_PG_URL });
}

beforeAll(async () => {
    await clearTrackedResources();
    for (const key of trackedLayers.keys()) {
        clearedLayers.add(key);
    }
    for (const root of trackedDepots.keys()) {
        clearedDepots.add(root);
    }
});

afterAll(async () => {
    await clearTrackedResources();
});

class ThrowOnceLayer implements SourceLayer {
    public readonly type: "sqlite" = "sqlite";
    public readonly identifier: string;
    private _throwOnInsert: boolean;

    constructor(private inner: SqliteLayer, opts?: { throwOnInsert?: boolean }) {
        this.identifier = inner.identifier;
        this._throwOnInsert = opts?.throwOnInsert ?? false;
    }

    async insertItem<T extends JSONable>(item: FloatingItem<T>, options?: PersistOptions): Promise<DbChangeResult<T>> {
        if (this._throwOnInsert) {
            this._throwOnInsert = false;
            throw new Error("Simulated crash during insert");
        }
        return await this.inner.insertItem(item, options);
    }

    async updateItem<T extends JSONable>(item: UncommittedItem<T>, options?: PersistOptions): Promise<DbChangeResult<T>> {
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

    async ensureTables(item: Item<any> | FloatingItem<any> | UncommittedItem<any>, destructive: boolean = false): Promise<string> {
        return await this.inner.ensureTables(item, destructive);
    }

    async getColumnKinds(namespace: string, kind: string): Promise<Map<string, ColumnKind>> {
        return await this.inner.getColumnKinds(namespace, kind);
    }

    async close(): Promise<void> {
        await this.inner.close();
    }
}

const makeTempDir = (): string => {
    return fs.mkdtempSync(path.join(os.tmpdir(), "korm-hostile-"));
};

const withSqlitePool = async <T>(fn: (pool: LayerPool, layer: SqliteLayer) => Promise<T>): Promise<T> => {
    const layer = await ensureSqliteLayer(":memory:");
    const pool = korm.pool().setLayers({ layer, ident: "sqlite" }).open();
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
    const result = await korm.item<Car>(pool).from.data({
        namespace: "cars",
        kind: "suv",
        data,
    }).create();
    if (result.isErr()) {
        throw result.error;
    }
    return result.unwrap();
};

const getCars = (pool: LayerPool, component: _QueryComponent) => {
    return korm.item<Car>(pool)
        .from.query(korm.rn("[rn]:cars:suv:*"))
        .where(component)
        .get();
};

const expectQueryFailure = async (promise: Promise<ResultLike>): Promise<void> => {
    try {
        const result = await promise;
        expect(result.isErr()).toBe(true);
    } catch (error) {
        expect(error).toBeDefined();
    }
};

const withEncryptionKey = async <T>(key: string, fn: () => Promise<T>): Promise<T> => {
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

const waitForBackupFiles = async (depot: Depot, rn: RN, minCount: number, timeoutMs: number = 2_000) => {
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

            const inResult = await getCars(pool, inList("name", [
                "alice",
                "bob",
                "'); DROP TABLE __items__cars__suv;--",
            ]));
            expect(inResult.isOk()).toBe(true);
            const names = inResult.unwrap().map((item) => item.data?.name ?? "").sort();
            expect(names).toEqual(["alice", "bob"]);

            const jsonResult = await getCars(pool, eq("profile.role", "admin' OR 1=1 --"));
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
                getCars(pool, eq('name"; DROP TABLE __items__cars__suv;--', "alice"))
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
                getCars(pool, eq("profile.name); DROP TABLE __items__cars__suv;--", "alice"))
            );
        });
    });

    test("rejects hostile column names on insert", async () => {
        await withPool(async (pool) => {
            const badResult = await korm.item(pool).from.data({
                namespace: "cars",
                kind: "suv",
                data: {
                    'name"; DROP TABLE __items__cars__suv;--': "evil",
                },
            }).create();
            expect(badResult.isErr()).toBe(true);

            const goodResult = await korm.item(pool).from.data({
                namespace: "cars",
                kind: "suv",
                data: { name: "good" },
            }).create();
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
        expect(() => korm.rn("[rn][depot::files]:safe:..\\\\secrets.txt")).toThrow();
    });

    test("rejects resolvePaths with unsafe keys", async () => {
        await withPool(async (pool) => {
            const created = await korm.item<any>(pool).from.data({
                namespace: "proto",
                kind: "case",
                data: { name: "alice" },
            }).create();
            expect(created.isOk()).toBe(true);
            const item = created.unwrap();

            const readProto = await korm.item<any>(pool).from.rn(item.rn!, {
                resolvePaths: ["__proto__.polluted"],
            });
            expect(readProto.isErr()).toBe(true);

            const readCtor = await korm.item<any>(pool).from.rn(item.rn!, {
                resolvePaths: ["constructor.prototype.polluted"],
            });
            expect(readCtor.isErr()).toBe(true);
        });
    });

    test("wildcard resolvePaths skips unsafe keys in data", async () => {
        await withPool(async (pool) => {
            const ownerResult = await korm.item<any>(pool).from.data({
                namespace: "refs",
                kind: "target",
                data: { name: "owner" },
            }).create();
            const protoResult = await korm.item<any>(pool).from.data({
                namespace: "refs",
                kind: "target",
                data: { polluted: "yes" },
            }).create();
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

            const holder = await korm.item<any>(pool).from.data({
                namespace: "refs",
                kind: "holder",
                data: { refs },
            }).create();
            expect(holder.isOk()).toBe(true);

            const read = await korm.item<any>(pool).from.rn(holder.unwrap().rn!, {
                resolvePaths: ["refs.*"],
            });
            expect(read.isOk()).toBe(true);
            const data = read.unwrap().data as any;

            expect(data.refs.owner?.name).toBe("owner");
            expect(Object.prototype.hasOwnProperty.call(data.refs, "__proto__")).toBe(true);
            expect(data.refs["__proto__"]).toBe(proto.rn!.value());
            expect((data.refs as any).polluted).toBeUndefined();
            expect(({} as any).polluted).toBeUndefined();
        });
    });

    test("serializes concurrent tx persists on shared RNs without deadlock", async () => {
        await withPool(async (pool) => {
            const createdA = await korm.item<LockRecord>(pool).from.data({
                namespace: "locks",
                kind: "pair",
                data: { name: "A", seq: 0 },
            }).create();
            const createdB = await korm.item<LockRecord>(pool).from.data({
                namespace: "locks",
                kind: "pair",
                data: { name: "B", seq: 0 },
            }).create();

            expect(createdA.isOk()).toBe(true);
            expect(createdB.isOk()).toBe(true);

            const itemA = createdA.unwrap();
            const itemB = createdB.unwrap();

            const tasks = Array.from({ length: 50 }, (_, index) => {
                const updateA = new UncommittedItem(pool, itemA.rn!, { name: "A", seq: index });
                const updateB = new UncommittedItem(pool, itemB.rn!, { name: "B", seq: index });
                const tx = index % 2 === 0
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
        const poolA = korm.pool()
            .setLayers({ layer: layerA, ident: "sqlite" })
            .withLocks({ layerIdent: "sqlite", ttlMs: 2_000, retryMs: 10 })
            .open();
        const poolB = korm.pool()
            .setLayers({ layer: layerB, ident: "sqlite" })
            .withLocks({ layerIdent: "sqlite", ttlMs: 2_000, retryMs: 10 })
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
                    const created = await korm.item<SecretRecord>(pool).from.data({
                        namespace: "vault",
                        kind: "record",
                        data: {
                            name: "alice",
                            secret: await korm.encrypt("topsecret"),
                        },
                    }).create();
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
                        [tampered, item.rn!.id!]
                    );

                    const read = await korm.item<SecretRecord>(pool).from.rn(item.rn!);
                    expect(read.isOk()).toBe(true);
                    const readItem = read.unwrap();
                    expect(readItem.data).toBeDefined();
                    const secret = readItem.data!.secret as Encrypt<string>;
                    expect(secret.reveal()).not.toBe("topsecret");
                }
            );
        });
    });

    test("tampered symmetric payload fails closed", async () => {
        await withSqlitePool(async (pool, layer) => {
            await withEncryptionKey(
                "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
                async () => {
                    const created = await korm.item<SecretRecord>(pool).from.data({
                        namespace: "vault",
                        kind: "record",
                        data: {
                            name: "bob",
                            secret: await korm.encrypt("supersecret"),
                        },
                    }).create();
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
                        [tampered, item.rn!.id!]
                    );

                    await expectQueryFailure(korm.item<SecretRecord>(pool).from.rn(item.rn!));
                }
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

        const pool = korm.pool()
            .setLayers(
                { layer: layerA, ident: "a" },
                { layer: flakyLayer, ident: "b" }
            )
            .setDepots({ depot: walDepot, ident: "wal" })
            .withWal({ depotIdent: "wal", walNamespace: "hostile-wal", retention: "keep" })
            .open();

        const itemA = korm.item<LockRecord>(pool).from.data({
            namespace: "atoms",
            kind: "a",
            mods: [{ key: "from", value: "a" }],
            data: { name: "A", seq: 1 },
        }).unwrap();
        const itemB = korm.item<LockRecord>(pool).from.data({
            namespace: "atoms",
            kind: "b",
            mods: [{ key: "from", value: "b" }],
            data: { name: "B", seq: 1 },
        }).unwrap();

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
            `[rn][depot::wal]:__korm_wal__:${wal.namespace}:${wal.poolId}:pending:*`
        );
        const pendingFiles = await walDepot.listFiles(pendingPrefix);
        expect(pendingFiles.length).toBeGreaterThan(0);

        await pool.close();

        const layerA2 = await ensureSqliteLayer(dbAPath);
        const layerB2 = await ensureSqliteLayer(dbBPath);
        const pool2 = korm.pool()
            .setLayers(
                { layer: layerA2, ident: "a" },
                { layer: layerB2, ident: "b" }
            )
            .setDepots({ depot: walDepot, ident: "wal" })
            .withWal({ depotIdent: "wal", walNamespace: wal.namespace, retention: "keep" })
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

    pgTest("destructive schema change keeps __korm_meta__ in sync (pg)", async () => {
        await withEncryptionKey(
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            async () => {
                const namespace = "meta";
                const kind = `probe${randomUUID().replace(/-/g, "").slice(0, 8)}`;
                const pgLayer = await ensurePgLayer(process.env.TESTING_PG_URL!);
                const pgPrivate = pgLayer as unknown as {
                    _db: { unsafe: (query: string, values?: unknown[]) => Promise<unknown[]> };
                    _domainsAvailable: boolean;
                    _domainsEnsured: boolean;
                };

                pgPrivate._domainsAvailable = false;
                pgPrivate._domainsEnsured = true;

                const pool = korm.pool()
                    .setLayers({ layer: pgLayer, ident: "pg" })
                    .open();

                try {
                    const created = await korm.item<SecretRecord>(pool).from.data({
                        namespace,
                        kind,
                        mods: [{ key: "from", value: "pg" }],
                        data: {
                            name: "row",
                            secret: await korm.encrypt("alpha"),
                        },
                    }).create();
                    expect(created.isOk()).toBe(true);
                    const item = created.unwrap();

                    const update = item.update({ secret: 42 }).unwrap();
                    const result = await korm.tx(update).persist({ destructive: true });
                    expect(result.isOk()).toBe(true);

                    const rawTable = `__items__${namespace}__${kind}`;
                    const metaRows = await pgPrivate._db.unsafe(
                        `SELECT kind FROM "__korm_meta__" WHERE table_name = $1 AND column_name = $2`,
                        [rawTable, "secret"]
                    );
                    expect(metaRows.length).toBe(0);

                    const columns = await pgPrivate._db.unsafe(
                        `SELECT data_type FROM information_schema.columns WHERE table_name = $1 AND column_name = $2`,
                        [rawTable, "secret"]
                    );
                    const column = columns[0] as { data_type?: unknown } | undefined;
                    const dataType = String(column?.data_type ?? "").toLowerCase();
                    expect(dataType).toBe("integer");
                } finally {
                    await pool.close();
                }
            }
        );
    });

    test("WAL depotOps replays pending file writes (shadowing probe)", async () => {
        const root = makeTempDir();
        const walRoot = path.join(root, "wal");
        const dataRoot = path.join(root, "data");
        const dbPath = path.join(root, "files.sqlite");

        const dataDepot = await ensureLocalDepot(dataRoot);
        const walDepot = await ensureLocalDepot(walRoot);
        const layer = await ensureSqliteLayer(dbPath);
        const flakyLayer = new ThrowOnceLayer(layer, { throwOnInsert: true });

        const pool = korm.pool()
            .setLayers({ layer: flakyLayer, ident: "sqlite" })
            .setDepots(
                { depot: dataDepot, ident: "data" },
                { depot: walDepot, ident: "wal" }
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

        const itemA = korm.item<DepotRecord>(pool).from.data({
            namespace: "files",
            kind: "blob",
            data: { name: "one", attachment: fileA },
        }).unwrap();
        const itemB = korm.item<DepotRecord>(pool).from.data({
            namespace: "files",
            kind: "blob",
            data: { name: "two", attachment: fileB },
        }).unwrap();

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
        const pool2 = korm.pool()
            .setLayers({ layer: layer2, ident: "sqlite" })
            .setDepots(
                { depot: dataDepot, ident: "data" },
                { depot: walDepot, ident: "wal" }
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

        const pool = korm.pool()
            .setLayers({ layer, ident: evilIdent })
            .setDepots({ depot, ident: "backup" })
            .withMeta({ layerIdent: evilIdent })
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

        const pool = korm.pool()
            .setLayers({ layer, ident: "sqlite" })
            .setDepots({ depot, ident: "backup" })
            .withMeta({ layerIdent: "sqlite" })
            .open();

        const secretValue = `backup-secret-${randomUUID()}`;

        try {
            await withEncryptionKey(
                "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
                async () => {
                    const created = await korm.item<SecretRecord>(pool).from.data({
                        namespace: "vault",
                        kind: "backup",
                        data: {
                            name: "alpha",
                            secret: await korm.encrypt(secretValue),
                        },
                    }).create();
                    expect(created.isOk()).toBe(true);
                }
            );

            const manager = new BackMan();
            manager.addInterval("sqlite", korm.interval.every("minute").runNow());
            pool.configureBackups("backup", manager);

            const prefix = korm.rn("[rn][depot::backup]:__korm_backups__:*");
            const files = await waitForBackupFiles(depot, prefix, 1);
            expect(files.length).toBe(1);

            const text = await files[0]!.text();
            expect(text).not.toContain(secretValue);
            expect(text).not.toContain("\"__ENCRYPT__\"");
            const payload = JSON.parse(text) as {
                tables: Array<{ name: string; rows: Array<{ secret?: string }> }>;
            };
            const table = payload.tables.find((entry) => entry.name === "__items__vault__backup");
            const secretRaw = table?.rows[0]?.secret;
            expect(typeof secretRaw).toBe("string");
            const secretPayload = JSON.parse(secretRaw ?? "{}") as { __ENCRYPTED__?: boolean };
            expect(secretPayload.__ENCRYPTED__).toBe(true);
        } finally {
            await pool.close();
        }
    });

    test("backups use shared locks to prevent duplicate runs", async () => {
        const root = makeTempDir();
        const depotRoot = path.join(root, "backups");
        const dbPath = path.join(root, "backup.sqlite");

        const depot = await ensureLocalDepot(depotRoot);
        const layerA = await ensureSqliteLayer(dbPath);
        const layerB = await ensureSqliteLayer(dbPath);

        const poolA = korm.pool()
            .setLayers({ layer: layerA, ident: "sqlite" })
            .setDepots({ depot, ident: "backup" })
            .withMeta({ layerIdent: "sqlite" })
            .open();

        const poolB = korm.pool()
            .setLayers({ layer: layerB, ident: "sqlite" })
            .setDepots({ depot, ident: "backup" })
            .withMeta({ layerIdent: "sqlite" })
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
