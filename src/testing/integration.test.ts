import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { existsSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { randomUUID } from "node:crypto";
import os from "node:os";
import { resolve } from "node:path";
import type { RowDataPacket } from "mysql2/promise";
import { korm } from "../korm";
import type { JSONable } from "../korm";
import type { Encrypt } from "../security/encryption";
import type { RN } from "../core/rn";
import type { _QueryComponent } from "../core/query";
import { DepotFile, isDepotFile, type DepotFileLike } from "../depot/depotFile";
import type { WalRecord } from "../wal/wal";
import { BackMan } from "../sources/backMan";

function loadEnvIfMissing(): void {
    if (process.env.TESTING_PG_URL && process.env.TESTING_MYSQL_URL) return;

    const envPath = resolve(import.meta.dir, "../../.env");
    if (!existsSync(envPath)) return;

    const lines = readFileSync(envPath, "utf8").split(/\r?\n/);
    for (const line of lines) {
        const trimmed = line.trim();
        if (!trimmed || trimmed.startsWith("#")) continue;
        const idx = trimmed.indexOf("=");
        if (idx === -1) continue;
        const key = trimmed.slice(0, idx).trim();
        let value = trimmed.slice(idx + 1).trim();
        if (
            (value.startsWith('"') && value.endsWith('"')) ||
            (value.startsWith("'") && value.endsWith("'"))
        ) {
            value = value.slice(1, -1);
        }
        if (process.env[key] === undefined) process.env[key] = value;
    }
}

loadEnvIfMissing();

const { layerPool, sqll, pg, mysql, localDepot, s3Depot } = await import("./layerDefs");
const { eq, and, gt, not, or, like, inList } = korm.qfns;

type LayerIdent = "sqlite" | "pg" | "mysql";

type DemoRecord = {
    name: string;
    count: number;
    active: boolean;
    meta: { owner: string; rating: number; flags: { hot: boolean } };
    note: string | null;
};

type User = { firstName: string; lastName: string };
type Car = { make: string; model: string; year: number; owner: string; meta: { inspected: boolean } };
type Registration = { carRef: string; active: boolean; note: string };
type SimpleRecord = { label: string; score: number };
type ArrayRecord = {
    name: string;
    tags: string[];
    meta: { rating: number; addresses: { city: string; zip: number }[] };
    matrix: number[][];
};
type MissingRefRecord = { label: string; owner: string };
type GroupRecord = {
    name: string;
    primary: RN<User>;
    backup: RN<User> | null;
    members: RN<User>[];
};
type PrefixRecord = { label: string; files: RN<JSONable> };
type FileBundleRecord = { label: string; files: DepotFileLike[] };
type SecureRecord = {
    username: string;
    password: Encrypt<string>;
    secret: Encrypt<{ code: string; flags: string[] }>;
};
type ResolvedUser = {
    firstName: string;
    lastName: string;
    password: Encrypt<string>;
    secret: Encrypt<{ code: string; flags: string[] }>;
};
type ResolvedCar = {
    make: string;
    model: string;
    year: number;
    owner: RN<ResolvedUser>;
    meta: { inspected: boolean };
};
type ResolvedRegistration = {
    carRef: RN<ResolvedCar>;
    active: boolean;
    note: string;
};
type DepotIdent = "local" | "s3";
type InvoiceRecord = { label: string; file: DepotFileLike };

type ColumnTypeExpectation = string | string[];
type MysqlColumnExpectation = { type: ColumnTypeExpectation; columnType?: ColumnTypeExpectation };

function tableName(namespace: string, kind: string): string {
    return `__items__${namespace}__${kind}`;
}

function mysqlTableName(name: string): string {
    return mysql.resolveTableName(name);
}

function makeId(prefix: string): string {
    const clean = prefix.toLowerCase().replace(/[^a-z0-9]/g, "");
    return `${clean}${Date.now().toString(36)}${Math.random().toString(36).slice(2, 8)}`;
}

function makeNames(prefix: string): { namespace: string; kind: string } {
    const id = makeId(prefix);
    return { namespace: `n${id}`, kind: `k${id}` };
}

function walPrefixRn(depotIdent: string, namespace: string, poolId: string, state: "pending" | "done"): RN<JSONable> {
    return korm.rn(`[rn][depot::${depotIdent}]:__korm_wal__:${namespace}:${poolId}:${state}:*`);
}

function fromMod(ident: LayerIdent): { key: string; value: string }[] {
    return [{ key: "from", value: ident }];
}

async function createItem<T extends JSONable>(
    ident: LayerIdent,
    namespace: string,
    kind: string,
    data: T
) {
    const res = await korm.item<T>(layerPool).from.data({
        namespace,
        kind,
        mods: fromMod(ident),
        data
    }).create();
    return res.unwrap();
}

function quoteIdent(name: string, quote: string): string {
    if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(name)) {
        throw new Error(`Unsafe table name: ${name}`);
    }
    if (quote === "`") return `\`${name.replace(/`/g, "``")}\``;
    return `"${name.replace(/"/g, '""')}"`;
}

function normalizeType(value: string): string {
    return value.trim().toLowerCase();
}

function expectType(actual: string, expected: ColumnTypeExpectation): void {
    const normalized = normalizeType(actual);
    if (Array.isArray(expected)) {
        expect(expected.map((e) => normalizeType(e))).toContain(normalized);
    } else {
        expect(normalized).toBe(normalizeType(expected));
    }
}

function normalizeJson(value: any): any {
    if (typeof value !== "string") return value;
    try {
        return JSON.parse(value);
    } catch {
        return value;
    }
}

function normalizeBoolean(value: any): any {
    if (value === 0 || value === 1) return Boolean(value);
    return value;
}

function normalizeRow(
    row: Record<string, any>,
    opts: { json?: string[]; boolean?: string[] }
): Record<string, any> {
    const out: Record<string, any> = { ...row };
    for (const key of opts.json ?? []) {
        if (key in out) out[key] = normalizeJson(out[key]);
    }
    for (const key of opts.boolean ?? []) {
        if (key in out) out[key] = normalizeBoolean(out[key]);
    }
    return out;
}

function pick<T extends Record<string, any>>(row: T, keys: string[]): Record<string, any> {
    const out: Record<string, any> = {};
    for (const key of keys) {
        out[key] = row[key];
    }
    return out;
}

async function pgTableExists(name: string): Promise<boolean> {
    const rows = await pg._db.unsafe<any[]>(
        `SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = $1`,
        [name]
    );
    return rows.length > 0;
}

async function mysqlTableExists(name: string): Promise<boolean> {
    const [rows] = await mysql._pool.query<RowDataPacket[]>(
        `SELECT COUNT(*) as cnt FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?`,
        [mysqlTableName(name)]
    );
    const count = Number((rows as any[])[0]?.cnt ?? 0);
    return count > 0;
}

function sqliteTableExists(name: string): boolean {
    const row = sqll._db
        .prepare(`SELECT 1 as e FROM sqlite_master WHERE type='table' AND name=?`)
        .get(name) as { e?: number } | undefined;
    return Boolean(row?.e);
}

async function pgColumns(name: string): Promise<Map<string, string>> {
    const rows = await pg._db.unsafe<any[]>(
        `SELECT column_name, data_type, domain_name
         FROM information_schema.columns
         WHERE table_schema = 'public' AND table_name = $1
         ORDER BY ordinal_position`,
        [name]
    );
    const map = new Map<string, string>();
    for (const row of rows) {
        const type = row.domain_name ?? row.data_type;
        map.set(String(row.column_name), String(type));
    }
    return map;
}

async function pgDomainExists(name: string): Promise<boolean> {
    const rows = await pg._db.unsafe<any[]>(
        `SELECT 1 FROM pg_type WHERE typtype = 'd' AND typname = $1`,
        [name]
    );
    return rows.length > 0;
}

async function mysqlColumns(name: string): Promise<Map<string, { dataType: string; columnType: string }>> {
    const tableName = mysqlTableName(name);
    const [rows] = await mysql._pool.query<RowDataPacket[]>(
        `SELECT column_name, data_type, column_type FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = ? ORDER BY ordinal_position`,
        [tableName]
    );
    const map = new Map<string, { dataType: string; columnType: string }>();
    for (const row of rows as any[]) {
        const columnName = row.column_name ?? row.COLUMN_NAME ?? row.name;
        if (!columnName) continue;
        map.set(String(columnName), {
            dataType: String(row.data_type ?? row.DATA_TYPE ?? ""),
            columnType: String(row.column_type ?? row.COLUMN_TYPE ?? ""),
        });
    }
    return map;
}

function sqliteColumns(name: string): Map<string, string> {
    const rows = sqll._db.prepare(`PRAGMA table_info(${quoteIdent(name, "\"")})`).all();
    const map = new Map<string, string>();
    for (const row of rows as any[]) {
        map.set(String(row.name), String(row.type));
    }
    return map;
}

async function pgRowById(name: string, rnId: string): Promise<Record<string, any> | undefined> {
    const safe = quoteIdent(name, "\"");
    const rows = await pg._db.unsafe<any[]>(`SELECT * FROM ${safe} WHERE "rnId" = $1`, [rnId]);
    return rows[0] as Record<string, any> | undefined;
}

async function pgRowByIdFresh(name: string, rnId: string): Promise<Record<string, any> | undefined> {
    const safe = quoteIdent(name, "\"");
    const nonce = Math.random().toString(36).slice(2);
    const rows = await pg._db.unsafe<any[]>(
        `SELECT * FROM ${safe} WHERE "rnId" = $1 /* ${nonce} */`,
        [rnId]
    );
    return rows[0] as Record<string, any> | undefined;
}

async function mysqlRowById(name: string, rnId: string): Promise<Record<string, any> | undefined> {
    const safe = quoteIdent(mysqlTableName(name), "`");
    const [rows] = await mysql._pool.query<RowDataPacket[]>(`SELECT * FROM ${safe} WHERE \`rnId\` = ?`, [rnId]);
    return (rows as any[])[0] as Record<string, any> | undefined;
}

function sqliteRowById(name: string, rnId: string): Record<string, any> | undefined {
    const safe = quoteIdent(name, "\"");
    return sqll._db.prepare(`SELECT * FROM ${safe} WHERE "rnId" = ?`).get(rnId) as Record<string, any> | undefined;
}

async function rowById(
    ident: LayerIdent,
    name: string,
    rnId: string
): Promise<Record<string, any> | undefined> {
    if (ident === "pg") return await pgRowById(name, rnId);
    if (ident === "mysql") return await mysqlRowById(name, rnId);
    return sqliteRowById(name, rnId);
}

async function pgCountRows(name: string): Promise<number> {
    const safe = quoteIdent(name, "\"");
    const rows = await pg._db.unsafe<any[]>(`SELECT COUNT(*)::int as cnt FROM ${safe}`);
    return Number(rows[0]?.cnt ?? 0);
}

async function mysqlCountRows(name: string): Promise<number> {
    const safe = quoteIdent(mysqlTableName(name), "`");
    const [rows] = await mysql._pool.query<RowDataPacket[]>(`SELECT COUNT(*) as cnt FROM ${safe}`);
    return Number((rows as any[])[0]?.cnt ?? 0);
}

function sqliteCountRows(name: string): number {
    const safe = quoteIdent(name, "\"");
    const row = sqll._db.prepare(`SELECT COUNT(*) as cnt FROM ${safe}`).get() as { cnt?: number };
    return Number(row?.cnt ?? 0);
}

async function assertPgColumns(name: string, expected: Record<string, ColumnTypeExpectation>): Promise<void> {
    const columns = await pgColumns(name);
    for (const [col, type] of Object.entries(expected)) {
        const actual = columns.get(col);
        expect(actual).toBeTruthy();
        expectType(actual!, type);
    }
}

async function assertMysqlColumns(name: string, expected: Record<string, MysqlColumnExpectation>): Promise<void> {
    const columns = await mysqlColumns(name);
    for (const [col, exp] of Object.entries(expected)) {
        const actual = columns.get(col);
        expect(actual).toBeTruthy();
        expectType(actual!.dataType, exp.type);
        if (exp.columnType) {
            expectType(actual!.columnType, exp.columnType);
        }
    }
}

function assertSqliteColumns(name: string, expected: Record<string, ColumnTypeExpectation>): void {
    const columns = sqliteColumns(name);
    for (const [col, type] of Object.entries(expected)) {
        const actual = columns.get(col);
        expect(actual).toBeTruthy();
        expectType(actual!, type);
    }
}

async function clearPg(): Promise<void> {
    const tables = await pg._db.unsafe<any[]>(
        `SELECT tablename FROM pg_tables WHERE schemaname = 'public'`
    );
    for (const row of tables) {
        const table = row.tablename ?? row.table_name ?? row.name;
        if (!table) continue;
        const safe = quoteIdent(String(table), "\"");
        await pg._db.unsafe(`DROP TABLE IF EXISTS ${safe} CASCADE`);
    }
    await pg._db.unsafe(`DROP DOMAIN IF EXISTS ${quoteIdent("korm_rn_ref_text", "\"")} CASCADE`);
    await pg._db.unsafe(`DROP DOMAIN IF EXISTS ${quoteIdent("korm_encrypted_json", "\"")} CASCADE`);
}

async function clearMysql(): Promise<void> {
    const [rows] = await mysql._pool.query<RowDataPacket[]>(
        `SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE'`
    );
    for (const row of rows as any[]) {
        const table = row.table_name ?? row.TABLE_NAME ?? row.name;
        if (!table) continue;
        const safe = quoteIdent(String(table), "`");
        await mysql._pool.query(`DROP TABLE IF EXISTS ${safe}`);
    }
}

function clearSqlite(): void {
    const tables = sqll._db.prepare(`SELECT name FROM sqlite_master WHERE type='table'`).all() as { name?: string }[];
    for (const row of tables) {
        const name = row.name;
        if (!name || name.startsWith("sqlite_")) continue;
        const safe = quoteIdent(name, "\"");
        sqll._db.run(`DROP TABLE IF EXISTS ${safe}`);
    }
}

async function clearAllDatabases(): Promise<void> {
    clearSqlite();
    await clearPg();
    await clearMysql();
}

async function clearDepot(ident: DepotIdent): Promise<void> {
    const rn = korm.rn(`[rn][depot::${ident}]:*`);
    const depot = ident === "local" ? localDepot : s3Depot;
    const files = await depot.listFiles(rn);
    await Promise.all(files.map((file) => depot.deleteFile(file.rn)));
}

async function clearAllDepots(): Promise<void> {
    await clearDepot("local");
    await clearDepot("s3");
}

async function seedDepotFile(rnValue: string, content: string): Promise<void> {
    const file = korm.file({
        rn: korm.rn(rnValue),
        file: new Blob([content], { type: "text/plain" })
    });
    await file.create(layerPool);
}

async function waitForBackupPayloads(
    depot: { listFiles: (rn: RN) => Promise<DepotFile[]> },
    rn: RN,
    layerIdents: string[],
    timeoutMs: number = 15_000
): Promise<Map<string, { layerType: string; tables: Array<{ name: string; rows: Record<string, unknown>[] }> }>> {
    const start = Date.now();
    const want = new Set(layerIdents);
    const parseFiles = async () => {
        const files = await depot.listFiles(rn);
        const payloads = await Promise.all(
            files.map(async (file) => JSON.parse(await file.text()) as {
                layerIdent: string;
                layerType: string;
                tables: Array<{ name: string; rows: Record<string, unknown>[] }>;
            })
        );
        const map = new Map<string, { layerType: string; tables: Array<{ name: string; rows: Record<string, unknown>[] }> }>();
        for (const payload of payloads) {
            if (!map.has(payload.layerIdent)) {
                map.set(payload.layerIdent, payload);
            }
        }
        return map;
    };

    while (Date.now() - start < timeoutMs) {
        const map = await parseFiles();
        const hasAll = Array.from(want).every((ident) => map.has(ident));
        if (hasAll) return map;
        await new Promise((resolve) => setTimeout(resolve, 50));
    }
    return await parseFiles();
}

beforeAll(async () => {
    await clearAllDatabases();
    await clearAllDepots();
});

async function queryItems<T extends JSONable>(
    ident: LayerIdent,
    namespace: string,
    kind: string,
    where?: _QueryComponent
) {
    const qb = korm.item<T>(layerPool).from.query(
        korm.rn(`[rn][from::${ident}]:${namespace}:${kind}:*`)
    );
    if (where) qb.where(where);
    const res = await qb.get();
    return res.unwrap();
}

describe("layers integration", () => {
    test("missing tables return empty result sets", async () => {
        const missing = makeNames("missing");
        const tname = tableName(missing.namespace, missing.kind);
        for (const ident of ["sqlite", "pg", "mysql"] as const) {
            const rows = await queryItems<DemoRecord>(ident, missing.namespace, missing.kind);
            expect(rows.length).toBe(0);
        }
        expect(await pgTableExists(tname)).toBe(false);
        expect(await mysqlTableExists(tname)).toBe(false);
        expect(sqliteTableExists(tname)).toBe(false);
    });

    test("backups snapshot each layer and update schedule metadata", async () => {
        const root = mkdtempSync(resolve(os.tmpdir(), "korm-backups-"));
        const depotRoot = resolve(root, "depot");
        const sqlitePath = resolve(root, "meta.sqlite");

        const backupDepot = korm.depot.local(depotRoot);
        const backupSqlite = korm.layers.sqlite(sqlitePath);
        const backupPg = korm.layers.pg(process.env.TESTING_PG_URL!);
        const backupMysql = korm.layers.mysql(process.env.TESTING_MYSQL_URL!);

        const backupPool = korm.pool()
            .setLayers(
                { layer: backupSqlite, ident: "sqlite" },
                { layer: backupPg, ident: "pg" },
                { layer: backupMysql, ident: "mysql" },
            )
            .setDepots({ depot: backupDepot, ident: "backups" })
            .withMeta({ layerIdent: "sqlite" })
            .open();

        try {
            await backupPool.ensureMetaReady();

            const sqliteNames = makeNames("backupsqlite");
            const pgNames = makeNames("backuppg");
            const mysqlNames = makeNames("backupmysql");

            const sqliteRes = await korm.item<SimpleRecord>(backupPool).from.data({
                namespace: sqliteNames.namespace,
                kind: sqliteNames.kind,
                mods: fromMod("sqlite"),
                data: { label: "backup-sqlite", score: 1 }
            }).create();
            expect(sqliteRes.isOk()).toBe(true);

            const pgRes = await korm.item<SimpleRecord>(backupPool).from.data({
                namespace: pgNames.namespace,
                kind: pgNames.kind,
                mods: fromMod("pg"),
                data: { label: "backup-pg", score: 2 }
            }).create();
            expect(pgRes.isOk()).toBe(true);

            const mysqlRes = await korm.item<SimpleRecord>(backupPool).from.data({
                namespace: mysqlNames.namespace,
                kind: mysqlNames.kind,
                mods: fromMod("mysql"),
                data: { label: "backup-mysql", score: 3 }
            }).create();
            expect(mysqlRes.isOk()).toBe(true);

            const manager = new BackMan();
            manager.addInterval("*", korm.interval.every("minute").runNow());
            backupPool.configureBackups("backups", manager);

            const backupPrefix = korm.rn(`[rn][depot::backups]:__korm_backups__:*`);
            const byLayer = await waitForBackupPayloads(
                backupDepot,
                backupPrefix,
                ["sqlite", "pg", "mysql"],
                30_000
            );

            const sqlitePayload = byLayer.get("sqlite");
            const pgPayload = byLayer.get("pg");
            const mysqlPayload = byLayer.get("mysql");
            expect(sqlitePayload?.layerType).toBe("sqlite");
            expect(pgPayload?.layerType).toBe("pg");
            expect(mysqlPayload?.layerType).toBe("mysql");

            const sqliteTable = tableName(sqliteNames.namespace, sqliteNames.kind);
            const pgTable = tableName(pgNames.namespace, pgNames.kind);
            const mysqlTable = tableName(mysqlNames.namespace, mysqlNames.kind);

            const sqliteDump = sqlitePayload?.tables.find((table) => table.name === sqliteTable);
            const pgDump = pgPayload?.tables.find((table) => table.name === pgTable);
            const mysqlDump = mysqlPayload?.tables.find((table) => table.name === mysqlTable);

            expect(sqliteDump?.rows.length).toBe(1);
            expect(pgDump?.rows.length).toBe(1);
            expect(mysqlDump?.rows.length).toBe(1);

            expect((sqliteDump?.rows[0] as any)?.label).toBe("backup-sqlite");
            expect((pgDump?.rows[0] as any)?.label).toBe("backup-pg");
            expect((mysqlDump?.rows[0] as any)?.label).toBe("backup-mysql");

            const scheduleRows = backupSqlite._db.prepare(
                `SELECT schedule_id, layer_ident, next_run_at, last_run_at FROM "__korm_backups__"`
            ).all() as Array<{ layer_ident?: string; next_run_at?: number; last_run_at?: number | null }>;
            const scheduleByLayer = new Map<string, { next_run_at?: number; last_run_at?: number | null }>(
                scheduleRows.map((row) => [String(row.layer_ident ?? ""), row])
            );
            for (const ident of ["sqlite", "pg", "mysql"] as const) {
                const row = scheduleByLayer.get(ident);
                expect(row).toBeTruthy();
                expect(Number(row!.next_run_at ?? 0)).toBeGreaterThan(0);
                expect(row!.last_run_at).not.toBeNull();
            }
        } finally {
            await backupPool.close();
            rmSync(root, { recursive: true, force: true });
        }
    }, 45_000);

    for (const ident of ["sqlite", "pg", "mysql"] as const) {
        test(`${ident} basic CRUD + JSON queries`, async () => {
            const names = makeNames(`crud${ident}`);
            const baseId = makeId(ident);
            const tname = tableName(names.namespace, names.kind);
            const data: DemoRecord = {
                name: `name-${ident}-${baseId}`,
                count: 1,
                active: true,
                meta: {
                    owner: `owner-${ident}-${baseId}`,
                    rating: 4.2,
                    flags: { hot: true }
                },
                note: null
            };

            const created = await createItem<DemoRecord>(ident, names.namespace, names.kind, data);
            expect(created.rn?.id).toBeTruthy();

            const byName = await queryItems<DemoRecord>(
                ident,
                names.namespace,
                names.kind,
                eq("name", data.name)
            );
            expect(byName.length).toBe(1);
            expect(byName[0]?.rn?.mods.get("from")).toBe(ident);
            expect(byName[0]?.data?.meta?.owner).toBe(data.meta.owner);

            const byOwner = await queryItems<DemoRecord>(
                ident,
                names.namespace,
                names.kind,
                eq("meta.owner", data.meta.owner)
            );
            expect(byOwner.length).toBe(1);

            const byRating = await queryItems<DemoRecord>(
                ident,
                names.namespace,
                names.kind,
                gt("meta.rating", 4)
            );
            expect(byRating.length).toBe(1);

            const byFlag = await queryItems<DemoRecord>(
                ident,
                names.namespace,
                names.kind,
                eq("meta.flags.hot", true)
            );
            expect(byFlag.length).toBe(1);

            const byNull = await queryItems<DemoRecord>(
                ident,
                names.namespace,
                names.kind,
                eq("note", null)
            );
            expect(byNull.length).toBe(1);

            if (ident === "pg") {
                expect(await pgTableExists(tname)).toBe(true);
                await assertPgColumns(tname, {
                    rnId: "text",
                    name: "text",
                    count: "integer",
                    active: "boolean",
                    meta: "jsonb",
                    note: "text",
                });
                expect(await pgCountRows(tname)).toBe(1);
                const row = await pgRowById(tname, created.rn!.id!);
                expect(row?.rnId).toBe(created.rn!.id);
                const normalized = normalizeRow(row ?? {}, { json: ["meta"], boolean: ["active"] });
                expect(pick(normalized, ["name", "count", "active", "meta", "note"])).toEqual({
                    name: data.name,
                    count: 1,
                    active: true,
                    meta: data.meta,
                    note: null,
                });
            } else if (ident === "mysql") {
                expect(await mysqlTableExists(tname)).toBe(true);
                await assertMysqlColumns(tname, {
                    rnId: { type: "varchar" },
                    name: { type: "text" },
                    count: { type: "int" },
                    active: { type: "tinyint", columnType: ["tinyint(1)", "tinyint"] },
                    meta: { type: "json" },
                    note: { type: "text" },
                });
                expect(await mysqlCountRows(tname)).toBe(1);
                const row = await mysqlRowById(tname, created.rn!.id!);
                expect(row?.rnId).toBe(created.rn!.id);
                const normalized = normalizeRow(row ?? {}, { json: ["meta"], boolean: ["active"] });
                expect(pick(normalized, ["name", "count", "active", "meta", "note"])).toEqual({
                    name: data.name,
                    count: 1,
                    active: true,
                    meta: data.meta,
                    note: null,
                });
            } else {
                expect(sqliteTableExists(tname)).toBe(true);
                assertSqliteColumns(tname, {
                    rnId: "id_text",
                    name: "text",
                    count: "integer",
                    active: "boolean",
                    meta: "json_text",
                    note: "text",
                });
                expect(sqliteCountRows(tname)).toBe(1);
                const row = sqliteRowById(tname, created.rn!.id!);
                expect(row?.rnId).toBe(created.rn!.id);
                const normalized = normalizeRow(row ?? {}, { json: ["meta"], boolean: ["active"] });
                expect(pick(normalized, ["name", "count", "active", "meta", "note"])).toEqual({
                    name: data.name,
                    count: 1,
                    active: true,
                    meta: data.meta,
                    note: null,
                });
            }

            const updated = await created.update({
                count: 2,
                active: false,
                meta: { rating: 4.9 }
            }).commit();
            expect(updated.isOk()).toBe(true);

            const byCount = await queryItems<DemoRecord>(
                ident,
                names.namespace,
                names.kind,
                eq("count", 2)
            );
            expect(byCount.length).toBe(1);
            expect(byCount[0]?.data?.active).toBe(false);

            const notNull = await queryItems<DemoRecord>(
                ident,
                names.namespace,
                names.kind,
                not(eq("note", null))
            );
            expect(notNull.length).toBe(0);

            const updatedExpected: DemoRecord = {
                ...data,
                count: 2,
                active: false,
                meta: { ...data.meta, rating: 4.9 }
            };

            if (ident === "pg") {
                const row = await pgRowById(tname, created.rn!.id!);
                const normalized = normalizeRow(row ?? {}, { json: ["meta"], boolean: ["active"] });
                expect(pick(normalized, ["name", "count", "active", "meta", "note"])).toEqual({
                    name: updatedExpected.name,
                    count: updatedExpected.count,
                    active: updatedExpected.active,
                    meta: updatedExpected.meta,
                    note: updatedExpected.note,
                });
            } else if (ident === "mysql") {
                const row = await mysqlRowById(tname, created.rn!.id!);
                const normalized = normalizeRow(row ?? {}, { json: ["meta"], boolean: ["active"] });
                expect(pick(normalized, ["name", "count", "active", "meta", "note"])).toEqual({
                    name: updatedExpected.name,
                    count: updatedExpected.count,
                    active: updatedExpected.active,
                    meta: updatedExpected.meta,
                    note: updatedExpected.note,
                });
            } else {
                const row = sqliteRowById(tname, created.rn!.id!);
                const normalized = normalizeRow(row ?? {}, { json: ["meta"], boolean: ["active"] });
                expect(pick(normalized, ["name", "count", "active", "meta", "note"])).toEqual({
                    name: updatedExpected.name,
                    count: updatedExpected.count,
                    active: updatedExpected.active,
                    meta: updatedExpected.meta,
                    note: updatedExpected.note,
                });
            }
        });
    }

    for (const ident of ["sqlite", "pg", "mysql"] as const) {
        test(`${ident} supports IN filtering across scalars, JSON paths, and nulls`, async () => {
            const names = makeNames(`in${ident}`);
            const baseId = makeId(`in${ident}`);
            const data1: DemoRecord = {
                name: `alpha-${baseId}`,
                count: 1,
                active: true,
                meta: {
                    owner: `owner-${baseId}-a`,
                    rating: 4.1,
                    flags: { hot: true }
                },
                note: null
            };
            const data2: DemoRecord = {
                name: `bravo-${baseId}`,
                count: 2,
                active: false,
                meta: {
                    owner: `owner-${baseId}-b`,
                    rating: 2.3,
                    flags: { hot: false }
                },
                note: "note"
            };
            const data3: DemoRecord = {
                name: `charlie-${baseId}`,
                count: 3,
                active: true,
                meta: {
                    owner: `owner-${baseId}-c`,
                    rating: 5,
                    flags: { hot: true }
                },
                note: "n/a"
            };

            const created1 = await createItem<DemoRecord>(ident, names.namespace, names.kind, data1);
            const created2 = await createItem<DemoRecord>(ident, names.namespace, names.kind, data2);
            const created3 = await createItem<DemoRecord>(ident, names.namespace, names.kind, data3);
            const ids = [created1.rn!.id!, created2.rn!.id!];

            const byName = await queryItems<DemoRecord>(
                ident,
                names.namespace,
                names.kind,
                inList("name", [data1.name, data3.name])
            );
            expect(byName.map((item) => item.data?.name).sort()).toEqual([data1.name, data3.name].sort());

            const byCount = await queryItems<DemoRecord>(
                ident,
                names.namespace,
                names.kind,
                inList("count", [2, 99])
            );
            expect(byCount.length).toBe(1);
            expect(byCount[0]!.data?.name).toBe(data2.name);

            const byActive = await queryItems<DemoRecord>(
                ident,
                names.namespace,
                names.kind,
                inList("active", [true])
            );
            expect(byActive.map((item) => item.data?.name).sort()).toEqual([data1.name, data3.name].sort());

            const byRating = await queryItems<DemoRecord>(
                ident,
                names.namespace,
                names.kind,
                inList("meta.rating", [data1.meta.rating, data2.meta.rating])
            );
            expect(byRating.map((item) => item.data?.name).sort()).toEqual([data1.name, data2.name].sort());

            const byFlag = await queryItems<DemoRecord>(
                ident,
                names.namespace,
                names.kind,
                inList("meta.flags.hot", [false])
            );
            expect(byFlag.length).toBe(1);
            expect(byFlag[0]!.data?.name).toBe(data2.name);

            const byNull = await queryItems<DemoRecord>(
                ident,
                names.namespace,
                names.kind,
                inList("note", [null])
            );
            expect(byNull.length).toBe(1);
            expect(byNull[0]!.data?.name).toBe(data1.name);

            const byNullOrValue = await queryItems<DemoRecord>(
                ident,
                names.namespace,
                names.kind,
                inList("note", [null, "n/a"])
            );
            expect(byNullOrValue.map((item) => item.data?.name).sort()).toEqual([data1.name, data3.name].sort());

            const byIds = await queryItems<DemoRecord>(
                ident,
                names.namespace,
                names.kind,
                inList("rnId", ids)
            );
            expect(byIds.map((item) => item.rn?.id).sort()).toEqual(ids.sort());

            const emptyList = await queryItems<DemoRecord>(
                ident,
                names.namespace,
                names.kind,
                inList("name", [])
            );
            expect(emptyList.length).toBe(0);
        });
    }

    for (const ident of ["sqlite", "pg", "mysql"] as const) {
        test(`${ident} supports array paths, wildcards, and LIKE filtering`, async () => {
            const names = makeNames(`arrayq${ident}`);
            const baseId = makeId(`arrayq${ident}`);
            const data1: ArrayRecord = {
                name: `alpha-${baseId}`,
                tags: ["alpha", "beta"],
                meta: {
                    rating: 4.4,
                    addresses: [
                        { city: "Paris", zip: 75000 },
                        { city: "Lyon", zip: 69000 }
                    ]
                },
                matrix: [
                    [1, 2],
                    [3, 4]
                ]
            };
            const data2: ArrayRecord = {
                name: `bravo-${baseId}`,
                tags: ["gamma", "delta"],
                meta: {
                    rating: 3.1,
                    addresses: [
                        { city: "Berlin", zip: 10115 }
                    ]
                },
                matrix: [
                    [9, 9],
                    [42, 5]
                ]
            };

            await createItem<ArrayRecord>(ident, names.namespace, names.kind, data1);
            await createItem<ArrayRecord>(ident, names.namespace, names.kind, data2);

            const byTagIndex = await queryItems<ArrayRecord>(
                ident,
                names.namespace,
                names.kind,
                eq("tags[0]", "alpha")
            );
            expect(byTagIndex.length).toBe(1);
            expect(byTagIndex[0]!.data!.name).toBe(data1.name);

            const byTagWildcard = await queryItems<ArrayRecord>(
                ident,
                names.namespace,
                names.kind,
                eq("tags[*]", "delta")
            );
            expect(byTagWildcard.length).toBe(1);
            expect(byTagWildcard[0]!.data!.name).toBe(data2.name);

            const byCityWildcard = await queryItems<ArrayRecord>(
                ident,
                names.namespace,
                names.kind,
                eq("meta.addresses[*].city", "Paris")
            );
            expect(byCityWildcard.length).toBe(1);
            expect(byCityWildcard[0]!.data!.name).toBe(data1.name);

            const byMatrixIndex = await queryItems<ArrayRecord>(
                ident,
                names.namespace,
                names.kind,
                eq("matrix[1][0]", 42)
            );
            expect(byMatrixIndex.length).toBe(1);
            expect(byMatrixIndex[0]!.data!.name).toBe(data2.name);

            const byLike = await queryItems<ArrayRecord>(
                ident,
                names.namespace,
                names.kind,
                like("name", "alpha%")
            );
            expect(byLike.length).toBe(1);
            expect(byLike[0]!.data!.name).toBe(data1.name);

            const mixedOr = await queryItems<ArrayRecord>(
                ident,
                names.namespace,
                names.kind,
                or(eq("name", data1.name), eq("meta.addresses[*].city", "Berlin"))
            );
            expect(mixedOr.length).toBe(2);

            const tagIn = await queryItems<ArrayRecord>(
                ident,
                names.namespace,
                names.kind,
                inList("tags[*]", ["alpha", "delta"])
            );
            expect(tagIn.length).toBe(2);

            const matrixIn = await queryItems<ArrayRecord>(
                ident,
                names.namespace,
                names.kind,
                inList("matrix[1][0]", [42, 99])
            );
            expect(matrixIn.length).toBe(1);
            expect(matrixIn[0]!.data!.name).toBe(data2.name);
        });
    }

    for (const ident of ["sqlite", "pg", "mysql"] as const) {
        test(`${ident} encrypts and decrypts fields`, async () => {
            const names = makeNames(`enc${ident}`);
            const baseId = makeId(`enc${ident}`);
            const tname = tableName(names.namespace, names.kind);
            const username = `user-${baseId}`;
            const passwordPlain = `pw-${baseId}`;
            const secretValue = { code: `secret-${baseId}`, flags: ["alpha", "beta"] };

            const created = await createItem<SecureRecord>(ident, names.namespace, names.kind, {
                username,
                password: await korm.password(passwordPlain),
                secret: await korm.encrypt(secretValue)
            });
            expect(created.rn?.id).toBeTruthy();

            if (ident === "pg") {
                const domainExpected = await pgDomainExists("korm_encrypted_json");
                const encryptedType: ColumnTypeExpectation = domainExpected ? "korm_encrypted_json" : "jsonb";
                await assertPgColumns(tname, {
                    rnId: "text",
                    username: "text",
                    password: encryptedType,
                    secret: encryptedType
                });
            } else if (ident === "mysql") {
                await assertMysqlColumns(tname, {
                    rnId: { type: "varchar" },
                    username: { type: "text" },
                    password: { type: "json" },
                    secret: { type: "json" }
                });
            } else {
                assertSqliteColumns(tname, {
                    rnId: "id_text",
                    username: "text",
                    password: "encrypted_json",
                    secret: "encrypted_json"
                });
            }

            let row: Record<string, any> | undefined;
            if (ident === "pg") {
                row = await pgRowById(tname, created.rn!.id!);
            } else if (ident === "mysql") {
                row = await mysqlRowById(tname, created.rn!.id!);
            } else {
                row = sqliteRowById(tname, created.rn!.id!);
            }

            const normalized = normalizeRow(row ?? {}, { json: ["password", "secret"] });
            expect(normalized.password?.__ENCRYPTED__).toBe(true);
            expect(normalized.password?.type).toBe("password");
            expect(typeof normalized.password?.value).toBe("string");
            expect(normalized.secret?.__ENCRYPTED__).toBe(true);
            expect(normalized.secret?.type).toBe("symmetric");
            expect(normalized.secret?.iv).toBeTruthy();
            expect(normalized.secret?.authTag).toBeTruthy();

            const fetched = await queryItems<SecureRecord>(
                ident,
                names.namespace,
                names.kind,
                eq("username", username)
            );
            expect(fetched.length).toBe(1);
            const fetchedData = fetched[0]!.data as {
                password: Encrypt<string>;
                secret: Encrypt<{ code: string; flags: string[] }>;
            };
            const passwordMatches = await fetchedData.password.verifyPassword(passwordPlain);
            expect(passwordMatches).toBe(true);
            expect(fetchedData.password.safeValue().value).toBe(normalized.password.value);
            expect(fetchedData.secret.reveal()).toEqual(secretValue);
        });

        test(`${ident} filters encrypted fields in memory`, async () => {
            const names = makeNames(`encf${ident}`);
            const baseId = makeId(`encfilter${ident}`);
            const username = `user-${baseId}`;
            const passwordPlain = `pw-${baseId}`;
            const secretValue = { code: `secret-${baseId}`, flags: ["alpha"] };

            await createItem<SecureRecord>(ident, names.namespace, names.kind, {
                username,
                password: await korm.password(passwordPlain),
                secret: await korm.encrypt(secretValue)
            });

            const fetched = await queryItems<SecureRecord>(
                ident,
                names.namespace,
                names.kind,
                eq("secret.code", secretValue.code)
            );
            expect(fetched.length).toBe(1);
            expect(fetched[0]!.data?.username).toBe(username);
        });

        test(`${ident} re-encrypts decrypted fields on commit`, async () => {
            const names = makeNames(`reenc${ident}`);
            const baseId = makeId(`reenc${ident}`);
            const tname = tableName(names.namespace, names.kind);
            const username = `user-${baseId}`;
            const passwordPlain = `pw-${baseId}`;
            const secretValue = { code: `secret-${baseId}`, flags: ["alpha"] };

            await createItem<SecureRecord>(ident, names.namespace, names.kind, {
                username,
                password: await korm.password(passwordPlain),
                secret: await korm.encrypt(secretValue)
            });

            const fetched = await queryItems<SecureRecord>(
                ident,
                names.namespace,
                names.kind,
                eq("username", username)
            );
            expect(fetched.length).toBe(1);

            const updated = fetched[0]!.update({ username: `${username}-next` }).unwrap();
            const committed = await updated.commit();
            expect(committed.isOk()).toBe(true);

            let row: Record<string, any> | undefined;
            if (ident === "pg") {
                row = await pgRowById(tname, fetched[0]!.rn!.id!);
            } else if (ident === "mysql") {
                row = await mysqlRowById(tname, fetched[0]!.rn!.id!);
            } else {
                row = sqliteRowById(tname, fetched[0]!.rn!.id!);
            }

            const normalized = normalizeRow(row ?? {}, { json: ["password", "secret"] });
            expect(normalized.password?.__ENCRYPTED__).toBe(true);
            expect(normalized.password?.type).toBe("password");
            expect(normalized.secret?.__ENCRYPTED__).toBe(true);
            expect(normalized.secret?.type).toBe("symmetric");
        });
    }

    test("resolvePaths respects allowMissing option", async () => {
        const names = makeNames("missingresolve");
        const userNames = makeNames("missingresolveuser");
        const missingId = randomUUID();
        const missingRn = korm.rn(
            `[rn][from::pg]:${userNames.namespace}:${userNames.kind}:${missingId}`
        );

        const created = await createItem<MissingRefRecord>("sqlite", names.namespace, names.kind, {
            label: `missing-${missingId}`,
            owner: missingRn.value()
        });

        const allowMissingRes = await korm.item<MissingRefRecord>(layerPool).from.rn(
            created.rn!,
            { resolvePaths: ["owner"] }
        );
        expect(allowMissingRes.isOk()).toBe(true);
        const allowMissingItem = allowMissingRes.unwrap();
        expect(allowMissingItem.data?.owner).toBe(missingRn.value());

        const strictRes = await korm.item<MissingRefRecord>(layerPool).from.rn(
            created.rn!,
            { resolvePaths: ["owner"], allowMissing: false }
        );
        expect(strictRes.isErr()).toBe(true);
    });

    test("mysql hashes table names longer than 64 characters", async () => {
        const namespace = `n${"a".repeat(40)}`;
        const kind = `k${"b".repeat(40)}`;
        const rawTable = tableName(namespace, kind);
        const resolved = mysqlTableName(rawTable);
        expect(rawTable.length).toBeGreaterThan(64);
        expect(resolved.length).toBeLessThanOrEqual(64);
        expect(resolved).not.toBe(rawTable);

        const created = await createItem<SimpleRecord>("mysql", namespace, kind, {
            label: "long-name",
            score: 7
        });
        expect(await mysqlTableExists(rawTable)).toBe(true);
        const row = await mysqlRowById(rawTable, created.rn!.id!);
        expect(pick(row ?? {}, ["label", "score"])).toEqual({ label: "long-name", score: 7 });
    });

    test("shared data across layers via RN references", async () => {
        const userNames = makeNames("users");
        const carNames = makeNames("cars");
        const regNames = makeNames("registrations");
        const baseId = makeId("shared");
        const userTable = tableName(userNames.namespace, userNames.kind);
        const carTable = tableName(carNames.namespace, carNames.kind);
        const regTable = tableName(regNames.namespace, regNames.kind);

        const user = await createItem<User>("pg", userNames.namespace, userNames.kind, {
            firstName: "Ada",
            lastName: `Lovelace-${baseId}`
        });

        const car = await createItem<Car>("mysql", carNames.namespace, carNames.kind, {
            make: "Citroen",
            model: "C4",
            year: 2014,
            owner: user.rn!.value(),
            meta: { inspected: true }
        });

        const reg = await createItem<Registration>("sqlite", regNames.namespace, regNames.kind, {
            carRef: car.rn!.value(),
            active: true,
            note: "ok"
        });

        const users = await queryItems<User>(
            "pg",
            userNames.namespace,
            userNames.kind,
            and(eq("firstName", "Ada"), eq("lastName", `Lovelace-${baseId}`))
        );
        expect(users.length).toBe(1);
        expect(users[0]?.rn?.mods.get("from")).toBe("pg");

        const cars = await queryItems<Car>(
            "mysql",
            carNames.namespace,
            carNames.kind,
            eq("owner", user.rn!.value())
        );
        expect(cars.length).toBe(1);
        expect(cars[0]?.rn?.mods.get("from")).toBe("mysql");

        const regs = await queryItems<Registration>(
            "sqlite",
            regNames.namespace,
            regNames.kind,
            eq("carRef", car.rn!.value())
        );
        expect(regs.length).toBe(1);
        expect(regs[0]?.rn?.mods.get("from")).toBe("sqlite");
        expect(reg.data?.carRef).toBe(car.rn!.value());

        expect(await pgTableExists(userTable)).toBe(true);
        await assertPgColumns(userTable, {
            rnId: "text",
            firstName: "text",
            lastName: "text",
        });
        const userRow = await pgRowById(userTable, user.rn!.id!);
        expect(userRow?.rnId).toBe(user.rn!.id);
        expect(pick(userRow ?? {}, ["firstName", "lastName"])).toEqual({
            firstName: "Ada",
            lastName: `Lovelace-${baseId}`
        });

        expect(await mysqlTableExists(carTable)).toBe(true);
        await assertMysqlColumns(carTable, {
            rnId: { type: "varchar" },
            make: { type: "text" },
            model: { type: "text" },
            year: { type: "int" },
            owner: { type: "text" },
            meta: { type: "json" },
        });
        const carRow = await mysqlRowById(carTable, car.rn!.id!);
        expect(carRow?.rnId).toBe(car.rn!.id);
        const normalizedCar = normalizeRow(carRow ?? {}, { json: ["meta"] });
        expect(pick(normalizedCar, ["make", "model", "year", "owner", "meta"])).toEqual({
            make: "Citroen",
            model: "C4",
            year: 2014,
            owner: user.rn!.value(),
            meta: { inspected: true }
        });

        expect(sqliteTableExists(regTable)).toBe(true);
        assertSqliteColumns(regTable, {
            rnId: "id_text",
            carRef: "rn_ref_text",
            active: "boolean",
            note: "text"
        });
        const regRow = sqliteRowById(regTable, reg.rn!.id!);
        expect(regRow?.rnId).toBe(reg.rn!.id);
        const normalizedReg = normalizeRow(regRow ?? {}, { boolean: ["active"] });
        expect(pick(normalizedReg, ["carRef", "active", "note"])).toEqual({
            carRef: car.rn!.value(),
            active: true,
            note: "ok"
        });

        const carsByOwner = await korm.item<Car>(layerPool).from.query(
            korm.rn(`[rn][from::mysql]:${carNames.namespace}:${carNames.kind}:*`)
        ).where(eq("owner.firstName", "Ada")).get();
        const carsByOwnerItems = carsByOwner.unwrap();
        expect(carsByOwnerItems.length).toBe(1);
        expect(carsByOwnerItems[0]!.data?.owner).toBe(user.rn!.value());
    });

    test("resolvePaths handles arrays and reference overrides", async () => {
        const userNames = makeNames("groupusers");
        const groupNames = makeNames("groups");
        const baseId = makeId("groupref");
        const userTable = tableName(userNames.namespace, userNames.kind);
        const groupTable = tableName(groupNames.namespace, groupNames.kind);

        const user1 = await createItem<User>("pg", userNames.namespace, userNames.kind, {
            firstName: "Ada",
            lastName: `Group-${baseId}`
        });

        const user2 = await createItem<User>("pg", userNames.namespace, userNames.kind, {
            firstName: "Bob",
            lastName: `Group-${baseId}`
        });

        const group = await createItem<GroupRecord>("sqlite", groupNames.namespace, groupNames.kind, {
            name: `group-${baseId}`,
            primary: user1.rn!,
            backup: user2.rn!,
            members: [user1.rn!, user2.rn!]
        });

        const groupRes = await korm.item<GroupRecord>(layerPool).from.query(
            korm.rn(`[rn][from::sqlite]:${groupNames.namespace}:${groupNames.kind}:*`)
        ).get({ resolvePaths: ["primary", "backup", "members[*]"] as const });
        const groups = groupRes.unwrap();
        expect(groups.length).toBe(1);

        const resolved = groups[0]!;
        expect(resolved.data?.primary.firstName).toBe("Ada");
        expect(resolved.data?.backup?.firstName).toBe("Bob");
        expect(resolved.data?.members?.[1]?.firstName).toBe("Bob");

        const updated = resolved.update({
            primary: user2.rn!.value() as any,
            backup: null,
            members: [user2.rn!.value()] as any
        }).unwrap();
        const committed = await updated.commit();
        expect(committed.isOk()).toBe(true);

        const groupRow = sqliteRowById(groupTable, group.rn!.id!);
        const normalizedGroup = normalizeRow(groupRow ?? {}, { json: ["members"] });
        expect(pick(normalizedGroup, ["primary", "backup", "members"])).toEqual({
            primary: user2.rn!.value(),
            backup: null,
            members: [user2.rn!.value()]
        });

        const userRow1 = await pgRowById(userTable, user1.rn!.id!);
        expect(pick(userRow1 ?? {}, ["firstName", "lastName"])).toEqual({
            firstName: "Ada",
            lastName: `Group-${baseId}`
        });
        const userRow2 = await pgRowById(userTable, user2.rn!.id!);
        expect(pick(userRow2 ?? {}, ["firstName", "lastName"])).toEqual({
            firstName: "Bob",
            lastName: `Group-${baseId}`
        });
    });

    test("resolvePaths cascades updates across layers and preserves RN storage", async () => {
        const userNames = makeNames("resolveUsers");
        const carNames = makeNames("resolveCars");
        const regNames = makeNames("resolveRegs");
        const baseId = makeId("resolve");
        const userTable = tableName(userNames.namespace, userNames.kind);
        const carTable = tableName(carNames.namespace, carNames.kind);
        const regTable = tableName(regNames.namespace, regNames.kind);

        const user = await createItem<ResolvedUser>("pg", userNames.namespace, userNames.kind, {
            firstName: "Ada",
            lastName: `Lovelace-${baseId}`,
            password: await korm.password(`pw-${baseId}`),
            secret: await korm.encrypt({ code: `secret-${baseId}`, flags: ["alpha"] })
        });

        const car = await createItem<ResolvedCar>("mysql", carNames.namespace, carNames.kind, {
            make: "Citroen",
            model: "C4",
            year: 2014,
            owner: user.rn!,
            meta: { inspected: true }
        });

        const reg = await createItem<ResolvedRegistration>("sqlite", regNames.namespace, regNames.kind, {
            carRef: car.rn!,
            active: true,
            note: "ok"
        });

        const resolvePaths = ["carRef.*"] as const;
        const regsResult = await korm.item<ResolvedRegistration>(layerPool).from.query(
            korm.rn(`[rn][from::sqlite]:${regNames.namespace}:${regNames.kind}:*`)
        ).get({ resolvePaths });
        const regs = regsResult.unwrap();
        expect(regs.length).toBe(1);

        const resolvedReg = regs[0]!;
        expect(resolvedReg.data?.carRef.owner.firstName).toBe("Ada");
        expect(resolvedReg.data?.carRef.owner.lastName).toBe(`Lovelace-${baseId}`);

        const nextPassword = await korm.password(`pw-next-${baseId}`);
        const nextSecret = await korm.encrypt({ code: `secret-next-${baseId}`, flags: ["beta"] });
        const updated = resolvedReg.update({
            carRef: {
                owner: {
                    firstName: "Grace",
                    password: nextPassword,
                    secret: nextSecret
                }
            }
        }).unwrap();

        const committed = await updated.commit();
        expect(committed.isOk()).toBe(true);
        const committedItem = committed.unwrap();
        expect(committedItem.data?.carRef.owner.firstName).toBe("Grace");

        const userRow = await pgRowById(userTable, user.rn!.id!);
        const normalizedUser = normalizeRow(userRow ?? {}, { json: ["password", "secret"] });
        expect(normalizedUser.firstName).toBe("Grace");
        expect(normalizedUser.lastName).toBe(`Lovelace-${baseId}`);
        expect(normalizedUser.password?.__ENCRYPTED__).toBe(true);
        expect(normalizedUser.password?.type).toBe("password");
        expect(normalizedUser.secret?.__ENCRYPTED__).toBe(true);
        expect(normalizedUser.secret?.type).toBe("symmetric");

        const carRow = await mysqlRowById(carTable, car.rn!.id!);
        const normalizedCar = normalizeRow(carRow ?? {}, { json: ["meta"] });
        expect(pick(normalizedCar, ["make", "model", "year", "owner", "meta"])).toEqual({
            make: "Citroen",
            model: "C4",
            year: 2014,
            owner: user.rn!.value(),
            meta: { inspected: true }
        });

        const regRow = sqliteRowById(regTable, reg.rn!.id!);
        const normalizedReg = normalizeRow(regRow ?? {}, { boolean: ["active"] });
        expect(pick(normalizedReg, ["carRef", "active", "note"])).toEqual({
            carRef: car.rn!.value(),
            active: true,
            note: "ok"
        });
    });

    test("depot prefix resolves to file lists", async () => {
        const names = makeNames("depotprefix");
        const prefixId = makeId("prefix");
        const prefixRn = `[rn][depot::local]:reports:${names.namespace}:${names.kind}:${prefixId}:*`;
        const fileRn1 = `[rn][depot::local]:reports:${names.namespace}:${names.kind}:${prefixId}:a.txt`;
        const fileRn2 = `[rn][depot::local]:reports:${names.namespace}:${names.kind}:${prefixId}:b.txt`;
        const content1 = `prefix-${prefixId}-a`;
        const content2 = `prefix-${prefixId}-b`;

        await seedDepotFile(fileRn1, content1);
        await seedDepotFile(fileRn2, content2);

        const created = await createItem<PrefixRecord>("sqlite", names.namespace, names.kind, {
            label: `prefix-${prefixId}`,
            files: korm.rn(prefixRn)
        });

        const resolved = (await korm.item<PrefixRecord>(layerPool).from.rn(
            created.rn!,
            { resolvePaths: ["files"] }
        )).unwrap();

        const resolvedFiles = resolved.data?.files as unknown;
        expect(Array.isArray(resolvedFiles)).toBe(true);
        const fileList = resolvedFiles as DepotFile[];
        expect(fileList.length).toBe(2);
        fileList.forEach((file) => expect(file).toBeInstanceOf(DepotFile));
        const contents = await Promise.all(fileList.map((file) => file.text()));
        expect(new Set(contents)).toEqual(new Set([content1, content2]));
    });

    for (const ident of ["sqlite", "pg", "mysql"] as const) {
        test(`array depot files persist and resolve with ${ident}`, async () => {
            const names = makeNames(`depotarray${ident}`);
            const table = tableName(names.namespace, names.kind);
            const fileId = makeId(`bundle${ident}`);
            const fileRn1 = `[rn][depot::local]:bundles:${names.namespace}:${names.kind}:${fileId}-a.txt`;
            const fileRn2 = `[rn][depot::local]:bundles:${names.namespace}:${names.kind}:${fileId}-b.txt`;
            const content1 = `bundle-${fileId}-a`;
            const content2 = `bundle-${fileId}-b`;

            const file1 = korm.file({
                rn: korm.rn(fileRn1),
                file: new Blob([content1], { type: "text/plain" })
            });
            const file2 = korm.file({
                rn: korm.rn(fileRn2),
                file: new Blob([content2], { type: "text/plain" })
            });

            const created = await createItem<FileBundleRecord>(ident, names.namespace, names.kind, {
                label: `bundle-${fileId}`,
                files: [file1, file2]
            });

            const rawRow = await rowById(ident, table, created.rn!.id!);
            const normalized = normalizeRow(rawRow ?? {}, { json: ["files"] });
            expect(normalized.files).toEqual([fileRn1, fileRn2]);

            const resolved = (await korm.item<FileBundleRecord>(layerPool).from.rn(
                created.rn!,
                { resolvePaths: ["files[*]"] }
            )).unwrap();

            const resolvedFiles = resolved.data?.files as DepotFile[];
            expect(resolvedFiles.length).toBe(2);
            const contents = await Promise.all(resolvedFiles.map((file) => file.text()));
            expect(new Set(contents)).toEqual(new Set([content1, content2]));
        });
    }

    for (const depotIdent of ["local", "s3"] as const) {
        for (const ident of ["sqlite", "pg", "mysql"] as const) {
            test(`${depotIdent} depot files persist and resolve with ${ident}`, async () => {
                const names = makeNames(`depot${depotIdent}${ident}`);
                const table = tableName(names.namespace, names.kind);
                const fileId = makeId(`file${depotIdent}${ident}`);
                const fileRn = `[rn][depot::${depotIdent}]:invoices:${names.namespace}:${names.kind}:${fileId}.txt`;
                const content = `invoice-${depotIdent}-${ident}-${fileId}`;

                const file = korm.file({
                    rn: korm.rn(fileRn),
                    file: new Blob([content], { type: "text/plain" })
                });

                const created = await createItem<InvoiceRecord>(ident, names.namespace, names.kind, {
                    label: `invoice-${fileId}`,
                    file
                });

                const rawRow = await rowById(ident, table, created.rn!.id!);
                expect(rawRow?.file).toBe(fileRn);

                const resolved = (await korm.item<InvoiceRecord>(layerPool).from.rn(
                    created.rn!,
                    { resolvePaths: ["file"] }
                )).unwrap();

                const resolvedFile = resolved.data?.file;
                expect(isDepotFile(resolvedFile)).toBe(true);
                expect(resolvedFile).toBeInstanceOf(DepotFile);
                const committedFile = resolvedFile as DepotFile;
                expect(committedFile.state).toBe("committed");
                expect(await committedFile.text()).toBe(content);

                const updated = await committedFile.update(async (current) => {
                    const previous = await current.text();
                    return new Blob([`${previous}-updated`], { type: "text/plain" });
                });
                expect(updated.state).toBe("uncommitted");
                const updatedCommitted = await updated.commit(layerPool);
                expect(updatedCommitted.state).toBe("committed");

                const resolvedAgain = (await korm.item<InvoiceRecord>(layerPool).from.rn(
                    created.rn!,
                    { resolvePaths: ["file"] }
                )).unwrap();

                const resolvedAgainFile = resolvedAgain.data?.file;
                expect(isDepotFile(resolvedAgainFile)).toBe(true);
                expect(resolvedAgainFile).toBeInstanceOf(DepotFile);
                expect(await (resolvedAgainFile as DepotFile).text()).toBe(`${content}-updated`);

                const rawRowAfter = await rowById(ident, table, created.rn!.id!);
                expect(rawRowAfter?.file).toBe(fileRn);
            });
        }
    }

    test("wal writes encrypted payloads without plaintext", async () => {
        const walNamespace = `walenc${makeId("walenc")}`;
        const walPool = korm.pool()
            .setLayers({ layer: sqll, ident: "sqlite" })
            .setDepots({ depot: localDepot, ident: "local" })
            .withWal({ depotIdent: "local", walNamespace, retention: "keep" })
            .open();

        const secretValue = `wal-secret-${makeId("secret")}`;
        const passwordValue = `wal-pass-${makeId("pass")}`;
        const password = await korm.password(passwordValue);
        const secret = await korm.encrypt({ code: secretValue, flags: ["a"] });

        const names = makeNames("walenc");
        const created = await korm.item<SecureRecord>(walPool).from.data({
            namespace: names.namespace,
            kind: names.kind,
            mods: fromMod("sqlite"),
            data: { username: "wal-user", password, secret }
        }).create();
        expect(created.isOk()).toBe(true);

        const wal = walPool.wal!;
        const doneFiles = await localDepot.listFiles(
            walPrefixRn("local", walNamespace, wal.poolId, "done")
        );
        expect(doneFiles.length).toBeGreaterThan(0);
        const text = await doneFiles[0]!.text();
        const record = JSON.parse(text) as WalRecord;
        const recordText = JSON.stringify(record);
        expect(record.state).toBe("done");
        expect(recordText).not.toContain(secretValue);
        expect(recordText).not.toContain(passwordValue);
        expect(recordText).not.toContain("\"__ENCRYPT__\"");
        expect(recordText).toContain("\"__ENCRYPTED__\":true");
    });

    test("wal records include before images for updates", async () => {
        const walNamespace = `walbefore${makeId("walbefore")}`;
        const walPool = korm.pool()
            .setLayers({ layer: sqll, ident: "sqlite" })
            .setDepots({ depot: localDepot, ident: "local" })
            .withWal({ depotIdent: "local", walNamespace, retention: "keep" })
            .open();

        const names = makeNames("walbefore");
        const created = await korm.item<SimpleRecord>(walPool).from.data({
            namespace: names.namespace,
            kind: names.kind,
            mods: fromMod("sqlite"),
            data: { label: "wal-before", score: 1 }
        }).create();
        expect(created.isOk()).toBe(true);

        const updated = created.unwrap().update({ score: 2 }).unwrap();
        const committed = await updated.commit();
        expect(committed.isOk()).toBe(true);

        const wal = walPool.wal!;
        const doneFiles = await localDepot.listFiles(
            walPrefixRn("local", walNamespace, wal.poolId, "done")
        );
        expect(doneFiles.length).toBeGreaterThan(0);
        const records = await Promise.all(doneFiles.map(async (file) => {
            const text = await file.text();
            return JSON.parse(text) as WalRecord;
        }));
        const updateRecord = records.find((record) => record.ops.some((op) => op.type === "update"));
        expect(updateRecord).toBeTruthy();
        const updateOp = updateRecord!.ops.find((op) => op.type === "update")!;
        expect(updateOp.before).toEqual({ label: "wal-before", score: 1 });
        expect(updateOp.data).toEqual({ label: "wal-before", score: 2 });
    });

    test("wal update records keep encrypted payloads without plaintext", async () => {
        const walNamespace = `walupdate${makeId("walupdate")}`;
        const walPool = korm.pool()
            .setLayers({ layer: sqll, ident: "sqlite" })
            .setDepots({ depot: localDepot, ident: "local" })
            .withWal({ depotIdent: "local", walNamespace, retention: "keep" })
            .open();

        const passwordValue = `wal-update-pass-${makeId("pass")}`;
        const secretValue = `wal-update-secret-${makeId("secret")}`;
        const nextSecret = `wal-update-secret-next-${makeId("secret")}`;

        const names = makeNames("walupdate");
        const created = await korm.item<SecureRecord>(walPool).from.data({
            namespace: names.namespace,
            kind: names.kind,
            mods: fromMod("sqlite"),
            data: {
                username: "wal-update-user",
                password: await korm.password(passwordValue),
                secret: await korm.encrypt({ code: secretValue, flags: ["x"] })
            }
        }).create();
        expect(created.isOk()).toBe(true);
        const createdItem = created.unwrap();

        const updated = createdItem.update({
            secret: await korm.encrypt({ code: nextSecret, flags: ["y"] })
        }).unwrap();
        const committed = await updated.commit();
        expect(committed.isOk()).toBe(true);

        const wal = walPool.wal!;
        const doneFiles = await localDepot.listFiles(
            walPrefixRn("local", walNamespace, wal.poolId, "done")
        );
        const records = await Promise.all(doneFiles.map(async (file) => {
            const text = await file.text();
            return JSON.parse(text) as WalRecord;
        }));
        const updateRecord = records.find((record) =>
            record.ops.some((op) => op.type === "update" && op.rn === createdItem.rn!.value())
        );
        expect(updateRecord).toBeTruthy();
        const recordText = JSON.stringify(updateRecord);
        expect(recordText).not.toContain(secretValue);
        expect(recordText).not.toContain(nextSecret);
        expect(recordText).not.toContain(passwordValue);
        expect(recordText).not.toContain("\"__ENCRYPT__\"");
        expect(recordText).toContain("\"__ENCRYPTED__\":true");
    });

    test("wal records include depot file payloads when enabled", async () => {
        const walNamespace = `waldepot${makeId("waldepot")}`;
        const walPool = korm.pool()
            .setLayers({ layer: sqll, ident: "sqlite" })
            .setDepots({ depot: localDepot, ident: "local" })
            .withWal({ depotIdent: "local", walNamespace, retention: "keep", depotOps: "record" })
            .open();

        const names = makeNames("waldepot");
        const fileId = makeId("waldepotfile");
        const fileRn = `[rn][depot::local]:wal:${names.namespace}:${names.kind}:${fileId}.txt`;
        const content = `wal-depot-${fileId}`;
        const file = korm.file({
            rn: korm.rn(fileRn),
            file: new Blob([content], { type: "text/plain" })
        });

        const created = await korm.item<InvoiceRecord>(walPool).from.data({
            namespace: names.namespace,
            kind: names.kind,
            mods: fromMod("sqlite"),
            data: { label: `wal-${fileId}`, file }
        }).create();
        expect(created.isOk()).toBe(true);

        const wal = walPool.wal!;
        const doneFiles = await localDepot.listFiles(
            walPrefixRn("local", walNamespace, wal.poolId, "done")
        );
        const records = await Promise.all(doneFiles.map(async (file) => {
            const text = await file.text();
            return JSON.parse(text) as WalRecord;
        }));
        const record = records.find((entry) => entry.depotOps?.some((op) => op.rn === fileRn));
        expect(record).toBeTruthy();
        const depotOp = record!.depotOps!.find((op) => op.rn === fileRn)!;
        const payloadFile = await localDepot.getFile(korm.rn(depotOp.payloadRn));
        expect(await payloadFile.text()).toBe(content);
    });

    test("wal recovery replays pending records", async () => {
        const walNamespace = `walreplay${makeId("walreplay")}`;
        const walPool = korm.pool()
            .setLayers({ layer: sqll, ident: "sqlite" })
            .setDepots({ depot: localDepot, ident: "local" })
            .withWal({ depotIdent: "local", walNamespace, retention: "delete" })
            .open();

        const names = makeNames("walreplay");
        const id = randomUUID();
        const rn = korm.rn(`[rn]:${names.namespace}:${names.kind}:${id}`);
        rn.mod("from", "sqlite");
        const data: SimpleRecord = { label: `wal-${id}`, score: 42 };

        await walPool.wal!.stage([
            { type: "insert", rn: rn.value(), data }
        ]);

        const recoveryPool = korm.pool()
            .setLayers({ layer: sqll, ident: "sqlite" })
            .setDepots({ depot: localDepot, ident: "local" })
            .withWal({ depotIdent: "local", walNamespace, retention: "delete" })
            .open();
        await recoveryPool.ensureWalReady();

        const table = tableName(names.namespace, names.kind);
        expect(sqliteTableExists(table)).toBe(true);
        const row = sqliteRowById(table, id);
        expect(pick(row ?? {}, ["label", "score"])).toEqual(data);

        const wal = recoveryPool.wal!;
        const pendingFiles = await localDepot.listFiles(
            walPrefixRn("local", walNamespace, wal.poolId, "pending")
        );
        const doneFiles = await localDepot.listFiles(
            walPrefixRn("local", walNamespace, wal.poolId, "done")
        );
        expect(pendingFiles.length).toBe(0);
        expect(doneFiles.length).toBe(0);
    });

    test("wal stages tx operations across layers", async () => {
        const walNamespace = `waltx${makeId("waltx")}`;
        const walPool = korm.pool()
            .setLayers(
                { layer: sqll, ident: "sqlite" },
                { layer: pg, ident: "pg" },
                { layer: mysql, ident: "mysql" },
            )
            .setDepots({ depot: localDepot, ident: "local" })
            .withWal({ depotIdent: "local", walNamespace, retention: "keep" })
            .open();

        const sqliteNames = makeNames("waltxsqlite");
        const pgNames = makeNames("waltxpg");
        const mysqlNames = makeNames("waltxmysql");

        const sqliteItem = korm.item<SimpleRecord>(walPool).from.data({
            namespace: sqliteNames.namespace,
            kind: sqliteNames.kind,
            mods: fromMod("sqlite"),
            data: { label: "wal-sqlite", score: 1 }
        }).unwrap();

        const pgItem = korm.item<SimpleRecord>(walPool).from.data({
            namespace: pgNames.namespace,
            kind: pgNames.kind,
            mods: fromMod("pg"),
            data: { label: "wal-pg", score: 2 }
        }).unwrap();

        const mysqlItem = korm.item<SimpleRecord>(walPool).from.data({
            namespace: mysqlNames.namespace,
            kind: mysqlNames.kind,
            mods: fromMod("mysql"),
            data: { label: "wal-mysql", score: 3 }
        }).unwrap();

        const txRes = await korm.tx(sqliteItem, pgItem, mysqlItem).persist();
        expect(txRes.isOk()).toBe(true);

        const wal = walPool.wal!;
        const doneFiles = await localDepot.listFiles(
            walPrefixRn("local", walNamespace, wal.poolId, "done")
        );
        expect(doneFiles.length).toBe(1);
        const record = JSON.parse(await doneFiles[0]!.text()) as WalRecord;
        expect(record.ops.length).toBe(3);
    });

    test("tx persists across layers", async () => {
        const sqliteNames = makeNames("txsqlite");
        const pgNames = makeNames("txpg");
        const mysqlNames = makeNames("txmysql");
        const sqliteTable = tableName(sqliteNames.namespace, sqliteNames.kind);
        const pgTable = tableName(pgNames.namespace, pgNames.kind);
        const mysqlTable = tableName(mysqlNames.namespace, mysqlNames.kind);

        const sqliteItem = korm.item<SimpleRecord>(layerPool).from.data({
            namespace: sqliteNames.namespace,
            kind: sqliteNames.kind,
            mods: fromMod("sqlite"),
            data: { label: "sqlite-ok", score: 1 }
        }).unwrap();

        const pgItem = korm.item<SimpleRecord>(layerPool).from.data({
            namespace: pgNames.namespace,
            kind: pgNames.kind,
            mods: fromMod("pg"),
            data: { label: "pg-ok", score: 2 }
        }).unwrap();

        const mysqlItem = korm.item<SimpleRecord>(layerPool).from.data({
            namespace: mysqlNames.namespace,
            kind: mysqlNames.kind,
            mods: fromMod("mysql"),
            data: { label: "mysql-ok", score: 3 }
        }).unwrap();

        const txRes = await korm.tx(sqliteItem, pgItem, mysqlItem).persist();
        expect(txRes.isOk()).toBe(true);

        const sqliteRows = await queryItems<SimpleRecord>(
            "sqlite",
            sqliteNames.namespace,
            sqliteNames.kind,
            eq("label", "sqlite-ok")
        );
        const pgRows = await queryItems<SimpleRecord>(
            "pg",
            pgNames.namespace,
            pgNames.kind,
            eq("label", "pg-ok")
        );
        const mysqlRows = await queryItems<SimpleRecord>(
            "mysql",
            mysqlNames.namespace,
            mysqlNames.kind,
            eq("label", "mysql-ok")
        );
        expect(sqliteRows.length).toBe(1);
        expect(pgRows.length).toBe(1);
        expect(mysqlRows.length).toBe(1);

        expect(sqliteTableExists(sqliteTable)).toBe(true);
        assertSqliteColumns(sqliteTable, {
            rnId: "id_text",
            label: "text",
            score: "integer"
        });
        const sqliteRow = sqliteRowById(sqliteTable, sqliteRows[0]!.rn!.id!);
        expect(pick(sqliteRow ?? {}, ["label", "score"])).toEqual({ label: "sqlite-ok", score: 1 });

        expect(await pgTableExists(pgTable)).toBe(true);
        await assertPgColumns(pgTable, {
            rnId: "text",
            label: "text",
            score: "integer"
        });
        const pgRow = await pgRowById(pgTable, pgRows[0]!.rn!.id!);
        expect(pick(pgRow ?? {}, ["label", "score"])).toEqual({ label: "pg-ok", score: 2 });

        expect(await mysqlTableExists(mysqlTable)).toBe(true);
        await assertMysqlColumns(mysqlTable, {
            rnId: { type: "varchar" },
            label: { type: "text" },
            score: { type: "int" }
        });
        const mysqlRow = await mysqlRowById(mysqlTable, mysqlRows[0]!.rn!.id!);
        expect(pick(mysqlRow ?? {}, ["label", "score"])).toEqual({ label: "mysql-ok", score: 3 });
    });

    test("tx persists uncommitted updates across layers", async () => {
        const sqliteNames = makeNames("txupdsqlite");
        const pgNames = makeNames("txupdpg");
        const sqliteTable = tableName(sqliteNames.namespace, sqliteNames.kind);
        const pgTable = tableName(pgNames.namespace, pgNames.kind);

        const sqliteCreated = await createItem<SimpleRecord>("sqlite", sqliteNames.namespace, sqliteNames.kind, {
            label: "sqlite-base",
            score: 1
        });
        const pgCreated = await createItem<SimpleRecord>("pg", pgNames.namespace, pgNames.kind, {
            label: "pg-base",
            score: 2
        });

        const sqliteUpdated = sqliteCreated.update({ score: 11 }).unwrap();
        const pgUpdated = pgCreated.update({ score: 22 }).unwrap();

        const txRes = await korm.tx(sqliteUpdated, pgUpdated).persist();
        expect(txRes.isOk()).toBe(true);

        const sqliteRow = sqliteRowById(sqliteTable, sqliteCreated.rn!.id!);
        expect(pick(sqliteRow ?? {}, ["label", "score"])).toEqual({ label: "sqlite-base", score: 11 });

        const pgRow = await pgRowById(pgTable, pgCreated.rn!.id!);
        expect(pick(pgRow ?? {}, ["label", "score"])).toEqual({ label: "pg-base", score: 22 });
    });

    test("tx rolls back prior inserts on schema mismatch", async () => {
        const pgNames = makeNames("txbadpg");
        const sqliteNames = makeNames("txbadsqlite");
        const pgTable = tableName(pgNames.namespace, pgNames.kind);
        const sqliteTable = tableName(sqliteNames.namespace, sqliteNames.kind);

        await createItem<SimpleRecord>("pg", pgNames.namespace, pgNames.kind, {
            label: "seed",
            score: 10
        });

        const sqliteItem = korm.item<SimpleRecord>(layerPool).from.data({
            namespace: sqliteNames.namespace,
            kind: sqliteNames.kind,
            mods: fromMod("sqlite"),
            data: { label: "should-rollback", score: 5 }
        }).unwrap();

        const pgBad = korm.item<any>(layerPool).from.data({
            namespace: pgNames.namespace,
            kind: pgNames.kind,
            mods: fromMod("pg"),
            data: { label: "bad", score: "wrong-type" }
        }).unwrap();

        const txRes = await korm.tx(sqliteItem, pgBad).persist();
        expect(txRes.isErr()).toBe(true);

        const sqliteRows = await queryItems<SimpleRecord>(
            "sqlite",
            sqliteNames.namespace,
            sqliteNames.kind,
            eq("label", "should-rollback")
        );
        expect(sqliteRows.length).toBe(0);

        expect(sqliteTableExists(sqliteTable)).toBe(true);
        expect(sqliteCountRows(sqliteTable)).toBe(0);

        expect(await pgTableExists(pgTable)).toBe(true);
        expect(await pgCountRows(pgTable)).toBe(1);
        const pgSeedRow = await pgRowById(pgTable, (await queryItems<SimpleRecord>("pg", pgNames.namespace, pgNames.kind))[0]!.rn!.id!);
        expect(pick(pgSeedRow ?? {}, ["label", "score"])).toEqual({ label: "seed", score: 10 });
    });

    for (const ident of ["sqlite", "pg", "mysql"] as const) {
        test(`${ident} destructive tx allows schema changes and resets column values`, async () => {
            const names = makeNames(`destructive${ident}`);
            const table = tableName(names.namespace, names.kind);

            const item1 = await createItem<SimpleRecord>(ident, names.namespace, names.kind, {
                label: "keep",
                score: 1
            });
            const item2 = await createItem<SimpleRecord>(ident, names.namespace, names.kind, {
                label: "drop",
                score: 2
            });

            const badUpdate = item1.update({ score: "bad" as any }).unwrap();
            const failed = await korm.tx(badUpdate).persist();
            expect(failed.isErr()).toBe(true);

            const row1Before = await rowById(ident, table, item1.rn!.id!);
            const row2Before = await rowById(ident, table, item2.rn!.id!);
            expect(row1Before?.score).toBe(1);
            expect(row2Before?.score).toBe(2);

            const fresh = (await korm.item<SimpleRecord>(layerPool).from.rn(item1.rn!)).unwrap();
            const destructiveUpdate = fresh.update({ score: "bad" as any }).unwrap();
            const ok = await korm.tx(destructiveUpdate).persist({ destructive: true });
            expect(ok.isOk()).toBe(true);

            const row1After = ident === "pg"
                ? await pgRowByIdFresh(table, item1.rn!.id!)
                : await rowById(ident, table, item1.rn!.id!);
            const row2After = ident === "pg"
                ? await pgRowByIdFresh(table, item2.rn!.id!)
                : await rowById(ident, table, item2.rn!.id!);
            expect(row1After?.score).toBe("bad");
            expect(row2After?.score === null || row2After?.score === undefined).toBe(true);
        });
    }

    describe("item-level locking", () => {
        test("KormLocker acquire and release", async () => {
            const locker = layerPool.locker;
            const rn = korm.rn<SimpleRecord>("[rn][from::sqlite]:locktest:basic:1c73f0a4-0c4f-4f5d-9a6e-8c1b8f5848f3");

            expect(locker.isLocked(rn)).toBe(false);

            const release = await locker.acquire(rn);
            expect(locker.isLocked(rn)).toBe(true);

            release();
            expect(locker.isLocked(rn)).toBe(false);
        });

        test("KormLocker tryAcquire returns undefined when locked", async () => {
            const locker = layerPool.locker;
            const rn = korm.rn<SimpleRecord>("[rn][from::sqlite]:locktest:tryacquire:2d1b9f7a-3b17-4c2e-8aa8-9d7a6b44e2e1");

            const release1 = await locker.acquire(rn);
            expect(locker.isLocked(rn)).toBe(true);

            const release2 = locker.tryAcquire(rn);
            expect(release2).toBeUndefined();

            release1();
            expect(locker.isLocked(rn)).toBe(false);

            const release3 = locker.tryAcquire(rn);
            expect(release3).toBeDefined();
            release3!();
        });

        test("KormLocker acquireMultiple acquires in sorted order", async () => {
            const locker = layerPool.locker;
            const rn1 = korm.rn<SimpleRecord>("[rn][from::sqlite]:locktest:multi:ffffffff-ffff-4fff-8fff-ffffffffffff");
            const rn2 = korm.rn<SimpleRecord>("[rn][from::sqlite]:locktest:multi:00000000-0000-4000-8000-000000000000");
            const rn3 = korm.rn<SimpleRecord>("[rn][from::sqlite]:locktest:multi:77777777-7777-4777-8777-777777777777");

            const release = await locker.acquireMultiple([rn1, rn2, rn3]);

            expect(locker.isLocked(rn1)).toBe(true);
            expect(locker.isLocked(rn2)).toBe(true);
            expect(locker.isLocked(rn3)).toBe(true);

            release();

            expect(locker.isLocked(rn1)).toBe(false);
            expect(locker.isLocked(rn2)).toBe(false);
            expect(locker.isLocked(rn3)).toBe(false);
        });

        test("KormLocker acquire waits for lock release", async () => {
            const locker = layerPool.locker;
            const rn = korm.rn<SimpleRecord>("[rn][from::sqlite]:locktest:wait:3e5c8b2a-5f7d-4a1c-bd2e-4f9b8a3c2d1e");

            const release1 = await locker.acquire(rn);
            const order: string[] = [];

            const waiter = (async () => {
                order.push("waiter-start");
                const release2 = await locker.acquire(rn);
                order.push("waiter-acquired");
                release2();
            })();

            // Give the waiter time to start waiting
            await new Promise((resolve) => setTimeout(resolve, 10));
            order.push("releaser-releasing");
            release1();

            await waiter;
            order.push("done");

            expect(order).toEqual([
                "waiter-start",
                "releaser-releasing",
                "waiter-acquired",
                "done"
            ]);
        });

        test("KormLocker acquire times out", async () => {
            const locker = layerPool.locker;
            const rn = korm.rn<SimpleRecord>("[rn][from::sqlite]:locktest:timeout:4f7b9c1d-8e2a-4b3f-8c4d-1e2f3a4b5c6d");

            const release = await locker.acquire(rn);

            let timedOut = false;
            try {
                await locker.acquire(rn, 50); // 50ms timeout
            } catch (error) {
                timedOut = true;
                expect((error as Error).name).toBe("LockTimeoutError");
                expect((error as Error).message).toContain("50ms");
            }

            expect(timedOut).toBe(true);
            release();
        });

        test("concurrent create operations are serialized", async () => {
            const names = makeNames("lockcreate");
            const operations: string[] = [];

            // Create multiple items concurrently - they should be serialized per-RN
            // but since each has a unique RN, they can run in parallel
            const items = await Promise.all([
                (async () => {
                    operations.push("item1-start");
                    const result = await createItem<SimpleRecord>("sqlite", names.namespace, names.kind, {
                        label: "item1",
                        score: 1
                    });
                    operations.push("item1-done");
                    return result;
                })(),
                (async () => {
                    operations.push("item2-start");
                    const result = await createItem<SimpleRecord>("sqlite", names.namespace, names.kind, {
                        label: "item2",
                        score: 2
                    });
                    operations.push("item2-done");
                    return result;
                })(),
            ]);

            expect(items[0].rn).toBeTruthy();
            expect(items[1].rn).toBeTruthy();
            expect(items[0].rn!.value()).not.toBe(items[1].rn!.value());

            const allItems = await queryItems<SimpleRecord>("sqlite", names.namespace, names.kind);
            expect(allItems.length).toBe(2);
        });

        test("concurrent updates to same item are serialized", async () => {
            const names = makeNames("lockupdate");

            const created = await createItem<SimpleRecord>("sqlite", names.namespace, names.kind, {
                label: "concurrent",
                score: 0
            });

            const operations: string[] = [];

            // Both updates target the same item, so they must be serialized
            const [result1, result2] = await Promise.all([
                (async () => {
                    operations.push("update1-start");
                    // Fetch fresh copy
                    const items = await queryItems<SimpleRecord>("sqlite", names.namespace, names.kind, eq("label", "concurrent"));
                    const item = items[0]!;
                    const updated = item.update({ score: 1 }).unwrap();
                    const result = await updated.commit();
                    operations.push("update1-done");
                    return result;
                })(),
                (async () => {
                    // Small delay to ensure update1 likely acquires lock first
                    await new Promise((resolve) => setTimeout(resolve, 5));
                    operations.push("update2-start");
                    const items = await queryItems<SimpleRecord>("sqlite", names.namespace, names.kind, eq("label", "concurrent"));
                    const item = items[0]!;
                    const updated = item.update({ score: 2 }).unwrap();
                    const result = await updated.commit();
                    operations.push("update2-done");
                    return result;
                })(),
            ]);

            // Both should succeed (one waits for the other)
            expect(result1.isOk()).toBe(true);
            expect(result2.isOk()).toBe(true);

            // Final value should be from the last update
            const finalItems = await queryItems<SimpleRecord>("sqlite", names.namespace, names.kind, eq("label", "concurrent"));
            expect(finalItems.length).toBe(1);
            // One of the scores should win
            expect([1, 2]).toContain(finalItems[0]!.data!.score);
        });

        test("tx.persist acquires locks for all items", async () => {
            const names1 = makeNames("locktx1");
            const names2 = makeNames("locktx2");

            const item1 = korm.item<SimpleRecord>(layerPool).from.data({
                namespace: names1.namespace,
                kind: names1.kind,
                mods: fromMod("sqlite"),
                data: { label: "tx-item1", score: 10 }
            }).unwrap();

            const item2 = korm.item<SimpleRecord>(layerPool).from.data({
                namespace: names2.namespace,
                kind: names2.kind,
                mods: fromMod("pg"),
                data: { label: "tx-item2", score: 20 }
            }).unwrap();

            // Acquire lock on item1's RN before tx
            const release = await layerPool.locker.acquire(item1.rn!);

            let txStarted = false;
            let txCompleted = false;

            const txPromise = (async () => {
                txStarted = true;
                const result = await korm.tx(item1, item2).persist();
                txCompleted = true;
                return result;
            })();

            // Give tx time to start and hit the lock
            await new Promise((resolve) => setTimeout(resolve, 20));

            // tx should have started but not completed (blocked on lock)
            expect(txStarted).toBe(true);
            expect(txCompleted).toBe(false);

            // Release the lock
            release();

            // Now tx should complete
            const txResult = await txPromise;
            expect(txResult.isOk()).toBe(true);
            expect(txCompleted).toBe(true);

            // Verify items were persisted
            const items1 = await queryItems<SimpleRecord>("sqlite", names1.namespace, names1.kind);
            const items2 = await queryItems<SimpleRecord>("pg", names2.namespace, names2.kind);
            expect(items1.length).toBe(1);
            expect(items2.length).toBe(1);
        });

        test("resolvePaths commit locks all affected RNs", async () => {
            const userNames = makeNames("lockuser");
            const carNames = makeNames("lockcar");

            const user = await createItem<User>("pg", userNames.namespace, userNames.kind, {
                firstName: "Lock",
                lastName: "Test"
            });

            const car = await createItem<Car>("mysql", carNames.namespace, carNames.kind, {
                make: "LockMobile",
                model: "X1",
                year: 2024,
                owner: user.rn!.value(),
                meta: { inspected: true }
            });

            // Fetch car with resolved owner
            const carsResult = await korm.item<Car>(layerPool).from.query(
                korm.rn(`[rn][from::mysql]:${carNames.namespace}:${carNames.kind}:*`)
            ).get({ resolvePaths: ["owner"] as const });
            const cars = carsResult.unwrap();
            expect(cars.length).toBe(1);

            const resolvedCar = cars[0]!;

            // Lock the user RN
            const userRelease = await layerPool.locker.acquire(user.rn!);

            let commitStarted = false;
            let commitCompleted = false;

            // Try to commit an update that affects the resolved user
            const commitPromise = (async () => {
                commitStarted = true;
                const updated = resolvedCar.update({
                    owner: { firstName: "Updated" } as any
                }).unwrap();
                const result = await updated.commit();
                commitCompleted = true;
                return result;
            })();

            // Give commit time to start and hit the lock
            await new Promise((resolve) => setTimeout(resolve, 20));

            expect(commitStarted).toBe(true);
            expect(commitCompleted).toBe(false);

            // Release the lock
            userRelease();

            // Commit should now complete
            const commitResult = await commitPromise;
            expect(commitResult.isOk()).toBe(true);
            expect(commitCompleted).toBe(true);

            // Verify user was updated
            const updatedUsers = await queryItems<User>("pg", userNames.namespace, userNames.kind);
            expect(updatedUsers.length).toBe(1);
            expect(updatedUsers[0]!.data!.firstName).toBe("Updated");
        });
    });
});

afterAll(async () => {
    await clearAllDatabases();
    await clearAllDepots();
    await pg._db.end({ timeout: 1 });
    await mysql._pool.end();
    sqll._db.close();
});
