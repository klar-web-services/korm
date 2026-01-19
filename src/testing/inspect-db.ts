import { existsSync, readFileSync } from "node:fs";
import { resolve } from "node:path";
import type { RowDataPacket } from "mysql2/promise";

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

const { sqll, pg, mysql } = await import("./layerDefs");

const args = new Map<string, string>();
for (const arg of Bun.argv.slice(2)) {
    if (!arg.startsWith("--")) continue;
    const [key, value = ""] = arg.slice(2).split("=", 2);
    if (!key) continue;
    args.set(key, value);
}

const limit = Number(args.get("limit") || "3");
const tableArg = args.get("table") || "";
const matchArg = args.get("match") || "";

function shouldInclude(name: string): boolean {
    if (tableArg) return name === tableArg;
    if (matchArg) return name.includes(matchArg);
    return name.startsWith("__items__");
}

function quoteIdent(name: string, quote: string): string | null {
    if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(name)) return null;
    if (quote === "`") return `\`${name.replace(/`/g, "``")}\``;
    return `"${name.replace(/"/g, '""')}"`;
}

async function inspectPg(): Promise<void> {
    const rows = await pg._db.unsafe<{ table_name: string }[]>(
        `
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
        ORDER BY table_name
        `
    );

    console.log(`[pg] tables: ${rows.length}`);
    for (const row of rows as any[]) {
        const name = (row as any).table_name ?? (row as any).TABLE_NAME ?? (row as any).name;
        if (!name) continue;
        if (!shouldInclude(name)) continue;
        const safe = quoteIdent(name, "\"");
        if (!safe) continue;

        const columns = await pg._db.unsafe<{ column_name: string; data_type: string }[]>(
            `SELECT column_name, data_type FROM information_schema.columns WHERE table_name = $1 ORDER BY ordinal_position`,
            [name]
        );
        const countRows = await pg._db.unsafe<{ cnt: number }[]>(`SELECT COUNT(*)::int as cnt FROM ${safe}`);
        const sampleRows = await pg._db.unsafe<any[]>(`SELECT * FROM ${safe} LIMIT ${limit}`);

        console.log(`[pg] ${name} columns:`, columns);
        console.log(`[pg] ${name} count:`, countRows[0]?.cnt ?? 0);
        console.log(`[pg] ${name} sample:`, sampleRows);
    }
}

async function inspectMysql(): Promise<void> {
    const [rows] = await mysql._pool.query<RowDataPacket[]>(
        `
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = DATABASE()
        ORDER BY table_name
        `
    );

    console.log(`[mysql] tables: ${(rows as any[]).length}`);
    for (const row of rows as any[]) {
        const name = (row as any).table_name ?? (row as any).TABLE_NAME ?? (row as any).name;
        if (!name) continue;
        if (!shouldInclude(name)) continue;
        const safe = quoteIdent(name, "`");
        if (!safe) continue;

        const [columns] = await mysql._pool.query(
            `SELECT column_name, data_type, column_type FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = ? ORDER BY ordinal_position`,
            [name]
        );
        const [countRows] = await mysql._pool.query<any[]>(`SELECT COUNT(*) as cnt FROM ${safe}`);
        const [sampleRows] = await mysql._pool.query<any[]>(`SELECT * FROM ${safe} LIMIT ${limit}`);

        console.log(`[mysql] ${name} columns:`, columns);
        console.log(`[mysql] ${name} count:`, (countRows as any[])[0]?.cnt ?? 0);
        console.log(`[mysql] ${name} sample:`, sampleRows);
    }
}

function inspectSqlite(): void {
    const rows = sqll._db
        .prepare<{ name: string }, any>(`SELECT name FROM sqlite_master WHERE type='table' ORDER BY name`)
        .all();

    console.log(`[sqlite] tables: ${rows.length}`);
    for (const row of rows) {
        const name = row.name;
        if (!shouldInclude(name)) continue;
        const safe = quoteIdent(name, "\"");
        if (!safe) continue;

        const columns = sqll._db.prepare(`PRAGMA table_info(${safe})`).all();
        const count = sqll._db.prepare<{ cnt: number }, any>(`SELECT COUNT(*) as cnt FROM ${safe}`).get();
        const sample = sqll._db.prepare<any, any>(`SELECT * FROM ${safe} LIMIT ${limit}`).all();

        console.log(`[sqlite] ${name} columns:`, columns);
        console.log(`[sqlite] ${name} count:`, count?.cnt ?? 0);
        console.log(`[sqlite] ${name} sample:`, sample);
    }
}

await inspectPg();
await inspectMysql();
inspectSqlite();

await pg._db.end({ timeout: 1 });
await mysql._pool.end();
sqll._db.close();
