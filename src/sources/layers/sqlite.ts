import { type Change, type DbChangeResult, type DbDeleteResult, type PersistOptions, type RevertFunction, type SourceLayer } from "../sourceLayer";
import { korm } from "../../korm";
import type { JSONable } from "../../korm";
import { Result } from "@fkws/klonk-result";
import { QueryBuilder, type _QueryComponent } from "../../core/query";
import { FloatingItem, UncommittedItem, Item } from "../../core/item";
import { Database as SqliteDatabase } from "bun:sqlite";
import { FloatingDepotFile } from "../../depot/depotFile";
import { decrypt, Encrypt } from "../../security/encryption";
import { cloneJson, type PathKey } from "../../core/resolveMeta";
import { setEncryptionMeta, type EncryptionMeta, type EncryptedPayload } from "../../core/encryptionMeta";
import type { ColumnKind } from "../../core/columnKind";
import type { RN } from "../../core/rn";
import { safeAssign } from "../../core/safeObject";
import { buildBackupPayload, buildBackupRn, serializeBackupPayload, type BackupContext, type BackupPayload, type BackupRestoreOptions } from "../backups";

const SQLITE_JSON_TYPE = "JSON_TEXT";
const SQLITE_RN_TYPE = "RN_REF_TEXT";
const SQLITE_ENCRYPTED_TYPE = "ENCRYPTED_JSON";

/**
 * SQLite-backed source layer.
 * Create via `korm.layers.sqlite(path)` and add to a pool.
 */
export class SqliteLayer implements SourceLayer {
    public _db: SqliteDatabase;
    public readonly type: "sqlite" = "sqlite"
    public readonly identifier: string;
    private _path: string;
    private _columnKindsCache: Map<string, Map<string, ColumnKind>> = new Map();
    private _tableInfoCache: Map<string, { name: string; type: string }[]> = new Map();

    /** Create a SQLite layer backed by the given file path. */
    constructor(path: string) {
        this._db = new SqliteDatabase(path);
        this._db.run(`PRAGMA journal_mode=DELETE;`);
        this.identifier = path;
        this._path = path;
    }

    /** Close the underlying SQLite connection. */
    close(): void {
        this._db.close();
    }

    /** @internal */
    async backup(context: BackupContext): Promise<void> {
        const now = context.now ?? new Date();
        const tables = this._db.prepare(
            `SELECT name FROM sqlite_master
             WHERE type = 'table'
             AND (name LIKE '__items__%' OR name = '__korm_meta__' OR name = '__korm_pool__')`
        ).all() as Array<{ name: string }>;
        const dumps = tables.map((row) => {
            const safeName = this._quoteIdent(row.name);
            const rows = this._db.prepare(`SELECT * FROM ${safeName}`).all() as Record<string, unknown>[];
            return { name: row.name, rows };
        });
        const payload = buildBackupPayload("sqlite", context.layerIdent, dumps, now);
        const rn = buildBackupRn(context.depotIdent, context.layerIdent, now, "json");
        const file = new FloatingDepotFile(rn, serializeBackupPayload(payload));
        await context.depot.createFile(file);
    }

    /** @internal */
    async restore(payload: BackupPayload, options?: BackupRestoreOptions): Promise<void> {
        if (payload.layerType !== "sqlite") {
            throw new Error(`Backup payload layer type "${payload.layerType}" does not match sqlite.`);
        }
        const mode = options?.mode ?? "replace";
        const poolTable = "__korm_pool__";
        const metaTable = "__korm_meta__";

        for (const table of payload.tables) {
            const rawName = table.name;
            const safeName = this._quoteIdent(rawName);
            const rows = table.rows ?? [];

            if (rawName === poolTable) {
                this._db.run(
                    `CREATE TABLE IF NOT EXISTS "${poolTable}" (
                        "id" TEXT PRIMARY KEY,
                        "config" TEXT NOT NULL,
                        "created_at" INTEGER NOT NULL,
                        "updated_at" INTEGER NOT NULL
                    )`
                );
            } else if (rawName === metaTable) {
                this._db.run(
                    `CREATE TABLE IF NOT EXISTS "${metaTable}" (
                        "table_name" TEXT NOT NULL,
                        "column_name" TEXT NOT NULL,
                        "kind" TEXT NOT NULL,
                        PRIMARY KEY ("table_name", "column_name")
                    )`
                );
            } else if (this._getTableInfo(rawName).length === 0) {
                const columns = new Set<string>();
                for (const row of rows) {
                    for (const key of Object.keys(row)) {
                        columns.add(key);
                    }
                }
                if (rawName.startsWith("__items__") && !columns.has("rnId")) {
                    columns.add("rnId");
                }
                const columnDefs: string[] = [];
                for (const key of columns) {
                    const type = this._inferBackupSqliteType(rows, key);
                    const columnName = this._quoteIdent(key);
                    if (rawName.startsWith("__items__") && key === "rnId") {
                        columnDefs.push(`${columnName} TEXT PRIMARY KEY`);
                    } else {
                        columnDefs.push(`${columnName} ${type}`);
                    }
                }
                if (columnDefs.length > 0) {
                    this._db.run(`CREATE TABLE IF NOT EXISTS ${safeName} (${columnDefs.join(", ")})`);
                }
            }

            const tableInfo = this._getTableInfo(rawName, { force: true });
            if (tableInfo.length === 0) {
                continue;
            }
            const existing = new Set(tableInfo.map((col) => col.name));
            const missing = new Set<string>();
            for (const row of rows) {
                for (const key of Object.keys(row)) {
                    if (!existing.has(key)) {
                        missing.add(key);
                    }
                }
            }
            if (rawName.startsWith("__items__") && !existing.has("rnId")) {
                missing.add("rnId");
            }
            for (const key of missing) {
                const type = this._inferBackupSqliteType(rows, key);
                const columnName = this._quoteIdent(key);
                this._db.run(`ALTER TABLE ${safeName} ADD COLUMN ${columnName} ${type}`);
            }

            if (mode === "replace") {
                this._db.run(`DELETE FROM ${safeName}`);
            }
            if (rows.length === 0) {
                continue;
            }
            const columnSet = new Set<string>();
            for (const row of rows) {
                for (const key of Object.keys(row)) {
                    columnSet.add(key);
                }
            }
            if (columnSet.size === 0) {
                continue;
            }
            const columns = Array.from(columnSet);
            const columnList = columns.map((key) => this._quoteIdent(key)).join(", ");
            const placeholders = columns.map(() => "?").join(", ");
            const insertVerb = mode === "merge" ? "INSERT OR IGNORE" : "INSERT";
            const stmt = this._db.prepare(
                `${insertVerb} INTO ${safeName} (${columnList}) VALUES (${placeholders})`
            );
            for (const row of rows) {
                const values = columns.map((key) => this._encodeSqlValue((row as any)[key]));
                stmt.run(...values);
            }
        }

        this._tableInfoCache.clear();
        this._columnKindsCache.clear();
    }

    /** @internal */
    getPoolConfig(): { type: "sqlite"; path: string } {
        return { type: "sqlite", path: this._path };
    }

    private _quoteIdent(name: string): string {
        if (!name || name.includes("\u0000")) throw new Error("Invalid identifier: " + name);
        if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(name)) {
            throw new Error(`Unsafe identifier: ${name}`);
        }
        return `"${name.replace(/"/g, '""')}"`;
    }

    private _isEncryptValue(value: any): value is { __ENCRYPT__: true; safeValue: () => any } {
        return Boolean(value && typeof value === "object" && value.__ENCRYPT__ === true && typeof value.safeValue === "function");
    }

    private _isEncryptedPayload(value: any): value is EncryptedPayload {
        return Boolean(
            value &&
            typeof value === "object" &&
            value.__ENCRYPTED__ === true &&
            typeof value.type === "string" &&
            typeof value.value === "string"
        );
    }

    private _isRnValue(value: any): value is { __RN__: true; value: () => string } {
        return Boolean(
            value &&
            typeof value === "object" &&
            value.__RN__ === true &&
            typeof value.value === "function"
        );
    }

    private _looksLikeRnString(value: any): value is string {
        return typeof value === "string" && value.startsWith("[rn]");
    }

    private _toEncryptedPayload(encrypted: {
        value: string;
        type: "password" | "symmetric";
        dataType?: string;
        iv?: string;
        authTag?: string;
    }): EncryptedPayload {
        const payload: EncryptedPayload = {
            __ENCRYPTED__: true,
            type: encrypted.type,
            value: encrypted.value,
        };
        if (encrypted.dataType) payload.dataType = encrypted.dataType;
        if (encrypted.type === "symmetric") {
            payload.iv = encrypted.iv;
            payload.authTag = encrypted.authTag;
        }
        return payload;
    }

    private _normalizeEncryptedValue(value: any): any {
        if (!value || typeof value !== "object") return value;
        if (this._isEncryptValue(value)) {
            const encrypted = value.safeValue();
            return this._toEncryptedPayload(encrypted);
        }
        if (this._isEncryptedPayload(value)) return value;
        return value;
    }

    private async _decodeEncryptedValue(value: any): Promise<any> {
        if (!this._isEncryptedPayload(value)) return value;
        if (value.type === "password") return Encrypt.fromEncryptedPayload(value);
        if (value.type === "symmetric") {
            const decoded = await decrypt(value as any);
            return Encrypt.fromEncryptedPayload(value, decoded as JSONable);
        }
        return value;
    }

    private async _decodeEncryptedTree(
        value: any,
        meta: EncryptionMeta,
        path: PathKey[]
    ): Promise<any> {
        if (this._isEncryptedPayload(value)) {
            if (value.type === "password") {
                const decoded = await this._decodeEncryptedValue(value);
                meta.entries.push({ path: [...path], payload: value, plainValue: value.value });
                return decoded;
            }
            const decodedPlain = await decrypt(value as any);
            meta.entries.push({ path: [...path], payload: value, plainValue: cloneJson(decodedPlain as JSONable) });
            return Encrypt.fromEncryptedPayload(value, decodedPlain as JSONable);
        }
        if (Array.isArray(value)) {
            const next = [];
            for (let i = 0; i < value.length; i++) {
                next.push(await this._decodeEncryptedTree(value[i], meta, [...path, i]));
            }
            return next;
        }
        if (value && typeof value === "object") {
            const out: Record<string, any> = {};
            for (const [key, child] of Object.entries(value)) {
                safeAssign(out, key, await this._decodeEncryptedTree(child, meta, [...path, key]));
            }
            return out;
        }
        return value;
    }

    private _inferSqliteTypeFromValue(v: any): string {
        if (v === null) return "TEXT"; // policy: allow null, default to TEXT
        if (v === undefined) return "TEXT"; // policy: undefined isn't stored; TEXT fallback
        if (this._isRnValue(v) || this._looksLikeRnString(v)) return SQLITE_RN_TYPE;
        if (this._isEncryptValue(v) || this._isEncryptedPayload(v)) return SQLITE_ENCRYPTED_TYPE;

        switch (typeof v) {
            case "string":
                return "TEXT";
            case "boolean":
                return "BOOLEAN";
            case "number":
                return Number.isInteger(v) ? "INTEGER" : "REAL";
            case "object":
                return SQLITE_JSON_TYPE;
            default:
                throw new Error("Unsupported type: " + typeof v);
        }
    }

    private _inferBackupSqliteType(rows: Record<string, unknown>[], key: string): string {
        for (const row of rows) {
            const value = (row as any)[key];
            if (value === null || value === undefined) continue;
            if (typeof value === "string") {
                try {
                    const parsed = JSON.parse(value);
                    if (parsed && typeof parsed === "object") {
                        if (this._isEncryptedPayload(parsed)) return SQLITE_ENCRYPTED_TYPE;
                        return SQLITE_JSON_TYPE;
                    }
                } catch {
                    // fall through
                }
            }
            return this._inferSqliteTypeFromValue(value);
        }
        return "TEXT";
    }

    private _encodeSqlValue(v: any): any {
        if (this._isRnValue(v)) return v.value();
        const normalized = this._normalizeEncryptedValue(v);
        if (normalized === undefined) return null; // policy: store undefined as NULL
        if (normalized === null) return null;
        if (typeof normalized === "boolean") return normalized ? 1 : 0;
        if (typeof normalized === "object") return JSON.stringify(normalized);
        return normalized;
    }

    private _getTableInfo(
        rawTableName: string,
        opts: { force?: boolean } = {}
    ): { name: string; type: string }[] {
        const cached = this._tableInfoCache.get(rawTableName);
        if (cached && !opts.force) return cached;
        const tableName = this._quoteIdent(rawTableName);
        const info = this._db.prepare<{ name: string; type: string }, any>(`PRAGMA table_info(${tableName})`).all();
        this._tableInfoCache.set(rawTableName, info);
        return info;
    }

    private _needsTableInfoRefresh(row: Record<string, any>, tableInfo: { name: string; type: string }[]): boolean {
        if (tableInfo.length === 0) return true;
        const columns = new Set(tableInfo.map((c) => c.name));
        for (const key of Object.keys(row)) {
            if (!columns.has(key)) return true;
        }
        return false;
    }

    private async _decodeRowUsingTableInfo<T extends Record<string, any>>(
        row: T,
        tableInfo: { name: string; type: string }[],
        opts: { decryptEncrypted?: boolean; encryptionMeta?: EncryptionMeta } = {}
    ): Promise<T> {
        const typeByName = new Map(tableInfo.map((c) => [c.name, (c.type || "").toUpperCase()]));
        const out: any = {};
        for (const [key, value] of Object.entries(row)) {
            safeAssign(out, key, value);
        }
        const decryptEncrypted = opts.decryptEncrypted ?? true;

        for (const [k, v] of Object.entries(out)) {
            const t = typeByName.get(k);
            if (!t) continue;

            if (t === "BOOLEAN") {
                // SQLite commonly returns 0/1 for BOOLEAN-ish columns
                if (v === null || v === undefined) continue;
                safeAssign(out, k, Boolean(v));
            } else if (t === SQLITE_JSON_TYPE || t === SQLITE_ENCRYPTED_TYPE || t === "JSON") {
                let parsed: any = v;
                if (typeof v === "string") {
                    try {
                        parsed = JSON.parse(v);
                    } catch {
                        parsed = v;
                    }
                }
                if (decryptEncrypted) {
                    if (opts.encryptionMeta) {
                        parsed = await this._decodeEncryptedTree(parsed, opts.encryptionMeta, [k]);
                    } else {
                        parsed = await this._decodeEncryptedValue(parsed);
                    }
                }
                safeAssign(out, k, parsed);
            } else if (t === "ID_TEXT") {
                // leave as-is (string)
            }
        }

        return out as T;
    }

    /** @inheritdoc */
    async insertItem<T extends JSONable>(item: FloatingItem<T>, options?: PersistOptions): Promise<DbChangeResult<T>> {
        const ensured = await this._safeEnsureTables("insert", item, options?.destructive ?? false);
        if (!ensured.success) {
            return ensured.result as DbChangeResult<T>;
        }
        const safeTableName = ensured.value;

        const existing = await this.readItemRaw(item.rn!);
        if (existing !== undefined) {
            const rnValue = item.rn?.value() ?? "(unknown rn)";
            const baseLayer = `${this.type} source layer '${this.identifier}'`;
            return {
                revert: () => {},
                success: false,
                error: new Error(`Tried to create item '${rnValue}' which already exists in ${baseLayer}.`),
                type: "insert",
                item: item
            };
        }

        const data = (item.data ?? {}) as Record<string, any>;
        const keys = Object.keys(data);

        let insertString = `INSERT INTO ${safeTableName} ( "rnId"`;
        for (const key of keys) {
            insertString += `, ${this._quoteIdent(key)}`;
        }
        insertString += `) VALUES ( ?`;
        for (let i = 0; i < keys.length; i++) {
            insertString += `, ?`;
        }
        insertString += `)`;

        const params = [
            item.rn!.id!,
            ...keys.map((k) => this._encodeSqlValue(data[k])),
        ];
        try {
            this._db.run(insertString, params);
            return {
                revert: this._revertFactory({ type: "insert", rn: item.rn!, pool: item.pool }, options?.destructive ?? false),
                success: true,
                item: new Item<T>(item.pool, item.data, item.rn),
                type: "insert"
            }
        } catch (error) {
            const message = this._friendlyMessage("create", item, error);
            return {
                revert: this._revertFactory({ type: "insert", rn: item.rn!, pool: item.pool }, options?.destructive ?? false),
                success: false,
                error: new Error(message),
                type: "insert",
                item: item
            }
        }
    }

    private _revertFactory(change: Change, destructive: boolean): RevertFunction {
        switch (change.type) {
            case "insert":
                return () => {
                    const rawTableName = `__items__${change.rn.namespace!}__${change.rn.kind!}`;
                    const safeTableName = this._quoteIdent(rawTableName);
                    const deleteString = `DELETE FROM ${safeTableName} WHERE "rnId" = ?`;
                    this._db.run(deleteString, [change.rn.id!]);
                };
            case "update":
                return () => {
                    void this.updateItem(new UncommittedItem(change.pool, change.rn, change.oldData), { destructive });
                };
        }
    }

    /** @inheritdoc */
    async updateItem<T extends JSONable>(item: UncommittedItem<T>, options?: PersistOptions): Promise<DbChangeResult<T>> {
        const ensured = await this._safeEnsureTables("update", item, options?.destructive ?? false);
        if (!ensured.success) {
            return ensured.result as DbChangeResult<T>;
        }
        const safeTableName = ensured.value;

        const rawTableName = `__items__${item.rn!.namespace!}__${item.rn!.kind!}`;
        let tableInfo = this._getTableInfo(rawTableName);
        // Get current data as oldData from db. Drop rnId so ensureTables doesn't try to alter the PK column during revert.
        const currentRow = this._db.prepare<any, any>(`SELECT * FROM ${safeTableName} WHERE "rnId" = ?`).get(item.rn!.id!);
        if (!currentRow) {
            const rnValue = item.rn?.value() ?? "(unknown rn)";
            const baseLayer = `${this.type} source layer '${this.identifier}'`;
            return {
                revert: () => {},
                success: false,
                error: new Error(`Tried to update item '${rnValue}' which does not exist in ${baseLayer}.`),
                type: "update",
                oldData: (item.data ?? {}) as T,
                item: item
            };
        }
        if (this._needsTableInfoRefresh(currentRow as Record<string, any>, tableInfo)) {
            tableInfo = this._getTableInfo(rawTableName, { force: true });
        }
        const { rnId: _rnId, ...currentDataRaw } = currentRow;
        const currentData = await this._decodeRowUsingTableInfo(currentDataRaw as any, tableInfo, { decryptEncrypted: false });

        const data = (item.data ?? {}) as Record<string, any>;
        const keys = Object.keys(data);
        if (keys.length === 0) {
            return {
                revert: () => {},
                success: true,
                item: new Item<T>(item.pool, currentData as T, item.rn),
                type: "update",
                oldData: currentData as T
            };
        }

        let updateString = `UPDATE ${safeTableName} SET `;
        for (const key of keys) {
            const columnName = this._quoteIdent(key);
            updateString += `${columnName} = ?, `;
        }
        updateString = updateString.slice(0, -2);
        updateString += ` WHERE "rnId" = ?`;

        const params = [
            ...keys.map((k) => this._encodeSqlValue(data[k])),
            item.rn!.id!,
        ];
        try {
            this._db.run(updateString, params);
            return {
                revert: this._revertFactory({ type: "update", oldData: currentData, rn: item.rn!, pool: item.pool }, options?.destructive ?? false),
                success: true,
                item: new Item<T>(item.pool, item.data, item.rn),
                type: "update",
                oldData: currentData
            }
        } catch (error) {
            const message = this._friendlyMessage("update", item, error);
            return {
                revert: this._revertFactory({ type: "update", oldData: currentData, rn: item.rn!, pool: item.pool }, options?.destructive ?? false),
                success: false,
                error: new Error(message),
                type: "update",
                oldData: currentData,
                item: item
            }
        }
    }

    /** @inheritdoc */
    async readItemRaw<T extends JSONable>(rn: RN): Promise<T | undefined> {
        const rawTableName = `__items__${rn.namespace!}__${rn.kind!}`;
        let tableInfo = this._getTableInfo(rawTableName);
        if (tableInfo.length === 0) return undefined;
        const safeTableName = this._quoteIdent(rawTableName);
        const currentRow = this._db.prepare<any, any>(`SELECT * FROM ${safeTableName} WHERE "rnId" = ?`).get(rn.id!);
        if (!currentRow) return undefined;
        if (this._needsTableInfoRefresh(currentRow as Record<string, any>, tableInfo)) {
            tableInfo = this._getTableInfo(rawTableName, { force: true });
        }
        const { rnId: _rnId, ...currentDataRaw } = currentRow;
        return await this._decodeRowUsingTableInfo(currentDataRaw as any, tableInfo, { decryptEncrypted: false });
    }

    /** @inheritdoc */
    async deleteItem(rn: RN): Promise<DbDeleteResult> {
        const rawTableName = `__items__${rn.namespace!}__${rn.kind!}`;
        const tableInfo = this._getTableInfo(rawTableName);
        if (tableInfo.length === 0) return { success: true };
        const safeTableName = this._quoteIdent(rawTableName);
        try {
            this._db.run(`DELETE FROM ${safeTableName} WHERE "rnId" = ?`, [rn.id!]);
            return { success: true };
        } catch (error) {
            const rnValue = rn.value() ?? "(unknown rn)";
            const baseLayer = `${this.type} source layer '${this.identifier}'`;
            return { success: false, error: new Error(`Failed to delete item '${rnValue}' in ${baseLayer}: ${error}`) };
        }
    }

    private _friendlyMessage<T extends JSONable>(op: "create" | "update", item: FloatingItem<T> | UncommittedItem<T>, error: unknown): string {
        const rnValue = item.rn?.value() ?? "(unknown rn)";
        const baseLayer = `${this.type} source layer '${this.identifier}'`;
        const errorText = String(error);
        if (op === "create" && errorText.includes("UNIQUE constraint")) {
            return `Tried to create item '${rnValue}' which already exists in ${baseLayer}.`;
        }
        return `Failed to ${op} item '${rnValue}' in ${baseLayer}: ${errorText}`;
    }

    private async _safeEnsureTables<T extends JSONable>(
        op: "insert" | "update",
        item: FloatingItem<T> | UncommittedItem<T>,
        destructive: boolean
    ): Promise<
        | { success: true; value: string }
        | { success: false; result: DbChangeResult<T> }
    > {
        try {
            const rawTableName = await this.ensureTables(item, destructive);
            return { success: true, value: this._quoteIdent(rawTableName) };
        } catch (error) {
            const message = this._friendlyMessage(op === "insert" ? "create" : "update", item, error);
            const isInsert = op === "insert";
            if (isInsert) {
                const result: DbChangeResult<T> = {
                    revert: () => {},
                    success: false,
                    error: new Error(message),
                    type: "insert",
                    item: item as FloatingItem<T>
                };
                return { success: false, result };
            } else {
                const result: DbChangeResult<T> = {
                    revert: () => {},
                    success: false,
                    error: new Error(message),
                    type: "update",
                    oldData: (item as UncommittedItem<T>).data as T,
                    item: item as UncommittedItem<T>
                };
                return { success: false, result };
            }
        }
    }

    private _buildQueryString(
        root: _QueryComponent,
        tableName: string,
        projection?: string[],
        currentString: string = ""
    ): { sql: string; params: any[] } {
        const params: any[] = [];

        const normalizeParam = (v: any): any => {
            if (v === null || v === undefined) return null;
            if (this._isRnValue(v)) return v.value();
            if (typeof v === "boolean") return v ? 1 : 0;
            if (typeof v === "object") return JSON.stringify(v);
            return v;
        };

        const pushParam = (v: any): string => {
            params.push(normalizeParam(v));
            return "?";
        };

        const normalizeInValues = (value: any): { values: any[]; hasNull: boolean } => {
            const list = Array.isArray(value) ? value : [value];
            const values: any[] = [];
            let hasNull = false;
            for (const entry of list) {
                if (entry === null || entry === undefined) {
                    hasNull = true;
                } else {
                    values.push(entry);
                }
            }
            return { values, hasNull };
        };

        const buildInClause = (lhs: string, value: any): string => {
            const { values, hasNull } = normalizeInValues(value);
            if (values.length === 0) {
                return hasNull ? `${lhs} IS NULL` : "0=1";
            }
            const inExpr = `${lhs} IN (${values.map(pushParam).join(", ")})`;
            if (!hasNull) return inExpr;
            return `(${inExpr} OR ${lhs} IS NULL)`;
        };

        const safeJsonPathFromDotted = (dotted: string): { base: string; path: string } => {
            const [base, ...rest] = dotted.split(".");
            if (!base || rest.length === 0) throw new Error(`Invalid dotted property: ${dotted}`);

            const isSafeSegment = (s: string) => /^[A-Za-z_][A-Za-z0-9_]*$/.test(s);
            for (const seg of rest) {
                if (!isSafeSegment(seg)) throw new Error(`Unsafe JSON path segment "${seg}" in "${dotted}"`);
            }
            return { base, path: `$.${rest.join(".")}` };
        };

        const buildNullComparison = (lhs: string, operator: string, value: any): string | null => {
            if (value === null || value === undefined) {
                if (operator === "=") return `${lhs} IS NULL`;
                if (operator === "!=") return `${lhs} IS NOT NULL`;
                return `${lhs} ${operator} NULL`;
            }
            return null;
        };

        const buildExpr = (node: _QueryComponent): string => {
            if (node.type === "comparison") {
                let lhs: string;

                if (node.property.includes(".")) {
                    const { base, path } = safeJsonPathFromDotted(node.property);
                    const baseIdent = this._quoteIdent(base);
                    lhs = `CASE WHEN json_valid(${baseIdent}) THEN json_extract(${baseIdent}, '${path}') ELSE NULL END`;
                    if (node.operator === "IN") {
                        return buildInClause(lhs, node.value);
                    }
                } else {
                    lhs = this._quoteIdent(node.property);
                }

                if (node.operator === "IN") {
                    return buildInClause(lhs, node.value);
                }

                const nullComparison = buildNullComparison(lhs, node.operator, node.value);
                if (nullComparison) return nullComparison;

                // No more contains/notContains. LIKE is handled here as a normal operator.
                return `${lhs} ${node.operator} ${pushParam(node.value)}`;
            }

            // group
            const parts = node.components
                .map(buildExpr)
                .map((s) => s.trim())
                .filter(Boolean);

            if (parts.length === 0) return "";

            if (node.method === "NOT") {
                if (parts.length !== 1) {
                    throw new Error(`NOT groups must have exactly 1 component, got ${parts.length}`);
                }
                return `(NOT (${parts[0]}))`;
            }

            if (parts.length === 1) return `(${parts[0]})`;
            return `(${parts.join(` ${node.method} `)})`;
        };

        if (currentString === "") {
            currentString = `${this._selectClause(tableName, projection)} WHERE `;
        }

        const expr = buildExpr(root);
        return { sql: currentString + (expr || "1=1"), params };
    }

    private _selectClause(tableName: string, projection?: string[]): string {
        if (!projection || projection.length === 0) {
            return `SELECT * FROM ${this._quoteIdent(tableName)}`;
        }
        const columns = ["rnId", ...projection];
        const seen = new Set<string>();
        const safeColumns: string[] = [];
        for (const col of columns) {
            if (seen.has(col)) continue;
            seen.add(col);
            safeColumns.push(this._quoteIdent(col));
        }
        return `SELECT ${safeColumns.join(", ")} FROM ${this._quoteIdent(tableName)}`;
    }

    /** @inheritdoc */
    async executeQuery<T extends JSONable>(query: QueryBuilder<any>): Promise<Result<Item<T>[]>> {
        const tableName = `__items__${query.rn!.namespace!}__${query.rn!.kind!}`;

        let queryString: string;
        let params: any[] = [];
        const projection = query.projection;
        if (!query.root) {
            queryString = this._selectClause(tableName, projection);
        } else {
            const built = this._buildQueryString(query.root, tableName, projection);
            queryString = built.sql;
            params = built.params;
        }

        let rawItems: (T & { rnId: string })[] = [];
        let tableInfo: { name: string; type: string }[] = [];

        try {
            tableInfo = this._getTableInfo(tableName);
            if (tableInfo.length === 0) {
                return new Result({ success: true, data: [] });
            }
            rawItems = this._db.prepare<T & { rnId: string }, any>(queryString).all(params);
            if (rawItems.length > 0 && this._needsTableInfoRefresh(rawItems[0] as Record<string, any>, tableInfo)) {
                tableInfo = this._getTableInfo(tableName, { force: true });
            }
        } catch (error) {
            return new Result({ success: false, error: new Error("Failed to execute query: " + error) });
        }

        const mods = Array.from(query.rn!.mods.entries());
        const items = await Promise.all(rawItems.map(async (row) => {
            const rowObj = row as Record<string, any>;
            const { rnId, ...withoutRnId } = rowObj;
            const encryptionMeta: EncryptionMeta = { entries: [] };
            const decoded = await this._decodeRowUsingTableInfo(withoutRnId as any, tableInfo, {
                encryptionMeta,
            });
            const rn = korm.rn(`[rn]:${query.rn!.namespace!}:${query.rn!.kind!}:${rnId}`);
            for (const [key, value] of mods) {
                rn.mod(key, value);
            }
            const item = new Item<T>(query.item.pool, decoded as T, rn);
            if (encryptionMeta.entries.length > 0) {
                setEncryptionMeta(item, encryptionMeta);
            }
            return item;
        }));

        return new Result({ success: true, data: items });
    }

    /** @inheritdoc */
    async ensureTables(
        item: Item<any> | FloatingItem<any> | UncommittedItem<any>,
        destructive: boolean = false
    ): Promise<string> {
        const rawTableName = `__items__${item.rn!.namespace!}__${item.rn!.kind!}`;
        const tableName = this._quoteIdent(rawTableName);
        let schemaChanged = false;

        const exists = this._db
            .prepare<{ e: 0 | 1 }, [string]>(
                `
            SELECT EXISTS(
                SELECT 1
                FROM sqlite_master
                WHERE type='table' AND name=?
            ) AS e;
        `
            )
            .get(rawTableName);

        if (!exists || exists.e === 0) {
            let createString = `CREATE TABLE IF NOT EXISTS ${tableName} ( "rnId" ID_TEXT PRIMARY KEY, `;

            for (const key in item.data) {
                const columnName = this._quoteIdent(key);
                const v = (item.data as any)[key];

                const t = this._inferSqliteTypeFromValue(v);
                if (t === "TEXT") createString += `${columnName} TEXT, `;
                else if (t === "INTEGER") createString += `${columnName} INTEGER, `;
                else if (t === "REAL") createString += `${columnName} REAL, `;
                else if (t === "BOOLEAN") createString += `${columnName} BOOLEAN CHECK( ${columnName} IN (0, 1) ), `;
                else if (t === SQLITE_JSON_TYPE) createString += `${columnName} ${SQLITE_JSON_TYPE}, `;
                else if (t === SQLITE_ENCRYPTED_TYPE) createString += `${columnName} ${SQLITE_ENCRYPTED_TYPE}, `;
                else if (t === SQLITE_RN_TYPE) createString += `${columnName} ${SQLITE_RN_TYPE}, `;
                else throw new Error("Unsupported inferred type: " + t);
            }

            createString = createString.slice(0, -2);
            createString += ` )`;
            this._db.run(createString);
            this._columnKindsCache.delete(rawTableName);
            schemaChanged = true;
            this._tableInfoCache.delete(rawTableName);
            return rawTableName;
        }

        // Table exists: ensure columns
        const columns = this._db.prepare<{ name: string; type: string }, any>(`PRAGMA table_info(${tableName})`).all();

        for (const key in item.data) {
            const rawColumnName = key;
            const columnName = this._quoteIdent(rawColumnName);
            const v = (item.data as any)[key];

            // If you ever pass partial objects with undefined, don't attempt to infer/alter.
            if (v === undefined) continue;

            const existingColumnType = (columns.find((c) => c.name === rawColumnName)?.type || "").toUpperCase();

            const normalizeType = (type: string): string => {
                if (type === SQLITE_RN_TYPE) return "TEXT";
                if (type === SQLITE_ENCRYPTED_TYPE) return SQLITE_JSON_TYPE;
                return type;
            };

            let columnWantedType = this._inferSqliteTypeFromValue(v).toUpperCase();
            let normalizedExisting = normalizeType(existingColumnType);
            let normalizedWanted = normalizeType(columnWantedType);

            // Important: if SQLite returned BOOLEAN values as 0/1, typeof(v) becomes "number".
            // Accept this as BOOLEAN if the existing column is BOOLEAN and the value is 0 or 1.
            if (normalizedExisting === "BOOLEAN" && (normalizedWanted === "INTEGER" || normalizedWanted === "REAL")) {
                if (v === 0 || v === 1) normalizedWanted = "BOOLEAN";
            }
            // When a JSON_TEXT column comes back as a raw string (e.g., during revert), treat it as JSON_TEXT.
            if (normalizedExisting === SQLITE_JSON_TYPE && normalizedWanted === "TEXT" && typeof v === "string") {
                try {
                    JSON.parse(v);
                    normalizedWanted = SQLITE_JSON_TYPE;
                    columnWantedType = existingColumnType === SQLITE_ENCRYPTED_TYPE ? SQLITE_ENCRYPTED_TYPE : SQLITE_JSON_TYPE;
                } catch {
                    // leave as TEXT if it's not valid JSON; mismatch will be handled below
                }
            }

            if (!existingColumnType) {
                // Column doesn't exist, add it
                const alterString = `ALTER TABLE ${tableName} ADD COLUMN ${columnName} ${columnWantedType}`;
                this._db.run(alterString);
                schemaChanged = true;
            } else if (normalizedExisting !== normalizedWanted) {
                if (!destructive) {
                    throw new Error(
                        `Rejecting change: Column ${rawColumnName} already exists with type ${existingColumnType} but wanted type ${columnWantedType}. ` +
                        `\n\nHint:\nYou have likely changed the shape of one of your types or passed wrongly shaped data.\n` + 
                        `If you intend to change the shape of a namespace-kind path, run a Tx with .persist(\x1b[36m{... destructive: true }\x1b[0m). This will ` + 
                        `\x1b[31mDESTROY ALL DATA\x1b[0m in the affected column(s). To avoid this, use a new namespace-kind path.`
                    );
                }

                console.log(
                    `Changing column ${rawColumnName} from ${existingColumnType} to ${columnWantedType}. This will delete all data in this column.`
                );

                const dropString = `ALTER TABLE ${tableName} DROP COLUMN ${columnName}`;
                this._db.run(dropString);

                const addString = `ALTER TABLE ${tableName} ADD COLUMN ${columnName} ${columnWantedType}`;
                this._db.run(addString);

                console.log("Dropped and added column", rawColumnName, "with type", columnWantedType);
                schemaChanged = true;
            }
        }

        this._columnKindsCache.delete(rawTableName);
        if (schemaChanged) {
            this._tableInfoCache.delete(rawTableName);
        }
        return rawTableName;
    }

    /** @inheritdoc */
    async getColumnKinds(namespace: string, kind: string): Promise<Map<string, ColumnKind>> {
        const rawTableName = `__items__${namespace}__${kind}`;
        const cached = this._columnKindsCache.get(rawTableName);
        if (cached) return new Map(cached);
        const tableInfo = this._getTableInfo(rawTableName);
        const map = new Map<string, ColumnKind>();
        if (tableInfo.length === 0) return map;
        for (const col of tableInfo) {
            const type = (col.type || "").toUpperCase();
            if (type === SQLITE_RN_TYPE) {
                map.set(col.name, "rn");
            } else if (type === SQLITE_ENCRYPTED_TYPE) {
                map.set(col.name, "encrypted");
            } else if (type === SQLITE_JSON_TYPE || type === "JSON") {
                map.set(col.name, "json");
            } else {
                map.set(col.name, "scalar");
            }
        }
        this._columnKindsCache.set(rawTableName, map);
        return new Map(map);
    }
}
