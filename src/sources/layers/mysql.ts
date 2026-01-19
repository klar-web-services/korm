import { type Change, type DbChangeResult, type DbDeleteResult, type PersistOptions, type RevertFunction, type SourceLayer } from "../sourceLayer";
import { korm } from "../../korm";
import type { JSONable } from "../../korm";
import { Result } from "@fkws/klonk-result";
import { QueryBuilder, type _QueryComponent } from "../../core/query";
import { FloatingItem, UncommittedItem, Item } from "../../core/item";
import { FloatingDepotFile } from "../../depot/depotFile";
import { createPool, type Pool, type PoolOptions, type RowDataPacket } from "mysql2/promise";
import { decrypt, Encrypt } from "../../security/encryption";
import { cloneJson, type PathKey } from "../../core/resolveMeta";
import { setEncryptionMeta, type EncryptionMeta, type EncryptedPayload } from "../../core/encryptionMeta";
import type { ColumnKind } from "../../core/columnKind";
import crypto from "node:crypto";
import type { RN } from "../../core/rn";
import { safeAssign } from "../../core/safeObject";
import { buildBackupPayload, buildBackupRn, serializeBackupPayload, type BackupContext, type BackupPayload, type BackupRestoreOptions } from "../backups";

const MYSQL_META_TABLE = "__korm_meta__";
const MYSQL_MAX_IDENTIFIER_LENGTH = 64;

type ColumnInfo = { name: string; type: string; column_type?: string };

/**
 * MySQL-backed source layer.
 * Create via `korm.layers.mysql(...)` and add to a pool.
 */
export class MysqlLayer implements SourceLayer {
    public _pool: Pool;
    public readonly type: "mysql" = "mysql";
    public readonly identifier: string;
    private _connectionInput: string | PoolOptions;
    private _metaEnsured: boolean = false;
    private _columnKindsCache: Map<string, Map<string, ColumnKind>> = new Map();
    private _tableInfoCache: Map<string, ColumnInfo[]> = new Map();

    /** Create a MySQL layer from a connection string or mysql2 PoolOptions. */
    constructor(connectionStringOrOptions: string | PoolOptions) {
        this._pool = createPool(connectionStringOrOptions as any);
        this.identifier = this._deriveIdentifier(connectionStringOrOptions);
        this._connectionInput = connectionStringOrOptions;
    }

    /**
     * Resolve a raw table name to the physical table name.
     * MySQL may hash long names to fit identifier limits.
     */
    public resolveTableName(rawTableName: string): string {
        return this._resolveTableName(rawTableName);
    }

    /** Close the underlying MySQL connection pool. */
    async close(): Promise<void> {
        await this._pool.end();
    }

    /** @internal */
    async backup(context: BackupContext): Promise<void> {
        const now = context.now ?? new Date();
        const [tables] = await this._pool.query(
            `SELECT table_name
             FROM information_schema.tables
             WHERE table_schema = DATABASE()
             AND (table_name LIKE '__items__%' OR table_name = '__korm_meta__' OR table_name = '__korm_pool__')`
        ) as [Array<{ table_name: string }>, unknown];
        const dumps: Array<{ name: string; rows: Record<string, unknown>[] }> = [];
        for (const row of tables) {
            const tableName = (row as { table_name?: string; TABLE_NAME?: string; name?: string }).table_name
                ?? (row as { TABLE_NAME?: string }).TABLE_NAME
                ?? (row as { name?: string }).name;
            if (!tableName) continue;
            const safeName = this._quoteIdent(tableName);
            const [rows] = await this._pool.query(
                `SELECT * FROM ${safeName}`
            ) as [RowDataPacket[], unknown];
            dumps.push({ name: tableName, rows: rows as Record<string, unknown>[] });
        }
        const payload = buildBackupPayload("mysql", context.layerIdent, dumps, now);
        const rn = buildBackupRn(context.depotIdent, context.layerIdent, now, "json");
        const file = new FloatingDepotFile(rn, serializeBackupPayload(payload));
        await context.depot.createFile(file);
    }

    /** @internal */
    async restore(payload: BackupPayload, options?: BackupRestoreOptions): Promise<void> {
        if (payload.layerType !== "mysql") {
            throw new Error(`Backup payload layer type "${payload.layerType}" does not match mysql.`);
        }
        const mode = options?.mode ?? "replace";
        const poolTable = "__korm_pool__";
        const metaTable = MYSQL_META_TABLE;
        let schemaChanged = false;

        for (const table of payload.tables) {
            const rawName = table.name;
            const actualName = this._resolveTableName(rawName);
            const safeName = this._quoteIdent(actualName);
            const rows = table.rows ?? [];

            if (rawName === poolTable) {
                await this._pool.query(
                    `CREATE TABLE IF NOT EXISTS \`${poolTable}\` (
                        \`id\` VARCHAR(64) PRIMARY KEY,
                        \`config\` JSON NOT NULL,
                        \`created_at\` BIGINT NOT NULL,
                        \`updated_at\` BIGINT NOT NULL
                    )`
                );
                schemaChanged = true;
            } else if (rawName === metaTable) {
                await this._ensureMetaTable();
            } else if ((await this._getTableInfo(rawName)).length === 0) {
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
                    const columnName = this._quoteIdent(key);
                    if (rawName.startsWith("__items__") && key === "rnId") {
                        columnDefs.push(`${columnName} VARCHAR(255) PRIMARY KEY`);
                        continue;
                    }
                    const t = this._inferBackupMySqlType(rows, key);
                    if (t === "TEXT") columnDefs.push(`${columnName} TEXT`);
                    else if (t === "INTEGER") columnDefs.push(`${columnName} INT`);
                    else if (t === "DOUBLE") columnDefs.push(`${columnName} DOUBLE`);
                    else if (t === "BOOLEAN") columnDefs.push(`${columnName} TINYINT(1)`);
                    else if (t === "JSON") columnDefs.push(`${columnName} JSON`);
                }
                if (columnDefs.length > 0) {
                    await this._pool.query(`CREATE TABLE IF NOT EXISTS ${safeName} ( ${columnDefs.join(", ")} )`);
                    schemaChanged = true;
                }
            }

            const tableInfo = await this._getTableInfo(rawName, { force: true });
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
                const columnName = this._quoteIdent(key);
                if (rawName.startsWith("__items__") && key === "rnId") {
                    await this._pool.query(`ALTER TABLE ${safeName} ADD COLUMN ${columnName} VARCHAR(255)`);
                } else {
                    const t = this._inferBackupMySqlType(rows, key);
                    const mapped = t === "BOOLEAN" ? "TINYINT(1)" : t;
                    await this._pool.query(`ALTER TABLE ${safeName} ADD COLUMN ${columnName} ${mapped}`);
                }
                schemaChanged = true;
            }

            if (mode === "replace") {
                await this._pool.query(`DELETE FROM ${safeName}`);
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
            const insertVerb = mode === "merge" ? "INSERT IGNORE" : "INSERT";
            const insertString = `${insertVerb} INTO ${safeName} (${columnList}) VALUES (${placeholders})`;
            for (const row of rows) {
                const values = columns.map((key) => this._encodeMySqlValue((row as any)[key]));
                await this._pool.query(insertString, values);
            }
        }

        if (schemaChanged) {
            this._tableInfoCache.clear();
        }
        this._columnKindsCache.clear();
    }

    /** @internal */
    getPoolConfig(): { type: "mysql"; mode: "url" | "options"; value: unknown } {
        if (typeof this._connectionInput === "string") {
            return { type: "mysql", mode: "url", value: this._connectionInput };
        }
        return { type: "mysql", mode: "options", value: this._connectionInput };
    }

    private _deriveIdentifier(connection: string | PoolOptions): string {
        const fallback = "mysql@localhost";

        if (typeof connection === "string") {
            try {
                const url = new URL(connection);
                const db = (url.pathname || "").replace(/^\//, "") || "mysql";
                const host = url.hostname || "localhost";
                return `${db}@${host}`;
            } catch {
                return fallback;
            }
        }

        const opts = connection as PoolOptions;
        const db = (opts.database as string) || "mysql";
        const host = (opts.host as string) || "localhost";
        return `${db}@${host}`;
    }

    private _quoteIdent(name: string): string {
        if (!name || name.includes("\u0000")) throw new Error("Invalid identifier: " + name);
        if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(name)) {
            throw new Error(`Unsafe identifier: ${name}`);
        }
        return `\`${name.replace(/`/g, "``")}\``;
    }

    private _resolveTableName(rawTableName: string): string {
        if (rawTableName.length <= MYSQL_MAX_IDENTIFIER_LENGTH) return rawTableName;
        const hash = crypto.createHash("sha256").update(rawTableName).digest("hex").slice(0, 32);
        return `__items__${hash}`;
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

    private _inferMySqlTypeFromValue(v: any): "TEXT" | "INTEGER" | "DOUBLE" | "BOOLEAN" | "JSON" {
        if (v === null || v === undefined) return "TEXT";
        if (this._isRnValue(v)) return "TEXT";

        switch (typeof v) {
            case "string":
                return "TEXT";
            case "boolean":
                return "BOOLEAN";
            case "number":
                return Number.isInteger(v) ? "INTEGER" : "DOUBLE";
            case "object":
                return "JSON";
            default:
                throw new Error("Unsupported type: " + typeof v);
        }
    }

    private _inferBackupMySqlType(rows: Record<string, unknown>[], key: string): "TEXT" | "INTEGER" | "DOUBLE" | "BOOLEAN" | "JSON" {
        for (const row of rows) {
            const value = (row as any)[key];
            if (value === null || value === undefined) continue;
            if (typeof value === "string") {
                try {
                    const parsed = JSON.parse(value);
                    if (parsed && typeof parsed === "object") {
                        return "JSON";
                    }
                } catch {
                    // fall through
                }
            }
            return this._inferMySqlTypeFromValue(value);
        }
        return "TEXT";
    }

    private _encodeMySqlValue(v: any): any {
        if (this._isRnValue(v)) return v.value();
        const normalized = this._normalizeEncryptedValue(v);
        if (normalized === undefined) return null;
        if (normalized === null) return null;
        if (typeof normalized === "boolean") return normalized ? 1 : 0;
        if (typeof normalized === "object") return JSON.stringify(normalized);
        return normalized;
    }

    private _normalizeExistingType(dataType: string, columnType?: string): "TEXT" | "INTEGER" | "DOUBLE" | "BOOLEAN" | "JSON" {
        const upperData = (dataType || "").toUpperCase();
        const upperColumnType = (columnType || "").toUpperCase();

        if (upperColumnType.startsWith("TINYINT(1)") || upperData === "TINYINT") {
            return "BOOLEAN";
        }
        if (upperData.includes("INT")) {
            return "INTEGER";
        }
        if (upperData.includes("DOUBLE") || upperData.includes("FLOAT") || upperData.includes("DECIMAL")) {
            return "DOUBLE";
        }
        if (upperData === "JSON") {
            return "JSON";
        }
        return "TEXT";
    }

    private async _ensureMetaTable(): Promise<void> {
        if (this._metaEnsured) return;
        const safe = this._quoteIdent(MYSQL_META_TABLE);
        await this._pool.query(
            `CREATE TABLE IF NOT EXISTS ${safe} (
                table_name VARCHAR(255) NOT NULL,
                column_name VARCHAR(255) NOT NULL,
                kind VARCHAR(32) NOT NULL,
                PRIMARY KEY (table_name, column_name)
            )`
        );
        this._metaEnsured = true;
    }

    private _inferColumnKindFromValue(value: any): ColumnKind {
        if (this._isEncryptValue(value) || this._isEncryptedPayload(value)) return "encrypted";
        if (this._isRnValue(value) || this._looksLikeRnString(value)) return "rn";
        if (value !== null && typeof value === "object") return "json";
        return "scalar";
    }

    private async _setColumnKind(tableName: string, columnName: string, kind: ColumnKind): Promise<void> {
        await this._ensureMetaTable();
        const safe = this._quoteIdent(MYSQL_META_TABLE);
        if (kind === "rn" || kind === "encrypted") {
            await this._pool.query(
                `INSERT INTO ${safe} (table_name, column_name, kind) VALUES (?, ?, ?)
                 ON DUPLICATE KEY UPDATE kind = VALUES(kind)`,
                [tableName, columnName, kind]
            );
        } else {
            await this._pool.query(
                `DELETE FROM ${safe} WHERE table_name = ? AND column_name = ?`,
                [tableName, columnName]
            );
        }
    }

    private async _getTableInfo(
        rawTableName: string,
        opts: { force?: boolean } = {}
    ): Promise<ColumnInfo[]> {
        const cached = this._tableInfoCache.get(rawTableName);
        if (cached && !opts.force) return cached;
        const tableName = this._resolveTableName(rawTableName);
        const [rows] = await this._pool.query<RowDataPacket[]>(
            `
            SELECT column_name as name, data_type as type, column_type
            FROM information_schema.columns 
            WHERE table_schema = DATABASE() 
              AND table_name = ?
        `,
            [tableName]
        );
        const info = rows as unknown as ColumnInfo[];
        this._tableInfoCache.set(rawTableName, info);
        return info;
    }

    private _needsTableInfoRefresh(row: Record<string, any>, tableInfo: ColumnInfo[]): boolean {
        if (tableInfo.length === 0) return true;
        const columns = new Set(tableInfo.map((c) => c.name));
        for (const key of Object.keys(row)) {
            if (!columns.has(key)) return true;
        }
        return false;
    }

    private async _decodeRowUsingTableInfo<T extends Record<string, any>>(
        row: T,
        tableInfo: ColumnInfo[],
        opts: { decryptEncrypted?: boolean; encryptionMeta?: EncryptionMeta } = {}
    ): Promise<T> {
        const typeByName = new Map(
            tableInfo.map((c) => [
                c.name,
                {
                    dataType: (c.type || "").toUpperCase(),
                    columnType: (c.column_type || "").toUpperCase()
                },
            ])
        );
        const out: any = {};
        for (const [key, value] of Object.entries(row)) {
            safeAssign(out, key, value);
        }
        const decryptEncrypted = opts.decryptEncrypted ?? true;

        for (const [k, v] of Object.entries(out)) {
            const info = typeByName.get(k);
            if (!info) continue;

            const normalized = this._normalizeExistingType(info.dataType, info.columnType);

            if (normalized === "BOOLEAN") {
                if (v === null || v === undefined) continue;
                safeAssign(out, k, Boolean(v));
            } else if (normalized === "JSON") {
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
            }
        }

        return out as T;
    }

    private _revertFactory(change: Change, destructive: boolean): RevertFunction {
        switch (change.type) {
            case "insert":
                return () => {
                    const rawTableName = `__items__${change.rn.namespace!}__${change.rn.kind!}`;
                    const safeTableName = this._quoteIdent(this._resolveTableName(rawTableName));
                    const deleteString = `DELETE FROM ${safeTableName} WHERE \`rnId\` = ?`;
                    void this._pool.query(deleteString, [change.rn.id!]);
                };
            case "update":
                return () => {
                    const item = new UncommittedItem(change.pool, change.rn, change.oldData);
                    void this.updateItem(item, { destructive });
                };
        }
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

        const keys = Object.keys(item.data as Record<string, any>);

        let insertString = `INSERT INTO ${safeTableName} ( \`rnId\``;
        for (const key of keys) {
            insertString += `, ${this._quoteIdent(key)}`;
        }
        insertString += `) VALUES (?`;
        for (let i = 0; i < keys.length; i++) {
            insertString += `, ?`;
        }
        insertString += `)`;

        const params = [
            item.rn!.id!,
            ...keys.map((k) => this._encodeMySqlValue((item.data as any)[k])),
        ];

        try {
            await this._pool.query(insertString, params);
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

    /** @inheritdoc */
    async updateItem<T extends JSONable>(item: UncommittedItem<T>, options?: PersistOptions): Promise<DbChangeResult<T>> {
        const ensured = await this._safeEnsureTables("update", item, options?.destructive ?? false);
        if (!ensured.success) {
            return ensured.result as DbChangeResult<T>;
        }
        const safeTableName = ensured.value;

        const rawTableName = `__items__${item.rn!.namespace!}__${item.rn!.kind!}`;
        let tableInfo = await this._getTableInfo(rawTableName);
        const [currentRows] = await this._pool.query<any[]>(`SELECT * FROM ${safeTableName} WHERE \`rnId\` = ?`, [item.rn!.id!]);
        const currentRow = (currentRows as any[])[0];
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
            tableInfo = await this._getTableInfo(rawTableName, { force: true });
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
        const params: any[] = [];
        for (const key of keys) {
            const columnName = this._quoteIdent(key);
            updateString += `${columnName} = ?, `;
            params.push(this._encodeMySqlValue(data[key]));
        }
        updateString = updateString.slice(0, -2);
        updateString += ` WHERE \`rnId\` = ?`;
        params.push(item.rn!.id!);

        try {
            await this._pool.query(updateString, params);
            return {
                revert: this._revertFactory({ type: "update", oldData: currentData, rn: item.rn!, pool: item.pool }, options?.destructive ?? false),
                success: true,
                item: new Item<T>(item.pool, item.data, item.rn),
                type: "update",
                oldData: currentData as T
            }
        } catch (error) {
            const message = this._friendlyMessage("update", item, error);
            return {
                revert: this._revertFactory({ type: "update", oldData: currentData, rn: item.rn!, pool: item.pool }, options?.destructive ?? false),
                success: false,
                error: new Error(message),
                type: "update",
                oldData: currentData as T,
                item: item
            }
        }
    }

    /** @inheritdoc */
    async readItemRaw<T extends JSONable>(rn: RN): Promise<T | undefined> {
        const rawTableName = `__items__${rn.namespace!}__${rn.kind!}`;
        let tableInfo = await this._getTableInfo(rawTableName);
        if (tableInfo.length === 0) return undefined;
        const safeTableName = this._quoteIdent(this._resolveTableName(rawTableName));
        const [currentRows] = await this._pool.query<any[]>(`SELECT * FROM ${safeTableName} WHERE \`rnId\` = ?`, [rn.id!]);
        const currentRow = (currentRows as any[])[0];
        if (!currentRow) return undefined;
        if (this._needsTableInfoRefresh(currentRow as Record<string, any>, tableInfo)) {
            tableInfo = await this._getTableInfo(rawTableName, { force: true });
        }
        const { rnId: _rnId, ...currentDataRaw } = currentRow;
        return await this._decodeRowUsingTableInfo(currentDataRaw as any, tableInfo, { decryptEncrypted: false });
    }

    /** @inheritdoc */
    async deleteItem(rn: RN): Promise<DbDeleteResult> {
        const rawTableName = `__items__${rn.namespace!}__${rn.kind!}`;
        const tableInfo = await this._getTableInfo(rawTableName);
        if (tableInfo.length === 0) return { success: true };
        const safeTableName = this._quoteIdent(this._resolveTableName(rawTableName));
        try {
            await this._pool.query(`DELETE FROM ${safeTableName} WHERE \`rnId\` = ?`, [rn.id!]);
            return { success: true };
        } catch (error) {
            const rnValue = rn.value() ?? "(unknown rn)";
            const baseLayer = `${this.type} source layer '${this.identifier}'`;
            return { success: false, error: new Error(`Failed to delete item '${rnValue}' in ${baseLayer}: ${error}`) };
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
            return v;
        };

        const pushParam = (v: any): string => {
            params.push(normalizeParam(v));
            return "?";
        };

        const pushJsonParam = (v: any): string => {
            params.push(JSON.stringify(v));
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

        const buildInClause = (
            lhs: string,
            values: any[],
            addParam: (v: any) => string,
            hasNull: boolean
        ): string => {
            if (values.length === 0) {
                return hasNull ? `${lhs} IS NULL` : "0=1";
            }
            const inExpr = `${lhs} IN (${values.map(addParam).join(", ")})`;
            if (!hasNull) return inExpr;
            return `(${inExpr} OR ${lhs} IS NULL)`;
        };

        const detectInType = (values: any[]): "string" | "number" | "boolean" | "other" => {
            let hasString = false;
            let hasNumber = false;
            let hasBoolean = false;
            let hasOther = false;
            for (const entry of values) {
                const t = typeof entry;
                if (t === "string") hasString = true;
                else if (t === "number") hasNumber = true;
                else if (t === "boolean") hasBoolean = true;
                else hasOther = true;
            }
            if (hasOther) return "other";
            const count = Number(hasString) + Number(hasNumber) + Number(hasBoolean);
            if (count !== 1) return "other";
            if (hasString) return "string";
            if (hasNumber) return "number";
            return "boolean";
        };

        const formatJsonBool = (v: boolean): string => {
            return `CAST(${pushParam(v ? "true" : "false")} AS JSON)`;
        };

        const formatJsonValue = (v: any): string => {
            return `CAST(${pushJsonParam(v)} AS JSON)`;
        };

        const safeJsonPathFromDotted = (dotted: string): { base: string; path: string[] } => {
            const [base, ...rest] = dotted.split(".");
            if (!base || rest.length === 0) throw new Error(`Invalid dotted property: ${dotted}`);

            const isSafeSegment = (s: string) => /^[A-Za-z_][A-Za-z0-9_]*$/.test(s);
            for (const seg of rest) {
                if (!isSafeSegment(seg)) throw new Error(`Unsafe JSON path segment "${seg}" in "${dotted}"`);
            }
            return { base, path: rest };
        };

        const buildNullComparison = (lhs: string, operator: string, value: any): string | null => {
            if (value === null || value === undefined) {
                if (operator === "=") return `${lhs} IS NULL`;
                if (operator === "!=") return `${lhs} IS NOT NULL`;
                return `${lhs} ${operator} NULL`;
            }
            return null;
        };

        const buildJsonComparison = (jsonExtract: string, operator: string, value: any): string => {
            const nullComparison = buildNullComparison(jsonExtract, operator, value);
            if (nullComparison) return nullComparison;

            if (operator === "LIKE") {
                const likeValue = typeof value === "string"
                    ? value
                    : typeof value === "object"
                        ? JSON.stringify(value)
                        : String(value);
                return `JSON_UNQUOTE(${jsonExtract}) ${operator} ${pushParam(likeValue)}`;
            }

            if (typeof value === "string") {
                return `JSON_UNQUOTE(${jsonExtract}) ${operator} ${pushParam(value)}`;
            }
            if (typeof value === "number") {
                return `CAST(${jsonExtract} AS DOUBLE) ${operator} ${pushParam(value)}`;
            }
            if (typeof value === "boolean") {
                return `${jsonExtract} ${operator} ${formatJsonBool(value)}`;
            }
            return `${jsonExtract} ${operator} ${formatJsonValue(value)}`;
        };

        const buildExpr = (node: _QueryComponent): string => {
            if (node.type === "comparison") {
                let lhs: string;

                if (node.property.includes(".")) {
                    const { base, path } = safeJsonPathFromDotted(node.property);
                    const jsonPath = `$.${path.join(".")}`;
                    const baseIdent = this._quoteIdent(base);
                    const jsonExtract = `CASE WHEN JSON_VALID(${baseIdent}) THEN JSON_EXTRACT(${baseIdent}, '${jsonPath}') ELSE NULL END`;
                    if (node.operator === "IN") {
                        const { values, hasNull } = normalizeInValues(node.value);
                        const valueType = detectInType(values);
                        if (valueType === "string") {
                            return buildInClause(`JSON_UNQUOTE(${jsonExtract})`, values, pushParam, hasNull);
                        }
                        if (valueType === "number") {
                            return buildInClause(`CAST(${jsonExtract} AS DOUBLE)`, values, pushParam, hasNull);
                        }
                        if (valueType === "boolean") {
                            return buildInClause(jsonExtract, values, formatJsonBool, hasNull);
                        }
                        return buildInClause(jsonExtract, values, formatJsonValue, hasNull);
                    }
                    return buildJsonComparison(jsonExtract, node.operator, node.value);
                } else {
                    lhs = this._quoteIdent(node.property);
                }

                if (node.operator === "IN") {
                    const { values, hasNull } = normalizeInValues(node.value);
                    return buildInClause(lhs, values, pushParam, hasNull);
                }

                const nullComparison = buildNullComparison(lhs, node.operator, node.value);
                if (nullComparison) return nullComparison;

                return `${lhs} ${node.operator} ${pushParam(node.value)}`;
            }

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
        const rawTableName = `__items__${query.rn!.namespace!}__${query.rn!.kind!}`;
        const tableName = this._resolveTableName(rawTableName);

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
        let tableInfo: ColumnInfo[] = [];

        try {
            tableInfo = await this._getTableInfo(rawTableName);
            if (tableInfo.length === 0) {
                return new Result({ success: true, data: [] });
            }
            const [rows] = await this._pool.query<any[]>(queryString, params);
            rawItems = rows as (T & { rnId: string })[];
            if (rawItems.length > 0 && this._needsTableInfoRefresh(rawItems[0] as Record<string, any>, tableInfo)) {
                tableInfo = await this._getTableInfo(rawTableName, { force: true });
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

    private _friendlyMessage<T extends JSONable>(op: "create" | "update", item: FloatingItem<T> | UncommittedItem<T>, error: unknown): string {
        const rnValue = item.rn?.value() ?? "(unknown rn)";
        const baseLayer = `${this.type} source layer '${this.identifier}'`;
        const errorText = String(error);
        if (op === "create" && errorText.toLowerCase().includes("duplicate entry")) {
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

    /** @inheritdoc */
    async ensureTables(
        item: Item<any> | FloatingItem<any> | UncommittedItem<any>,
        destructive: boolean = false
    ): Promise<string> {
        const rawTableName = `__items__${item.rn!.namespace!}__${item.rn!.kind!}`;
        const actualTableName = this._resolveTableName(rawTableName);
        const tableName = this._quoteIdent(actualTableName);
        let schemaChanged = false;

        const [existsRows] = await this._pool.query<RowDataPacket[]>(
            `
            SELECT COUNT(*) as cnt
            FROM information_schema.tables
            WHERE table_schema = DATABASE()
              AND table_name = ?
        `,
            [actualTableName]
        );
        const exists = Boolean((existsRows as RowDataPacket[])[0]?.cnt);

        if (!exists) {
            let createString = `CREATE TABLE IF NOT EXISTS ${tableName} ( \`rnId\` VARCHAR(255) PRIMARY KEY, `;

            for (const key in item.data) {
                const columnName = this._quoteIdent(key);
                const v = (item.data as any)[key];

                const t = this._inferMySqlTypeFromValue(v);
                if (t === "TEXT") createString += `${columnName} TEXT, `;
                else if (t === "INTEGER") createString += `${columnName} INT, `;
                else if (t === "DOUBLE") createString += `${columnName} DOUBLE, `;
                else if (t === "BOOLEAN") createString += `${columnName} TINYINT(1), `;
                else if (t === "JSON") createString += `${columnName} JSON, `;
                else throw new Error("Unsupported inferred type: " + t);
            }

            createString = createString.slice(0, -2);
            createString += ` )`;

            await this._pool.query(createString);
            for (const key in item.data) {
                const v = (item.data as any)[key];
                const kind = this._inferColumnKindFromValue(v);
                await this._setColumnKind(rawTableName, key, kind);
            }
            this._columnKindsCache.delete(rawTableName);
            schemaChanged = true;
            this._tableInfoCache.delete(rawTableName);
            return actualTableName;
        }

        const columns = await this._getTableInfo(rawTableName);

        for (const key in item.data) {
            const rawColumnName = key;
            const columnName = this._quoteIdent(rawColumnName);
            const v = (item.data as any)[key];

            if (v === undefined) continue;

            const existingColumn = columns.find((c) => c.name === rawColumnName);
            const existingColumnType = this._normalizeExistingType(existingColumn?.type || "", existingColumn?.column_type);

            let columnWantedType = this._inferMySqlTypeFromValue(v).toUpperCase();

            if (!existingColumn) {
                const alterString = `ALTER TABLE ${tableName} ADD COLUMN ${columnName} ${columnWantedType === "BOOLEAN" ? "TINYINT(1)" : columnWantedType}`;
                await this._pool.query(alterString);
                schemaChanged = true;
            } else if (existingColumnType !== columnWantedType) {
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

                await this._pool.query(`ALTER TABLE ${tableName} DROP COLUMN ${columnName}`);
                const mappedType = columnWantedType === "BOOLEAN" ? "TINYINT(1)" : columnWantedType;
                await this._pool.query(`ALTER TABLE ${tableName} ADD COLUMN ${columnName} ${mappedType}`);

                console.log("Dropped and added column", rawColumnName, "with type", columnWantedType);
                schemaChanged = true;
            }
            const kind = this._inferColumnKindFromValue(v);
            await this._setColumnKind(rawTableName, rawColumnName, kind);
        }

        this._columnKindsCache.delete(rawTableName);
        if (schemaChanged) {
            this._tableInfoCache.delete(rawTableName);
        }
        return actualTableName;
    }

    /** @inheritdoc */
    async getColumnKinds(namespace: string, kind: string): Promise<Map<string, ColumnKind>> {
        const rawTableName = `__items__${namespace}__${kind}`;
        const cached = this._columnKindsCache.get(rawTableName);
        if (cached) return new Map(cached);
        const columns = await this._getTableInfo(rawTableName);
        const map = new Map<string, ColumnKind>();
        if (columns.length === 0) return map;

        for (const col of columns) {
            const normalized = this._normalizeExistingType(col.type || "", col.column_type);
            map.set(col.name, normalized === "JSON" ? "json" : "scalar");
        }

        await this._ensureMetaTable();
        const safe = this._quoteIdent(MYSQL_META_TABLE);
        const [rows] = await this._pool.query<RowDataPacket[]>(
            `SELECT column_name, kind FROM ${safe} WHERE table_name = ?`,
            [rawTableName]
        );
        for (const row of rows as any[]) {
            const columnName = row.column_name ?? row.columnName;
            const kindValue = row.kind;
            if (!columnName || typeof kindValue !== "string") continue;
            if (kindValue === "rn" || kindValue === "encrypted") {
                map.set(String(columnName), kindValue);
            }
        }
        this._columnKindsCache.set(rawTableName, map);
        return new Map(map);
    }
}
