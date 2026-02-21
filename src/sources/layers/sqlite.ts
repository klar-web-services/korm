import {
  type Change,
  type DbChangeResult,
  type DbDeleteResult,
  type PersistOptions,
  type RevertFunction,
  type SourceLayer,
} from "../sourceLayer";
import { korm } from "../../korm";
import type { JSONable } from "../../korm";
import { Result } from "@fkws/klonk-result";
import { QueryBuilder, type _QueryComponent } from "../../core/query";
import { FloatingItem, UncommittedItem, Item } from "../../core/item";
import { FloatingDepotFile } from "../../depot/depotFile";
import {
  createSqliteClient,
  type SqliteClient,
} from "../../runtime/sqliteClient";
import { decrypt, Encrypt } from "../../security/encryption";
import { cloneJson, type PathKey } from "../../core/resolveMeta";
import {
  setEncryptionMeta,
  type EncryptionMeta,
  type EncryptedPayload,
} from "../../core/encryptionMeta";
import type { ColumnKind } from "../../core/columnKind";
import { RN } from "../../core/rn";
import {
  Unique,
  fingerprintUniqueValue,
} from "../../core/unique";
import { safeAssign } from "../../core/safeObject";
import { createHash } from "node:crypto";
import {
  BACKUP_EXTENSION,
  buildBackupHeaderEvent,
  buildBackupRn,
  streamBackupEvents,
  type BackupContext,
  type BackupRestoreOptions,
  type BackupRestorePayload,
} from "../backups";

const SQLITE_JSON_TYPE = "JSON_TEXT";
const SQLITE_RN_TYPE = "RN_REF_TEXT";
const SQLITE_ENCRYPTED_TYPE = "ENCRYPTED_JSON";
const SQLITE_UNIQUE_SHADOW_PREFIX = "__korm_unique__";

/**
 * SQLite-backed source layer.
 * Create via `korm.layers.sqlite(path)` and add to a pool.
 */
export class SqliteLayer implements SourceLayer {
  public _db: SqliteClient;
  public readonly type: "sqlite" = "sqlite";
  public readonly identifier: string;
  private _path: string;
  private _columnKindsCache: Map<string, Map<string, ColumnKind>> = new Map();
  private _tableInfoCache: Map<string, { name: string; type: string }[]> =
    new Map();

  /** Create a SQLite layer backed by the given file path. */
  constructor(path: string) {
    this._db = createSqliteClient(path);
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
    const tables = this._db
      .prepare(
        `SELECT name FROM sqlite_master
             WHERE type = 'table'
             AND (name LIKE '__items__%' OR name = '__korm_meta__' OR name = '__korm_pool__')`,
      )
      .all() as Array<{ name: string }>;
    const rn = buildBackupRn(
      context.depotIdent,
      context.layerIdent,
      now,
      BACKUP_EXTENSION,
    );
    const events = async function* (
      self: SqliteLayer,
    ): AsyncGenerator<
      | ReturnType<typeof buildBackupHeaderEvent>
      | { t: "table"; name: string }
      | { t: "row"; table: string; row: Record<string, unknown> }
      | { t: "endTable"; name: string }
      | { t: "end" }
    > {
      yield buildBackupHeaderEvent("sqlite", context.layerIdent, now);
      for (const table of tables) {
        yield { t: "table", name: table.name };
        const safeName = self._quoteIdent(table.name);
        const stmt = self._db.prepare(`SELECT * FROM ${safeName}`);
        for (const row of stmt.iterate()) {
          yield {
            t: "row",
            table: table.name,
            row: row as Record<string, unknown>,
          };
        }
        yield { t: "endTable", name: table.name };
      }
      yield { t: "end" };
    };
    const stream = streamBackupEvents(events(this));
    const file = new FloatingDepotFile(rn, stream);
    await context.depot.createFile(file);
  }

  /** @internal */
  async restore(
    payload: BackupRestorePayload,
    options?: BackupRestoreOptions,
  ): Promise<void> {
    if (payload.header.layerType !== "sqlite") {
      throw new Error(
        `Backup payload layer type "${payload.header.layerType}" does not match sqlite.`,
      );
    }
    const mode = options?.mode ?? "replace";
    const poolTable = "__korm_pool__";
    const metaTable = "__korm_meta__";
    let schemaChanged = false;

    type TableState = {
      rawName: string;
      safeName: string;
      created: boolean;
      columnTypes: Map<string, string>;
      pendingColumns: Set<string>;
      isItems: boolean;
    };

    const ensurePoolTable = (state?: TableState): void => {
      this._db.run(
        `CREATE TABLE IF NOT EXISTS "${poolTable}" (
                    "id" TEXT PRIMARY KEY,
                    "config" TEXT NOT NULL,
                    "created_at" INTEGER NOT NULL,
                    "updated_at" INTEGER NOT NULL
                )`,
      );
      schemaChanged = true;
      if (state) {
        state.created = true;
        state.columnTypes.set("id", "TEXT");
        state.columnTypes.set("config", "TEXT");
        state.columnTypes.set("created_at", "INTEGER");
        state.columnTypes.set("updated_at", "INTEGER");
      }
    };

    const ensureMetaTable = (state?: TableState): void => {
      this._db.run(
        `CREATE TABLE IF NOT EXISTS "${metaTable}" (
                    "table_name" TEXT NOT NULL,
                    "column_name" TEXT NOT NULL,
                    "kind" TEXT NOT NULL,
                    PRIMARY KEY ("table_name", "column_name")
                )`,
      );
      schemaChanged = true;
      if (state) {
        state.created = true;
        state.columnTypes.set("table_name", "TEXT");
        state.columnTypes.set("column_name", "TEXT");
        state.columnTypes.set("kind", "TEXT");
      }
    };

    const createTable = (state: TableState): void => {
      if (state.created) return;
      if (state.columnTypes.size === 0 && state.pendingColumns.size > 0) {
        for (const key of state.pendingColumns) {
          if (!state.columnTypes.has(key)) {
            state.columnTypes.set(key, "TEXT");
          }
        }
        state.pendingColumns.clear();
      }
      if (state.columnTypes.size === 0) return;
      const columnDefs: string[] = [];
      for (const [key, type] of state.columnTypes) {
        const columnName = this._quoteIdent(key);
        if (state.isItems && key === "rnId") {
          columnDefs.push(`${columnName} TEXT PRIMARY KEY`);
        } else {
          columnDefs.push(`${columnName} ${type}`);
        }
      }
      if (columnDefs.length === 0) return;
      this._db.run(
        `CREATE TABLE IF NOT EXISTS ${state.safeName} (${columnDefs.join(", ")})`,
      );
      state.created = true;
      schemaChanged = true;
    };

    const addColumn = (state: TableState, key: string, type: string): void => {
      if (state.columnTypes.has(key)) return;
      const columnName = this._quoteIdent(key);
      if (state.created) {
        this._db.run(
          `ALTER TABLE ${state.safeName} ADD COLUMN ${columnName} ${type}`,
        );
        schemaChanged = true;
      }
      state.columnTypes.set(key, type);
    };

    const ensureRnId = (state: TableState): void => {
      if (!state.isItems || state.columnTypes.has("rnId")) return;
      addColumn(state, "rnId", "TEXT");
    };

    const initTableState = (rawName: string): TableState => {
      const safeName = this._quoteIdent(rawName);
      const tableInfo = this._getTableInfo(rawName, { force: true });
      const columnTypes = new Map<string, string>(
        tableInfo.map((col) => [col.name, (col.type || "TEXT").toUpperCase()]),
      );
      const state: TableState = {
        rawName,
        safeName,
        created: tableInfo.length > 0,
        columnTypes,
        pendingColumns: new Set<string>(),
        isItems: rawName.startsWith("__items__"),
      };

      if (rawName === poolTable && !state.created) {
        ensurePoolTable(state);
      } else if (rawName === metaTable && !state.created) {
        ensureMetaTable(state);
      }

      ensureRnId(state);

      if (mode === "replace" && state.created) {
        this._db.run(`DELETE FROM ${safeName}`);
      }

      return state;
    };

    const finalizeTable = (state: TableState | null): void => {
      if (!state) return;
      if (!state.created) {
        if (state.isItems && !state.columnTypes.has("rnId")) {
          state.columnTypes.set("rnId", "TEXT");
        }
        createTable(state);
      }
      if (state.pendingColumns.size > 0) {
        for (const key of state.pendingColumns) {
          addColumn(state, key, "TEXT");
        }
        state.pendingColumns.clear();
      }
    };

    let current: TableState | null = null;

    while (true) {
      const event = await payload.reader.next();
      if (!event) break;
      if (event.t === "header") {
        throw new Error("Unexpected backup header event during restore.");
      }
      if (event.t === "table") {
        finalizeTable(current);
        current = initTableState(event.name);
        continue;
      }
      if (event.t === "endTable") {
        if (current && event.name !== current.rawName) {
          throw new Error(
            `Backup endTable event "${event.name}" does not match "${current.rawName}".`,
          );
        }
        finalizeTable(current);
        current = null;
        continue;
      }
      if (event.t === "end") {
        finalizeTable(current);
        current = null;
        break;
      }
      if (event.t === "row") {
        if (!current) {
          throw new Error("Backup row event arrived without a table context.");
        }
        const state = current;
        if (event.table !== state.rawName) {
          throw new Error(
            `Backup row event "${event.table}" does not match "${state.rawName}".`,
          );
        }
        const row = event.row ?? {};
        for (const key of Object.keys(row)) {
          if (state.columnTypes.has(key)) continue;
          const value = (row as any)[key];
          if (value === null || value === undefined) {
            state.pendingColumns.add(key);
            continue;
          }
          if (state.pendingColumns.has(key)) {
            state.pendingColumns.delete(key);
          }
          const type = this._inferBackupSqliteTypeFromValue(value);
          if (!state.created) {
            state.columnTypes.set(key, type);
          } else {
            addColumn(state, key, type);
          }
        }

        if (!state.created) {
          createTable(state);
        }

        const columns = Object.keys(row).filter((key) =>
          state.columnTypes.has(key),
        );
        if (columns.length === 0) {
          continue;
        }
        const columnList = columns
          .map((key) => this._quoteIdent(key))
          .join(", ");
        const placeholders = columns.map(() => "?").join(", ");
        const insertVerb = mode === "merge" ? "INSERT OR IGNORE" : "INSERT";
        const stmt = this._db.prepare(
          `${insertVerb} INTO ${state.safeName} (${columnList}) VALUES (${placeholders})`,
        );
        const values = columns.map((key) =>
          this._encodeSqlValue((row as any)[key]),
        );
        stmt.run(...values);
      }
    }

    if (schemaChanged) {
      this._tableInfoCache.clear();
      this._columnKindsCache.clear();
    }
  }

  /** @internal */
  getPoolConfig(): { type: "sqlite"; path: string } {
    return { type: "sqlite", path: this._path };
  }

  private _quoteIdent(name: string): string {
    if (!name || name.includes("\u0000"))
      throw new Error("Invalid identifier: " + name);
    if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(name)) {
      throw new Error(`Unsafe identifier: ${name}`);
    }
    return `"${name.replace(/"/g, '""')}"`;
  }

  private _isEncryptValue(
    value: any,
  ): value is { __ENCRYPT__: true; safeValue: () => any } {
    return Boolean(
      value &&
      typeof value === "object" &&
      value.__ENCRYPT__ === true &&
      typeof value.safeValue === "function",
    );
  }

  private _isEncryptedPayload(value: any): value is EncryptedPayload {
    return Boolean(
      value &&
      typeof value === "object" &&
      value.__ENCRYPTED__ === true &&
      typeof value.type === "string" &&
      typeof value.value === "string",
    );
  }

  private _isRnValue(
    value: any,
  ): value is { __RN__: true; value: () => string } {
    return Boolean(
      value &&
      typeof value === "object" &&
      value.__RN__ === true &&
      typeof value.value === "function",
    );
  }

  private _looksLikeRnString(value: any): value is string {
    return typeof value === "string" && value.startsWith("[rn]");
  }

  private _uniqueShadowColumnName(columnName: string): string {
    return `${SQLITE_UNIQUE_SHADOW_PREFIX}${columnName}`;
  }

  private _baseColumnNameFromUniqueShadow(
    columnName: string,
  ): string | undefined {
    if (!columnName.startsWith(SQLITE_UNIQUE_SHADOW_PREFIX)) return undefined;
    const base = columnName.slice(SQLITE_UNIQUE_SHADOW_PREFIX.length);
    return base.length > 0 ? base : undefined;
  }

  private _isUniqueShadowColumn(columnName: string): boolean {
    return this._baseColumnNameFromUniqueShadow(columnName) !== undefined;
  }

  private _getUniqueColumnsFromTableInfo(
    tableInfo: { name: string; type: string }[],
  ): Set<string> {
    const uniqueColumns = new Set<string>();
    for (const column of tableInfo) {
      const base = this._baseColumnNameFromUniqueShadow(column.name);
      if (!base) continue;
      uniqueColumns.add(base);
    }
    return uniqueColumns;
  }

  private _isUniqueValue(value: any): value is Unique<JSONable> {
    return Boolean(
      value &&
      typeof value === "object" &&
      value.__UNIQUE__ === true &&
      typeof value.value === "function" &&
      typeof value.fingerprint === "function",
    );
  }

  private _unwrapUniqueValue(value: any): any {
    if (!this._isUniqueValue(value)) return value;
    return value.value();
  }

  private _fingerprintForUniqueValue(value: any): string | null {
    const unwrapped = this._unwrapUniqueValue(value);
    if (unwrapped === null || unwrapped === undefined) return null;
    if (this._isUniqueValue(value)) return value.fingerprint();
    return fingerprintUniqueValue(unwrapped as JSONable);
  }

  private _uniqueShadowIndexName(
    rawTableName: string,
    columnName: string,
  ): string {
    const digest = createHash("sha256")
      .update(`${rawTableName}:${columnName}`)
      .digest("hex")
      .slice(0, 24);
    return `__korm_u_${digest}`;
  }

  private _ensureUniqueShadowIndex(
    rawTableName: string,
    columnName: string,
  ): void {
    const safeTableName = this._quoteIdent(rawTableName);
    const safeIndexName = this._quoteIdent(
      this._uniqueShadowIndexName(rawTableName, columnName),
    );
    const safeShadowColumn = this._quoteIdent(
      this._uniqueShadowColumnName(columnName),
    );
    this._db.run(
      `CREATE UNIQUE INDEX IF NOT EXISTS ${safeIndexName} ON ${safeTableName} (${safeShadowColumn})`,
    );
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
    path: PathKey[],
  ): Promise<any> {
    if (this._isEncryptedPayload(value)) {
      if (value.type === "password") {
        const decoded = await this._decodeEncryptedValue(value);
        meta.entries.push({
          path: [...path],
          payload: value,
          plainValue: value.value,
        });
        return decoded;
      }
      const decodedPlain = await decrypt(value as any);
      meta.entries.push({
        path: [...path],
        payload: value,
        plainValue: cloneJson(decodedPlain as JSONable),
      });
      return Encrypt.fromEncryptedPayload(value, decodedPlain as JSONable);
    }
    if (Array.isArray(value)) {
      const next = [];
      for (let i = 0; i < value.length; i++) {
        next.push(
          await this._decodeEncryptedTree(value[i], meta, [...path, i]),
        );
      }
      return next;
    }
    if (value && typeof value === "object") {
      const out: Record<string, any> = {};
      for (const [key, child] of Object.entries(value)) {
        safeAssign(
          out,
          key,
          await this._decodeEncryptedTree(child, meta, [...path, key]),
        );
      }
      return out;
    }
    return value;
  }

  private _inferSqliteTypeFromValue(v: any): string {
    const value = this._unwrapUniqueValue(v);
    if (value === null) return "TEXT"; // policy: allow null, default to TEXT
    if (value === undefined) return "TEXT"; // policy: undefined isn't stored; TEXT fallback
    if (this._isRnValue(value) || this._looksLikeRnString(value))
      return SQLITE_RN_TYPE;
    if (this._isEncryptValue(value) || this._isEncryptedPayload(value))
      return SQLITE_ENCRYPTED_TYPE;

    switch (typeof value) {
      case "string":
        return "TEXT";
      case "boolean":
        return "BOOLEAN";
      case "number":
        return Number.isInteger(value) ? "INTEGER" : "REAL";
      case "object":
        return SQLITE_JSON_TYPE;
      default:
        throw new Error("Unsupported type: " + typeof value);
    }
  }

  private _inferBackupSqliteTypeFromValue(value: unknown): string {
    if (value === null || value === undefined) return "TEXT";
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

  private _encodeSqlValue(v: any): any {
    const unwrapped = this._unwrapUniqueValue(v);
    if (this._isRnValue(unwrapped)) return unwrapped.value();
    const normalized = this._normalizeEncryptedValue(unwrapped);
    if (normalized === undefined) return null; // policy: store undefined as NULL
    if (normalized === null) return null;
    if (typeof normalized === "boolean") return normalized ? 1 : 0;
    if (typeof normalized === "object") return JSON.stringify(normalized);
    return normalized;
  }

  private _getTableInfo(
    rawTableName: string,
    opts: { force?: boolean } = {},
  ): { name: string; type: string }[] {
    const cached = this._tableInfoCache.get(rawTableName);
    if (cached && cached.length > 0 && !opts.force) return cached;
    const tableName = this._quoteIdent(rawTableName);
    const info = this._db
      .prepare(`PRAGMA table_info(${tableName})`)
      .all<{ name: string; type: string }>();
    if (info.length > 0) {
      this._tableInfoCache.set(rawTableName, info);
    } else {
      this._tableInfoCache.delete(rawTableName);
    }
    return info;
  }

  private _needsTableInfoRefresh(
    row: Record<string, any>,
    tableInfo: { name: string; type: string }[],
  ): boolean {
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
    opts: { decryptEncrypted?: boolean; encryptionMeta?: EncryptionMeta } = {},
  ): Promise<T> {
    const uniqueColumns = this._getUniqueColumnsFromTableInfo(tableInfo);
    const typeByName = new Map(
      tableInfo.map((c) => [c.name, (c.type || "").toUpperCase()]),
    );
    const out: any = {};
    for (const [key, value] of Object.entries(row)) {
      if (this._isUniqueShadowColumn(key)) continue;
      safeAssign(out, key, value);
    }
    const decryptEncrypted = opts.decryptEncrypted ?? true;

    for (const [k, v] of Object.entries(out)) {
      const t = typeByName.get(k);
      if (!t) continue;

      if (t === SQLITE_RN_TYPE) {
        if (typeof v === "string" && v.startsWith("[rn]")) {
          const parsed = RN.create(v);
          if (parsed.isOk()) {
            safeAssign(out, k, parsed.unwrap());
          }
        }
      } else if (t === "BOOLEAN") {
        // SQLite commonly returns 0/1 for BOOLEAN-ish columns
        if (v === null || v === undefined) continue;
        safeAssign(out, k, Boolean(v));
      } else if (
        t === SQLITE_JSON_TYPE ||
        t === SQLITE_ENCRYPTED_TYPE ||
        t === "JSON"
      ) {
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
            parsed = await this._decodeEncryptedTree(
              parsed,
              opts.encryptionMeta,
              [k],
            );
          } else {
            parsed = await this._decodeEncryptedValue(parsed);
          }
        }
        safeAssign(out, k, parsed);
      } else if (t === "ID_TEXT") {
        // leave as-is (string)
      }

      const decodedValue = out[k];
      if (
        uniqueColumns.has(k) &&
        decodedValue !== undefined &&
        decodedValue !== null &&
        !this._isUniqueValue(decodedValue)
      ) {
        safeAssign(out, k, new Unique(decodedValue as JSONable));
      }
    }

    return out as T;
  }

  /** @inheritdoc */
  async insertItem<T extends JSONable>(
    item: FloatingItem<T>,
    options?: PersistOptions,
  ): Promise<DbChangeResult<T>> {
    const ensured = await this._safeEnsureTables(
      "insert",
      item,
      options?.destructive ?? false,
    );
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
        error: new Error(
          `Tried to create item '${rnValue}' which already exists in ${baseLayer}.`,
        ),
        type: "insert",
        item: item,
      };
    }

    const data = (item.data ?? {}) as Record<string, any>;
    const keys = Object.keys(data);
    const rawTableName = `__items__${item.rn!.namespace!}__${item.rn!.kind!}`;
    const tableInfo = this._getTableInfo(rawTableName, { force: true });
    const uniqueColumns = this._getUniqueColumnsFromTableInfo(tableInfo);
    for (const key of keys) {
      if (this._isUniqueValue(data[key])) {
        uniqueColumns.add(key);
      }
    }
    const uniqueKeys = keys.filter((key) => uniqueColumns.has(key));

    let insertString = `INSERT INTO ${safeTableName} ( "rnId"`;
    for (const key of keys) {
      insertString += `, ${this._quoteIdent(key)}`;
    }
    for (const key of uniqueKeys) {
      insertString += `, ${this._quoteIdent(this._uniqueShadowColumnName(key))}`;
    }
    insertString += `) VALUES ( ?`;
    for (let i = 0; i < keys.length + uniqueKeys.length; i++) {
      insertString += `, ?`;
    }
    insertString += `)`;

    const params = [
      item.rn!.id!,
      ...keys.map((k) => this._encodeSqlValue(data[k])),
      ...uniqueKeys.map((k) => this._fingerprintForUniqueValue(data[k])),
    ];
    try {
      this._db.run(insertString, params);
      return {
        revert: this._revertFactory(
          { type: "insert", rn: item.rn!, pool: item.pool },
          options?.destructive ?? false,
        ),
        success: true,
        item: new Item<T>(item.pool, item.data, item.rn),
        type: "insert",
      };
    } catch (error) {
      const message = this._friendlyMessage("create", item, error);
      return {
        revert: this._revertFactory(
          { type: "insert", rn: item.rn!, pool: item.pool },
          options?.destructive ?? false,
        ),
        success: false,
        error: new Error(message),
        type: "insert",
        item: item,
      };
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
          void this.updateItem(
            new UncommittedItem(change.pool, change.rn, change.oldData),
            { destructive },
          );
        };
    }
  }

  /** @inheritdoc */
  async updateItem<T extends JSONable>(
    item: UncommittedItem<T>,
    options?: PersistOptions,
  ): Promise<DbChangeResult<T>> {
    const ensured = await this._safeEnsureTables(
      "update",
      item,
      options?.destructive ?? false,
    );
    if (!ensured.success) {
      return ensured.result as DbChangeResult<T>;
    }
    const safeTableName = ensured.value;

    const rawTableName = `__items__${item.rn!.namespace!}__${item.rn!.kind!}`;
    let tableInfo = this._getTableInfo(rawTableName);
    // Get current data as oldData from db. Drop rnId so ensureTables doesn't try to alter the PK column during revert.
    const currentRow = this._db
      .prepare(`SELECT * FROM ${safeTableName} WHERE "rnId" = ?`)
      .get<Record<string, any>>(item.rn!.id!);
    if (!currentRow) {
      const rnValue = item.rn?.value() ?? "(unknown rn)";
      const baseLayer = `${this.type} source layer '${this.identifier}'`;
      return {
        revert: () => {},
        success: false,
        error: new Error(
          `Tried to update item '${rnValue}' which does not exist in ${baseLayer}.`,
        ),
        type: "update",
        oldData: (item.data ?? {}) as T,
        item: item,
      };
    }
    if (
      this._needsTableInfoRefresh(currentRow as Record<string, any>, tableInfo)
    ) {
      tableInfo = this._getTableInfo(rawTableName, { force: true });
    }
    const { rnId: _rnId, ...currentDataRaw } = currentRow;
    const currentData = await this._decodeRowUsingTableInfo(
      currentDataRaw as any,
      tableInfo,
      { decryptEncrypted: false },
    );

    const data = (item.data ?? {}) as Record<string, any>;
    const keys = Object.keys(data);
    const uniqueColumns = this._getUniqueColumnsFromTableInfo(tableInfo);
    for (const key of keys) {
      if (this._isUniqueValue(data[key])) {
        uniqueColumns.add(key);
      }
    }
    if (keys.length === 0) {
      return {
        revert: () => {},
        success: true,
        item: new Item<T>(item.pool, currentData as T, item.rn),
        type: "update",
        oldData: currentData as T,
      };
    }

    let updateString = `UPDATE ${safeTableName} SET `;
    for (const key of keys) {
      const columnName = this._quoteIdent(key);
      updateString += `${columnName} = ?, `;
    }
    for (const key of keys) {
      if (!uniqueColumns.has(key)) continue;
      const shadowColumnName = this._quoteIdent(
        this._uniqueShadowColumnName(key),
      );
      updateString += `${shadowColumnName} = ?, `;
    }
    updateString = updateString.slice(0, -2);
    updateString += ` WHERE "rnId" = ?`;

    const params = [
      ...keys.map((k) => this._encodeSqlValue(data[k])),
      ...keys
        .filter((k) => uniqueColumns.has(k))
        .map((k) => this._fingerprintForUniqueValue(data[k])),
      item.rn!.id!,
    ];
    try {
      this._db.run(updateString, params);
      return {
        revert: this._revertFactory(
          {
            type: "update",
            oldData: currentData,
            rn: item.rn!,
            pool: item.pool,
          },
          options?.destructive ?? false,
        ),
        success: true,
        item: new Item<T>(item.pool, item.data, item.rn),
        type: "update",
        oldData: currentData,
      };
    } catch (error) {
      const message = this._friendlyMessage("update", item, error);
      return {
        revert: this._revertFactory(
          {
            type: "update",
            oldData: currentData,
            rn: item.rn!,
            pool: item.pool,
          },
          options?.destructive ?? false,
        ),
        success: false,
        error: new Error(message),
        type: "update",
        oldData: currentData,
        item: item,
      };
    }
  }

  /** @inheritdoc */
  async readItemRaw<T extends JSONable>(rn: RN): Promise<T | undefined> {
    const rawTableName = `__items__${rn.namespace!}__${rn.kind!}`;
    let tableInfo = this._getTableInfo(rawTableName);
    if (tableInfo.length === 0) return undefined;
    const safeTableName = this._quoteIdent(rawTableName);
    const currentRow = this._db
      .prepare(`SELECT * FROM ${safeTableName} WHERE "rnId" = ?`)
      .get<Record<string, any>>(rn.id!);
    if (!currentRow) return undefined;
    if (
      this._needsTableInfoRefresh(currentRow as Record<string, any>, tableInfo)
    ) {
      tableInfo = this._getTableInfo(rawTableName, { force: true });
    }
    const { rnId: _rnId, ...currentDataRaw } = currentRow;
    return await this._decodeRowUsingTableInfo(
      currentDataRaw as any,
      tableInfo,
      { decryptEncrypted: false },
    );
  }

  /** @inheritdoc */
  async deleteItem(rn: RN): Promise<DbDeleteResult> {
    const rawTableName = `__items__${rn.namespace!}__${rn.kind!}`;
    const tableInfo = this._getTableInfo(rawTableName);
    if (tableInfo.length === 0) {
      return {
        success: false,
        error: new Error(this._friendlyDeleteMessage(rn)),
      };
    }
    const safeTableName = this._quoteIdent(rawTableName);
    const existing = this._db
      .prepare(`SELECT 1 FROM ${safeTableName} WHERE "rnId" = ?`)
      .get<Record<string, unknown>>(rn.id!);
    if (!existing) {
      return {
        success: false,
        error: new Error(this._friendlyDeleteMessage(rn)),
      };
    }
    try {
      this._db.run(`DELETE FROM ${safeTableName} WHERE "rnId" = ?`, [rn.id!]);
      return { success: true };
    } catch (error) {
      return {
        success: false,
        error: new Error(this._friendlyDeleteMessage(rn, error)),
      };
    }
  }

  private _friendlyMessage<T extends JSONable>(
    op: "create" | "update",
    item: FloatingItem<T> | UncommittedItem<T>,
    error: unknown,
  ): string {
    const rnValue = item.rn?.value() ?? "(unknown rn)";
    const baseLayer = `${this.type} source layer '${this.identifier}'`;
    const errorText = String(error);
    if (errorText.includes("UNIQUE constraint")) {
      return `Failed to ${op} item '${rnValue}' in ${baseLayer}: unique field constraint violated.`;
    }
    return `Failed to ${op} item '${rnValue}' in ${baseLayer}: ${errorText}`;
  }

  private _friendlyDeleteMessage(rn: RN, error?: unknown): string {
    const rnValue = rn.value() ?? "(unknown rn)";
    const baseLayer = `${this.type} source layer '${this.identifier}'`;
    if (!error) {
      return `Tried to delete item '${rnValue}' which does not exist in ${baseLayer}.`;
    }
    return `Failed to delete item '${rnValue}' in ${baseLayer}: ${String(error)}`;
  }

  private async _safeEnsureTables<T extends JSONable>(
    op: "insert" | "update",
    item: FloatingItem<T> | UncommittedItem<T>,
    destructive: boolean,
  ): Promise<
    | { success: true; value: string }
    | { success: false; result: DbChangeResult<T> }
  > {
    try {
      const rawTableName = await this.ensureTables(item, destructive);
      return { success: true, value: this._quoteIdent(rawTableName) };
    } catch (error) {
      const message = this._friendlyMessage(
        op === "insert" ? "create" : "update",
        item,
        error,
      );
      const isInsert = op === "insert";
      if (isInsert) {
        const result: DbChangeResult<T> = {
          revert: () => {},
          success: false,
          error: new Error(message),
          type: "insert",
          item: item as FloatingItem<T>,
        };
        return { success: false, result };
      } else {
        const result: DbChangeResult<T> = {
          revert: () => {},
          success: false,
          error: new Error(message),
          type: "update",
          oldData: (item as UncommittedItem<T>).data as T,
          item: item as UncommittedItem<T>,
        };
        return { success: false, result };
      }
    }
  }

  private _buildQueryString(
    root: _QueryComponent,
    tableName: string,
    projection?: string[],
    currentString: string = "",
  ): { sql: string; params: any[] } {
    const params: any[] = [];

    const normalizeParam = (v: any): any => {
      const value = this._unwrapUniqueValue(v);
      if (value === null || value === undefined) return null;
      if (this._isRnValue(value)) return value.value();
      if (typeof value === "boolean") return value ? 1 : 0;
      if (typeof value === "object") return JSON.stringify(value);
      return value;
    };

    const pushParam = (v: any): string => {
      params.push(normalizeParam(v));
      return "?";
    };

    const normalizeInValues = (
      value: any,
    ): { values: any[]; hasNull: boolean } => {
      const list = Array.isArray(value) ? value : [value];
      const values: any[] = [];
      let hasNull = false;
      for (const entry of list) {
        const normalized = this._unwrapUniqueValue(entry);
        if (normalized === null || normalized === undefined) {
          hasNull = true;
        } else {
          values.push(normalized);
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

    const safeJsonPathFromDotted = (
      dotted: string,
    ): { base: string; path: string } => {
      const [base, ...rest] = dotted.split(".");
      if (!base || rest.length === 0)
        throw new Error(`Invalid dotted property: ${dotted}`);

      const isSafeSegment = (s: string) => /^[A-Za-z_][A-Za-z0-9_]*$/.test(s);
      for (const seg of rest) {
        if (!isSafeSegment(seg))
          throw new Error(`Unsafe JSON path segment "${seg}" in "${dotted}"`);
      }
      return { base, path: `$.${rest.join(".")}` };
    };

    const buildNullComparison = (
      lhs: string,
      operator: string,
      value: any,
    ): string | null => {
      if (value === null || value === undefined) {
        if (operator === "=") return `${lhs} IS NULL`;
        if (operator === "!=") return `${lhs} IS NOT NULL`;
        return `${lhs} ${operator} NULL`;
      }
      return null;
    };

    const buildExpr = (node: _QueryComponent): string => {
      if (node.type === "comparison") {
        const nodeValue = this._unwrapUniqueValue(node.value);
        let lhs: string;

        if (node.property.includes(".")) {
          const { base, path } = safeJsonPathFromDotted(node.property);
          const baseIdent = this._quoteIdent(base);
          lhs = `CASE WHEN json_valid(${baseIdent}) THEN json_extract(${baseIdent}, '${path}') ELSE NULL END`;
          if (node.operator === "IN") {
            return buildInClause(lhs, nodeValue);
          }
        } else {
          lhs = this._quoteIdent(node.property);
        }

        if (node.operator === "IN") {
          return buildInClause(lhs, nodeValue);
        }

        const nullComparison = buildNullComparison(
          lhs,
          node.operator,
          nodeValue,
        );
        if (nullComparison) return nullComparison;

        // No more contains/notContains. LIKE is handled here as a normal operator.
        return `${lhs} ${node.operator} ${pushParam(nodeValue)}`;
      }

      // group
      const parts = node.components
        .map(buildExpr)
        .map((s) => s.trim())
        .filter(Boolean);

      if (parts.length === 0) return "";

      if (node.method === "NOT") {
        if (parts.length !== 1) {
          throw new Error(
            `NOT groups must have exactly 1 component, got ${parts.length}`,
          );
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
  async executeQuery<T extends JSONable>(
    query: QueryBuilder<T>,
  ): Promise<Result<Item<T>[]>> {
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
      rawItems = this._db
        .prepare(queryString)
        .all<T & { rnId: string }>(params);
      if (
        rawItems.length > 0 &&
        this._needsTableInfoRefresh(
          rawItems[0] as Record<string, any>,
          tableInfo,
        )
      ) {
        tableInfo = this._getTableInfo(tableName, { force: true });
      }
    } catch (error) {
      return new Result({
        success: false,
        error: new Error("Failed to execute query: " + error),
      });
    }

    const mods = Array.from(query.rn!.mods.entries());
    const items = await Promise.all(
      rawItems.map(async (row) => {
        const rowObj = row as Record<string, any>;
        const { rnId, ...withoutRnId } = rowObj;
        const encryptionMeta: EncryptionMeta = { entries: [] };
        const decoded = await this._decodeRowUsingTableInfo(
          withoutRnId as any,
          tableInfo,
          {
            encryptionMeta,
          },
        );
        const rn = korm.rn(
          `[rn]:${query.rn!.namespace!}:${query.rn!.kind!}:${rnId}`,
        );
        for (const [key, value] of mods) {
          rn.mod(key, value);
        }
        const item = new Item<T>(query.item.pool, decoded as T, rn);
        if (encryptionMeta.entries.length > 0) {
          setEncryptionMeta(item, encryptionMeta);
        }
        return item;
      }),
    );

    return new Result({ success: true, data: items });
  }

  /** @inheritdoc */
  async ensureTables(
    item:
      | Item<JSONable>
      | FloatingItem<JSONable>
      | UncommittedItem<JSONable>,
    destructive: boolean = false,
  ): Promise<string> {
    const rawTableName = `__items__${item.rn!.namespace!}__${item.rn!.kind!}`;
    const tableName = this._quoteIdent(rawTableName);
    const itemData = (item.data ?? {}) as Record<string, JSONable>;
    let schemaChanged = false;

    const exists = this._db
      .prepare(
        `
            SELECT EXISTS(
                SELECT 1
                FROM sqlite_master
                WHERE type='table' AND name=?
            ) AS e;
        `,
      )
      .get<{ e: 0 | 1 }>(rawTableName);

    if (!exists || exists.e === 0) {
      let createString = `CREATE TABLE IF NOT EXISTS ${tableName} ( "rnId" ID_TEXT PRIMARY KEY, `;
      const uniqueColumns: string[] = [];

      for (const key in itemData) {
        const columnName = this._quoteIdent(key);
        const v = itemData[key];
        const wantsUnique = this._isUniqueValue(v);

        const t = this._inferSqliteTypeFromValue(v);
        if (t === "TEXT") createString += `${columnName} TEXT, `;
        else if (t === "INTEGER") createString += `${columnName} INTEGER, `;
        else if (t === "REAL") createString += `${columnName} REAL, `;
        else if (t === "BOOLEAN")
          createString += `${columnName} BOOLEAN CHECK( ${columnName} IN (0, 1) ), `;
        else if (t === SQLITE_JSON_TYPE)
          createString += `${columnName} ${SQLITE_JSON_TYPE}, `;
        else if (t === SQLITE_ENCRYPTED_TYPE)
          createString += `${columnName} ${SQLITE_ENCRYPTED_TYPE}, `;
        else if (t === SQLITE_RN_TYPE)
          createString += `${columnName} ${SQLITE_RN_TYPE}, `;
        else throw new Error("Unsupported inferred type: " + t);

        if (wantsUnique) {
          uniqueColumns.push(key);
          const shadowColumnName = this._quoteIdent(
            this._uniqueShadowColumnName(key),
          );
          createString += `${shadowColumnName} TEXT, `;
        }
      }

      createString = createString.slice(0, -2);
      createString += ` )`;
      this._db.run(createString);
      for (const key of uniqueColumns) {
        this._ensureUniqueShadowIndex(rawTableName, key);
      }
      this._columnKindsCache.delete(rawTableName);
      schemaChanged = true;
      this._tableInfoCache.delete(rawTableName);
      return rawTableName;
    }

    // Table exists: ensure columns
    const columns = this._db
      .prepare(`PRAGMA table_info(${tableName})`)
      .all<{ name: string; type: string }>();

    for (const key in itemData) {
      const rawColumnName = key;
      const columnName = this._quoteIdent(rawColumnName);
      const v = itemData[key];
      const shadowRawColumnName = this._uniqueShadowColumnName(rawColumnName);
      const shadowColumnName = this._quoteIdent(shadowRawColumnName);
      const hasUniqueShadow = columns.some((c) => c.name === shadowRawColumnName);
      const wantsUnique = this._isUniqueValue(v) || hasUniqueShadow;

      // If you ever pass partial objects with undefined, don't attempt to infer/alter.
      if (v === undefined) continue;

      const existingColumnType = (
        columns.find((c) => c.name === rawColumnName)?.type || ""
      ).toUpperCase();

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
      if (
        normalizedExisting === "BOOLEAN" &&
        (normalizedWanted === "INTEGER" || normalizedWanted === "REAL")
      ) {
        if (v === 0 || v === 1) normalizedWanted = "BOOLEAN";
      }
      // When a JSON_TEXT column comes back as a raw string (e.g., during revert), treat it as JSON_TEXT.
      if (
        normalizedExisting === SQLITE_JSON_TYPE &&
        normalizedWanted === "TEXT" &&
        typeof v === "string"
      ) {
        try {
          JSON.parse(v);
          normalizedWanted = SQLITE_JSON_TYPE;
          columnWantedType =
            existingColumnType === SQLITE_ENCRYPTED_TYPE
              ? SQLITE_ENCRYPTED_TYPE
              : SQLITE_JSON_TYPE;
        } catch {
          // leave as TEXT if it's not valid JSON; mismatch will be handled below
        }
      }

      if (!existingColumnType) {
        // Column doesn't exist, add it
        const alterString = `ALTER TABLE ${tableName} ADD COLUMN ${columnName} ${columnWantedType}`;
        this._db.run(alterString);
        columns.push({ name: rawColumnName, type: columnWantedType });
        schemaChanged = true;
      } else if (normalizedExisting !== normalizedWanted) {
        if (!destructive) {
          throw new Error(
            `Rejecting change: Column ${rawColumnName} already exists with type ${existingColumnType} but wanted type ${columnWantedType}. ` +
              `\n\nHint:\nYou have likely changed the shape of one of your types or passed wrongly shaped data.\n` +
              `If you intend to change the shape of a namespace-kind path, run a Tx with .persist(\x1b[36m{... destructive: true }\x1b[0m). This will ` +
              `\x1b[31mDESTROY ALL DATA\x1b[0m in the affected column(s). To avoid this, use a new namespace-kind path.`,
          );
        }

        console.log(
          `Changing column ${rawColumnName} from ${existingColumnType} to ${columnWantedType}. This will delete all data in this column.`,
        );

        const dropString = `ALTER TABLE ${tableName} DROP COLUMN ${columnName}`;
        this._db.run(dropString);

        const addString = `ALTER TABLE ${tableName} ADD COLUMN ${columnName} ${columnWantedType}`;
        this._db.run(addString);

        if (hasUniqueShadow) {
          this._db.run(`ALTER TABLE ${tableName} DROP COLUMN ${shadowColumnName}`);
          this._db.run(`ALTER TABLE ${tableName} ADD COLUMN ${shadowColumnName} TEXT`);
        }

        console.log(
          "Dropped and added column",
          rawColumnName,
          "with type",
          columnWantedType,
        );
        schemaChanged = true;
      }

      if (wantsUnique) {
        if (!hasUniqueShadow) {
          this._db.run(`ALTER TABLE ${tableName} ADD COLUMN ${shadowColumnName} TEXT`);
          columns.push({ name: shadowRawColumnName, type: "TEXT" });
          schemaChanged = true;
        }
        this._ensureUniqueShadowIndex(rawTableName, rawColumnName);
      }
    }

    this._columnKindsCache.delete(rawTableName);
    if (schemaChanged) {
      this._tableInfoCache.delete(rawTableName);
    }
    return rawTableName;
  }

  /** @inheritdoc */
  async getColumnKinds(
    namespace: string,
    kind: string,
  ): Promise<Map<string, ColumnKind>> {
    const rawTableName = `__items__${namespace}__${kind}`;
    const cached = this._columnKindsCache.get(rawTableName);
    if (cached) return new Map(cached);
    const tableInfo = this._getTableInfo(rawTableName);
    const map = new Map<string, ColumnKind>();
    if (tableInfo.length === 0) return map;
    for (const col of tableInfo) {
      if (this._isUniqueShadowColumn(col.name)) continue;
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
