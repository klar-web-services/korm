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
  createPgClient,
  type PgClient,
  type PgConnectionInput,
} from "../../runtime/pgClient";
import { decrypt, Encrypt } from "../../security/encryption";
import { cloneJson, type PathKey } from "../../core/resolveMeta";
import {
  setEncryptionMeta,
  type EncryptionMeta,
  type EncryptedPayload,
} from "../../core/encryptionMeta";
import type { ColumnKind } from "../../core/columnKind";
import { RN } from "../../core/rn";
import { safeAssign } from "../../core/safeObject";
import {
  BACKUP_EXTENSION,
  buildBackupHeaderEvent,
  buildBackupRn,
  streamBackupEvents,
  type BackupContext,
  type BackupRestoreOptions,
  type BackupRestorePayload,
} from "../backups";

const PG_RN_DOMAIN = "korm_rn_ref_text";
const PG_ENCRYPTED_DOMAIN = "korm_encrypted_json";
const PG_META_TABLE = "__korm_meta__";

type ColumnInfo = {
  name: string;
  dataType: string;
  udtName?: string;
  domainName?: string;
};

/**
 * Postgres-backed source layer.
 * Create via `korm.layers.pg(...)` and add to a pool.
 */
export class PgLayer implements SourceLayer {
  public _db: PgClient;
  public readonly type: "pg" = "pg";
  public readonly identifier: string;
  private _connectionInput: PgConnectionInput;
  private _domainsEnsured: boolean = false;
  private _domainsAvailable: boolean = true;
  private _columnKindsCache: Map<string, Map<string, ColumnKind>> = new Map();
  private _tableInfoCache: Map<string, ColumnInfo[]> = new Map();
  private _metaEnsured: boolean = false;
  private _schemaVersion: number = 0;

  /** Create a Postgres layer from a connection string or native options. */
  constructor(connectionStringOrOptions: PgConnectionInput) {
    this._db = createPgClient(connectionStringOrOptions);
    this.identifier = this._deriveIdentifier(connectionStringOrOptions);
    this._connectionInput = connectionStringOrOptions;
  }

  /** Close the underlying Postgres connection. */
  async close(): Promise<void> {
    await this._db.end({ timeout: 1 });
  }

  /** @internal */
  async backup(context: BackupContext): Promise<void> {
    const now = context.now ?? new Date();
    const tables = (await this._db.unsafe(
      `SELECT table_schema, table_name
             FROM information_schema.tables
             WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
             AND (table_name LIKE '__items__%' OR table_name = '__korm_meta__' OR table_name = '__korm_pool__')`,
    )) as Array<{ table_schema: string; table_name: string }>;
    const rn = buildBackupRn(
      context.depotIdent,
      context.layerIdent,
      now,
      BACKUP_EXTENSION,
    );
    const events = async function* (
      self: PgLayer,
    ): AsyncGenerator<
      | ReturnType<typeof buildBackupHeaderEvent>
      | { t: "table"; name: string }
      | { t: "row"; table: string; row: Record<string, unknown> }
      | { t: "endTable"; name: string }
      | { t: "end" }
    > {
      yield buildBackupHeaderEvent("pg", context.layerIdent, now);
      for (const row of tables) {
        const safeSchema = self._quoteIdent(row.table_schema);
        const safeTable = self._quoteIdent(row.table_name);
        const safeName = `${safeSchema}.${safeTable}`;
        const orderBy = row.table_name.startsWith("__items__")
          ? self._quoteIdent("rnId")
          : undefined;
        yield { t: "table", name: row.table_name };
        for await (const item of self._iterateBackupRows(safeName, orderBy)) {
          yield {
            t: "row",
            table: row.table_name,
            row: item,
          };
        }
        yield { t: "endTable", name: row.table_name };
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
    if (payload.header.layerType !== "pg") {
      throw new Error(
        `Backup payload layer type "${payload.header.layerType}" does not match pg.`,
      );
    }
    await this._ensureDomains();
    const mode = options?.mode ?? "replace";
    const poolTable = "__korm_pool__";
    const metaTable = PG_META_TABLE;
    let schemaChanged = false;
    type TableState = {
      rawName: string;
      safeName: string;
      created: boolean;
      columnTypes: Map<string, string>;
      pendingColumns: Set<string>;
      isItems: boolean;
    };

    const ensurePoolTable = async (state?: TableState): Promise<void> => {
      await this._unsafe(
        `CREATE TABLE IF NOT EXISTS "${poolTable}" (
                    "id" TEXT PRIMARY KEY,
                    "config" JSONB NOT NULL,
                    "created_at" BIGINT NOT NULL,
                    "updated_at" BIGINT NOT NULL
                )`,
      );
      schemaChanged = true;
      if (state) {
        state.created = true;
        state.columnTypes.set("id", "TEXT");
        state.columnTypes.set("config", "JSONB");
        state.columnTypes.set("created_at", "BIGINT");
        state.columnTypes.set("updated_at", "BIGINT");
      }
    };

    const ensureMetaTable = async (state?: TableState): Promise<void> => {
      await this._ensureMetaTable();
      schemaChanged = true;
      if (state) {
        state.created = true;
        state.columnTypes.set("table_name", "TEXT");
        state.columnTypes.set("column_name", "TEXT");
        state.columnTypes.set("kind", "TEXT");
      }
    };

    const createTable = async (state: TableState): Promise<void> => {
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
      await this._unsafe(
        `CREATE TABLE IF NOT EXISTS ${state.safeName} (${columnDefs.join(", ")})`,
      );
      state.created = true;
      schemaChanged = true;
    };

    const addColumn = async (
      state: TableState,
      key: string,
      type: string,
    ): Promise<void> => {
      if (state.columnTypes.has(key)) return;
      const columnName = this._quoteIdent(key);
      if (state.created) {
        await this._unsafe(
          `ALTER TABLE ${state.safeName} ADD COLUMN ${columnName} ${type}`,
        );
        schemaChanged = true;
      }
      state.columnTypes.set(key, type);
    };

    const ensureRnId = async (state: TableState): Promise<void> => {
      if (!state.isItems || state.columnTypes.has("rnId")) return;
      await addColumn(state, "rnId", "TEXT");
    };

    const initTableState = async (rawName: string): Promise<TableState> => {
      const safeName = this._quoteIdent(rawName);
      const tableInfo = await this._getTableInfo(rawName, { force: true });
      const columnTypes = new Map<string, string>(
        tableInfo.map((col) => [
          col.name,
          (col.domainName || col.dataType || "TEXT").toUpperCase(),
        ]),
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
        await ensurePoolTable(state);
      } else if (rawName === metaTable && !state.created) {
        await ensureMetaTable(state);
      }

      await ensureRnId(state);

      if (mode === "replace" && state.created) {
        await this._unsafe(`DELETE FROM ${safeName}`);
      }

      return state;
    };

    const finalizeTable = async (state: TableState | null): Promise<void> => {
      if (!state) return;
      if (!state.created) {
        if (state.isItems && !state.columnTypes.has("rnId")) {
          state.columnTypes.set("rnId", "TEXT");
        }
        await createTable(state);
      }
      if (state.pendingColumns.size > 0) {
        for (const key of state.pendingColumns) {
          await addColumn(state, key, "TEXT");
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
        await finalizeTable(current);
        current = await initTableState(event.name);
        continue;
      }
      if (event.t === "endTable") {
        if (current && event.name !== current.rawName) {
          throw new Error(
            `Backup endTable event "${event.name}" does not match "${current.rawName}".`,
          );
        }
        await finalizeTable(current);
        current = null;
        continue;
      }
      if (event.t === "end") {
        await finalizeTable(current);
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
          const type = this._inferBackupPgTypeFromValue(value);
          if (!state.created) {
            state.columnTypes.set(key, type);
          } else {
            await addColumn(state, key, type);
          }
        }

        if (!state.created) {
          await createTable(state);
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
        const placeholders = columns.map((_, idx) => `$${idx + 1}`).join(", ");
        let insertString = `INSERT INTO ${state.safeName} (${columnList}) VALUES (${placeholders})`;
        if (mode === "merge") {
          insertString += " ON CONFLICT DO NOTHING";
        }
        const values = columns.map((key) =>
          this._encodePgValue((row as any)[key]),
        );
        await this._unsafe(insertString, values);
      }
    }

    if (schemaChanged) {
      this._bumpSchemaVersion();
      this._tableInfoCache.clear();
    }
    this._columnKindsCache.clear();
  }

  private async *_iterateBackupRows(
    safeName: string,
    orderBy?: string,
  ): AsyncGenerator<Record<string, unknown>> {
    const chunkSize = 1000;
    let offset = 0;
    const orderClause = orderBy ? ` ORDER BY ${orderBy}` : "";
    while (true) {
      const rows = await this._unsafe<Record<string, unknown>[]>(
        `SELECT * FROM ${safeName}${orderClause} LIMIT ${chunkSize} OFFSET ${offset}`,
      );
      if (!rows || rows.length === 0) break;
      for (const row of rows) {
        yield row;
      }
      if (rows.length < chunkSize) break;
      offset += rows.length;
    }
  }

  /** @internal */
  getPoolConfig(): { type: "pg"; mode: "url" | "options"; value: unknown } {
    if (typeof this._connectionInput === "string") {
      return { type: "pg", mode: "url", value: this._connectionInput };
    }
    return { type: "pg", mode: "options", value: this._connectionInput };
  }

  private _deriveIdentifier(
    connectionStringOrOptions: PgConnectionInput,
  ): string {
    const fallback = "pg@localhost";

    if (typeof connectionStringOrOptions === "string") {
      try {
        const url = new URL(connectionStringOrOptions);
        const db = (url.pathname || "").replace(/^\//, "") || "pg";
        const host = url.hostname || "localhost";
        return `${db}@${host}`;
      } catch {
        return fallback;
      }
    }

    const opts = connectionStringOrOptions as any;
    const db = opts.database || opts.db || "pg";
    const host = opts.hostname || opts.host || "localhost";
    return `${db}@${host}`;
  }

  private _quoteIdent(name: string): string {
    if (!name || name.includes("\u0000"))
      throw new Error("Invalid identifier: " + name);
    if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(name)) {
      throw new Error(`Unsafe identifier: ${name}`);
    }
    return `"${name.replace(/"/g, '""')}"`;
  }

  private _withSchemaVersion(query: string): string {
    return `${query} /* korm_schema:${this._schemaVersion} */`;
  }

  private _unsafe<T = any>(query: string, values?: any[]): Promise<T> {
    const stamped = this._withSchemaVersion(query);
    if (values === undefined) {
      return this._db.unsafe<T>(stamped);
    }
    return this._db.unsafe<T>(stamped, values);
  }

  private _bumpSchemaVersion(): void {
    this._schemaVersion += 1;
  }

  private async _clearPlanCache(): Promise<void> {
    try {
      await this._db.unsafe("DISCARD PLANS");
      return;
    } catch {}
    try {
      await this._db.unsafe("DEALLOCATE ALL");
    } catch {}
  }

  private async _ensureDomains(): Promise<void> {
    if (this._domainsEnsured) return;
    this._domainsEnsured = true;
    try {
      const existing = await this._unsafe<any[]>(
        `SELECT typname
                 FROM pg_type
                 WHERE typtype = 'd' AND typname = ANY($1)`,
        [[PG_RN_DOMAIN, PG_ENCRYPTED_DOMAIN]],
      );
      const existingSet = new Set(
        (existing || []).map((row) => String(row.typname)),
      );

      if (!existingSet.has(PG_RN_DOMAIN)) {
        const rnDomain = this._quoteIdent(PG_RN_DOMAIN);
        await this._unsafe(`CREATE DOMAIN ${rnDomain} AS TEXT`);
      }
      if (!existingSet.has(PG_ENCRYPTED_DOMAIN)) {
        const encryptedDomain = this._quoteIdent(PG_ENCRYPTED_DOMAIN);
        await this._unsafe(`CREATE DOMAIN ${encryptedDomain} AS JSONB`);
      }
    } catch {
      this._domainsAvailable = false;
      this._columnKindsCache.clear();
    }
  }

  private async _ensureMetaTable(): Promise<void> {
    if (this._metaEnsured) return;
    const safe = this._quoteIdent(PG_META_TABLE);
    await this._unsafe(
      `CREATE TABLE IF NOT EXISTS ${safe} (
                table_name TEXT NOT NULL,
                column_name TEXT NOT NULL,
                kind TEXT NOT NULL,
                PRIMARY KEY (table_name, column_name)
            )`,
    );
    this._metaEnsured = true;
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

  private _inferColumnKindFromValue(value: any): ColumnKind {
    if (this._isEncryptValue(value) || this._isEncryptedPayload(value))
      return "encrypted";
    if (this._isRnValue(value) || this._looksLikeRnString(value)) return "rn";
    if (value !== null && typeof value === "object") return "json";
    return "scalar";
  }

  private async _setColumnKind(
    tableName: string,
    columnName: string,
    kind: ColumnKind,
  ): Promise<void> {
    await this._ensureMetaTable();
    const safe = this._quoteIdent(PG_META_TABLE);
    if (kind === "rn" || kind === "encrypted") {
      await this._unsafe(
        `INSERT INTO ${safe} (table_name, column_name, kind)
                 VALUES ($1, $2, $3)
                 ON CONFLICT (table_name, column_name)
                 DO UPDATE SET kind = EXCLUDED.kind`,
        [tableName, columnName, kind],
      );
    } else {
      await this._unsafe(
        `DELETE FROM ${safe} WHERE table_name = $1 AND column_name = $2`,
        [tableName, columnName],
      );
    }
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

  private _inferPgTypeFromValue(v: any): string {
    if (v === null) return "TEXT";
    if (v === undefined) return "TEXT";
    if (this._isRnValue(v) || this._looksLikeRnString(v)) {
      return this._domainsAvailable ? PG_RN_DOMAIN : "TEXT";
    }
    if (this._isEncryptValue(v) || this._isEncryptedPayload(v)) {
      return this._domainsAvailable ? PG_ENCRYPTED_DOMAIN : "JSONB";
    }

    switch (typeof v) {
      case "string":
        return "TEXT";
      case "boolean":
        return "BOOLEAN";
      case "number":
        return Number.isInteger(v) ? "INTEGER" : "DOUBLE PRECISION";
      case "object":
        return "JSONB";
      default:
        throw new Error("Unsupported type: " + typeof v);
    }
  }

  private _inferBackupPgTypeFromValue(value: unknown): string {
    if (value === null || value === undefined) return "TEXT";
    if (typeof value === "string") {
      try {
        const parsed = JSON.parse(value);
        if (parsed && typeof parsed === "object") {
          if (this._isEncryptedPayload(parsed)) {
            return this._domainsAvailable ? PG_ENCRYPTED_DOMAIN : "JSONB";
          }
          return "JSONB";
        }
      } catch {
        // fall through
      }
    }
    return this._inferPgTypeFromValue(value);
  }

  private _encodePgValue(v: any): any {
    if (this._isRnValue(v)) return v.value();
    const normalized = this._normalizeEncryptedValue(v);
    if (normalized === undefined) return null;
    if (normalized === null) return null;
    if (typeof normalized === "object") return normalized;
    return normalized;
  }

  private async _getTableInfo(
    rawTableName: string,
    opts: { force?: boolean } = {},
  ): Promise<ColumnInfo[]> {
    const cached = this._tableInfoCache.get(rawTableName);
    if (cached && !opts.force) return cached;
    const result = await this._unsafe(
      `SELECT column_name as name, data_type as data_type, udt_name, domain_name
             FROM information_schema.columns
             WHERE table_name = $1`,
      [rawTableName],
    );
    const info = (result as any[]).map((row) => ({
      name: String(row.name ?? row.column_name ?? ""),
      dataType: String(row.data_type ?? row.dataType ?? ""),
      udtName: row.udt_name ?? row.udtName ?? undefined,
      domainName: row.domain_name ?? row.domainName ?? undefined,
    }));
    this._tableInfoCache.set(rawTableName, info);
    return info;
  }

  private _needsTableInfoRefresh(
    row: Record<string, any>,
    tableInfo: ColumnInfo[],
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
    tableInfo: ColumnInfo[],
    opts: { decryptEncrypted?: boolean; encryptionMeta?: EncryptionMeta } = {},
  ): Promise<T> {
    const typeByName = new Map(
      tableInfo.map((c) => [
        c.name,
        {
          dataType: (c.dataType || "").toUpperCase(),
          domainName: (c.domainName || "").toLowerCase(),
          udtName: (c.udtName || "").toLowerCase(),
        },
      ]),
    );
    const out: any = {};
    for (const [key, value] of Object.entries(row)) {
      safeAssign(out, key, value);
    }
    const decryptEncrypted = opts.decryptEncrypted ?? true;

    for (const [k, v] of Object.entries(out)) {
      const info = typeByName.get(k);
      if (!info) continue;
      const isRnDomain =
        info.domainName === PG_RN_DOMAIN || info.udtName === PG_RN_DOMAIN;
      const isEncryptedDomain =
        info.domainName === PG_ENCRYPTED_DOMAIN ||
        info.udtName === PG_ENCRYPTED_DOMAIN;
      const isJson =
        info.dataType === "JSONB" ||
        info.dataType === "JSON" ||
        isEncryptedDomain;

      if (isRnDomain) {
        if (typeof v === "string" && v.startsWith("[rn]")) {
          const parsed = RN.create(v);
          if (parsed.isOk()) {
            safeAssign(out, k, parsed.unwrap());
          }
        }
      } else if (isJson) {
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
      }
    }

    return out as T;
  }

  private _revertFactory(change: Change, destructive: boolean): RevertFunction {
    switch (change.type) {
      case "insert":
        return () => {
          const rawTableName = `__items__${change.rn.namespace!}__${change.rn.kind!}`;
          const safeTableName = this._quoteIdent(rawTableName);
          const deleteString = `DELETE FROM ${safeTableName} WHERE "rnId" = $1`;
          void this._unsafe(deleteString, [change.rn.id!]);
        };
      case "update":
        return () => {
          const item = new UncommittedItem(
            change.pool,
            change.rn,
            change.oldData,
          );
          void this.updateItem(item, { destructive });
        };
    }
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

    const keys = Object.keys(item.data as Record<string, any>);

    let insertString = `INSERT INTO ${safeTableName} ( "rnId"`;
    for (const key of keys) {
      insertString += `, ${this._quoteIdent(key)}`;
    }
    insertString += `) VALUES ( $1`;

    for (let i = 0; i < keys.length; i++) {
      insertString += `, $${i + 2}`;
    }
    insertString += ` )`;

    const params = [
      item.rn!.id!,
      ...keys.map((k) => this._encodePgValue((item.data as any)[k])),
    ];

    try {
      await this._unsafe(insertString, params);
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
    let tableInfo = await this._getTableInfo(rawTableName);
    const currentRow = (
      await this._unsafe(`SELECT * FROM ${safeTableName} WHERE "rnId" = $1`, [
        item.rn!.id!,
      ])
    )[0];
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
      tableInfo = await this._getTableInfo(rawTableName, { force: true });
    }
    const { rnId: _rnId, ...currentDataRaw } = currentRow;
    const currentData = await this._decodeRowUsingTableInfo(
      currentDataRaw as any,
      tableInfo,
      { decryptEncrypted: false },
    );

    const data = (item.data ?? {}) as Record<string, any>;
    const keys = Object.keys(data);
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
    let i = 1;
    for (const key of keys) {
      const columnName = this._quoteIdent(key);
      updateString += `${columnName} = $${i++}, `;
    }
    updateString = updateString.slice(0, -2);
    updateString += ` WHERE "rnId" = $${i}`;

    const params = [
      ...keys.map((k) => this._encodePgValue(data[k])),
      item.rn!.id!,
    ];

    try {
      await this._unsafe(updateString, params);
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
        oldData: currentData as T,
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
        oldData: currentData as T,
        item: item,
      };
    }
  }

  /** @inheritdoc */
  async readItemRaw<T extends JSONable>(rn: RN): Promise<T | undefined> {
    const rawTableName = `__items__${rn.namespace!}__${rn.kind!}`;
    let tableInfo = await this._getTableInfo(rawTableName);
    if (tableInfo.length === 0) return undefined;
    const safeTableName = this._quoteIdent(rawTableName);
    const currentRow = (
      await this._unsafe(`SELECT * FROM ${safeTableName} WHERE "rnId" = $1`, [
        rn.id!,
      ])
    )[0];
    if (!currentRow) return undefined;
    if (
      this._needsTableInfoRefresh(currentRow as Record<string, any>, tableInfo)
    ) {
      tableInfo = await this._getTableInfo(rawTableName, { force: true });
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
    const tableInfo = await this._getTableInfo(rawTableName);
    if (tableInfo.length === 0) {
      return {
        success: false,
        error: new Error(this._friendlyDeleteMessage(rn)),
      };
    }
    const safeTableName = this._quoteIdent(rawTableName);
    const existing = await this._unsafe(
      `SELECT 1 FROM ${safeTableName} WHERE "rnId" = $1`,
      [rn.id!],
    );
    if (existing.length === 0) {
      return {
        success: false,
        error: new Error(this._friendlyDeleteMessage(rn)),
      };
    }
    try {
      await this._unsafe(`DELETE FROM ${safeTableName} WHERE "rnId" = $1`, [
        rn.id!,
      ]);
      return { success: true };
    } catch (error) {
      return {
        success: false,
        error: new Error(this._friendlyDeleteMessage(rn, error)),
      };
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
      if (v === null || v === undefined) return null;
      if (this._isRnValue(v)) return v.value();
      if (typeof v === "object") return JSON.stringify(v);
      return v;
    };

    const pushParam = (v: any): string => {
      params.push(normalizeParam(v));
      return `$${params.length}`;
    };

    const normalizeInValues = (
      value: any,
    ): { values: any[]; hasNull: boolean } => {
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
      hasNull: boolean,
    ): string => {
      if (values.length === 0) {
        return hasNull ? `${lhs} IS NULL` : "0=1";
      }
      const inExpr = `${lhs} IN (${values.map(addParam).join(", ")})`;
      if (!hasNull) return inExpr;
      return `(${inExpr} OR ${lhs} IS NULL)`;
    };

    const detectInType = (
      values: any[],
    ): "string" | "number" | "boolean" | "other" => {
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

    const safeJsonPathFromDotted = (
      dotted: string,
    ): { base: string; path: string[] } => {
      const [base, ...rest] = dotted.split(".");
      if (!base || rest.length === 0)
        throw new Error(`Invalid dotted property: ${dotted}`);

      const isSafeSegment = (s: string) => /^[A-Za-z_][A-Za-z0-9_]*$/.test(s);
      for (const seg of rest) {
        if (!isSafeSegment(seg))
          throw new Error(`Unsafe JSON path segment "${seg}" in "${dotted}"`);
      }
      return { base, path: rest };
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

    const buildJsonComparison = (
      jsonText: string,
      operator: string,
      value: any,
    ): string => {
      const nullComparison = buildNullComparison(jsonText, operator, value);
      if (nullComparison) return nullComparison;

      if (operator !== "LIKE" && typeof value === "number") {
        return `(${jsonText})::double precision ${operator} ${pushParam(value)}`;
      }
      if (operator !== "LIKE" && typeof value === "boolean") {
        return `(${jsonText})::boolean ${operator} ${pushParam(value)}`;
      }
      return `${jsonText} ${operator} ${pushParam(value)}`;
    };

    const buildExpr = (node: _QueryComponent): string => {
      if (node.type === "comparison") {
        let lhs: string;

        if (node.property.includes(".")) {
          const { base, path } = safeJsonPathFromDotted(node.property);
          const pathStr = path.join(",");
          const baseIdent = this._quoteIdent(base);
          lhs =
            `CASE ` +
            `WHEN pg_typeof(${baseIdent}) = 'jsonb'::regtype THEN ${baseIdent} #>> '{${pathStr}}' ` +
            `WHEN pg_typeof(${baseIdent}) = 'json'::regtype THEN ${baseIdent} #>> '{${pathStr}}' ` +
            `ELSE NULL END`;
          if (node.operator === "IN") {
            const { values, hasNull } = normalizeInValues(node.value);
            const valueType = detectInType(values);
            if (valueType === "number") {
              return buildInClause(
                `(${lhs})::double precision`,
                values,
                pushParam,
                hasNull,
              );
            }
            if (valueType === "boolean") {
              return buildInClause(
                `(${lhs})::boolean`,
                values,
                pushParam,
                hasNull,
              );
            }
            return buildInClause(lhs, values, pushParam, hasNull);
          }
          return buildJsonComparison(lhs, node.operator, node.value);
        } else {
          lhs = this._quoteIdent(node.property);
        }

        if (node.operator === "IN") {
          const { values, hasNull } = normalizeInValues(node.value);
          return buildInClause(lhs, values, pushParam, hasNull);
        }

        const nullComparison = buildNullComparison(
          lhs,
          node.operator,
          node.value,
        );
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
    query: QueryBuilder<any>,
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
    let tableInfo: ColumnInfo[] = [];

    try {
      tableInfo = await this._getTableInfo(tableName);
      if (tableInfo.length === 0) {
        return new Result({ success: true, data: [] });
      }

      rawItems = await this._unsafe(queryString, params);
      if (
        rawItems.length > 0 &&
        this._needsTableInfoRefresh(
          rawItems[0] as Record<string, any>,
          tableInfo,
        )
      ) {
        tableInfo = await this._getTableInfo(tableName, { force: true });
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

  private _friendlyMessage<T extends JSONable>(
    op: "create" | "update",
    item: FloatingItem<T> | UncommittedItem<T>,
    error: unknown,
  ): string {
    const rnValue = item.rn?.value() ?? "(unknown rn)";
    const baseLayer = `${this.type} source layer '${this.identifier}'`;
    const errorText = String(error);
    if (op === "create" && errorText.toLowerCase().includes("duplicate key")) {
      return `Tried to create item '${rnValue}' which already exists in ${baseLayer}.`;
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

  /** @inheritdoc */
  async ensureTables(
    item: Item<any> | FloatingItem<any> | UncommittedItem<any>,
    destructive: boolean = false,
  ): Promise<string> {
    const rawTableName = `__items__${item.rn!.namespace!}__${item.rn!.kind!}`;
    const tableName = this._quoteIdent(rawTableName);
    await this._ensureDomains();

    const existsRes = await this._unsafe(
      `SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_name = $1
            ) as e`,
      [rawTableName],
    );
    const exists = existsRes[0]?.e || existsRes[0]?.exists;

    if (!exists) {
      let createString = `CREATE TABLE IF NOT EXISTS ${tableName} ( "rnId" TEXT PRIMARY KEY, `;

      for (const key in item.data) {
        const columnName = this._quoteIdent(key);
        const v = (item.data as any)[key];

        const t = this._inferPgTypeFromValue(v);
        if (t === "TEXT") createString += `${columnName} TEXT, `;
        else if (t === "INTEGER") createString += `${columnName} INTEGER, `;
        else if (t === "DOUBLE PRECISION")
          createString += `${columnName} DOUBLE PRECISION, `;
        else if (t === "BOOLEAN") createString += `${columnName} BOOLEAN, `;
        else if (t === "JSONB") createString += `${columnName} JSONB, `;
        else if (t === PG_RN_DOMAIN)
          createString += `${columnName} ${PG_RN_DOMAIN}, `;
        else if (t === PG_ENCRYPTED_DOMAIN)
          createString += `${columnName} ${PG_ENCRYPTED_DOMAIN}, `;
        else throw new Error("Unsupported inferred type: " + t);
      }

      createString = createString.slice(0, -2);
      createString += ` )`;

      await this._unsafe(createString);
      if (!this._domainsAvailable) {
        for (const key in item.data) {
          const v = (item.data as any)[key];
          const kind = this._inferColumnKindFromValue(v);
          await this._setColumnKind(rawTableName, key, kind);
        }
      }
      this._columnKindsCache.delete(rawTableName);
      this._tableInfoCache.delete(rawTableName);
      this._bumpSchemaVersion();
      return rawTableName;
    }

    const columns = await this._getTableInfo(rawTableName);
    let schemaChanged = false;

    for (const key in item.data) {
      const rawColumnName = key;
      const columnName = this._quoteIdent(rawColumnName);
      const v = (item.data as any)[key];

      if (v === undefined) continue;

      const existingColumn = columns.find((c) => c.name === rawColumnName);
      const existingColumnType = (existingColumn?.dataType || "").toUpperCase();
      const existingDomain = (
        existingColumn?.domainName ||
        existingColumn?.udtName ||
        ""
      ).toLowerCase();

      const normalizeType = (type: string, domainName?: string): string => {
        const upper = (type || "").toUpperCase();
        const domain = (domainName || "").toLowerCase();
        if (domain === PG_RN_DOMAIN || upper.toLowerCase() === PG_RN_DOMAIN)
          return "TEXT";
        if (
          domain === PG_ENCRYPTED_DOMAIN ||
          upper.toLowerCase() === PG_ENCRYPTED_DOMAIN
        )
          return "JSONB";
        if (upper.includes("CHAR") || upper.includes("TEXT")) return "TEXT";
        if (upper === "INTEGER") return "INTEGER";
        if (upper.includes("DOUBLE") || upper.includes("REAL"))
          return "DOUBLE PRECISION";
        if (upper.includes("NUMERIC") || upper.includes("DECIMAL"))
          return "DOUBLE PRECISION";
        if (upper === "BOOLEAN") return "BOOLEAN";
        if (upper === "JSONB" || upper === "JSON") return "JSONB";
        return upper;
      };

      let columnWantedType = this._inferPgTypeFromValue(v);
      let normalizedExisting = normalizeType(
        existingColumnType,
        existingDomain,
      );
      let normalizedWanted = normalizeType(columnWantedType);

      if (
        normalizedWanted === "INTEGER" &&
        normalizedExisting === "DOUBLE PRECISION"
      ) {
        normalizedWanted = "DOUBLE PRECISION";
      }
      if (
        normalizedExisting === "JSONB" &&
        normalizedWanted === "TEXT" &&
        typeof v === "string"
      ) {
        try {
          JSON.parse(v);
          normalizedWanted = "JSONB";
          columnWantedType = existingDomain || "JSONB";
        } catch {}
      }

      if (!existingColumnType) {
        const alterString = `ALTER TABLE ${tableName} ADD COLUMN ${columnName} ${columnWantedType}`;
        await this._unsafe(alterString);
        await this._clearPlanCache();
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

        await this._unsafe(
          `ALTER TABLE ${tableName} DROP COLUMN ${columnName}`,
        );
        await this._unsafe(
          `ALTER TABLE ${tableName} ADD COLUMN ${columnName} ${columnWantedType}`,
        );
        await this._clearPlanCache();
        schemaChanged = true;

        console.log(
          "Dropped and added column",
          rawColumnName,
          "with type",
          columnWantedType,
        );
      }
      if (!this._domainsAvailable) {
        const kind = this._inferColumnKindFromValue(v);
        await this._setColumnKind(rawTableName, rawColumnName, kind);
      }
    }

    if (schemaChanged) {
      this._bumpSchemaVersion();
      this._tableInfoCache.delete(rawTableName);
    }
    this._columnKindsCache.delete(rawTableName);
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
    await this._ensureDomains();
    const columns = await this._getTableInfo(rawTableName);
    const map = new Map<string, ColumnKind>();
    if (columns.length === 0) return map;
    for (const col of columns) {
      const dataType = (col.dataType || "").toUpperCase();
      const domain = (col.domainName || col.udtName || "").toLowerCase();
      if (domain === PG_RN_DOMAIN) {
        map.set(col.name, "rn");
      } else if (domain === PG_ENCRYPTED_DOMAIN) {
        map.set(col.name, "encrypted");
      } else if (dataType === "JSONB" || dataType === "JSON") {
        map.set(col.name, "json");
      } else {
        map.set(col.name, "scalar");
      }
    }
    if (!this._domainsAvailable) {
      await this._ensureMetaTable();
      const safe = this._quoteIdent(PG_META_TABLE);
      const rows = await this._unsafe<any[]>(
        `SELECT column_name, kind FROM ${safe} WHERE table_name = $1`,
        [rawTableName],
      );
      for (const row of rows) {
        const columnName = row.column_name ?? row.columnName;
        const kindValue = row.kind;
        if (!columnName || typeof kindValue !== "string") continue;
        if (kindValue === "rn" || kindValue === "encrypted") {
          map.set(String(columnName), kindValue);
        }
      }
    }
    this._columnKindsCache.set(rawTableName, map);
    return new Map(map);
  }
}
