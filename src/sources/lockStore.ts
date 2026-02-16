import crypto from "node:crypto";
import type { SourceLayer } from "./sourceLayer";
import { SqliteLayer } from "./layers/sqlite";
import { PgLayer } from "./layers/pg";
import { MysqlLayer } from "./layers/mysql";

/**
 * Shared lock configuration for `korm.pool().withLocks(...)`.
 * Enables cross-process coordination via a lock table.
 */
export type LockMode = {
  /** Layer identifier used to persist locks. */
  layerIdent: string;
  /** Lock expiration in milliseconds. */
  ttlMs?: number;
  /** Retry delay in milliseconds when waiting on a lock. */
  retryMs?: number;
  /** Refresh interval in milliseconds for held locks. */
  refreshMs?: number;
  /** Optional owner identifier for this process. */
  ownerId?: string;
};

/**
 * Storage backend for shared locks.
 * Provided by korm; implement if you are extending lock storage.
 */
export type LockStore = {
  ensure(): Promise<void>;
  tryAcquire(lockId: string, ownerId: string, ttlMs: number): Promise<boolean>;
  refresh(lockId: string, ownerId: string, ttlMs: number): Promise<boolean>;
  release(lockId: string, ownerId: string): Promise<void>;
};

const LOCK_TABLE = "__korm_locks__";
const MYSQL_LOCK_ID_LENGTH = 512;

function normalizeLockId(raw: string): { lockId: string; raw: string } {
  if (raw.length <= MYSQL_LOCK_ID_LENGTH) {
    return { lockId: raw, raw };
  }
  const hashed = crypto.createHash("sha256").update(raw).digest("hex");
  return { lockId: hashed, raw };
}

/**
 * Lock store backed by a korm source layer.
 * Created automatically when you enable `withLocks(...)`.
 */
export class LayerLockStore implements LockStore {
  private _ensured: Promise<void> | undefined;
  private _layer: SourceLayer;

  constructor(layer: SourceLayer) {
    this._layer = layer;
  }

  async ensure(): Promise<void> {
    if (this._ensured) {
      return this._ensured;
    }
    this._ensured = this._ensureLocked();
    return this._ensured;
  }

  private async _ensureLocked(): Promise<void> {
    if (this._layer.type === "sqlite") {
      const layer = this._layer as SqliteLayer;
      layer._db.run(
        `CREATE TABLE IF NOT EXISTS "${LOCK_TABLE}" (
                    "lock_id" TEXT PRIMARY KEY,
                    "owner" TEXT NOT NULL,
                    "expires_at" INTEGER NOT NULL,
                    "created_at" INTEGER NOT NULL,
                    "updated_at" INTEGER NOT NULL
                )`,
      );
      return;
    }
    if (this._layer.type === "pg") {
      const layer = this._layer as PgLayer;
      await layer._db.unsafe(
        `CREATE TABLE IF NOT EXISTS "${LOCK_TABLE}" (
                    "lock_id" TEXT PRIMARY KEY,
                    "owner" TEXT NOT NULL,
                    "expires_at" BIGINT NOT NULL,
                    "created_at" BIGINT NOT NULL,
                    "updated_at" BIGINT NOT NULL
                )`,
      );
      return;
    }
    if (this._layer.type === "mysql") {
      const layer = this._layer as MysqlLayer;
      await layer._pool.query(
        `CREATE TABLE IF NOT EXISTS \`${LOCK_TABLE}\` (
                    \`lock_id\` VARCHAR(${MYSQL_LOCK_ID_LENGTH}) PRIMARY KEY,
                    \`owner\` VARCHAR(255) NOT NULL,
                    \`expires_at\` BIGINT NOT NULL,
                    \`created_at\` BIGINT NOT NULL,
                    \`updated_at\` BIGINT NOT NULL,
                    INDEX \`idx_expires_at\` (\`expires_at\`)
                )`,
      );
      return;
    }
    throw new Error(
      `Lock store does not support layer type "${this._layer.type}".`,
    );
  }

  async tryAcquire(
    lockId: string,
    ownerId: string,
    ttlMs: number,
  ): Promise<boolean> {
    await this.ensure();
    const now = Date.now();
    const expiresAt = now + ttlMs;
    const normalized = normalizeLockId(lockId);

    if (this._layer.type === "sqlite") {
      const layer = this._layer as SqliteLayer;
      const updateResult = layer._db.run(
        `UPDATE "${LOCK_TABLE}"
                 SET "owner" = ?, "expires_at" = ?, "updated_at" = ?
                 WHERE "lock_id" = ? AND ("expires_at" <= ? OR "owner" = ?)`,
        [ownerId, expiresAt, now, normalized.lockId, now, ownerId],
      ) as { changes?: number };
      if ((updateResult?.changes ?? 0) > 0) return true;
      try {
        const insertResult = layer._db.run(
          `INSERT INTO "${LOCK_TABLE}" ("lock_id", "owner", "expires_at", "created_at", "updated_at")
                     VALUES (?, ?, ?, ?, ?)`,
          [normalized.lockId, ownerId, expiresAt, now, now],
        ) as { changes?: number };
        return (insertResult?.changes ?? 0) > 0;
      } catch {
        return false;
      }
    }

    if (this._layer.type === "pg") {
      const layer = this._layer as PgLayer;
      const updated = await layer._db.unsafe(
        `UPDATE "${LOCK_TABLE}"
                 SET "owner" = $1, "expires_at" = $2, "updated_at" = $3
                 WHERE "lock_id" = $4 AND ("expires_at" <= $5 OR "owner" = $1)
                 RETURNING "lock_id"`,
        [ownerId, expiresAt, now, normalized.lockId, now],
      ) as Array<{ lock_id: string }>;
      if (updated.length > 0) return true;
      try {
        await layer._db.unsafe(
          `INSERT INTO "${LOCK_TABLE}" ("lock_id", "owner", "expires_at", "created_at", "updated_at")
                     VALUES ($1, $2, $3, $4, $5)`,
          [normalized.lockId, ownerId, expiresAt, now, now],
        );
        return true;
      } catch {
        return false;
      }
    }

    if (this._layer.type === "mysql") {
      const layer = this._layer as MysqlLayer;
      const [updateResult] = await layer._pool.query(
        `UPDATE \`${LOCK_TABLE}\`
                 SET \`owner\` = ?, \`expires_at\` = ?, \`updated_at\` = ?
                 WHERE \`lock_id\` = ? AND (\`expires_at\` <= ? OR \`owner\` = ?)`,
        [ownerId, expiresAt, now, normalized.lockId, now, ownerId],
      );
      if ((updateResult as { affectedRows?: number }).affectedRows) return true;
      try {
        const [insertResult] = await layer._pool.query(
          `INSERT INTO \`${LOCK_TABLE}\` (\`lock_id\`, \`owner\`, \`expires_at\`, \`created_at\`, \`updated_at\`)
                     VALUES (?, ?, ?, ?, ?)`,
          [normalized.lockId, ownerId, expiresAt, now, now],
        );
        return (insertResult as { affectedRows?: number }).affectedRows === 1;
      } catch {
        return false;
      }
    }

    return false;
  }

  async refresh(
    lockId: string,
    ownerId: string,
    ttlMs: number,
  ): Promise<boolean> {
    await this.ensure();
    const now = Date.now();
    const expiresAt = now + ttlMs;
    const normalized = normalizeLockId(lockId);

    if (this._layer.type === "sqlite") {
      const layer = this._layer as SqliteLayer;
      const result = layer._db.run(
        `UPDATE "${LOCK_TABLE}"
                 SET "expires_at" = ?, "updated_at" = ?
                 WHERE "lock_id" = ? AND "owner" = ?`,
        [expiresAt, now, normalized.lockId, ownerId],
      ) as { changes?: number };
      return (result?.changes ?? 0) > 0;
    }

    if (this._layer.type === "pg") {
      const layer = this._layer as PgLayer;
      const result = await layer._db.unsafe(
        `UPDATE "${LOCK_TABLE}"
                 SET "expires_at" = $1, "updated_at" = $2
                 WHERE "lock_id" = $3 AND "owner" = $4
                 RETURNING "lock_id"`,
        [expiresAt, now, normalized.lockId, ownerId],
      ) as Array<{ lock_id: string }>;
      return result.length > 0;
    }

    if (this._layer.type === "mysql") {
      const layer = this._layer as MysqlLayer;
      const [result] = await layer._pool.query(
        `UPDATE \`${LOCK_TABLE}\`
                 SET \`expires_at\` = ?, \`updated_at\` = ?
                 WHERE \`lock_id\` = ? AND \`owner\` = ?`,
        [expiresAt, now, normalized.lockId, ownerId],
      );
      return (result as { affectedRows?: number }).affectedRows === 1;
    }

    return false;
  }

  async release(lockId: string, ownerId: string): Promise<void> {
    await this.ensure();
    const normalized = normalizeLockId(lockId);

    if (this._layer.type === "sqlite") {
      const layer = this._layer as SqliteLayer;
      layer._db.run(
        `DELETE FROM "${LOCK_TABLE}" WHERE "lock_id" = ? AND "owner" = ?`,
        [normalized.lockId, ownerId],
      );
      return;
    }

    if (this._layer.type === "pg") {
      const layer = this._layer as PgLayer;
      await layer._db.unsafe(
        `DELETE FROM "${LOCK_TABLE}" WHERE "lock_id" = $1 AND "owner" = $2`,
        [normalized.lockId, ownerId],
      );
      return;
    }

    if (this._layer.type === "mysql") {
      const layer = this._layer as MysqlLayer;
      await layer._pool.query(
        `DELETE FROM \`${LOCK_TABLE}\` WHERE \`lock_id\` = ? AND \`owner\` = ?`,
        [normalized.lockId, ownerId],
      );
      return;
    }
  }
}
