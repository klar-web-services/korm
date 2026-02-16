import { FloatingItem, UncommittedItem, Item } from "../core/item";
import { Result } from "@fkws/klonk-result";
import { QueryBuilder } from "../core/query";
import type { JSONable } from "../korm";
import type { RN } from "../core/rn";
import type { LayerPool } from "./layerPool";
import type { ColumnKind } from "../core/columnKind";
import type {
  BackupContext,
  BackupRestoreOptions,
  BackupRestorePayload,
} from "./backups";

/** Options for insert/update/delete operations. */
export type PersistOptions = {
  /** Allow destructive schema changes when item shapes evolve. */
  destructive?: boolean;
};

/**
 * Storage layer interface for SQL backends.
 * Implementers handle schema creation, queries, and CRUD.
 */
export interface SourceLayer {
  /** Insert a new item and return a change result. */
  insertItem<T extends JSONable>(
    item: FloatingItem<T>,
    options?: PersistOptions,
  ): Promise<DbChangeResult<T>>;
  /** Update an existing item and return a change result. */
  updateItem<T extends JSONable>(
    item: UncommittedItem<T>,
    options?: PersistOptions,
  ): Promise<DbChangeResult<T>>;
  /** Read raw data for an RN without resolving references. */
  readItemRaw<T extends JSONable>(rn: RN): Promise<T | undefined>;
  /** Delete an item by RN. */
  deleteItem(rn: RN, options?: PersistOptions): Promise<DbDeleteResult>;
  /** Execute a query and return Items. */
  executeQuery<T extends JSONable>(
    query: QueryBuilder<T>,
  ): Promise<Result<Item<T>[]>>;
  /** Ensure tables/columns exist for the item shape. */
  ensureTables(
    item:
      | Item<JSONable>
      | FloatingItem<JSONable>
      | UncommittedItem<JSONable>,
    destructive?: boolean,
  ): Promise<string>;
  /** Return column kinds for the namespace/kind table. */
  getColumnKinds(
    namespace: string,
    kind: string,
  ): Promise<Map<string, ColumnKind>>;
  /** Optional layer-specific backup hook used by BackMan. */
  backup?: (context: BackupContext) => Promise<void> | void;
  /** Optional layer-specific restore hook used by BackMan. */
  restore?: (
    payload: BackupRestorePayload,
    options?: BackupRestoreOptions,
  ) => Promise<void> | void;
  close?: () => Promise<void> | void;

  readonly type: "sqlite" | "pg" | "mysql";
  readonly identifier: string;
}

/** Change descriptor used by WAL and revert handlers. */
export type Change =
  | {
      type: "insert";
      rn: RN;
      pool: LayerPool;
    }
  | {
      type: "update";
      oldData: JSONable;
      rn: RN;
      pool: LayerPool;
    };

/** Result of insert/update operations with revert support. */
export type DbChangeResult<T extends JSONable> =
  | {
      success: true;
      type: "update";
      oldData: T;
      revert: RevertFunction;
      item: Item<T>;
    }
  | {
      success: true;
      type: "insert";
      revert: RevertFunction;
      item: Item<T>;
    }
  | {
      revert: RevertFunction;
      success: false;
      error: Error;
      type: "insert";
      item: FloatingItem<T>;
    }
  | {
      revert: RevertFunction;
      success: false;
      error: Error;
      oldData: T;
      type: "update";
      item: UncommittedItem<T>;
    };

/** Function that reverts a previously applied change. */
export type RevertFunction = () => void;

/** Result for delete operations. */
export type DbDeleteResult =
  | {
      success: true;
    }
  | {
      success: false;
      error: Error;
    };
