import { SqliteLayer } from "./layers/sqlite";
import { PgLayer } from "./layers/pg";
import { MysqlLayer } from "./layers/mysql";
import type { PoolOptions } from "mysql2/promise";
import type { PgConnectionInput } from "../runtime/pgClient";

/**
 * Source layer factories.
 * Use these with `korm.pool().setLayers(...).open()` or via `korm.layers.*`.
 */
const layers = {
  /** Create a SQLite layer backed by a local file path. */
  sqlite: (path: string): SqliteLayer => new SqliteLayer(path),
  /** Create a Postgres layer from a connection string or native client options. */
  pg: (params: PgConnectionInput): PgLayer => new PgLayer(params),
  /** Create a MySQL layer from a connection string or mysql2 PoolOptions. */
  mysql: (params: string | PoolOptions): MysqlLayer => new MysqlLayer(params),
};

export { layers, SqliteLayer, PgLayer, MysqlLayer };
