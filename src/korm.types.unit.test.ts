import { describe, expect, test } from "bun:test";
import { korm } from "./korm";

describe("korm.types exports", () => {
  test("exposes layer-oriented public type aliases", () => {
    const value = "ok";
    expect(value).toBe("ok");
  });
});

if (false) {
  type SourceLayer = korm.types.SourceLayer;
  type PersistOptions = korm.types.PersistOptions;
  type DbChangeResult<T extends korm.types.JSONable> = korm.types.DbChangeResult<T>;
  type DbDeleteResult = korm.types.DbDeleteResult;
  type ColumnKind = korm.types.ColumnKind;
  type PgConnectionInput = korm.types.PgConnectionInput;
  type PgConnectionOptions = korm.types.PgConnectionOptions;
  type MysqlConnectionInput = korm.types.MysqlConnectionInput;
  type MysqlConnectionOptions = korm.types.MysqlConnectionOptions;

  const pgUrl: PgConnectionInput = "postgres://localhost:5432/app";
  const pgOpts: PgConnectionOptions = {
    host: "localhost",
    port: 5432,
    database: "app",
    username: "app",
    password: "secret",
  };
  const myUrl: MysqlConnectionInput = "mysql://root:pw@localhost:3306/app";
  const myOpts: MysqlConnectionInput = { host: "localhost", database: "app" };
  const myTypedOpts: MysqlConnectionOptions = {
    host: "localhost",
    database: "app",
  };

  const acceptsSourceLayer = (_layer: SourceLayer): void => {};
  const acceptsPersistOptions = (_options?: PersistOptions): void => {};
  const acceptsDeleteResult = (_result: DbDeleteResult): void => {};
  const acceptsChangeResult = (
    _result: DbChangeResult<{ value: string }>,
  ): void => {};
  const acceptsColumnKind = (_kind: ColumnKind): void => {};

  void acceptsSourceLayer;
  void acceptsPersistOptions;
  void acceptsDeleteResult;
  void acceptsChangeResult;
  void acceptsColumnKind;
  void pgUrl;
  void pgOpts;
  void myUrl;
  void myOpts;
  void myTypedOpts;
}
