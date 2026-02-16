import { describe, expect, test } from "bun:test";
import type { PgConnectionInput, PgConnectionOptions } from "./pgClient";

describe("pg client input typing", () => {
  test("accepts a connection URL input", () => {
    const input: PgConnectionInput = "postgres://localhost:5432/app";
    expect(typeof input).toBe("string");
  });
});

if (false) {
  const options: PgConnectionOptions = {
    host: "localhost",
    port: 5432,
    database: "app",
    username: "app",
    password: "secret",
  };
  const input: PgConnectionInput = options;

  const host: string | string[] | undefined = options.host;
  const port: number | number[] | undefined = options.port;
  const database: string | undefined = options.database;
  const username: string | undefined = options.username;
  const password: string | (() => string | Promise<string>) | undefined =
    options.password;

  void input;
  void host;
  void port;
  void database;
  void username;
  void password;
}
