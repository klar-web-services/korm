import { describe, expect, mock, test } from "bun:test";
import { createRequire as createNodeRequire } from "node:module";

const uniqueSuffix = (): string =>
  `?test=${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`;
const fallbackRequire = createNodeRequire(import.meta.url);

describe("runtime adapters (node path via module mocks)", () => {
  test("createPgClient uses postgres factory when Bun runtime is disabled", async () => {
    mock.module("./engine", () => ({
      getBunGlobal: () => undefined,
      isBunRuntime: () => false,
      getRuntimeEngine: () => "node",
    }));
    mock.module("node:module", () => ({
      createRequire: () => {
        return (id: string) => {
          if (id !== "postgres") return fallbackRequire(id);
          return (input?: unknown) => ({
            unsafe: async (query: string, values?: unknown[]) => ({
              query,
              values,
              input,
            }),
            end: async (options?: unknown) => options,
          });
        };
      },
    }));

    try {
      const modPath = `./pgClient.ts${uniqueSuffix()}`;
      const { createPgClient } = (await import(modPath)) as typeof import("./pgClient");
      const client = createPgClient({ host: "db.local", database: "demo" });
      const result = await client.unsafe<{
        query: string;
        values?: unknown[];
        input: unknown;
      }>("SELECT 1", [1]);
      await client.end({ timeout: 2 });
      expect(result.query).toBe("SELECT 1");
      expect(result.values).toEqual([1]);
      expect(result.input).toEqual({ host: "db.local", database: "demo" });
    } finally {
      mock.restore();
    }
  });

  test("createSqliteClient uses better-sqlite3 adapter when Bun runtime is disabled", async () => {
    const calls: Array<{ type: string; payload: unknown }> = [];
    mock.module("./engine", () => ({
      getBunGlobal: () => undefined,
      isBunRuntime: () => false,
      getRuntimeEngine: () => "node",
    }));
    mock.module("node:module", () => ({
      createRequire: () => {
        return (id: string) => {
          if (id !== "better-sqlite3") return fallbackRequire(id);
          return class FakeBetterSqlite {
            constructor(_path: string) {}
            prepare(sql: string) {
              calls.push({ type: "prepare", payload: sql });
              return {
                run: (...params: unknown[]) => {
                  calls.push({ type: "run", payload: params });
                  return { changes: 1 };
                },
                get: (...params: unknown[]) => {
                  calls.push({ type: "get", payload: params });
                  return { params };
                },
                all: (...params: unknown[]) => {
                  calls.push({ type: "all", payload: params });
                  return [{ params }];
                },
                *iterate(...params: unknown[]) {
                  calls.push({ type: "iterate", payload: params });
                  yield { params };
                },
              };
            }
            close(): void {
              calls.push({ type: "close", payload: true });
            }
          };
        };
      },
    }));

    try {
      const modPath = `./sqliteClient.ts${uniqueSuffix()}`;
      const { createSqliteClient } =
        (await import(modPath)) as typeof import("./sqliteClient");
      const client = createSqliteClient("/tmp/mock.sqlite");

      client.run("INSERT INTO t VALUES (?)", [1]);
      client.run("UPDATE t SET a = :a", { a: 1 });
      const stmt = client.prepare("SELECT * FROM t WHERE id = ?");
      const one = stmt.get<{ params: unknown[] }>([9]);
      const many = stmt.all<{ params: unknown[] }>([8]);
      const iter = [...stmt.iterate<{ params: unknown[] }>([7])];
      client.close();

      expect(calls[0]).toEqual({
        type: "prepare",
        payload: "PRAGMA busy_timeout=5000;",
      });
      expect(calls[1]).toEqual({ type: "run", payload: [] });
      expect(one?.params).toEqual([9]);
      expect(many[0]?.params).toEqual([8]);
      expect(iter[0]?.params).toEqual([7]);
      expect(calls.some((entry) => entry.type === "close")).toBe(true);
    } finally {
      mock.restore();
    }
  });

  test("createSqliteClient uses bun:sqlite adapter when Bun runtime is enabled", async () => {
    const calls: Array<{ type: string; payload: unknown }> = [];
    mock.module("./engine", () => ({
      getBunGlobal: () => ({}),
      isBunRuntime: () => true,
      getRuntimeEngine: () => "bun",
    }));
    mock.module("node:module", () => ({
      createRequire: () => {
        return (id: string) => {
          if (id !== "bun:sqlite") {
            return fallbackRequire(id);
          }
          class FakeBunSqlite {
            constructor(_path: string) {}
            run(sql: string, params?: unknown): { changes: number } {
              calls.push({ type: "run", payload: { sql, params } });
              return { changes: 1 };
            }
            prepare(sql: string) {
              calls.push({ type: "prepare", payload: sql });
              return {
                run: (...params: unknown[]) => {
                  calls.push({ type: "stmt.run", payload: params });
                  return { changes: 1 };
                },
                get: (...params: unknown[]) => {
                  calls.push({ type: "stmt.get", payload: params });
                  return { params };
                },
                all: (...params: unknown[]) => {
                  calls.push({ type: "stmt.all", payload: params });
                  return [{ params }];
                },
                *iterate(...params: unknown[]) {
                  calls.push({ type: "stmt.iterate", payload: params });
                  yield { params };
                },
              };
            }
            close(): void {
              calls.push({ type: "close", payload: true });
            }
          }
          return { Database: FakeBunSqlite };
        };
      },
    }));

    try {
      const modPath = `./sqliteClient.ts${uniqueSuffix()}`;
      const { createSqliteClient } =
        (await import(modPath)) as typeof import("./sqliteClient");
      const client = createSqliteClient("/tmp/mock.sqlite");
      client.run("INSERT INTO t VALUES (?)", [1]);
      client.close();

      expect(calls[0]).toEqual({
        type: "run",
        payload: { sql: "PRAGMA busy_timeout=5000;", params: undefined },
      });
      expect(calls.some((entry) => entry.type === "close")).toBe(true);
    } finally {
      mock.restore();
    }
  });

  test("createS3Client uses AWS SDK adapter when Bun runtime is disabled", async () => {
    const commands: Array<{ kind: string; params: Record<string, unknown> }> =
      [];
    const clientOptions: Array<Record<string, unknown>> = [];

    mock.module("./engine", () => ({
      getBunGlobal: () => undefined,
      isBunRuntime: () => false,
      getRuntimeEngine: () => "node",
    }));
    mock.module("node:module", () => ({
      createRequire: () => {
        return (id: string) => {
          if (id !== "@aws-sdk/client-s3") return fallbackRequire(id);
          class PutObjectCommand {
            params: Record<string, unknown>;
            constructor(params: Record<string, unknown>) {
              this.params = params;
            }
          }
          class GetObjectCommand {
            params: Record<string, unknown>;
            constructor(params: Record<string, unknown>) {
              this.params = params;
            }
          }
          class DeleteObjectCommand {
            params: Record<string, unknown>;
            constructor(params: Record<string, unknown>) {
              this.params = params;
            }
          }
          class ListObjectsV2Command {
            params: Record<string, unknown>;
            constructor(params: Record<string, unknown>) {
              this.params = params;
            }
          }
          class S3Client {
            constructor(options: Record<string, unknown>) {
              clientOptions.push(options);
            }
            async send(command: {
              params: Record<string, unknown>;
              constructor: { name: string };
            }): Promise<unknown> {
              commands.push({
                kind: command.constructor.name,
                params: command.params,
              });
              if (command.constructor.name === "GetObjectCommand") {
                const key = String(command.params.Key ?? "");
                if (key === "bytes") {
                  return {
                    Body: {
                      transformToByteArray: async () =>
                        new TextEncoder().encode("bytes-body"),
                    },
                  };
                }
                if (key === "web") {
                  return {
                    Body: {
                      transformToWebStream: () =>
                        new ReadableStream({
                          start(controller) {
                            controller.enqueue(
                              new TextEncoder().encode("web-body"),
                            );
                            controller.close();
                          },
                        }),
                    },
                  };
                }
                if (key === "iter") {
                  return {
                    Body: {
                      async *[Symbol.asyncIterator]() {
                        yield new TextEncoder().encode("iter-");
                        yield "body";
                      },
                    },
                  };
                }
                if (key === "bad") {
                  return {
                    Body: {
                      async *[Symbol.asyncIterator]() {
                        yield 123;
                      },
                    },
                  };
                }
                return { Body: undefined };
              }
              if (command.constructor.name === "ListObjectsV2Command") {
                return {
                  Contents: [{ Key: "alpha.txt" }, { Key: undefined }],
                  CommonPrefixes: [{ Prefix: "users:" }, { Prefix: undefined }],
                };
              }
              return {};
            }
          }
          return {
            S3Client,
            PutObjectCommand,
            GetObjectCommand,
            DeleteObjectCommand,
            ListObjectsV2Command,
          };
        };
      },
    }));

    try {
      const modPath = `./s3Client.ts${uniqueSuffix()}`;
      const { createS3Client } = (await import(modPath)) as typeof import("./s3Client");
      const client = createS3Client({
        bucket: "demo",
        endpoint: "localhost:9000",
        region: "us-east-1",
        accessKeyId: "ak",
        secretAccessKey: "sk",
        sessionToken: "st",
      });

      await client.write("alpha.txt", new Response(new Blob(["payload"])));
      expect(await (await client.read("bytes")).text()).toBe("bytes-body");
      expect(await (await client.read("web")).text()).toBe("web-body");
      expect(await (await client.read("iter")).text()).toBe("iter-body");
      await expect(client.read("bad")).rejects.toThrow(
        "Unsupported S3 body chunk type.",
      );
      await client.unlink("alpha.txt");
      const listed = await client.list({ prefix: "a", delimiter: ":" });

      expect(listed.contents).toEqual([{ key: "alpha.txt" }]);
      expect(listed.commonPrefixes).toEqual([{ prefix: "users:" }]);
      expect(clientOptions[0]?.endpoint).toBe("https://localhost:9000");
      expect(commands.some((cmd) => cmd.kind === "PutObjectCommand")).toBe(
        true,
      );
      expect(commands.some((cmd) => cmd.kind === "DeleteObjectCommand")).toBe(
        true,
      );
    } finally {
      mock.restore();
    }
  });
});
