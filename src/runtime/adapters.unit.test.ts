import { describe, expect, test } from "bun:test";
import { createPgClient } from "./pgClient";
import { createS3Client } from "./s3Client";

type BunLike = {
  SQL: unknown;
  S3Client: unknown;
};

describe("runtime adapters (bun path)", () => {
  test("createPgClient delegates to Bun.SQL", async () => {
    const bunAny = Bun as unknown as BunLike;
    const originalSql = bunAny.SQL;
    const calls: Array<{ query: string; values?: unknown[] }> = [];
    const ended: unknown[] = [];

    class FakeSql {
      constructor(_input: unknown) {}
      async unsafe<T = unknown>(query: string, values?: unknown[]): Promise<T> {
        calls.push({ query, values });
        return [{ ok: true }] as unknown as T;
      }
      async end(options?: unknown): Promise<void> {
        ended.push(options);
      }
    }

    bunAny.SQL = FakeSql as unknown;
    try {
      const client = createPgClient("postgres://localhost/test");
      const rows = await client.unsafe<Array<{ ok: boolean }>>("SELECT 1");
      await client.unsafe("SELECT $1", [99]);
      await client.end({ timeout: 3 });
      expect(rows[0]?.ok).toBe(true);
      expect(calls).toHaveLength(2);
      expect(calls[1]?.values).toEqual([99]);
      expect(ended).toEqual([{ timeout: 3 }]);
    } finally {
      bunAny.SQL = originalSql;
    }
  });

  test("createS3Client delegates to Bun.S3Client", async () => {
    const bunAny = Bun as unknown as BunLike;
    const originalS3 = bunAny.S3Client;
    const writes: string[] = [];
    const deletes: string[] = [];

    class FakeS3Client {
      constructor(_opts: unknown) {}
      async write(key: string, _payload: Blob | Response): Promise<void> {
        writes.push(key);
      }
      async unlink(key: string): Promise<void> {
        deletes.push(key);
      }
      async list(): Promise<{
        contents: Array<{ key: string }>;
        commonPrefixes: Array<{ prefix: string }>;
      }> {
        return {
          contents: [{ key: "a.txt" }],
          commonPrefixes: [{ prefix: "users:" }],
        };
      }
      file(_key: string): { arrayBuffer: () => Promise<ArrayBuffer> } {
        return {
          arrayBuffer: async () =>
            await new Blob(["hello"]).arrayBuffer(),
        };
      }
    }

    bunAny.S3Client = FakeS3Client as unknown;
    try {
      const client = createS3Client({ bucket: "demo" });
      await client.write("a.txt", new Blob(["hello"]));
      const blob = await client.read("a.txt");
      await client.unlink("a.txt");
      const listed = await client.list({ prefix: "a" });
      expect(await blob.text()).toBe("hello");
      expect(writes).toEqual(["a.txt"]);
      expect(deletes).toEqual(["a.txt"]);
      expect(listed.contents?.[0]?.key).toBe("a.txt");
      expect(listed.commonPrefixes?.[0]?.prefix).toBe("users:");
    } finally {
      bunAny.S3Client = originalS3;
    }
  });
});
