import { describe, expect, mock, test } from "bun:test";
import { RN } from "../core/rn";
import { FloatingDepotFile } from "./depotFile";

type S3ListResult = {
  contents?: Array<{ key: string }>;
  commonPrefixes?: Array<{ prefix: string }>;
};

class FakeS3Client {
  private _store = new Map<string, Blob>();

  constructor(_opts: unknown) {}

  async list(params: {
    prefix?: string;
    delimiter?: string;
  }): Promise<S3ListResult> {
    const prefix = params.prefix ?? "";
    const delimiter = params.delimiter;
    const keys = [...this._store.keys()].filter((key) =>
      key.startsWith(prefix),
    );
    if (!delimiter) {
      return { contents: keys.map((key) => ({ key })) };
    }
    const prefixes = new Set<string>();
    for (const key of keys) {
      const rest = key.slice(prefix.length);
      const idx = rest.indexOf(delimiter);
      if (idx >= 0) {
        prefixes.add(`${prefix}${rest.slice(0, idx + 1)}`);
      }
    }
    return {
      commonPrefixes: [...prefixes].map((prefixEntry) => ({
        prefix: prefixEntry,
      })),
    };
  }

  async write(key: string, file: Blob | Response): Promise<void> {
    const blob =
      file instanceof Response ? new Blob([await file.arrayBuffer()]) : file;
    this._store.set(key, blob);
  }

  file(key: string): Blob {
    return this._store.get(key) ?? new Blob([]);
  }

  async unlink(key: string): Promise<void> {
    this._store.delete(key);
  }
}

const uniqueSuffix = (): string =>
  `?test=${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`;

function makeStream(text: string): ReadableStream<Uint8Array> {
  const encoder = new TextEncoder();
  return new ReadableStream({
    start(controller) {
      controller.enqueue(encoder.encode(text));
      controller.close();
    },
  });
}

describe("S3Depot (mocked)", () => {
  test("writes, reads, lists, edits, and deletes files without network", async () => {
    mock.module("../runtime/engine", () => ({
      getBunGlobal: () => ({ S3Client: FakeS3Client }),
      isBunRuntime: () => true,
      getRuntimeEngine: () => "bun",
    }));

    try {
      const modPath = `./depots/s3Depot.ts${uniqueSuffix()}`;
      const { S3Depot } = (await import(modPath)) as typeof import("./depots/s3Depot");
      const depot = new S3Depot({
        bucket: "unit-test",
        prefix: "root",
        autoCreateBucket: false,
      });
      const rn = RN.create(
        `[rn][depot::${depot.identifier}]:docs:readme.txt`,
      ).unwrap();
      const streamRn = RN.create(
        `[rn][depot::${depot.identifier}]:docs:stream.txt`,
      ).unwrap();
      const prefix = RN.create(
        `[rn][depot::${depot.identifier}]:docs:*`,
      ).unwrap();
      const rootPrefix = RN.create(
        `[rn][depot::${depot.identifier}]:*`,
      ).unwrap();

      await depot.createFile(new FloatingDepotFile(rn, new Blob(["hello"])));
      const fetched = await depot.getFile(rn);
      expect(await fetched.text()).toBe("hello");

      const listed = await depot.listFiles(prefix);
      expect(listed.length).toBe(1);

      await depot.editFile(
        rn,
        async () => new FloatingDepotFile(rn, new Blob(["updated"])),
      );
      const updated = await depot.getFile(rn);
      expect(await updated.text()).toBe("updated");

      await depot.createFile(
        new FloatingDepotFile(streamRn, makeStream("streamed")),
      );
      const streamed = await depot.getFile(streamRn);
      expect(await streamed.text()).toBe("streamed");

      await depot.editFile(
        streamRn,
        async () => new FloatingDepotFile(streamRn, makeStream("streamed-2")),
      );
      const streamedUpdated = await depot.getFile(streamRn);
      expect(await streamedUpdated.text()).toBe("streamed-2");

      const dirs = await depot.listDirs(rootPrefix);
      expect(dirs).toContain("docs");

      expect(await depot.deleteFile(rn)).toBe(true);
      expect(await depot.deleteFile(streamRn)).toBe(true);
      const afterDelete = await depot.listFiles(prefix);
      expect(afterDelete.length).toBe(0);
    } finally {
      mock.restore();
    }
  });
});
