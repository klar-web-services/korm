import { describe, expect, test } from "bun:test";
import { RN } from "../core/rn";
import { S3Depot } from "./depots/s3Depot";
import { FloatingDepotFile } from "./depotFile";

type S3ListResult = {
    contents?: Array<{ key: string }>;
    commonPrefixes?: Array<{ prefix: string }>;
};

class FakeS3Client {
    private _store = new Map<string, Blob>();

    constructor(_opts: unknown) {
    }

    async list(params: { prefix?: string; delimiter?: string }): Promise<S3ListResult> {
        const prefix = params.prefix ?? "";
        const delimiter = params.delimiter;
        const keys = [...this._store.keys()].filter((key) => key.startsWith(prefix));
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
        return { commonPrefixes: [...prefixes].map((prefixEntry) => ({ prefix: prefixEntry })) };
    }

    async write(key: string, file: Blob): Promise<void> {
        this._store.set(key, file);
    }

    file(key: string): Blob {
        return this._store.get(key) ?? new Blob([]);
    }

    async unlink(key: string): Promise<void> {
        this._store.delete(key);
    }
}

describe("S3Depot (mocked)", () => {
    test("writes, reads, lists, edits, and deletes files without network", async () => {
        const bunAny = Bun as unknown as { S3Client: unknown };
        const original = bunAny.S3Client;
        bunAny.S3Client = FakeS3Client as unknown as typeof Bun.S3Client;

        try {
            const depot = new S3Depot({
                bucket: "unit-test",
                prefix: "root",
                autoCreateBucket: false,
            });
            const rn = RN.create(`[rn][depot::${depot.identifier}]:docs:readme.txt`).unwrap();
            const prefix = RN.create(`[rn][depot::${depot.identifier}]:docs:*`).unwrap();
            const rootPrefix = RN.create(`[rn][depot::${depot.identifier}]:*`).unwrap();

            await depot.createFile(new FloatingDepotFile(rn, new Blob(["hello"])));
            const fetched = await depot.getFile(rn);
            expect(await fetched.text()).toBe("hello");

            const listed = await depot.listFiles(prefix);
            expect(listed.length).toBe(1);

            await depot.editFile(rn, async () => new FloatingDepotFile(rn, new Blob(["updated"])));
            const updated = await depot.getFile(rn);
            expect(await updated.text()).toBe("updated");

            const dirs = await depot.listDirs(rootPrefix);
            expect(dirs).toContain("docs");

            expect(await depot.deleteFile(rn)).toBe(true);
            const afterDelete = await depot.listFiles(prefix);
            expect(afterDelete.length).toBe(0);
        } finally {
            bunAny.S3Client = original;
        }
    });
});
