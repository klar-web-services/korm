import { describe, expect, test } from "bun:test";
import type { JSONable } from "../korm";
import { RN } from "../core/rn";
import type { LayerPool } from "../sources/layerPool";
import type { ResolvedMeta } from "../core/resolveMeta";
import {
    DepotFile,
    DepotFileBase,
    FloatingDepotFile,
    UncommittedDepotFile,
} from "./depotFile";
import { persistDepotFilesInData, uploadDepotFilesFromRefs } from "./depotPersist";

const UUID = "3dd91ede-37a4-4c25-a86a-6f1a9e132186";

class CustomDepotFile extends DepotFileBase {
    constructor(rn: RN<JSONable>, file: Blob) {
        super(rn, file, "floating");
    }
}

function makeDepotRn(): RN<JSONable> {
    return RN.create("[rn][depot::files]:docs:2024:note.txt").unwrap();
}

function makePool(calls: { find: number; files: unknown[] }): LayerPool {
    const depot = {
        createFile: async (file: unknown) => {
            calls.files.push(file);
            return "ok";
        },
    };
    return {
        findDepotForRn: () => {
            calls.find += 1;
            return depot;
        },
    } as unknown as LayerPool;
}

describe("depotPersist", () => {
    test("persists depot files in data and collects ops", async () => {
        const rn = makeDepotRn();
        const floating = new FloatingDepotFile(rn, new Blob(["a"]));
        const uncommitted = new UncommittedDepotFile(rn, new Blob(["b"]));
        const committed = new DepotFile(rn, new Blob(["c"]));
        const calls = { find: 0, files: [] as unknown[] };
        const pool = makePool(calls);
        const depotOps: DepotFileBase[] = [];

        const result = await persistDepotFilesInData(
            { files: [floating, uncommitted, committed] } as JSONable,
            pool,
            { depotOps, skipUpload: true }
        );

        expect(result).toEqual({ files: [rn.value(), rn.value(), rn.value()] });
        expect(depotOps).toEqual([floating, uncommitted]);
        expect(calls.files).toHaveLength(0);
    });

    test("uploads files and caches depots by identifier", async () => {
        const rn = makeDepotRn();
        const fileA = new CustomDepotFile(rn, new Blob(["a"]));
        const fileB = new CustomDepotFile(rn, new Blob(["b"]));
        const calls = { find: 0, files: [] as unknown[] };
        const pool = makePool(calls);

        const result = await persistDepotFilesInData(
            { a: fileA, b: fileB } as JSONable,
            pool
        );

        expect(result).toEqual({ a: rn.value(), b: rn.value() });
        expect(calls.find).toBe(1);
        expect(calls.files).toHaveLength(2);
    });

    test("uploads floating and uncommitted files via pool", async () => {
        const rn = makeDepotRn();
        const floating = new FloatingDepotFile(rn, new Blob(["a"]));
        const uncommitted = new UncommittedDepotFile(rn, new Blob(["b"]));
        const calls = { find: 0, files: [] as unknown[] };
        const pool = makePool(calls);

        const result = await persistDepotFilesInData(
            { a: floating, b: uncommitted } as JSONable,
            pool
        );

        expect(result).toEqual({ a: rn.value(), b: rn.value() });
        expect(calls.files).toHaveLength(2);
    });

    test("rejects uploads when RN is not a depot", async () => {
        const itemRn = RN.create("cars", "suv", UUID).unwrap();
        const badFile = new CustomDepotFile(itemRn, new Blob(["x"]));
        const calls = { find: 0, files: [] as unknown[] };
        const pool = makePool(calls);

        await expect(persistDepotFilesInData({ bad: badFile } as JSONable, pool)).rejects.toThrow(
            "does not refer to a depot"
        );
    });

    test("rejects uploads when RN is a depot prefix", async () => {
        const prefixRn = RN.create("[rn][depot::files]:docs:*").unwrap();
        const badFile = new CustomDepotFile(prefixRn, new Blob(["x"]));
        const calls = { find: 0, files: [] as unknown[] };
        const pool = makePool(calls);

        await expect(persistDepotFilesInData({ bad: badFile } as JSONable, pool)).rejects.toThrow(
            "Expected depot file RN"
        );
    });

    test("uploads files from resolved refs", async () => {
        const rn = makeDepotRn();
        const floating = new FloatingDepotFile(rn, new Blob(["a"]));
        const arrayFile = new FloatingDepotFile(rn, new Blob(["b"]));
        const calls = { find: 0, files: [] as unknown[] };
        const pool = makePool(calls);
        const depotOps: DepotFileBase[] = [];

        const meta = {
            rawData: { file: "placeholder", files: [rn.value()] },
            resolvedData: { file: floating, files: [arrayFile] },
            refs: [{ path: ["file"] }, { path: ["files"] }],
        } satisfies ResolvedMeta;

        await uploadDepotFilesFromRefs(meta, pool, { depotOps, skipUpload: true });
        expect(meta.rawData).toEqual({ file: rn.value(), files: [rn.value()] });
        expect(depotOps).toEqual([floating, arrayFile]);
        expect(calls.files).toHaveLength(0);
    });

    test("uploads resolved refs when skipUpload is false", async () => {
        const rn = makeDepotRn();
        const floating = new FloatingDepotFile(rn, new Blob(["a"]));
        const uncommitted = new UncommittedDepotFile(rn, new Blob(["b"]));
        const calls = { find: 0, files: [] as unknown[] };
        const pool = makePool(calls);

        const meta = {
            rawData: { file: "placeholder", other: "placeholder" },
            resolvedData: { file: floating, other: uncommitted },
            refs: [{ path: ["file"] }, { path: ["other"] }],
        } satisfies ResolvedMeta;

        await uploadDepotFilesFromRefs(meta, pool);
        expect(meta.rawData).toEqual({ file: rn.value(), other: rn.value() });
        expect(calls.files).toHaveLength(2);
    });

    test("uploads custom depot file refs via fallback", async () => {
        const rn = makeDepotRn();
        const custom = new CustomDepotFile(rn, new Blob(["c"]));
        const calls = { find: 0, files: [] as unknown[] };
        const pool = makePool(calls);

        const meta = {
            rawData: { custom: "placeholder" },
            resolvedData: { custom },
            refs: [{ path: ["custom"] }],
        } satisfies ResolvedMeta;

        await uploadDepotFilesFromRefs(meta, pool);
        expect(meta.rawData).toEqual({ custom: rn.value() });
        expect(calls.files).toHaveLength(1);
    });

    test("no-ops when there are no refs", async () => {
        const calls = { find: 0, files: [] as unknown[] };
        const pool = makePool(calls);
        const meta = { rawData: {}, resolvedData: {}, refs: [] } satisfies ResolvedMeta;
        await uploadDepotFilesFromRefs(meta, pool);
        expect(calls.files).toHaveLength(0);
    });
});
