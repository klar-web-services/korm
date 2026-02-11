import { describe, expect, test } from "bun:test";
import util from "node:util";
import type { JSONable } from "../korm";
import { RN } from "../core/rn";
import type { LayerPool } from "../sources/layerPool";
import {
  DepotFile,
  FloatingDepotFile,
  UncommittedDepotFile,
  isDepotFile,
} from "./depotFile";

const UUID = "3dd91ede-37a4-4c25-a86a-6f1a9e132186";

function makeDepotRn(): RN<JSONable> {
  return RN.create("[rn][depot::files]:invoices:2024:receipt.txt").unwrap();
}

function makeStream(text: string): ReadableStream<Uint8Array> {
  const encoder = new TextEncoder();
  return new ReadableStream({
    start(controller) {
      controller.enqueue(encoder.encode(text));
      controller.close();
    },
  });
}

function makePool(calls: { files: unknown[] }): LayerPool {
  const depot = {
    createFile: async (file: unknown) => {
      calls.files.push(file);
      return "ok";
    },
    getFile: async (rn: RN<JSONable>) => new DepotFile(rn, new Blob(["ok"])),
  };
  return { findDepotForRn: () => depot } as unknown as LayerPool;
}

describe("DepotFile", () => {
  test("serializes and reads Blob payloads", async () => {
    const rn = makeDepotRn();
    const blob = new Blob(["hello"]);
    const file = new DepotFile(rn, blob);
    expect(file.toJSON()).toBe(rn.value());
    expect(await file.text()).toBe("hello");
    const buffer = await file.arrayBuffer();
    expect(buffer.byteLength).toBe(5);
    const stream = file.stream();
    expect(stream).toBeInstanceOf(ReadableStream);
    const slice = file.slice(0, 2);
    expect(await slice.text()).toBe("he");
  });

  test("throws when slicing unsupported payloads", () => {
    const rn = makeDepotRn();
    const noSlice = {
      arrayBuffer: async () => new ArrayBuffer(0),
      text: async () => "",
      stream: () => new ReadableStream(),
    };
    const file = new DepotFile(rn, noSlice as unknown as Blob);
    expect(() => file.slice(0, 1)).toThrow(
      "DepotFile.slice() is not supported",
    );
  });

  test("reads ReadableStream payloads", async () => {
    const rn = makeDepotRn();
    const file = new DepotFile(rn, makeStream("hello"));
    expect(await file.text()).toBe("hello");

    const file2 = new DepotFile(rn, makeStream("hello"));
    const buffer = await file2.arrayBuffer();
    expect(new TextDecoder().decode(buffer)).toBe("hello");

    const stream = makeStream("hello");
    const file3 = new DepotFile(rn, stream);
    expect(file3.stream()).toBe(stream);
  });

  test("updates to an uncommitted depot file", async () => {
    const rn = makeDepotRn();
    const file = new DepotFile(rn, new Blob(["old"]));
    const updated = await file.update(async () => new Blob(["new"]));
    expect(updated.state).toBe("uncommitted");
    expect(await updated.text()).toBe("new");
  });

  test("accepts edits returning depot files and validates RN", async () => {
    const rn = makeDepotRn();
    const file = new DepotFile(rn, new Blob(["old"]));
    const edited = new DepotFile(rn, new Blob(["edited"]));
    const updated = await file.update(() => edited);
    expect(updated.state).toBe("uncommitted");
    expect(await updated.text()).toBe("edited");
    const other = RN.create(
      "[rn][depot::files]:invoices:2024:other.txt",
    ).unwrap();
    const wrong = new DepotFile(other, new Blob(["wrong"]));
    await expect(file.update(() => wrong)).rejects.toThrow("does not match");
  });

  test("creates and commits using a pool", async () => {
    const rn = makeDepotRn();
    const calls = { files: [] as unknown[] };
    const pool = makePool(calls);
    const floating = new FloatingDepotFile(rn, new Blob(["f"]));
    const committed = await floating.create(pool);
    expect(committed.state).toBe("committed");
    expect(calls.files).toHaveLength(1);

    const uncommitted = new UncommittedDepotFile(rn, new Blob(["u"]));
    const committed2 = await uncommitted.commit(pool);
    expect(committed2.state).toBe("committed");
    expect(calls.files).toHaveLength(2);
  });

  test("validates RN type on create and commit", async () => {
    const rn = RN.create("cars", "suv", UUID).unwrap();
    const calls = { files: [] as unknown[] };
    const pool = makePool(calls);
    const floating = new FloatingDepotFile(rn, new Blob(["x"]));
    await expect(floating.create(pool)).rejects.toThrow(
      "Expected depot file RN",
    );
    const uncommitted = new UncommittedDepotFile(rn, new Blob(["y"]));
    await expect(uncommitted.commit(pool)).rejects.toThrow(
      "Expected depot file RN",
    );
  });

  test("supports inspection and type guards", () => {
    const rn = makeDepotRn();
    const file = new DepotFile(rn, new Blob(["hello"]));
    const inspected = util.inspect(file);
    expect(inspected).toContain("DepotFile");
    expect(inspected).toContain(rn.value());
    expect(isDepotFile(file)).toBe(true);
    expect(isDepotFile({})).toBe(false);
  });
});
