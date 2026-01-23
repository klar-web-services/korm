import { describe, expect, test } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { korm } from "../korm";
import { buildPoolMetaConfig, hydratePoolMetaConfig } from "./poolMeta";
import type { Depot } from "../depot/depot";
import type { SourceLayer } from "./sourceLayer";

const makeTempDir = (): string =>
  fs.mkdtempSync(path.join(os.tmpdir(), "korm-meta-"));

const makeSqlitePath = (): string => path.join(makeTempDir(), "pool.sqlite");

const withEncryptionKey = async (
  key: string,
  run: () => Promise<void>,
): Promise<void> => {
  const previousKey = process.env.KORM_ENCRYPTION_KEY;
  process.env.KORM_ENCRYPTION_KEY = key;
  try {
    await run();
  } finally {
    if (previousKey === undefined) {
      delete process.env.KORM_ENCRYPTION_KEY;
    } else {
      process.env.KORM_ENCRYPTION_KEY = previousKey;
    }
  }
};

describe("pool metadata", () => {
  test("allows matching config across instances", async () => {
    const dbPath = makeSqlitePath();
    const layer1 = korm.layers.sqlite(dbPath);
    const pool1 = korm
      .pool()
      .setLayers(korm.use.layer(layer1).as("db"))
      .withMeta(korm.target.layer("db"))
      .open();
    await pool1.ensureWalReady();

    const layer2 = korm.layers.sqlite(dbPath);
    const pool2 = korm
      .pool()
      .setLayers(korm.use.layer(layer2).as("db"))
      .withMeta(korm.target.layer("db"))
      .open();
    await pool2.ensureWalReady();

    await pool1.close();
    await pool2.close();
  });

  test("rejects mismatched config", async () => {
    const dbPath = makeSqlitePath();
    const layer1 = korm.layers.sqlite(dbPath);
    const pool1 = korm
      .pool()
      .setLayers(korm.use.layer(layer1).as("db"))
      .withMeta(korm.target.layer("db"))
      .open();
    await pool1.ensureWalReady();
    await pool1.close();

    const layer2 = korm.layers.sqlite(dbPath);
    const pool2 = korm
      .pool()
      .setLayers(korm.use.layer(layer2).as("db"))
      .withMeta(korm.target.layer("db"))
      .withLocks(korm.target.layer("db"))
      .open();

    await expect(pool2.ensureWalReady()).rejects.toThrow(
      "ATTENTION: parts of this pool have been used by another instance of korm.",
    );
    try {
      await pool2.close();
    } catch {
      // ignore meta mismatch on close
    }
  });

  test("discovers pool configuration from metadata", async () => {
    const dbPath = makeSqlitePath();
    const depotRoot = makeTempDir();
    const layer = korm.layers.sqlite(dbPath);
    const depot = korm.depots.local(depotRoot);
    const pool = korm
      .pool()
      .setLayers(korm.use.layer(layer).as("db"))
      .setDepots(korm.use.depot(depot).as("files"))
      .withMeta(korm.target.layer("db"))
      .open();
    await pool.ensureWalReady();
    await pool.close();

    const discovered = await korm.discover(korm.layers.sqlite(dbPath));
    await discovered.ensureWalReady();
    expect(discovered.getLayer("db")).toBeTruthy();
    expect(discovered.getDepot("files")).toBeTruthy();
    await discovered.close();
  });

  test("reset clears pool metadata", async () => {
    const dbPath = makeSqlitePath();
    const layer1 = korm.layers.sqlite(dbPath);
    const pool1 = korm
      .pool()
      .setLayers(korm.use.layer(layer1).as("db"))
      .withMeta(korm.target.layer("db"))
      .open();
    await pool1.ensureWalReady();
    await korm.danger(korm.reset(pool1, { mode: "meta only" }));
    try {
      await pool1.close();
    } catch {
      // ignore meta mismatch on close
    }

    const layer2 = korm.layers.sqlite(dbPath);
    const pool2 = korm
      .pool()
      .setLayers(korm.use.layer(layer2).as("db"))
      .withMeta(korm.target.layer("db"))
      .withLocks(korm.target.layer("db"), { ttlMs: 5_000 })
      .open();
    await pool2.ensureWalReady();
    await pool2.close();
  });

  test("serializes complex config options for discovery", async () => {
    await withEncryptionKey(
      "b7b6d2f594f1f4a4f2f6f10aeac69547fd0b9cbd398081b62b68a26252c5bb22",
      async () => {
        const buffer = Buffer.from("secret");
        const view = new Uint8Array([1, 2, 3]);
        const floatView = new Float32Array([1.25, 2.5]);
        const dataView = new DataView(Uint8Array.from([9, 8, 7]).buffer);
        const when = new Date("2024-01-01T00:00:00.000Z");
        const endpoint = new URL("https://example.com");
        const map = new Map<unknown, unknown>([
          ["a", 1],
          [2, "b"],
        ]);
        const set = new Set<unknown>(["x", 2]);
        const regexp = /korm/gi;
        const options = {
          ssl: {
            ca: buffer,
            key: view,
            cert: floatView,
            view: dataView,
            when,
            endpoint,
            map,
            set,
            regexp,
            nan: Number.NaN,
            inf: Number.POSITIVE_INFINITY,
            ninf: Number.NEGATIVE_INFINITY,
            big: BigInt("9007199254740993"),
          },
        };
        const layers = new Map<string, SourceLayer>([
          [
            "db",
            {
              type: "pg",
              identifier: "pg@test",
              getPoolConfig: () => ({
                type: "pg",
                mode: "options",
                value: options,
              }),
            } as unknown as SourceLayer,
          ],
        ]);
        const depots = new Map<string, Depot>([
          [
            "files",
            {
              __DEPOT__: true,
              type: "s3",
              identifier: "s3@test",
              getPoolConfig: () => ({
                type: "s3",
                options: {
                  bucket: "demo",
                  accessKeyId: Buffer.from("key"),
                  secretAccessKey: Buffer.from("secret"),
                },
              }),
            } as unknown as Depot,
          ],
        ]);

        const meta = await buildPoolMetaConfig({ layers, depots });
        const hydrated = await hydratePoolMetaConfig(meta);
        const hydratedLayer = hydrated.layers.find(
          (layer) => layer.ident === "db",
        );
        expect(hydratedLayer).toBeTruthy();
        const hydratedOptions = hydratedLayer?.value as any;

        expect(Buffer.isBuffer(hydratedOptions.ssl.ca)).toBe(true);
        expect(Buffer.from(hydratedOptions.ssl.ca).toString("utf8")).toBe(
          "secret",
        );
        expect(hydratedOptions.ssl.key).toBeInstanceOf(Uint8Array);
        expect(Array.from(hydratedOptions.ssl.key as Uint8Array)).toEqual([
          1, 2, 3,
        ]);
        expect(hydratedOptions.ssl.cert).toBeInstanceOf(Float32Array);
        expect(Array.from(hydratedOptions.ssl.cert as Float32Array)).toEqual([
          1.25, 2.5,
        ]);
        expect(hydratedOptions.ssl.view).toBeInstanceOf(DataView);
        expect((hydratedOptions.ssl.view as DataView).byteLength).toBe(3);
        expect(hydratedOptions.ssl.when).toBeInstanceOf(Date);
        expect((hydratedOptions.ssl.when as Date).toISOString()).toBe(
          "2024-01-01T00:00:00.000Z",
        );
        expect(hydratedOptions.ssl.endpoint).toBeInstanceOf(URL);
        expect((hydratedOptions.ssl.endpoint as URL).toString()).toBe(
          "https://example.com/",
        );
        expect(hydratedOptions.ssl.map).toBeInstanceOf(Map);
        expect(
          (hydratedOptions.ssl.map as Map<unknown, unknown>).get("a"),
        ).toBe(1);
        expect(hydratedOptions.ssl.set).toBeInstanceOf(Set);
        expect((hydratedOptions.ssl.set as Set<unknown>).has("x")).toBe(true);
        expect(hydratedOptions.ssl.regexp).toBeInstanceOf(RegExp);
        expect((hydratedOptions.ssl.regexp as RegExp).source).toBe("korm");
        expect((hydratedOptions.ssl.regexp as RegExp).flags).toBe("gi");
        expect(Number.isNaN(hydratedOptions.ssl.nan)).toBe(true);
        expect(hydratedOptions.ssl.inf).toBe(Number.POSITIVE_INFINITY);
        expect(hydratedOptions.ssl.ninf).toBe(Number.NEGATIVE_INFINITY);
        expect(typeof hydratedOptions.ssl.big).toBe("bigint");
        expect(hydratedOptions.ssl.big).toBe(BigInt("9007199254740993"));
      },
    );
  });

  test("rejects functions in config options", async () => {
    await withEncryptionKey(
      "6f0725a767f2ed27e7c2e6f114f9304318e1542b8a64ae6cd43194a8fc8d7150",
      async () => {
        const layers = new Map<string, SourceLayer>([
          [
            "db",
            {
              type: "pg",
              identifier: "pg@test",
              getPoolConfig: () => ({
                type: "pg",
                mode: "options",
                value: {
                  ssl: {
                    checkServerIdentity: () => true,
                  },
                },
              }),
            } as unknown as SourceLayer,
          ],
        ]);
        const depots = new Map<string, Depot>();
        await expect(buildPoolMetaConfig({ layers, depots })).rejects.toThrow(
          "Pool metadata cannot serialize function at layers.db.connection.ssl.checkServerIdentity.",
        );
      },
    );
  });
});
