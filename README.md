<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="./.github/assets/logo-darkmode.png">
    <source media="(prefers-color-scheme: light)" srcset="./.github/assets/logo-lightmode.png">
    <img alt="Klonk Logo" src="./.github/assets/logo-lightmode.png", width="50%">
  </picture>
</p>

---

# @fkws/korm - Unified Data Runtime

korm is a Unified Data Runtime for Bun that treats SQL databases, references, and file storage as one cohesive data model. You get type-safe items, cross-layer references, encrypted fields, depot-backed files, and an optional undo/redo WAL for crash safety.

[![Tests](https://github.com/klar-web-services/korm/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/klar-web-services/korm/actions/workflows/ci.yml)

## Highlights

- Multi-layer data model: SQLite, Postgres, and MySQL in a single pool.
- Resource Names (RNs) for stable, portable references across layers and depots.
- Type-safe resolution (`resolvePaths`) that turns RN fields into actual objects.
- Built-in encryption and redaction for sensitive fields.
- Depot files (local or S3-compatible) that persist alongside items.
- Optional undo/redo WAL for crash safety across `create`, `commit`, and `tx` (and optionally depot file writes).
- Optional scheduled backups with retention and restore.
- Optional shared locks via a db-backed lock table for cross-process coordination.
- Optional pool metadata to detect mismatched configs and enable discovery.
- Automatic schema creation with safe (or destructive) evolution.

## Test Suites

These run in GitHub Actions on `main` and publish both a run summary and a structured test report.

- **Unit**: fast checks over individual modules in `src/**/*.unit.test.ts` (core APIs, types, helpers).
- **Integration**: full multi-layer behavior across SQLite/Postgres/MySQL/depots, WAL, backups, resolvePaths, encryption (`src/testing/integration.test.ts`).
- **Hostile**: adversarial probes against injection, unsafe identifiers, RN/path traversal, WAL tampering (`src/testing/hostile.test.ts`).

## Install

```bash
bun add @fkws/korm
```

korm is built for Bun. The examples in `examples/` assume Bun runtime.

## Quick Start

```ts
import { korm, type RN, type Encrypt, type Password } from "@fkws/korm";

// Layers
const carDb = korm.layers.sqlite("./cars.sqlite");
const userDb = korm.layers.pg(process.env.PG_URL!);
const docsDb = korm.layers.mysql(process.env.MYSQL_URL!);

// Depots (local or S3-compatible)
const invoiceDepot = korm.depots.s3({
  bucket: "invoices",
  endpoint: process.env.S3_ENDPOINT!,
  accessKeyId: process.env.S3_ACCESS_KEY_ID!,
  secretAccessKey: process.env.S3_SECRET_ACCESS_KEY!
});
const walDepot = korm.depots.local("./wal");

// Pool (layers + depots + optional shared locks + optional WAL)
const pool = korm.pool()
  .setLayers(
    korm.use.layer(carDb).as("cardb"),
    korm.use.layer(userDb).as("userdb"),
    korm.use.layer(docsDb).as("docsdb")
  )
  .withMeta(korm.target.layer("cardb")) // optional unless you want backups; stores pool metadata for mismatch detection & discovery
  .withLocks(korm.target.layer("cardb")) // optional: shared locks across processes
  .setDepots(
    korm.use.depot(invoiceDepot).as("invoiceDepot"),
    korm.use.depot(walDepot).as("walDepot")
  )
  .withWal({ depotIdent: "walDepot", walNamespace: "demo", retention: "keep", depotOps: "record" })
  .open();

// Models
type User = {
  firstName: string;
  lastName: string;
  password: Password<string>;
  username: string;
};

type Car = {
  make: string;
  model: string;
  year: number;
  owner: RN<User>; // RN reference
  registered: boolean;
  registrationNumber: Encrypt<string>; // mark sensitive data for encryption
};

// Create items
const user = (await korm.item<User>(pool).from.data({
  namespace: "users",
  kind: "freetier",
  mods: [{ key: "from", value: "userdb" }],
  data: {
    firstName: "Fred",
    lastName: "Flintstone",
    username: "freddie",
    password: await korm.password("super-secret") // Immediately hashes with argon2id - prevents cleartext access
  }
}).create()).unwrap(); // See @fkws/klonk-result for guidance. Unwrapping is usually discouraged but fine for this demo

const car = (await korm.item<Car>(pool).from.data({
  namespace: "cars",
  kind: "suv",
  mods: [{ key: "from", value: "cardb" }],
  data: {
    make: "Citroen",
    model: "C4",
    year: 2014,
    owner: user.rn!,
    registered: true,
    registrationNumber: await korm.encrypt("1234567890")
  }
}).create()).unwrap();

// Query + resolve RN references
const cars = (await korm.item<Car>(pool)
  .from.query(korm.rn("[rn][from::cardb]:cars:suv:*"))
  .where(korm.qfns.eq("owner.username", "freddie"))
  .get({ resolvePaths: ["owner"] })
).unwrap();

console.log(cars[0]?.data?.owner.firstName); // fully typed

await pool.close();
```

Use `korm.depots.s3(...)` and `korm.depots.local(...)` for depot creation.

## Examples

- `examples/maximal.example.ts` - Full API demo (multi-layer pool, WAL, backups, depots).
- `examples/minimal.example.ts` - Minimal single-layer usage.

## Core Concepts

### JSONable data model

korm stores JSON-compatible values. Types must be assignable to:

```ts
type JSONable =
  | string
  | number
  | boolean
  | null
  | JSONable[]
  | { [k: string]: JSONable }
  | { toJSON(): JSONable };
```

### Resource Names (RN)

RNs are typed identifiers used for items, collections, and depots.

- Item RN:
  - `[rn][from::cardb]:cars:suv:UUID`
- Collection RN (query target):
  - `[rn][from::cardb]:cars:suv:*`
- Depot file RN:
  - `[rn][depot::invoiceDepot]:invoices:fred:invoice-001.txt`
- Depot prefix (list files):
  - `[rn][depot::invoiceDepot]:invoices:fred:*`

Create an RN from a string:

```ts
const rn = korm.rn("[rn][from::userdb]:users:freetier:*" );
```

If you build an RN from parts, namespace/kind must match `[a-z][a-z0-9]*` and item id must be a UUIDv4 (or `*` for collections). Depot RNs can only have `*` as the final segment.

### Layers and pools

A pool aggregates data layers and depots:

```ts
const pool = korm.pool()
  .setLayers(
    korm.use.layer(korm.layers.sqlite("./db.sqlite")).as("sqlite"),
    korm.use.layer(korm.layers.pg(process.env.PG_URL!)).as("pg")
  )
  .setDepots(
    korm.use.depot(korm.depots.local("./files")).as("files")
  )
  .open();
```

- If you have multiple layers, you must target one with the `from` RN mod.
- If there is only one layer, you can omit the `from` mod.
- `pool.close()` closes all layers and depots. If your program never exits, you forgot to call this.

### Item lifecycle

korm distinguishes three item states:

- `FloatingItem<T>`: exists only in memory; call `.create()` to persist.
- `Item<T>`: persisted and in sync.
- `UncommittedItem<T>`: persisted but has unsaved changes; call `.commit()`.

The APIs:

```ts
// create
const floating = korm.item<T>(pool).from.data({ ... }).unwrap();
const created = (await floating.create()).unwrap();

// update
const updated = created.update({ ... }).unwrap();
const committed = (await updated.commit()).unwrap();
```

### Querying

Queries run on collection RNs and are composed with query helpers:

```ts
const { eq, and, or, gt, like } = korm.qfns;

const cars = (await korm.item<Car>(pool)
  .from.query(korm.rn("[rn][from::cardb]:cars:suv:*"))
  .where(
    and(
      eq("color", "blue"),
      gt("year", 2010)
    )
  ).get()
).unwrap();
```

#### JSON paths and arrays

Use dot notation and bracket indexes:

```ts
.where(eq("meta.flags.hot", true))
.where(eq("owner.addresses[1].city", "Bedrock"))
.where(eq("owner.addresses[*].city", "Bedrock"))
```

### resolvePaths (typed joins)

`resolvePaths` turns RN fields into their referenced objects and updates the TypeScript type accordingly.

```ts
const cars = (await korm.item<Car>(pool)
  .from.query(korm.rn("[rn][from::cardb]:cars:suv:*"))
  .get({ resolvePaths: ["owner", "owner.addresses[*].city"] })
).unwrap();

cars[0]?.data?.owner.firstName; // owner is typed as User
```

Supported patterns:

- `owner` (resolve a single reference)
- `owner.*` (resolve all direct references on owner, no drilling)
- `owner.*.*` (resolve two levels deep)
- `owner.addresses[*].city` (resolve all array entries)

If you don't pass `resolvePaths` but query a nested RN path (e.g. `owner.username`), korm will automatically resolve just enough to filter safely. It groups RN lookups by layer to keep the number of DB round trips small.

### References and cascading updates

When you resolve an RN and modify the referenced object, korm will persist the changes **in the referenced layer**, while the parent object continues to store the RN string.

This preserves referential integrity and avoids embedding large resolved blobs in unrelated tables.

## Depots and files

Depots store files and are accessed via depot RNs.

### RN format

- File: `[rn][depot::myDepot]:partition:subpartition:file.ext`
- Prefix listing: `[rn][depot::myDepot]:partition:subpartition:*`

### Depot types

Use the `korm.depots` helpers:

```ts
const localDepot = korm.depots.local("./files");
const s3Depot = korm.depots.s3({
  bucket: "my-bucket",
  endpoint: "https://minio.local",
  accessKeyId: "...",
  secretAccessKey: "..."
});
```

`korm.depots.s3(...)` supports any S3-compatible endpoint and accepts:
- `bucket` (required; korm will create this for you if it doesn't exist)
- `endpoint`
- `region`
- `accessKeyId`
- `secretAccessKey`
- `sessionToken`
- `virtualHostedStyle`
- `prefix`
- `autoCreateBucket`
- `identifier`

### Depot files

`korm.file(...)` creates a `FloatingDepotFile`:

```ts
const invoiceFile = korm.file({
  rn: korm.rn("[rn][depot::invoiceDepot]:invoices:fred:invoice-001.txt"),
  file: new Blob(["Invoice contents"], { type: "text/plain" })
});

await invoiceFile.create(pool); // upload without any DB op
```

State machine:

- `FloatingDepotFile.create(pool)` -> `DepotFile` (committed)
- `DepotFile.update(editFn)` -> `UncommittedDepotFile`
- `UncommittedDepotFile.commit(pool)` -> `DepotFile`

When a `DepotFileLike` is present in item data, korm uploads it automatically and stores the RN string in the database.

### DepotFile RN resolution

If you resolve a depot RN field via `resolvePaths`, you'll get a `DepotFile` object instead of an RN string, so you can call `text()`, `arrayBuffer()`, or `stream()`.

## Encryption

korm provides built-in encryption with safe redaction:

- `korm.encrypt(value)` -> symmetric encryption (AES-256-GCM)
- `korm.password(value)` -> password hashing (argon2id)

Encrypted fields are stored as encrypted payloads (not cleartext). In memory, you work with `Encrypt<T>`:

```ts
const secret = await korm.encrypt("1234567890");
const pass = await korm.password("p@ssw0rd");

secret.reveal();           // cleartext
await pass.verifyPassword("p@ssw0rd");
```

Inspecting an `Encrypt` object will redact cleartext. WAL records never store cleartext values for encrypted fields.
If `depotOps: "record"` is enabled, WAL payload snapshots for depot files are stored as raw bytes in the WAL depot.

### Encryption key

Symmetric encryption uses `KORM_ENCRYPTION_KEY` (hex, 32 bytes).

```bash
generate-encryption-key
```

If the key is missing in non-production, korm generates a temporary key and warns you.

## Transactions

Use `korm.tx(...)` to persist multiple changes together:

```ts
const txRes = await korm.tx(updatedCar, newWarning).persist();
if (txRes.isErr()) throw txRes.error;
```

- On failure, korm reverts successfully applied operations.
- This is not a distributed DB transaction; failures can still leave side effects outside the DB. Use WAL for crash recovery, and enable `depotOps: "record"` if you want WAL to replay depot file writes.

### Destructive schema changes

If your model shape changes, pass `destructive: true` in `persist` to allow column recreation.

```ts
await korm.tx(item).persist({ destructive: true });
```

## Item-level locking

korm serializes operations on the same RN within a single process. `create`, `commit`, and `tx.persist` acquire locks automatically, and updates that cascade through `resolvePaths` lock all touched RNs in a stable order to avoid deadlocks.

If you need your own critical section, use the pool locker:

```ts
const release = await pool.locker.acquire(car.rn!);
try {
  // critical section
} finally {
  release();
}
```

`tryAcquire` returns `undefined` when locked. Lock acquisition waits up to 30s by default and throws `LockTimeoutError` on timeout.

If you run multiple processes that share the same pool, enable shared locks to persist locks in a source layer:

```ts
const pool = korm.pool()
  .setLayers(korm.use.layer(carDb).as("cardb"))
  .withLocks(korm.target.layer("cardb"))
  .open();
```

Shared locks are stored in `__korm_locks__` on the chosen layer. The locker still uses local mutexes, but `acquire` also uses the shared lock and refreshes a TTL. When shared locks are enabled, `tryAcquire` and `isLocked` remain local-only helpers.

## Pool metadata and discovery

If you want korm to detect mismatched configurations across processes, enable pool metadata:

```ts
const pool = korm.pool()
  .setLayers(korm.use.layer(carDb).as("cardb"))
  .withMeta(korm.target.layer("cardb"))
  .open();
```

korm stores a pool snapshot in `__korm_pool__` on the selected layer and checks every layer on startup. If a mismatch is detected, korm throws with instructions to either match the config, discover the pool, or reset it.
Backups require `withMeta(...)` because backup schedules are stored in `__korm_backups__` on the meta layer.

To recreate a pool from metadata:

```ts
const pool = await korm.discover(korm.layers.pg(process.env.PG_URL!));
```

Discovery requires the same `KORM_ENCRYPTION_KEY` because pool credentials are encrypted. Use `korm.danger.reset(pool, { mode })` only if you intend to wipe korm-managed data (`mode` can scope to `"layers"`, `"depots"`, or `"meta"` / `"meta only"`).

## Write-Ahead Log (WAL)

WAL is optional and undo/redo (undo then retry).

```ts
const pool = korm.pool()
  .setLayers(korm.use.layer(carDb).as("cardb"))
  .setDepots(korm.use.depot(walDepot).as("walDepot"))
  .withWal({
    depotIdent: "walDepot",
    walNamespace: "demo",
    retention: "keep", // keep done records for audit
    depotOps: "record" // also WAL depot file writes
  })
  .open();
```

What it does:

- Writes a WAL record before each `create`, `commit`, or `tx.persist`, including before-images.
- On startup, undoes pending WALs using before-images, then retries them and marks them done.
- Pool operations wait for WAL recovery on startup. If you read depot files directly, call `await pool.ensureWalReady()` first.
- When shared locks are enabled via `withLocks`, WAL recovery is guarded so only one instance replays at a time.
- WAL records contain encrypted payloads only (never cleartext) for encrypted fields, including before-images.
- When `depotOps: "record"` is enabled, WAL snapshots depot file payloads into the WAL depot and replays them.
- `retention: "keep"` stores done records for audit; `"delete"` removes them.

## Backups

Backups are scheduled full snapshots per layer, written to a depot. Each snapshot exports:

- All `__items__*` tables for the layer.
- `__korm_meta__` column kind metadata.
- `__korm_pool__` pool metadata (so discovery can rebuild a pool after restore).

Backups require pool metadata (`withMeta(...)`) because schedules and ownership are stored in `__korm_backups__` on the meta layer. When multiple korm instances share a pool, they coordinate by locking schedule entries so only one instance runs a given backup. Each instance also keeps in-memory timers so backups fire on time.

Backups are stored as JSON files under the depot RN prefix `__korm_backups__:{layer}:{timestamp}:backup-<uuid>.json`. Encrypted fields stay encrypted in the payload.

### Configure schedules and retention

```ts
const pool = korm.pool()
  .setLayers(korm.use.layer(carDb).as("cardb"))
  .setDepots(korm.use.depot(backupDepot).as("backups"))
  .withMeta(korm.target.layer("cardb"))
  .backups("backups")
    .addInterval("*", korm.interval.every("day").at(2, 0))
    .retain(7).days() // prune backups older than 7 days
  .open();
```

Retention is enforced after each backup run:

- `retain("all")` keeps everything.
- `retain("none")` deletes backups after each run.
- `retain(n).days()` keeps backups newer than `n` days.
- `retain(n).backups()` keeps the newest `n` backups per layer.

### Restore a backup

Use a `BackMan` instance when you need to restore. `play(...)` restores a single backup file into its matching layer (type must match), creating missing tables/columns as needed.

```ts
import { BackMan } from "@fkws/korm";

const manager = new BackMan();
const pool = korm.pool()
  .setLayers(korm.use.layer(restoreDb).as("restore"))
  .setDepots(korm.use.depot(backupDepot).as("backups"))
  .withMeta(korm.target.layer("restore"))
  .open();

pool.configureBackups("backups", manager);
await manager.play(
  korm.rn("[rn][depot::backups]:__korm_backups__:cardb:20240102T030405Z:backup-...json"),
  { mode: "replace" }
);
```

Restore modes:

- `replace` (default): clears the target tables before inserting snapshot rows.
- `merge`: inserts rows that do not exist yet (SQLite uses `INSERT OR IGNORE`, PG uses `ON CONFLICT DO NOTHING`, MySQL uses `INSERT IGNORE`).

## Schema and column kinds

korm creates tables on demand and infers column types:

- Scalars -> TEXT/INTEGER/BOOLEAN/DOUBLE
- JSON objects/arrays -> JSON columns
- RN references -> RN_REF_TEXT (SQLite) or korm_rn_ref_text (Postgres domain)
- Encrypted fields -> ENCRYPTED_JSON (SQLite) or korm_encrypted_json (Postgres domain)

MySQL stores column kinds in a metadata table (`__korm_meta__`). Long table names are shortened using a deterministic hash to fit MySQL's 64-character limit.

## Testing and development

```bash
npx tsc --noEmit
```

```bash
fish -lc "bun run test:unit"
fish -lc "bun run test:integration"
```

## API reference (high level)

### korm

- `korm.item<T>(pool)` -> `UninitializedItem<T>`
- `korm.rn(str)` -> `RN<T>`
- `korm.file({ rn, file })` -> `FloatingDepotFile`
- `korm.layers.sqlite(path)` / `pg(...)` / `mysql(...)`
- `korm.qfns` -> `{ eq, and, or, not, gt, gte, lt, lte, like, inList }`
- `korm.encrypt(value)` / `korm.password(value)`
- `korm.tx(...items)` -> Tx builder
- `korm.pool()` -> Pool builder (`setLayers`, `setDepots`, `withMeta`, `withLocks`, `withWal`, `backups`, `open`)
- `korm.use.layer(layer).as(ident)` / `korm.use.depot(depot).as(ident)` -> Named pool entries
- `korm.target.layer(ident)` -> Pool layer target for `withMeta` / `withLocks`
- `BackMan` -> backups manager (scheduling + restore with `play`)
- `korm.discover(layer)` -> Discover a pool from metadata stored in a layer
- `korm.danger.reset(pool, { mode })` -> Drop korm-managed data (`mode`: `"all" | "layers" | "depots" | "meta" | "meta only"`)
- `korm.pool(...entries, options?)` -> `LayerPool` (legacy)

### LayerPool

- `pool.close()` -> closes all layers and depots
- `pool.ensureMetaReady()` -> waits for pool metadata checks
- `pool.ensureWalReady()` -> waits for WAL recovery (if enabled)
- `pool.locker` -> `KormLocker` (`acquire`, `tryAcquire`, `acquireMultiple`, `isLocked`)
- `pool.configureBackups(depotIdent, manager)` -> attach a BackMan instance after opening a pool

### Item methods

- `FloatingItem.create()`
- `Item.update(delta)`
- `UncommittedItem.commit()`
- `Item.show({ color, what })`

### Query

- `from.query(rn).where(component).get({ resolvePaths, allowMissing })`
- `from.rn(rn, { resolvePaths, allowMissing })`

### Depots

- `korm.depots.local(rootPath)`
- `korm.depots.s3(options)`

### WAL options

```ts
withWal({
  depotIdent: string;
  walNamespace?: string;
  retention?: "keep" | "delete";
  depotOps?: "off" | "record";
})
```

### Backups options

```ts
backups(depotIdent?)
  .addInterval(layerIdent, intervalSpec)
  .retain("all" | "none" | number) // number selects .days() or .backups()
```

### Pool metadata options

```ts
withMeta(korm.target.layer("layerIdent"))
```

### Lock options

```ts
withLocks(
  korm.target.layer("layerIdent"),
  {
    ttlMs?: number;
    retryMs?: number;
    refreshMs?: number;
    ownerId?: string;
  }
)
```

Legacy `korm.pool(..., { walMode, lockMode, metaMode })` is still supported.

## Gotchas and tips

- If your pool has more than one layer, always set the `from` mod.
- Depot RN wildcards are only allowed as the last segment.
- Queries on nested RN properties may resolve and filter in memory when needed.
- `resolvePaths` can be mixed with wildcards and array indices.
- WAL uses before-images to undo and retry incomplete writes; set `depotOps: "record"` to cover depot file writes. Other external side effects are not rolled back.

## License

MPL 2.0
