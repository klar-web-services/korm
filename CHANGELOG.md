# Changelog

## 1.3.0

Makes korm runtime-agnostic across Bun and Node.js by replacing Bun-only bindings with runtime adapters and Node-compatible build output.

**Runtime**
- Added runtime adapters for Postgres, SQLite, and S3 (`src/runtime/{pgClient,sqliteClient,s3Client}.ts`) with Bun/Node engine detection in `src/runtime/engine.ts`.
- Updated `PgLayer` and `SqliteLayer` to remove static `bun`/`bun:sqlite` imports and use runtime-neutral clients.
- Updated S3 and local depots to remove Bun-only file/driver assumptions (`Bun.S3Client`, `Bun.file`, `Bun.write`) while preserving existing depot APIs.
- Simplified `DepotBlob` to runtime-neutral payloads (`Blob` or `ReadableStream<Uint8Array>`).

**Packaging**
- Switched build target to Node-compatible ESM output while keeping Bun support at runtime.
- Externalized native/runtime drivers in the build (`argon2`, `mysql2`, `better-sqlite3`, `postgres`, `@aws-sdk/client-s3`).
- Added Node runtime dependencies for adapters (`postgres`, `better-sqlite3`, `@aws-sdk/client-s3`).

**Testing**
- Added `src/runtime/nodeCompat.unit.test.ts` to smoke-test importing `dist/index.js` under Node.
- Added `src/sources/layers/pg.unit.test.ts` and `src/sources/layers/mysql.unit.test.ts` to cover layer helper/query/schema paths without external databases.
- Added `src/runtime/nodeAdapters.unit.test.ts` to cover Node adapter branches for Postgres, SQLite, and S3 via module mocks.
- Updated `test:unit` to discover unit tests recursively via `find`, ensuring deeply nested test files are included in coverage.
- Updated unit test execution in scripts/CI to run after build and with controlled concurrency, preventing global module mocks from leaking across files.
- Added `lint:actions` (actionlint) and chained it into test scripts so workflow issues fail locally before CI.
- Removed cross-suite destructive cleanup in integration/hostile tests so they can run in parallel against shared test services without intermittent table/depot collisions.
- Kept existing unit/integration/hostile suites passing after adapter migration.

**Docs**
- Updated README runtime messaging and requirements for Bun + Node parity.

## 1.2.0

Moves public type exports under `korm.types`, removing root-level type exports, adds streaming depot uploads, and streams backups to NDJSON.

**Core API**
- Exposed public types via `korm.types` and removed root type exports.
- Added readable stream support for depot uploads.
- Streamed backups as NDJSON to avoid loading full tables into memory during backup/restore.

**Docs**
- Updated README and examples to reference `korm.types` for public types.
- Documented streaming depot uploads in `README.md`.

## 1.1.2

Avoids eager argon2 native loads by preferring Bun's password hashing, lazy-loads the argon2 fallback for non-Bun runtimes, and declares supported engines for native compatibility.

**Security**
- Prefer `Bun.password` for argon2id hashing and defer argon2 native loading until needed.

**Packaging**
- Added Bun/Node engine requirements to match argon2 native compatibility.
- Kept argon2 external in the Bun bundle to avoid hard-coded build paths for native binaries.

**Docs**
- Documented runtime requirements and password hashing behavior in `README.md`.

## 1.1.1

Adds an explicit empty item builder plus danger wrappers, and extends delete support to persist item removals with WAL coverage.

**Core API**
- Added `korm.item(pool).empty()` to create a placeholder `Item` and taught `Item.isEmpty()` to honor explicit empties.
- Added `BaseNeedsDanger`, `needsDanger`, and `danger` wrappers for guarding destructive operations behind an explicit call.
- Aligned layer deletes with insert/update conventions by erroring on missing rows and standardizing delete error messages.
- Implemented `Item.delete()` to persist deletions and return a `DeletedItem` snapshot for optional restore.
- WAL now records delete operations and replays them during recovery.
- WAL undo tolerates missing rows when deleting insert before-images, matching delete semantics.
- Added `DepotFile.delete(pool)` and WAL depot delete ops (with before-image payloads) when `depotOps: "record"` is enabled.
- Replaced `korm.danger.reset(...)` with `korm.reset(...)` (returns `BaseNeedsDanger`) and `korm.danger(...)` for execution.

**Docs**
- Documented empty items and `Item.isEmpty()` in `README.md`.
- Updated delete/WAL guidance to reflect persisted deletes and delete WAL entries.
- Documented delete/restore snapshots and `DeletedItem` helpers in `README.md`.

**Testing**
- Added local Docker compose resources and `bun run test:stage` for integration/hostile test dependencies.
- Updated CI integration/hostile jobs to stage local test resources via `test:stage`.
- Updated the release workflow to stage local test resources via `test:stage`.

## 1.1.0

Main changes center on the new pool builder naming/targeting helpers (`korm.use`/`korm.target` plus the `korm.depots` rename) and expanded WAL/tx test coverage; the rest is documentation/typing polish and formatting alignment across core and tests.

**Core API**
- Introduced branded pool builder helpers `korm.use.layer`/`korm.use.depot` (plus a direct ident overload) and `korm.target.layer`; `setLayers`/`setDepots` now validate these entries and `withLocks` accepts a target + options in `src/korm.ts`.
- Renamed depot factories to `korm.depots.*` (`local`, `s3`) in `src/korm.ts`.

**Docs & Examples**
- Updated pool setup and depot usage to the new API in `README.md`, `examples/maximal.example.ts`, `examples/minimal.example.ts`.
- Added test suite badges and a “Test Suites” section outlining unit/integration/hostile coverage in `README.md`.

**Tests & Fixtures**
- Added WAL recovery coverage (manual pending replay + crash replay), WAL tx staging across layers, and multi-layer tx persistence assertions in `src/testing/integration.test.ts`.
- Added a crash-simulation worker for WAL tests in `src/testing/wal-kill-worker.ts`.
- Updated unit/hostile tests and fixtures to the new pool builder and depot naming in `src/testing/kormFlow.unit.test.ts`, `src/sources/poolMeta.unit.test.ts`, `src/sources/backMan.unit.test.ts`, `src/testing/layerDefs.ts`, `src/testing/hostile.test.ts`.

**CI & Reporting**
- Split the GitHub Actions test workflows into unit/integration/hostile runs and publish JUnit reports with summaries in `.github/workflows/tests-*.yml`.
- Added a `release:notes` helper that prints the `CHANGELOG.md` section for a version tag.
- Fixed Postgres backup snapshots to include korm tables across non-system schemas in `src/sources/layers/pg.ts`.

**Internal Docs/Style**
- Expanded inline docs around backup intervals, pool metadata, lock storage, and encryption alias usage, plus formatting normalization in `src/sources/backMan.ts`, `src/sources/poolMeta.ts`, `src/sources/lockStore.ts`, `src/sources/layerPool.ts`, `src/security/encryption.ts`.
