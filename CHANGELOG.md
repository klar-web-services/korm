# Changelog

## 1.4.0

Redesigns query read options into a variadic helper API, adds built-in `first` and `sortBy` query options, and tightens option validation semantics.

**Core API**
- Replaced object-style read options with discriminated variadic helpers for `QueryBuilder.get(...)` and `from.rn(...)` (`src/core/query.ts`, `src/core/item.ts`).
- Added `korm.first(n?)`, `korm.sortBy(key, direction?, { allowStringify? })`, and `korm.disallowMissingReferences()` helpers, and updated `korm.resolve(...)` to return helper-option objects (`src/korm.ts`).
- Added strict option normalization with duplicate-kind rejection, invalid `first(n)` validation, and runtime rejection of query-only options on `from.rn(...)` (`src/core/query.ts`, `src/core/item.ts`).
- Added query-side sorting semantics with nested RN path resolution, direction-based null placement, wildcard sort-path rejection, and optional non-scalar stringify mode (`src/core/query.ts`).
- Added `first()`/`first(1)` single-item return mode (error on zero matches) while keeping `first(n>1)` as up-to-`n` arrays (`src/core/query.ts`).

**Typing**
- Added public option types under `korm.types` (`ResolveGetOption`, `FirstGetOption`, `SortByGetOption`, `DisallowMissingReferencesGetOption`, `QueryGetOption`, `RnGetOption`) and tuple-based return-shape inference for `get(...)` (`src/korm.ts`, `src/core/query.ts`).
- Enforced type-level restriction that `from.rn(...)` accepts only resolve/missing-reference options (`src/core/item.ts`, `src/core/query.unit.test.ts`).

**Testing**
- Added core unit coverage for option validation, sort behavior, first-result behavior, and type-level assertions (`src/core/query.unit.test.ts`).
- Added integration coverage for `first`/`sortBy`, nested RN-path sorting, missing-reference sort handling, and strict missing-reference mode (`src/testing/integration.test.ts`).
- Added hostile tests for duplicate option rejection, wildcard sort-path rejection, and runtime rejection of query-only options on `from.rn(...)` (`src/testing/hostile.test.ts`).

**Docs**
- Updated README query examples and API reference to the new variadic helper model and documented `first`/`sortBy` semantics (`README.md`).
- Updated `examples/minimal.example.ts` and `examples/maximal.example.ts` to demonstrate variadic query options, including `sortBy`, `first`, and strict missing-reference reads.

**Migration Guide (1.4.0)**
- Replace object options with variadic helper options:
  - Before: `query.get({ resolvePaths: ["owner"], allowMissing: false })`
  - After: `query.get(korm.resolve("owner"), korm.disallowMissingReferences())`
- Replace manual "first row" workarounds with built-in helpers:
  - Before: `const rows = (await query.get()).unwrap(); const one = rows[0];`
  - After: `const one = (await query.get(korm.first())).unwrap();`
- Replace manual sorting and ad-hoc typing workarounds with `korm.sortBy(...)`:
  - Before: `const rows = (await query.get()).unwrap().sort(customCompare) as MyType[];`
  - After: `const rows = (await query.get(korm.sortBy("owner.name", "asc"))).unwrap();`
- `first()` / `first(1)` now returns a single item result (and errors when no rows match), so previous custom wrappers for single-result typing can be removed.

## 1.3.2

Hardens SQLite contention handling during multi-process startup by avoiding lock-sensitive constructor PRAGMAs and applying a default SQLite lock wait policy.

**Runtime**
- Removed forced `PRAGMA journal_mode=DELETE` from `SqliteLayer` construction so layer initialization no longer attempts journal mode mutations on open (`src/sources/layers/sqlite.ts`).
- Added default `PRAGMA busy_timeout=5000` initialization for Bun (`bun:sqlite`) and Node (`better-sqlite3`) SQLite adapters (`src/runtime/sqliteClient.ts`).
- Kept SQLite CRUD/query/lock-store semantics unchanged beyond native SQLite busy timeout behavior.

**Testing**
- Added a SQLite layer regression test that holds an exclusive lock and verifies `SqliteLayer` construction succeeds without journal-mode startup mutation (`src/sources/layers/sqlite.unit.test.ts`).
- Extended runtime adapter unit coverage to assert `busy_timeout` initialization on both Node and Bun SQLite adapter paths (`src/runtime/nodeAdapters.unit.test.ts`).
- Stabilized Node dist-import compatibility test by asserting child spawn errors and relying on exit status instead of stdout text, avoiding false negatives from empty stdout capture (`src/runtime/nodeCompat.unit.test.ts`).

**Docs**
- Documented default SQLite busy timeout initialization and journal-mode preservation behavior in `README.md`.

## 1.3.1

Fixes RN reference decode consistency so unresolved RN fields read from SQL layers are hydrated back to `RN` objects at runtime.

**Runtime**
- Rehydrated RN-typed columns during row decode in SQLite, Postgres, and MySQL layers so unresolved references consistently expose `.value()` (`src/sources/layers/{sqlite,pg,mysql}.ts`).
- Kept fail-soft behavior for malformed RN-like strings by leaving invalid values as their original string payload.
- Preserved storage format parity across layers: references remain RN strings in SQL rows while reads now hydrate RN objects for RN columns.

**Testing**
- Added SQLite/PG/MySQL decode-focused unit coverage to assert RN column hydration and malformed RN pass-through (`src/sources/layers/{sqlite,pg,mysql}.unit.test.ts`).
- Added end-to-end korm flow coverage that reproduces querying typed RN references and verifies `.value()` works (`src/testing/kormFlow.unit.test.ts`).
- Updated integration assertions to validate RN object shape for unresolved RN fields where applicable (`src/testing/integration.test.ts`).

**Docs**
- Documented unresolved RN read behavior as RN objects while clarifying storage remains RN strings in `README.md`.

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
- Updated Bun adapter unit tests to mock runtime detection instead of mutating global Bun bindings, improving compatibility across Bun versions.
- Updated `test:unit` to discover unit tests recursively via `find`, ensuring deeply nested test files are included in coverage.
- Updated unit test execution in scripts/CI to run after build and with controlled concurrency, preventing global module mocks from leaking across files.
- Added `lint:actions` (actionlint) and chained it into test scripts so workflow issues fail locally before CI.
- Aligned release workflow unit test invocation with CI (recursive test discovery + controlled concurrency) to avoid release-only test flakiness.
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
