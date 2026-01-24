# Changelog

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
