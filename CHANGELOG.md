# Changelog

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
- Fixed Postgres backup snapshots to respect the active schema search path in `src/sources/layers/pg.ts`.

**Internal Docs/Style**
- Expanded inline docs around backup intervals, pool metadata, lock storage, and encryption alias usage, plus formatting normalization in `src/sources/backMan.ts`, `src/sources/poolMeta.ts`, `src/sources/lockStore.ts`, `src/sources/layerPool.ts`, `src/security/encryption.ts`.
