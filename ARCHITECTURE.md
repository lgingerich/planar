# Architecture

Project-specific invariants live here; universal Rust workflow lives in `AGENTS.md`. Design detail lives in `README.md` (control/data plane, data model) and `docs/architecture/*.md` (per-topic designs such as `db_control_plane.md`, `file_formats.md`, `data_types.md`); `SPEC.md` is a stub. Many of those docs describe planned design, not committed code.

## Purpose

Planar is a transaction-based open table format: a database-backed catalog (control plane) over immutable data files (data plane). The library crate (`rlib` + `cdylib`) exposes a transactional table-metadata catalog with optimistic commits, and Arrow `RecordBatch` readers/writers for Parquet, Lance, and Vortex. It does not implement query planning, compaction, CDC, or streaming buffers; those exist only as design docs under `docs/architecture/`. The Python API (`docs/python_api.md`) is a draft; no `pyo3` code exists in `src/` yet (`build.rs` only sets macOS link args for the cdylib).

## Module Boundaries

- `src/catalog/`: table metadata and transactions. `mod.rs` holds the `Catalog` trait and a generic sqlx-based implementation; `database.rs` holds the `SqlBackend` trait / `DbKind` (SQLite vs Postgres dialect differences: write-transaction begin, row-lock clause, placeholder rewrite); `models.rs` public types; `schema.rs`, `data_type.rs` (Arrow type IPC encoding and schema-evolution rules); `limits.rs` (explicit upper bounds); `error.rs`; `helpers.rs` (private).
- `src/storage/`: format dispatch and I/O. `Reader` / `Writer` traits, `Format`, and per-format code in `file_format/{parquet,lance,vortex}.rs`; `utils.rs` is `pub(crate)`.
- `db/migrations/{sqlite,postgres}/`: schema migrations, one tree per backend (`build.rs` reruns on changes).
- Dependency direction: `catalog` depends on `storage` (`models.rs` uses `storage::Format`); `storage` must not depend on `catalog`.
- Features: `sqlite` (default) and `postgres` select backends; code and tests for each are gated with `#[cfg(feature = ...)]`.

## Core Invariants

- **Immutable version chain**: every commit creates a new transaction; point-in-time views are computed by filtering files and schemas by transaction id (no snapshot objects). Files carry `added_in` / `removed_in` lifecycle.
- **Optimistic commits**: `Catalog::commit` uses optimistic concurrency and returns `CatalogError::Conflict` on stale heads. Commits serialize on the table-head row lock (SQLite `BEGIN IMMEDIATE`; Postgres `SELECT ... FOR UPDATE`), giving a strictly monotonic per-table transaction sequence independent of UUID ordering.
- **Bounded work**: every catalog read/scan has an explicit limit in `src/catalog/limits.rs` (e.g. `MAX_FILES_PER_QUERY`, `MAX_TABLES_PER_LIST`, `MAX_TRANSACTIONS_PER_SCAN`); queries fetch limit+1 rows and error on overflow. New operations need a limit.
- **Arrow is the type system**: data types are stored via Arrow IPC schema encoding (`encode_data_type` / `decode_data_type`); schema evolution is gated by `can_evolve_to`.
- **Backend differences stay in `SqlBackend`**: catalog SQL is written once against `sqlx::Database` with canonical `?N` placeholders; pass a small backend enum (`DbKind`) into a single selector function for DB-specific limits/config (e.g. `limits::db_bind_limit`) rather than many tiny backend-specific helpers.
- **Concrete options, JSON only at persistence boundaries**: core APIs take concrete option types (`FormatReadOptions` / `FormatWriteOptions`, per-format option structs); JSON is kept only for persisted `format_options`. Reject permissive JSON shapes in core metadata/spec inputs: require an object where keys are expected. `validate_format_options` must use the same per-format parsers as real writes.
- **Format parity**: keep read/write/stream behavior aligned across formats so sync/async behavior matches at the core layer, and align helper names across format implementations (e.g. each format has `apply_read_options`).
- **Paths**: validate UTF-8 once through the shared helper (`file_format::path_to_utf8`); no silent lossy conversions.
- Prefer enums over strings for constrained domains (formats, states, operation kinds). Keep small helpers inline unless extracting prevents substantial duplication.

## Forbidden Patterns / Dependencies

- `storage` must not import from `catalog`.
- No backend-specific SQL branches scattered through catalog code; put dialect differences behind `SqlBackend` / `DbKind`.
- No unbounded catalog queries (see `limits.rs`).
- No lossy `Path` to string conversions; no JSON in core option APIs.
- Do not use `unwrap()` / `expect()` in library code or production paths (they are linted); `unwrap()` is acceptable only in tests and unreachable-by-construction internal code, with a short comment.
