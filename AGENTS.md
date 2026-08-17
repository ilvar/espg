# AGENTS.md

This repository is a Rust application maintained under the `ilvar/strictrs` contract.

## Project intent

`espg` is an Elasticsearch-compatible HTTP facade backed by PostgreSQL `JSONB`. Compatibility with the existing API is more important than adding broad Elasticsearch surface area.

- Preserve existing HTTP routes, response shapes, status codes, PostgreSQL behavior, and passthrough semantics unless a change is explicitly requested.
- Do not reintroduce the previous Go implementation.
- Do not turn this project into a full Elasticsearch reimplementation; add compatibility features deliberately and test them.

## Source layout

- `src/main.rs` — process startup and explicit capability boundary for direct network/process effects.
- `src/config.rs` — environment-driven PostgreSQL, pool, HTTP, and passthrough configuration.
- `src/app.rs` — Axum routes, request handling, PostgreSQL operations, metadata storage, and passthrough handling.
- `src/query.rs` — Elasticsearch query/sort/aggregation translation and NDJSON parsing.
- `scripts/run-integration-tests.sh` — Docker Compose smoke test against PostgreSQL and the built service.

Keep new code in the narrowest appropriate module rather than growing `main.rs`.

## Compatibility invariants

- Each local index maps to a PostgreSQL table with `id TEXT PRIMARY KEY`, `document JSONB NOT NULL`, and `created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`.
- Index names and JSON fields interpolated into SQL must satisfy `[A-Za-z_][A-Za-z0-9_]*`.
- Query values must remain parameterized; never interpolate user values into SQL.
- Mapping and settings compatibility metadata is intentionally in-memory and is lost on process restart; documents remain in PostgreSQL.
- `PASSTHROUGH_URL` enables upstream Elasticsearch passthrough. Index-specific passthrough is limited by `PASSTHROUGH_INDICES` wildcard patterns.
- Preserve the existing query and aggregation subset unless extending it intentionally with focused tests and README updates.

## Rust / strictrs rules

- Rust toolchain is pinned to **1.97.1** in `rust-toolchain.toml` and `Cargo.toml`.
- `Cargo.lock` is committed. Dependency and toolchain upgrades must be intentional and validated together.
- Do not use `unsafe`.
- Do not use `unwrap`, `expect`, or unchecked indexing in production code.
- Do not use numeric `as` casts or wildcard imports.
- Public functions must declare explicit return types.
- Handle `must_use` results explicitly.
- Keep direct filesystem/network/process effects inside a module marked with `strictrs: capability` when standard-library capability APIs are required.
- Prefer explicit `Result`/`Option` handling and dependency-light code.
- Do not suppress rustc, Clippy, or `strictrs` diagnostics merely to make CI green. Fix the underlying issue; document any narrowly justified lint exception beside the code.

## Tests

Behavior changes require focused tests. Keep pure query/NDJSON behavior covered close to `src/query.rs` and HTTP/application behavior close to `src/app.rs`.

For changes affecting PostgreSQL behavior, routing, Docker packaging, or end-to-end HTTP semantics, also run:

```bash
./scripts/run-integration-tests.sh
```

The integration script requires Docker Compose and `curl`; it builds the service, starts PostgreSQL, checks readiness, creates an index/document, and verifies a search result.

## Validation

Run the same core checks as CI before committing or pushing:

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features
strictrs check .
```

For API/database/container changes, run the integration smoke test as well:

```bash
./scripts/run-integration-tests.sh
```

Keep `README.md` synchronized whenever configuration, supported Elasticsearch behavior, passthrough semantics, toolchain requirements, or validation commands change.
