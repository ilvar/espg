# espg

What if Postgres pretends to be Elasticsearch?

`espg` is a small Elasticsearch-compatible HTTP facade backed by PostgreSQL `JSONB`. The service is written in Rust with Axum, Tokio, `tokio-postgres`, `deadpool-postgres`, and `reqwest`, and is kept inside the [`strictrs`](https://github.com/ilvar/strictrs) Rust subset.

The goal is compatibility with a useful Elasticsearch subset, not a full Elasticsearch reimplementation.

## Architecture

Request handling is split into a few explicit modules:

- `src/main.rs` — process startup plus the `strictrs` capability boundary used to bind the HTTP listener.
- `src/config.rs` — environment-driven PostgreSQL, pool, HTTP, and passthrough configuration.
- `src/app.rs` — Axum routes, PostgreSQL operations, index metadata, Elasticsearch-compatible responses, and passthrough handling.
- `src/query.rs` — query/sort/aggregation translation and NDJSON parsing for bulk/msearch requests.

Each local Elasticsearch index maps to one PostgreSQL table. Documents are stored as `JSONB`; supported Elasticsearch queries are translated into parameterized PostgreSQL SQL. Index and field identifiers that are inserted into SQL are validated before use.

Mappings and settings are compatibility metadata held in memory. They do **not** survive a process restart; stored documents do.

## Configuration

PostgreSQL can be configured with `DATABASE_URL` or the standard PG environment variables:

- `DATABASE_URL`
- `PGHOST` (default `localhost`)
- `PGPORT` (default `5432`)
- `PGUSER` (default `postgres`)
- `PGPASSWORD`
- `PGDATABASE` (default `postgres`)
- `PGPOOL_SIZE` (default `10`)
- `PORT` (HTTP port, default `3000`)

Optional Elasticsearch passthrough:

- `PASSTHROUGH_URL` — base URL of a real Elasticsearch cluster.
- `PASSTHROUGH_INDICES` — comma-separated index patterns with `*` wildcards.

Example:

```bash
PASSTHROUGH_URL=https://elasticsearch.example.internal:9200
PASSTHROUGH_INDICES=logs-*,metrics-*
```

Cluster/admin passthrough endpoints use `PASSTHROUGH_URL`. Index-specific fallback is only used for indices matching `PASSTHROUGH_INDICES`.

## Run

The repository pins Rust **1.97.1** and commits `Cargo.lock`.

```bash
cargo run --locked
```

Or run the service and PostgreSQL with Docker Compose:

```bash
docker compose up --build
```

The HTTP service listens on port `3000` by default.

## PostgreSQL schema

Each local index maps to one PostgreSQL table:

```sql
CREATE TABLE <index> (
  id TEXT PRIMARY KEY,
  document JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

Index names and JSON field names used in generated SQL follow this identifier contract:

```text
[A-Za-z_][A-Za-z0-9_]*
```

Query values are passed as PostgreSQL parameters rather than interpolated into SQL.

## Implemented API surface

### Cluster compatibility

- `GET /`
- `GET /_cluster/health`
- passthrough when configured: `/_cluster/state`, `/_nodes`, `/_tasks`, `/_snapshot/*`, `/_ilm/*`, `/_security/*`, `/_alias`, `/_cat/*`

### Index APIs

- `HEAD /:index`
- `GET /:index`
- `PUT /:index`
- `DELETE /:index`
- `GET|PUT /:index/_mapping`
- `GET|PUT /:index/_settings`

### Document APIs

- `POST /:index/_doc`
- `GET|PUT|DELETE /:index/_doc/:id`
- `POST /:index/_update/:id`
- `POST /:index/_delete_by_query`
- `POST /:index/_update_by_query`
- `POST /:index/_bulk`
- `POST /:index/_mget`
- `POST /_mget`

Bulk accepts Elasticsearch-style NDJSON `index` action/source pairs.

### Search APIs

- `POST /:index/_search`
- `GET|POST /:index/_count`
- `POST /:index/_msearch`
- `POST /_msearch`

Supported queries:

- `term`
- `match`
- `terms`
- `range` with `gte` / `lte`
- `bool.must`
- `bool.filter`
- `bool.must_not`
- `bool.should`

Search also supports `from`, `size`, and basic field / `_id` sorting.

Supported aggregations:

- `terms`
- `histogram`
- `date_histogram`
- `avg`
- `min`
- `max`
- `sum`
- `stats`
- `cardinality`

Unsupported Elasticsearch endpoints and DSL features are not silently emulated. Extend the subset deliberately and add focused compatibility tests when doing so.

## Development and validation

The CI baseline is:

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features
strictrs check .
```

Install `strictrs` directly from its source repository if needed:

```bash
cargo install --git https://github.com/ilvar/strictrs --locked
```

For changes affecting PostgreSQL behavior, HTTP routing, Docker packaging, or end-to-end semantics, also run:

```bash
./scripts/run-integration-tests.sh
```

That script uses Docker Compose and `curl` to build/start PostgreSQL and `espg`, check service readiness, create an index and document, and verify a search result.

Production code follows the `strictrs` restrictions: no unsafe code, no panic APIs, no unchecked numeric casts, no wildcard imports, explicit handling of must-use values, and explicit capability boundaries for direct standard-library effects.

See [`AGENTS.md`](AGENTS.md) for repository-specific rules for coding agents.
