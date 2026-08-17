# espg

What if Postgres pretends to be Elasticsearch?

`espg` is a small Elasticsearch-compatible HTTP facade backed by PostgreSQL `JSONB`. The service is written in Rust with Axum, Tokio, `tokio-postgres`, and `deadpool-postgres`, and is kept inside the [`strictrs`](https://github.com/ilvar/strictrs) Rust subset.

## Configuration

Postgres can be configured with `DATABASE_URL` or the standard PG environment variables:

- `DATABASE_URL`
- `PGHOST` (default `localhost`)
- `PGPORT` (default `5432`)
- `PGUSER` (default `postgres`)
- `PGPASSWORD`
- `PGDATABASE` (default `postgres`)
- `PGPOOL_SIZE` (default `10`)
- `PORT` (HTTP port, default `3000`)

Optional Elasticsearch passthrough:

- `PASSTHROUGH_URL` — base URL of a real Elasticsearch cluster
- `PASSTHROUGH_INDICES` — comma-separated index patterns, with `*` wildcards

Example: `PASSTHROUGH_INDICES=logs-*,metrics-*`.

## Run

```bash
cargo run
```

Or:

```bash
docker compose up --build
```

## PostgreSQL schema

Each local index maps to one PostgreSQL table:

```sql
CREATE TABLE <index> (
  id TEXT PRIMARY KEY,
  document JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

Index names and JSON field names used in generated SQL follow the existing identifier contract: `[A-Za-z_][A-Za-z0-9_]*`.

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

Mappings/settings are compatibility metadata held in memory; documents remain in PostgreSQL.

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

Queries:

- `term`
- `match`
- `terms`
- `range` with `gte` / `lte`
- `bool.must`
- `bool.filter`
- `bool.must_not`
- `bool.should`

Search also supports `from`, `size`, and basic field / `_id` sorting.

Aggregations:

- `terms`
- `histogram`
- `date_histogram`
- `avg`
- `min`
- `max`
- `sum`
- `stats`
- `cardinality`

This is intentionally an Elasticsearch compatibility subset, not a full Elasticsearch reimplementation.

## strictrs development contract

The repository pins Rust 1.97.1. Before committing:

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features
strictrs check .
```

Install the checker directly from the source repository if needed:

```bash
cargo install --git https://github.com/ilvar/strictrs
```

Production code follows the `strictrs` restrictions: no unsafe code, no panic APIs, no unchecked numeric casts, no wildcard imports, explicit handling of must-use values, and explicit capability boundaries.
