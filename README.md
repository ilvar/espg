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

Mapped fields are stored in real typed PostgreSQL columns. Anything not in the mapping — including dynamic fields — stays in a residual `document` JSONB column, and `_source` is reassembled from both on read.

Because mapped fields are real columns, `range`, `sort`, and aggregations on them use PostgreSQL's own types. Unmapped fields are still compared as text out of JSONB, so numeric ranges and sorts on them remain lexicographic (`"10" < "9"`); map a field to get correct numeric ordering.

Settings are compatibility metadata held in memory and do **not** survive a restart. The mapping does survive: it is rebuilt from the table's columns, since the PostgreSQL catalog is the source of truth for which fields are columns.

### Mappings

`PUT /:index` accepts `mappings` and `settings` in the request body, and `PUT /:index/_mapping` adds fields to an existing index:

```bash
curl -XPUT localhost:3000/books -H 'Content-Type: application/json' -d '{
  "mappings": {"properties": {"title": {"type": "text"}, "views": {"type": "long"}}},
  "settings": {"index": {"number_of_shards": 1}}
}'

curl -XPUT localhost:3000/books/_mapping -H 'Content-Type: application/json' -d '{
  "properties": {"published_at": {"type": "date"}}
}'
```

Mapping rules:

- Field names must satisfy `[A-Za-z_][A-Za-z0-9_]*`, the same contract as index names, and may not start with the reserved prefix `_espg_col_`.
- Each field definition must be an object with a `type` of `binary`, `boolean`, `byte`, `date`, `double`, `float`, `geo_point`, `half_float`, `integer`, `ip`, `keyword`, `long`, `object`, `short`, or `text`. Other keys on the definition (such as `format`) are stored and returned unchanged.
- Subfields are **not** supported: a field definition containing `properties` or `fields` is rejected with `400`.
- `PUT /:index/_mapping` merges into the existing mapping and adds a column for each new field. Adding a field or repeating an identical definition succeeds; changing the type of an existing field is rejected with `400`, since the column type would have to change under existing data. Changes that keep the same column type (`keyword` to `text`) are allowed.
- Values are converted by PostgreSQL when written. A value that will not convert (`"abc"` into a `long`) is rejected with `400`.
- A `date` field is stored as `timestamptz` and rendered back in `_source` as `YYYY-MM-DDTHH:MM:SS.mmmZ`, so the exact input string is not necessarily preserved.
- After a restart the mapping is rebuilt from the columns, which reports the representative type for a shared column type — a `text` field reads back as `keyword`, and `ip`/`binary` read back as `keyword`.
- The body may either wrap fields in `properties` or list them directly. In the direct form, mapping-level keys (`_meta`, `_source`, `date_detection`, `dynamic`, `dynamic_templates`, `numeric_detection`, `runtime`) are accepted and ignored; a field sharing one of those names must be sent inside `properties`.
- `settings` is accepted both as `{"settings": {"index": {...}}}` and as a bare `{"settings": {...}}`.

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

Each local index maps to one PostgreSQL table, with one typed column per mapped field:

```sql
CREATE TABLE <index> (
  id TEXT PRIMARY KEY,
  document JSONB NOT NULL,          -- unmapped and dynamic fields
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  title TEXT,                       -- from {"title": {"type": "text"}}
  views BIGINT                      -- from {"views": {"type": "long"}}
);
```

Elasticsearch field types map to column types as follows:

| Elasticsearch | PostgreSQL |
| --- | --- |
| `text`, `keyword`, `ip`, `binary` | `TEXT` |
| `long` | `BIGINT` |
| `integer` | `INTEGER` |
| `short`, `byte` | `SMALLINT` |
| `double` | `DOUBLE PRECISION` |
| `float`, `half_float` | `REAL` |
| `boolean` | `BOOLEAN` |
| `date` | `TIMESTAMP WITH TIME ZONE` |
| `object`, `geo_point` | `JSONB` |

Every mapped column is indexed, alongside the column itself, so fields added later via `PUT /:index/_mapping` are indexed too:

| Elasticsearch type | Index |
| --- | --- |
| `text` | GIN with `gin_trgm_ops` |
| `object`, `geo_point` | GIN |
| everything else | btree |

`text` is the prose type, and `match` on it compiles to `ILIKE '%...%'`, which a btree cannot serve — a trigram GIN can, and it has no btree size limit. `keyword`, `ip`, and `binary` share the `TEXT` column but keep btree, because trigram GIN supports neither `=` nor `ORDER BY`.

Mapping a `text` field therefore requires the **`pg_trgm`** extension. espg runs `CREATE EXTENSION IF NOT EXISTS pg_trgm` when a mapping first needs it; if the database user is not permitted to install it, the request fails with a message naming the extension.

> **Size limit on btree-indexed columns.** A btree entry cannot exceed 2704 bytes, so writing a `keyword`, `ip`, or `binary` value larger than that (after compression) is rejected with `400 index row size ... exceeds btree version 4 maximum`. Highly compressible values are fine well past that size. Map the field as `text` if it needs to hold long values.

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
