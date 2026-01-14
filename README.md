# espg
What if Postgres pretends to be Elasticsearch?

## Service overview
This repository provides a minimal HTTP service that maps Elasticsearch-like APIs onto Postgres tables.
The service is implemented in Go with Fiber and `pgx`.

### Configuration
The service uses standard Postgres environment variables or a `DATABASE_URL`:

- `DATABASE_URL`
- `PGHOST`
- `PGPORT`
- `PGUSER`
- `PGPASSWORD`
- `PGDATABASE`
- `PGPOOL_SIZE`
- `PORT` (HTTP port, default `3000`)

### Running locally
```bash
go run .
```

### Framework choice (2026 snapshot)
Fiber remains a fast, battle-tested choice for high-throughput JSON APIs in 2026, so this service uses it as the default HTTP layer. If you want to compare alternatives, other modern Go options include Gin, Echo, and Chi with the standard `net/http` stack.

### Index schema
Each index maps to a Postgres table created as:

```sql
CREATE TABLE <index> (
  id TEXT PRIMARY KEY,
  document JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

### Endpoints
#### `PUT /:index`
Creates a Postgres table for the index.

#### `DELETE /:index`
Drops the Postgres table.

#### `POST /:index/_bulk`
Bulk insert/update. Accepts NDJSON payloads with `index` actions followed by documents:

```
{ "index": { "_id": "1" } }
{ "title": "Hello" }
{ "index": { "_id": "2" } }
{ "title": "World" }
```

#### `POST /:index/_search`
Supports basic queries with `term`, `match`, `range`, and `bool.must`, plus `from`/`size` pagination. Example:

```json
{
  "query": {
    "bool": {
      "must": [
        { "term": { "status": "published" } },
        { "range": { "published_at": { "gte": "2024-01-01" } } }
      ]
    }
  },
  "from": 0,
  "size": 25,
  "aggs": {
    "by_author": { "terms": { "field": "author" } },
    "by_day": { "date_histogram": { "field": "published_at", "calendar_interval": "day" } }
  }
}
```

Supported aggregations:
- `terms` → `GROUP BY document ->> field`
- `date_histogram` → `date_trunc(interval, (document ->> field)::timestamptz)`

### Future work / TODO
- Pass through selected indices (for example, an allowlist like `logs-*` or `metrics-*`) to a real Elasticsearch cluster while keeping the rest backed by Postgres.
- Automatically create indices when slow queries or aggregations are detected (for example, track query latency and trigger index creation when it consistently exceeds a threshold like 500ms for a given index/field).
