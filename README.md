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

### Missing endpoints and query features
Only the endpoints listed above are implemented. Every other Elasticsearch endpoint is currently missing, including (but not limited to):
- Index management: `PUT /:index/_mapping`, `GET /:index/_mapping`, `PUT /:index/_settings`, `GET /:index/_settings`, `POST /:index/_aliases`, `GET /_alias`, `GET /_cat/*`.
- Document APIs: `POST /:index/_update/:id`, `POST /:index/_update_by_query`, `POST /:index/_mget`, `POST /_mget`, `POST /:index/_msearch`, `POST /_msearch`.
- Search/analytics: `GET /:index/_count`, `POST /:index/_search/scroll`, `POST /_search/scroll`, `POST /_pit`, `DELETE /_pit`, `POST /_search/template`.
- Cluster/admin: `GET /_cluster/state`, `GET /_nodes`, `GET /_tasks`, `GET /_snapshot/*`, `GET /_ilm/*`, `GET /_security/*`.

Missing query features (only `term`, `match`, `range` with `gte`/`lte`, and `bool.must` are supported today):
- Boolean query clauses: `bool.filter`, `bool.should`, `bool.must_not`, `minimum_should_match`.
- Additional query types: `terms`, `match_phrase`, `match_phrase_prefix`, `multi_match`, `query_string`, `simple_query_string`, `prefix`, `wildcard`, `regexp`, `fuzzy`, `ids`, `exists`, `nested`, `geo_*`.
- Search options: `sort`, `search_after`, `track_total_hits`, `_source` filtering, `stored_fields`, `docvalue_fields`, `highlight`, `suggest`, `rescore`, `collapse`, `timeout`.
- Aggregations beyond `terms`, `histogram`, and `date_histogram` (for example `avg`, `sum`, `min`, `max`, `stats`, `extended_stats`, `cardinality`, `filters`, `composite`, `significant_terms`, `top_hits`, `nested`).
