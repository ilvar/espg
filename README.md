# espg
What if Postgres pretends to be Elasticsearch?

## Future work
* Pass through an explicit allowlist of indices (for example, `logs-*` and `metrics-*`) to a real Elasticsearch cluster while routing everything else through Postgres-backed emulation.
* Automatically create Postgres-backed indices for slow queries and aggregations by flagging requests that exceed a latency threshold (for example, >500ms) or appear repeatedly in slow-query logs.
