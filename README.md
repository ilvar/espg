# espg
What if Postgres pretends to be Elasticsearch?

## Future work
* Pass through select indices (for example, `logs-*` and `metrics-*`) directly to a real Elasticsearch cluster while routing the rest through Postgres-backed emulation.
* Automatically create Postgres-backed indices when a query or aggregation is detected as slow, based on latency thresholds (for example, queries exceeding 500ms) and logging-based detection of repeated slow patterns.
