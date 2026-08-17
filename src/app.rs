use crate::config::Config;
use crate::mapping::{merge_mapping_properties, parse_mapping_properties};
use crate::query::{
    assert_identifier, build_search_plan, build_where_clause, match_pattern, parse_bulk,
    parse_multi_search, quote_identifier, Aggregation, AggregationKind,
};
use axum::body::{to_bytes, Body, Bytes};
use axum::extract::{Path, State};
use axum::http::header::{CONTENT_LENGTH, HOST};
use axum::http::{HeaderMap, Method, Request, StatusCode, Uri};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use deadpool_postgres::{Manager, Object, Pool};
use reqwest::{Client as HttpClient, Url};
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tokio_postgres::types::ToSql;
use tokio_postgres::NoTls;

const MAX_PROXY_BODY: usize = 64 * 1024 * 1024;

#[derive(Clone)]
pub struct AppState {
    pool: Pool,
    index_store: IndexStore,
    passthrough: Option<PassthroughConfig>,
    http_client: HttpClient,
}

#[derive(Clone)]
struct IndexStore {
    items: Arc<RwLock<BTreeMap<String, IndexMetadata>>>,
}

#[derive(Clone)]
struct IndexMetadata {
    mappings: Map<String, Value>,
    settings: Map<String, Value>,
}

#[derive(Clone)]
struct PassthroughConfig {
    base_url: Url,
    patterns: Vec<String>,
}

struct SearchError {
    status: StatusCode,
    message: String,
}

pub fn build_state(config: Config) -> Result<AppState, String> {
    let manager = Manager::new(config.postgres, NoTls);
    let pool = Pool::builder(manager)
        .max_size(config.pool_size)
        .build()
        .map_err(|error| format!("failed to configure postgres pool: {error}"))?;
    let passthrough = match config.passthrough_url {
        Some(raw_url) => match Url::parse(&raw_url) {
            Ok(base_url) => Some(PassthroughConfig {
                base_url,
                patterns: config.passthrough_indices,
            }),
            Err(error) => {
                eprintln!("invalid PASSTHROUGH_URL: {error}");
                None
            }
        },
        None => None,
    };
    let http_client = HttpClient::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .map_err(|error| format!("failed to build passthrough client: {error}"))?;

    Ok(AppState {
        pool,
        index_store: IndexStore::new(),
        passthrough,
        http_client,
    })
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/", get(root))
        .route("/_cluster/health", get(cluster_health))
        .route("/_mget", post(multi_get_global))
        .route("/_msearch", post(multi_search_global))
        .route(
            "/{index}",
            get(get_index)
                .put(create_index)
                .delete(delete_index)
                .head(head_index),
        )
        .route("/{index}/_mapping", get(get_mapping).put(put_mapping))
        .route("/{index}/_settings", get(get_settings).put(put_settings))
        .route("/{index}/_doc", post(create_document))
        .route(
            "/{index}/_doc/{id}",
            get(get_document)
                .put(upsert_document)
                .delete(delete_document),
        )
        .route("/{index}/_update/{id}", post(update_document))
        .route("/{index}/_delete_by_query", post(delete_by_query))
        .route("/{index}/_update_by_query", post(update_by_query))
        .route("/{index}/_bulk", post(bulk))
        .route("/{index}/_search", post(search))
        .route("/{index}/_count", get(count).post(count))
        .route("/{index}/_mget", post(multi_get_index))
        .route("/{index}/_msearch", post(multi_search_index))
        .fallback(fallback)
        .layer(middleware::from_fn_with_state(
            state.clone(),
            index_passthrough_middleware,
        ))
        .with_state(state)
}

async fn root() -> Response {
    json_response(
        StatusCode::OK,
        json!({
            "name": "espg",
            "cluster_name": "espg",
            "version": {
                "number": "8.0.0",
                "build_flavor": "default",
                "build_type": "docker",
                "build_snapshot": true
            },
            "tagline": "You Know, for Search"
        }),
    )
}

async fn cluster_health() -> Response {
    json_response(
        StatusCode::OK,
        json!({
            "cluster_name": "espg",
            "status": "green",
            "number_of_nodes": 1,
            "active_primary_shards": 0,
            "active_shards": 0,
            "relocating_shards": 0,
            "initializing_shards": 0,
            "unassigned_shards": 0,
            "delayed_unassigned_shards": 0,
            "number_of_pending_tasks": 0,
            "number_of_in_flight_fetch": 0,
            "task_max_waiting_in_queue_millis": 0,
            "active_shards_percent_as_number": 100
        }),
    )
}

async fn head_index(State(state): State<AppState>, Path(index): Path<String>) -> Response {
    if let Err(response) = validate_index(&index) {
        return response;
    }
    match index_exists(&state, &index).await {
        Ok(true) => StatusCode::OK.into_response(),
        Ok(false) => StatusCode::NOT_FOUND.into_response(),
        Err(response) => response,
    }
}

async fn get_index(State(state): State<AppState>, Path(index): Path<String>) -> Response {
    if let Err(response) = validate_index(&index) {
        return response;
    }
    match index_exists(&state, &index).await {
        Ok(true) => {}
        Ok(false) => return api_error(StatusCode::NOT_FOUND, "index not found"),
        Err(response) => return response,
    }

    let (mapping, settings) = state.index_store.get(&index).await;
    let mut index_object = Map::new();
    index_object.insert("aliases".to_owned(), Value::Object(Map::new()));
    index_object.insert(
        "mappings".to_owned(),
        json!({"properties": Value::Object(mapping)}),
    );
    index_object.insert(
        "settings".to_owned(),
        json!({
            "index": {
                "number_of_shards": settings.get("number_of_shards").cloned().unwrap_or(Value::Null),
                "number_of_replicas": settings.get("number_of_replicas").cloned().unwrap_or(Value::Null)
            }
        }),
    );
    let mut payload = Map::new();
    payload.insert(index, Value::Object(index_object));
    json_response(StatusCode::OK, Value::Object(payload))
}

async fn create_index(
    State(state): State<AppState>,
    Path(index): Path<String>,
    body: Bytes,
) -> Response {
    if let Err(response) = validate_index(&index) {
        return response;
    }
    let body = match parse_optional_object(&body) {
        Ok(body) => body,
        Err(response) => return response,
    };
    // Validate before creating the table so a rejected mapping leaves no index behind.
    let mappings = match body.get("mappings") {
        Some(mappings) => match parse_mapping_properties(mappings) {
            Ok(mappings) => mappings,
            Err(error) => return api_error(StatusCode::BAD_REQUEST, &error),
        },
        None => Map::new(),
    };
    let settings = index_settings(body.get("settings"));

    let table = quote_identifier(&index);
    let sql = format!(
        "CREATE TABLE IF NOT EXISTS {table} (id TEXT PRIMARY KEY, document JSONB NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW())"
    );
    let client = match db_client(&state).await {
        Ok(client) => client,
        Err(response) => return response,
    };
    if let Err(error) = client.execute(&sql, &[]).await {
        return api_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string());
    }
    state.index_store.ensure(&index).await;
    if !mappings.is_empty() {
        state.index_store.set_mapping(&index, mappings).await;
    }
    if !settings.is_empty() {
        state.index_store.set_settings(&index, settings).await;
    }
    json_response(
        StatusCode::OK,
        json!({"acknowledged": true, "index": index}),
    )
}

/// Accept both `{"settings": {"index": {...}}}` and a bare `{"settings": {...}}`.
fn index_settings(settings: Option<&Value>) -> Map<String, Value> {
    let Some(settings) = settings.and_then(Value::as_object) else {
        return Map::new();
    };
    match settings.get("index").and_then(Value::as_object) {
        Some(nested) => nested.clone(),
        None => settings.clone(),
    }
}

async fn delete_index(State(state): State<AppState>, Path(index): Path<String>) -> Response {
    if let Err(response) = validate_index(&index) {
        return response;
    }
    let table = quote_identifier(&index);
    let sql = format!("DROP TABLE IF EXISTS {table}");
    let client = match db_client(&state).await {
        Ok(client) => client,
        Err(response) => return response,
    };
    if let Err(error) = client.execute(&sql, &[]).await {
        return api_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string());
    }
    state.index_store.delete(&index).await;
    json_response(
        StatusCode::OK,
        json!({"acknowledged": true, "index": index}),
    )
}

async fn create_document(
    State(state): State<AppState>,
    Path(index): Path<String>,
    body: Bytes,
) -> Response {
    if let Err(response) = validate_existing_index(&state, &index).await {
        return response;
    }
    let document = match parse_required_document(&body) {
        Ok(document) => document,
        Err(response) => return response,
    };
    let id = match generated_id(0) {
        Ok(id) => id,
        Err(error) => return api_error(StatusCode::INTERNAL_SERVER_ERROR, &error),
    };
    upsert_document_response(&state, &index, &id, document).await
}

async fn upsert_document(
    State(state): State<AppState>,
    Path((index, id)): Path<(String, String)>,
    body: Bytes,
) -> Response {
    if let Err(response) = validate_existing_index(&state, &index).await {
        return response;
    }
    let document = match parse_required_document(&body) {
        Ok(document) => document,
        Err(response) => return response,
    };
    upsert_document_response(&state, &index, &id, document).await
}

async fn upsert_document_response(
    state: &AppState,
    index: &str,
    id: &str,
    document: Map<String, Value>,
) -> Response {
    let client = match db_client(state).await {
        Ok(client) => client,
        Err(response) => return response,
    };
    let table = quote_identifier(index);
    let sql = format!(
        "INSERT INTO {table} (id, document) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET document = EXCLUDED.document RETURNING (xmax = 0)"
    );
    let value = Value::Object(document);
    let params: [&(dyn ToSql + Sync); 2] = [&id, &value];
    let row = match client.query_one(&sql, &params).await {
        Ok(row) => row,
        Err(error) => return api_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string()),
    };
    let inserted = match row.try_get::<_, bool>(0) {
        Ok(inserted) => inserted,
        Err(error) => return api_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string()),
    };
    let status = if inserted {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    let result = if inserted { "created" } else { "updated" };
    json_response(
        status,
        json!({"_index": index, "_id": id, "result": result}),
    )
}

async fn get_document(
    State(state): State<AppState>,
    Path((index, id)): Path<(String, String)>,
) -> Response {
    if let Err(response) = validate_existing_index(&state, &index).await {
        return response;
    }
    let client = match db_client(&state).await {
        Ok(client) => client,
        Err(response) => return response,
    };
    let table = quote_identifier(&index);
    let sql = format!("SELECT document FROM {table} WHERE id = $1");
    let row = match client.query_opt(&sql, &[&id]).await {
        Ok(row) => row,
        Err(error) => return api_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string()),
    };
    let Some(row) = row else {
        return json_response(
            StatusCode::NOT_FOUND,
            json!({"_index": index, "_id": id, "found": false}),
        );
    };
    let document = match row.try_get::<_, Value>(0) {
        Ok(document) => document,
        Err(error) => return api_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string()),
    };
    json_response(
        StatusCode::OK,
        json!({"_index": index, "_id": id, "found": true, "_source": document}),
    )
}

async fn delete_document(
    State(state): State<AppState>,
    Path((index, id)): Path<(String, String)>,
) -> Response {
    if let Err(response) = validate_existing_index(&state, &index).await {
        return response;
    }
    let client = match db_client(&state).await {
        Ok(client) => client,
        Err(response) => return response,
    };
    let table = quote_identifier(&index);
    let sql = format!("DELETE FROM {table} WHERE id = $1");
    let affected = match client.execute(&sql, &[&id]).await {
        Ok(affected) => affected,
        Err(error) => return api_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string()),
    };
    if affected == 0 {
        return json_response(
            StatusCode::NOT_FOUND,
            json!({"_index": index, "_id": id, "result": "not_found"}),
        );
    }
    json_response(
        StatusCode::OK,
        json!({"_index": index, "_id": id, "result": "deleted"}),
    )
}

async fn delete_by_query(
    State(state): State<AppState>,
    Path(index): Path<String>,
    body: Bytes,
) -> Response {
    if let Err(response) = validate_existing_index(&state, &index).await {
        return response;
    }
    let body = match parse_optional_object(&body) {
        Ok(body) => body,
        Err(response) => return response,
    };
    let mut values = Vec::new();
    let where_clause = match build_where_clause(body.get("query"), &mut values) {
        Ok(clause) => clause,
        Err(error) => return api_error(StatusCode::BAD_REQUEST, &error),
    };
    let table = quote_identifier(&index);
    let sql = format!("DELETE FROM {table} {where_clause}");
    let client = match db_client(&state).await {
        Ok(client) => client,
        Err(response) => return response,
    };
    let params = text_params(&values);
    let affected = match client.execute(&sql, &params).await {
        Ok(affected) => affected,
        Err(error) => return api_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string()),
    };
    json_response(StatusCode::OK, json!({"deleted": affected}))
}

async fn update_document(
    State(state): State<AppState>,
    Path((index, id)): Path<(String, String)>,
    body: Bytes,
) -> Response {
    if let Err(response) = validate_existing_index(&state, &index).await {
        return response;
    }
    let body = match parse_optional_object(&body) {
        Ok(body) => body,
        Err(response) => return response,
    };
    let Some(doc) = body
        .get("doc")
        .and_then(Value::as_object)
        .filter(|doc| !doc.is_empty())
    else {
        return api_error(StatusCode::BAD_REQUEST, "update requires doc");
    };
    let doc_as_upsert = body
        .get("doc_as_upsert")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let has_upsert = body
        .get("upsert")
        .and_then(Value::as_object)
        .is_some_and(|upsert| !upsert.is_empty());

    let mut client = match db_client(&state).await {
        Ok(client) => client,
        Err(response) => return response,
    };
    let table = quote_identifier(&index);
    let update_sql =
        format!("UPDATE {table} SET document = document || $1 WHERE id = $2 RETURNING id");
    let doc_value = Value::Object(doc.clone());
    let params: [&(dyn ToSql + Sync); 2] = [&doc_value, &id];
    let row = match client.query_opt(&update_sql, &params).await {
        Ok(row) => row,
        Err(error) => return api_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string()),
    };
    if row.is_some() {
        return json_response(
            StatusCode::OK,
            json!({"_index": index, "_id": id, "result": "updated"}),
        );
    }
    if doc_as_upsert || has_upsert {
        return upsert_document_with_client(&mut client, &index, &id, doc_value).await;
    }
    json_response(
        StatusCode::NOT_FOUND,
        json!({"_index": index, "_id": id, "result": "not_found"}),
    )
}

async fn upsert_document_with_client(
    client: &mut Object,
    index: &str,
    id: &str,
    document: Value,
) -> Response {
    let table = quote_identifier(index);
    let sql = format!(
        "INSERT INTO {table} (id, document) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET document = EXCLUDED.document RETURNING (xmax = 0)"
    );
    let params: [&(dyn ToSql + Sync); 2] = [&id, &document];
    let row = match client.query_one(&sql, &params).await {
        Ok(row) => row,
        Err(error) => return api_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string()),
    };
    let inserted = match row.try_get::<_, bool>(0) {
        Ok(inserted) => inserted,
        Err(error) => return api_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string()),
    };
    let status = if inserted {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    let result = if inserted { "created" } else { "updated" };
    json_response(
        status,
        json!({"_index": index, "_id": id, "result": result}),
    )
}

async fn update_by_query(
    State(state): State<AppState>,
    Path(index): Path<String>,
    body: Bytes,
) -> Response {
    if let Err(response) = validate_existing_index(&state, &index).await {
        return response;
    }
    let body = match parse_optional_object(&body) {
        Ok(body) => body,
        Err(response) => return response,
    };
    let Some(doc) = body
        .get("doc")
        .and_then(Value::as_object)
        .filter(|doc| !doc.is_empty())
    else {
        return api_error(StatusCode::BAD_REQUEST, "update_by_query requires doc");
    };
    let mut numbered_values = vec![String::new()];
    let where_clause = match build_where_clause(body.get("query"), &mut numbered_values) {
        Ok(clause) => clause,
        Err(error) => return api_error(StatusCode::BAD_REQUEST, &error),
    };
    let values = numbered_values.into_iter().skip(1).collect::<Vec<String>>();
    let table = quote_identifier(&index);
    let sql = format!("UPDATE {table} SET document = document || $1 {where_clause}");
    let client = match db_client(&state).await {
        Ok(client) => client,
        Err(response) => return response,
    };
    let doc_value = Value::Object(doc.clone());
    let text_params = text_params(&values);
    let mut params: Vec<&(dyn ToSql + Sync)> = Vec::with_capacity(text_params.len() + 1);
    params.push(&doc_value);
    params.extend(text_params);
    let affected = match client.execute(&sql, &params).await {
        Ok(affected) => affected,
        Err(error) => return api_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string()),
    };
    json_response(StatusCode::OK, json!({"updated": affected}))
}

async fn bulk(State(state): State<AppState>, Path(index): Path<String>, body: Bytes) -> Response {
    if let Err(response) = validate_index(&index) {
        return response;
    }
    let operations = match parse_bulk(&body) {
        Ok(operations) => operations,
        Err(error) => return api_error(StatusCode::BAD_REQUEST, &error),
    };
    if operations.is_empty() {
        return json_response(StatusCode::OK, json!({"errors": false, "items": []}));
    }

    let mut client = match db_client(&state).await {
        Ok(client) => client,
        Err(response) => return response,
    };
    let transaction = match client.transaction().await {
        Ok(transaction) => transaction,
        Err(error) => return api_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string()),
    };
    let table = quote_identifier(&index);
    let sql = format!(
        "INSERT INTO {table} (id, document) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET document = EXCLUDED.document"
    );
    let mut items = Vec::with_capacity(operations.len());
    for (position, operation) in operations.into_iter().enumerate() {
        let id = match operation.id {
            Some(id) => id,
            None => {
                let offset = match u128::try_from(position) {
                    Ok(offset) => offset,
                    Err(error) => {
                        return api_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string())
                    }
                };
                match generated_id(offset) {
                    Ok(id) => id,
                    Err(error) => return api_error(StatusCode::INTERNAL_SERVER_ERROR, &error),
                }
            }
        };
        let source = Value::Object(operation.source);
        let params: [&(dyn ToSql + Sync); 2] = [&id, &source];
        if let Err(error) = transaction.execute(&sql, &params).await {
            return api_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string());
        }
        items.push(json!({"index": {"_id": id, "status": 201}}));
    }
    if let Err(error) = transaction.commit().await {
        return api_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string());
    }
    json_response(StatusCode::OK, json!({"errors": false, "items": items}))
}

async fn search(State(state): State<AppState>, Path(index): Path<String>, body: Bytes) -> Response {
    if let Err(response) = validate_existing_index(&state, &index).await {
        return response;
    }
    let body = match parse_optional_object(&body) {
        Ok(body) => body,
        Err(response) => return response,
    };
    match execute_search(&state, &index, &body).await {
        Ok(payload) => json_response(StatusCode::OK, payload),
        Err(error) => api_error(error.status, &error.message),
    }
}

async fn count(State(state): State<AppState>, Path(index): Path<String>, body: Bytes) -> Response {
    if let Err(response) = validate_existing_index(&state, &index).await {
        return response;
    }
    let body = match parse_optional_object(&body) {
        Ok(body) => body,
        Err(response) => return response,
    };
    let mut values = Vec::new();
    let where_clause = match build_where_clause(body.get("query"), &mut values) {
        Ok(clause) => clause,
        Err(error) => return api_error(StatusCode::BAD_REQUEST, &error),
    };
    let table = quote_identifier(&index);
    let sql = format!("SELECT COUNT(*)::bigint FROM {table} {where_clause}");
    let client = match db_client(&state).await {
        Ok(client) => client,
        Err(response) => return response,
    };
    let params = text_params(&values);
    let row = match client.query_one(&sql, &params).await {
        Ok(row) => row,
        Err(error) => return api_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string()),
    };
    let count = match row.try_get::<_, i64>(0) {
        Ok(count) => count,
        Err(error) => return api_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string()),
    };
    json_response(
        StatusCode::OK,
        json!({
            "count": count,
            "_shards": {"total": 1, "successful": 1, "skipped": 0, "failed": 0}
        }),
    )
}

async fn get_mapping(State(state): State<AppState>, Path(index): Path<String>) -> Response {
    if let Err(response) = validate_existing_index(&state, &index).await {
        return response;
    }
    let (mapping, _) = state.index_store.get(&index).await;
    let mut payload = Map::new();
    payload.insert(index, json!({"mappings": {"properties": mapping}}));
    json_response(StatusCode::OK, Value::Object(payload))
}

async fn put_mapping(
    State(state): State<AppState>,
    Path(index): Path<String>,
    body: Bytes,
) -> Response {
    if let Err(response) = validate_existing_index(&state, &index).await {
        return response;
    }
    let body = match parse_optional_object(&body) {
        Ok(body) => body,
        Err(response) => return response,
    };
    let incoming = match parse_mapping_properties(&Value::Object(body)) {
        Ok(incoming) => incoming,
        Err(error) => return api_error(StatusCode::BAD_REQUEST, &error),
    };
    match state.index_store.merge_mapping(&index, &incoming).await {
        Ok(()) => json_response(StatusCode::OK, json!({"acknowledged": true})),
        Err(error) => api_error(StatusCode::BAD_REQUEST, &error),
    }
}

async fn get_settings(State(state): State<AppState>, Path(index): Path<String>) -> Response {
    if let Err(response) = validate_existing_index(&state, &index).await {
        return response;
    }
    let (_, settings) = state.index_store.get(&index).await;
    let mut payload = Map::new();
    payload.insert(index, json!({"settings": {"index": settings}}));
    json_response(StatusCode::OK, Value::Object(payload))
}

async fn put_settings(
    State(state): State<AppState>,
    Path(index): Path<String>,
    body: Bytes,
) -> Response {
    if let Err(response) = validate_existing_index(&state, &index).await {
        return response;
    }
    let body = match parse_optional_object(&body) {
        Ok(body) => body,
        Err(response) => return response,
    };
    let settings = body
        .get("index")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    state.index_store.set_settings(&index, settings).await;
    json_response(StatusCode::OK, json!({"acknowledged": true}))
}

async fn multi_get_index(
    State(state): State<AppState>,
    Path(index): Path<String>,
    body: Bytes,
) -> Response {
    if let Err(response) = validate_index(&index) {
        return response;
    }
    multi_get(&state, Some(index), &body).await
}

async fn multi_get_global(State(state): State<AppState>, body: Bytes) -> Response {
    multi_get(&state, None, &body).await
}

async fn multi_get(state: &AppState, route_index: Option<String>, body: &[u8]) -> Response {
    let body = match parse_optional_object(body) {
        Ok(body) => body,
        Err(response) => return response,
    };
    let mut requests = Vec::new();
    if let Some(docs) = body.get("docs").and_then(Value::as_array) {
        for doc in docs {
            if let Some(object) = doc.as_object() {
                requests.push(object.clone());
            }
        }
    } else if let Some(ids) = body.get("ids").and_then(Value::as_array) {
        for id in ids {
            let mut request = Map::new();
            request.insert("_id".to_owned(), id.clone());
            requests.push(request);
        }
    }
    if requests.is_empty() {
        return api_error(StatusCode::BAD_REQUEST, "mget requires docs or ids");
    }

    let mut grouped: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut order = Vec::new();
    for request in requests {
        let target_index = match &route_index {
            Some(index) => index.clone(),
            None => match request.get("_index").and_then(Value::as_str) {
                Some(index) if !index.is_empty() => index.to_owned(),
                Some(_) | None => return api_error(StatusCode::BAD_REQUEST, "mget requires index"),
            },
        };
        if let Err(error) = assert_identifier(&target_index, "index name") {
            return api_error(StatusCode::BAD_REQUEST, &error);
        }
        let id = request
            .get("_id")
            .map(value_to_id)
            .unwrap_or_else(|| "<nil>".to_owned());
        grouped
            .entry(target_index.clone())
            .or_default()
            .push(id.clone());
        order.push((target_index, id));
    }

    let client = match db_client(state).await {
        Ok(client) => client,
        Err(response) => return response,
    };
    let mut found: BTreeMap<String, BTreeMap<String, Value>> = BTreeMap::new();
    for (index, ids) in grouped {
        let exists = match index_exists_with_client(&client, &index).await {
            Ok(exists) => exists,
            Err(response) => return response,
        };
        if !exists {
            found.insert(index, BTreeMap::new());
            continue;
        }
        let table = quote_identifier(&index);
        let sql = format!("SELECT id, document FROM {table} WHERE id = ANY($1)");
        let rows = match client.query(&sql, &[&ids]).await {
            Ok(rows) => rows,
            Err(error) => return api_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string()),
        };
        let mut documents = BTreeMap::new();
        for row in rows {
            let id = match row.try_get::<_, String>(0) {
                Ok(id) => id,
                Err(error) => {
                    return api_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string())
                }
            };
            let document = match row.try_get::<_, Value>(1) {
                Ok(document) => document,
                Err(error) => {
                    return api_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string())
                }
            };
            documents.insert(id, document);
        }
        found.insert(index, documents);
    }

    let mut responses = Vec::new();
    for (index, id) in order {
        let document = found.get(&index).and_then(|documents| documents.get(&id));
        match document {
            Some(document) => responses.push(json!({
                "_index": index,
                "_id": id,
                "found": true,
                "_source": document
            })),
            None => responses.push(json!({"_index": index, "_id": id, "found": false})),
        }
    }
    json_response(StatusCode::OK, json!({"docs": responses}))
}

async fn multi_search_index(
    State(state): State<AppState>,
    Path(index): Path<String>,
    body: Bytes,
) -> Response {
    if let Err(response) = validate_index(&index) {
        return response;
    }
    multi_search(&state, Some(index), &body).await
}

async fn multi_search_global(State(state): State<AppState>, body: Bytes) -> Response {
    multi_search(&state, None, &body).await
}

async fn multi_search(state: &AppState, route_index: Option<String>, body: &[u8]) -> Response {
    let queries = match parse_multi_search(body) {
        Ok(queries) => queries,
        Err(error) => return api_error(StatusCode::BAD_REQUEST, &error),
    };
    let mut responses = Vec::new();
    for query in queries {
        let target_index = if query.index.is_empty() {
            route_index.clone().unwrap_or_default()
        } else {
            query.index
        };
        if target_index.is_empty() {
            responses.push(json!({
                "error": {"type": "index_missing_exception", "reason": "index missing"}
            }));
            continue;
        }
        if let Err(error) = assert_identifier(&target_index, "index name") {
            responses.push(json!({
                "error": {"type": "invalid_index_name_exception", "reason": error}
            }));
            continue;
        }
        let exists = match index_exists(state, &target_index).await {
            Ok(exists) => exists,
            Err(response) => return response,
        };
        if !exists {
            responses.push(json!({
                "error": {"type": "index_not_found_exception", "reason": "index not found"}
            }));
            continue;
        }
        match execute_search(state, &target_index, &query.body).await {
            Ok(payload) => responses.push(payload),
            Err(error) => responses.push(json!({
                "error": {"type": "search_phase_execution_exception", "reason": error.message},
                "status": error.status.as_u16()
            })),
        }
    }
    json_response(StatusCode::OK, json!({"responses": responses}))
}

async fn execute_search(
    state: &AppState,
    index: &str,
    body: &Map<String, Value>,
) -> Result<Value, SearchError> {
    let table = quote_identifier(index);
    let plan = build_search_plan(body, &table).map_err(|message| SearchError {
        status: StatusCode::BAD_REQUEST,
        message,
    })?;
    let sql = format!(
        "SELECT id, document FROM {table} {} {} LIMIT {} OFFSET {}",
        plan.where_clause, plan.sort_clause, plan.size, plan.from
    );
    let client = state.pool.get().await.map_err(|error| SearchError {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        message: error.to_string(),
    })?;
    let params = text_params(&plan.values);
    let rows = client
        .query(&sql, &params)
        .await
        .map_err(|error| SearchError {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: error.to_string(),
        })?;
    let mut hits = Vec::new();
    for row in rows {
        let id = row.try_get::<_, String>(0).map_err(|error| SearchError {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: error.to_string(),
        })?;
        let document = row.try_get::<_, Value>(1).map_err(|error| SearchError {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: error.to_string(),
        })?;
        hits.push(json!({"_id": id, "_source": document}));
    }
    let total = hits.len();
    let aggregations = match plan.aggregation {
        Some(aggregation) => execute_aggregation(&client, aggregation, &plan.values).await?,
        None => Map::new(),
    };
    Ok(json!({
        "hits": {"total": total, "hits": hits},
        "aggregations": aggregations
    }))
}

async fn execute_aggregation(
    client: &Object,
    aggregation: Aggregation,
    values: &[String],
) -> Result<Map<String, Value>, SearchError> {
    let params = text_params(values);
    let mut result = Map::new();
    match aggregation.kind {
        AggregationKind::Terms => {
            let rows = query_agg_rows(client, &aggregation.sql, &params).await?;
            let mut buckets = Vec::new();
            for row in rows {
                let key = row
                    .try_get::<_, Option<String>>(0)
                    .map_err(db_search_error)?;
                let count = row.try_get::<_, i64>(1).map_err(db_search_error)?;
                buckets.push(json!({"key": key, "doc_count": count}));
            }
            result.insert(aggregation.name, json!({"buckets": buckets}));
        }
        AggregationKind::Histogram => {
            let rows = query_agg_rows(client, &aggregation.sql, &params).await?;
            let mut buckets = Vec::new();
            for row in rows {
                let key = row.try_get::<_, Option<f64>>(0).map_err(db_search_error)?;
                let count = row.try_get::<_, i64>(1).map_err(db_search_error)?;
                buckets.push(json!({"key": key, "doc_count": count}));
            }
            result.insert(aggregation.name, json!({"buckets": buckets}));
        }
        AggregationKind::DateHistogram => {
            let rows = query_agg_rows(client, &aggregation.sql, &params).await?;
            let mut buckets = Vec::new();
            for row in rows {
                let key = row.try_get::<_, Option<f64>>(0).map_err(db_search_error)?;
                let key_as_string = row
                    .try_get::<_, Option<String>>(1)
                    .map_err(db_search_error)?;
                let count = row.try_get::<_, i64>(2).map_err(db_search_error)?;
                buckets.push(json!({
                    "key": key,
                    "key_as_string": key_as_string,
                    "doc_count": count
                }));
            }
            result.insert(aggregation.name, json!({"buckets": buckets}));
        }
        AggregationKind::Avg
        | AggregationKind::Min
        | AggregationKind::Max
        | AggregationKind::Sum => {
            let row = client
                .query_one(&aggregation.sql, &params)
                .await
                .map_err(db_search_error)?;
            let value = row.try_get::<_, Option<f64>>(0).map_err(db_search_error)?;
            result.insert(aggregation.name, json!({"value": value}));
        }
        AggregationKind::Stats => {
            let row = client
                .query_one(&aggregation.sql, &params)
                .await
                .map_err(db_search_error)?;
            let count = row.try_get::<_, i64>(0).map_err(db_search_error)?;
            let min = row.try_get::<_, Option<f64>>(1).map_err(db_search_error)?;
            let max = row.try_get::<_, Option<f64>>(2).map_err(db_search_error)?;
            let avg = row.try_get::<_, Option<f64>>(3).map_err(db_search_error)?;
            let sum = row.try_get::<_, Option<f64>>(4).map_err(db_search_error)?;
            result.insert(
                aggregation.name,
                json!({"count": count, "min": min, "max": max, "avg": avg, "sum": sum}),
            );
        }
        AggregationKind::Cardinality => {
            let row = client
                .query_one(&aggregation.sql, &params)
                .await
                .map_err(db_search_error)?;
            let value = row.try_get::<_, i64>(0).map_err(db_search_error)?;
            result.insert(aggregation.name, json!({"value": value}));
        }
    }
    Ok(result)
}

async fn query_agg_rows(
    client: &Object,
    sql: &str,
    params: &[&(dyn ToSql + Sync)],
) -> Result<Vec<tokio_postgres::Row>, SearchError> {
    client.query(sql, params).await.map_err(db_search_error)
}

fn db_search_error(error: tokio_postgres::Error) -> SearchError {
    SearchError {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        message: error.to_string(),
    }
}

async fn validate_existing_index(state: &AppState, index: &str) -> Result<(), Response> {
    validate_index(index)?;
    match index_exists(state, index).await {
        Ok(true) => Ok(()),
        Ok(false) => Err(api_error(StatusCode::NOT_FOUND, "index not found")),
        Err(response) => Err(response),
    }
}

#[allow(clippy::result_large_err)]
fn validate_index(index: &str) -> Result<(), Response> {
    assert_identifier(index, "index name")
        .map_err(|error| api_error(StatusCode::BAD_REQUEST, &error))
}

async fn index_exists(state: &AppState, index: &str) -> Result<bool, Response> {
    let client = db_client(state).await?;
    index_exists_with_client(&client, index).await
}

async fn index_exists_with_client(client: &Object, index: &str) -> Result<bool, Response> {
    let relation = format!("public.{index}");
    let row = client
        .query_one("SELECT to_regclass($1) IS NOT NULL", &[&relation])
        .await
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string()))?;
    row.try_get::<_, bool>(0)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string()))
}

async fn db_client(state: &AppState) -> Result<Object, Response> {
    state
        .pool
        .get()
        .await
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string()))
}

fn text_params(values: &[String]) -> Vec<&(dyn ToSql + Sync)> {
    let mut params: Vec<&(dyn ToSql + Sync)> = Vec::with_capacity(values.len());
    for value in values {
        params.push(value);
    }
    params
}

#[allow(clippy::result_large_err)]
fn parse_required_document(body: &[u8]) -> Result<Map<String, Value>, Response> {
    if body.is_empty() {
        return Err(api_error(StatusCode::BAD_REQUEST, "document body required"));
    }
    let document = serde_json::from_slice::<Map<String, Value>>(body)
        .map_err(|_| api_error(StatusCode::BAD_REQUEST, "invalid json body"))?;
    if document.is_empty() {
        return Err(api_error(StatusCode::BAD_REQUEST, "document body required"));
    }
    Ok(document)
}

#[allow(clippy::result_large_err)]
fn parse_optional_object(body: &[u8]) -> Result<Map<String, Value>, Response> {
    if body.is_empty() {
        return Ok(Map::new());
    }
    serde_json::from_slice::<Map<String, Value>>(body)
        .map_err(|_| api_error(StatusCode::BAD_REQUEST, "invalid json body"))
}

fn generated_id(offset: u128) -> Result<String, String> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| format!("system clock before unix epoch: {error}"))?;
    let value = duration
        .as_nanos()
        .checked_add(offset)
        .ok_or_else(|| "generated id overflow".to_owned())?;
    Ok(value.to_string())
}

fn value_to_id(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Null => "<nil>".to_owned(),
        Value::Bool(_) | Value::Number(_) | Value::Array(_) | Value::Object(_) => value.to_string(),
    }
}

fn json_response(status: StatusCode, payload: Value) -> Response {
    (status, Json(payload)).into_response()
}

fn api_error(status: StatusCode, message: &str) -> Response {
    json_response(status, json!({"error": message}))
}

impl IndexStore {
    fn new() -> Self {
        Self {
            items: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    async fn ensure(&self, index: &str) {
        let mut items = self.items.write().await;
        items
            .entry(index.to_owned())
            .or_insert_with(IndexMetadata::default);
    }

    async fn get(&self, index: &str) -> (Map<String, Value>, Map<String, Value>) {
        let items = self.items.read().await;
        match items.get(index) {
            Some(metadata) => (metadata.mappings.clone(), metadata.settings.clone()),
            None => {
                let metadata = IndexMetadata::default();
                (metadata.mappings, metadata.settings)
            }
        }
    }

    async fn set_mapping(&self, index: &str, mapping: Map<String, Value>) {
        let mut items = self.items.write().await;
        let metadata = items
            .entry(index.to_owned())
            .or_insert_with(IndexMetadata::default);
        metadata.mappings = mapping;
    }

    /// Merge new properties into the stored mapping, holding the write lock for
    /// the whole read-modify-write so concurrent updates cannot lose fields.
    async fn merge_mapping(
        &self,
        index: &str,
        incoming: &Map<String, Value>,
    ) -> Result<(), String> {
        let mut items = self.items.write().await;
        let metadata = items
            .entry(index.to_owned())
            .or_insert_with(IndexMetadata::default);
        metadata.mappings = merge_mapping_properties(&metadata.mappings, incoming)?;
        Ok(())
    }

    async fn set_settings(&self, index: &str, settings: Map<String, Value>) {
        let mut items = self.items.write().await;
        let metadata = items
            .entry(index.to_owned())
            .or_insert_with(IndexMetadata::default);
        for (key, value) in settings {
            metadata.settings.insert(key, value);
        }
    }

    async fn delete(&self, index: &str) {
        let mut items = self.items.write().await;
        let _ = items.remove(index);
    }
}

impl Default for IndexMetadata {
    fn default() -> Self {
        let mut settings = Map::new();
        settings.insert("number_of_shards".to_owned(), Value::String("1".to_owned()));
        settings.insert(
            "number_of_replicas".to_owned(),
            Value::String("0".to_owned()),
        );
        Self {
            mappings: Map::new(),
            settings,
        }
    }
}

async fn index_passthrough_middleware(
    State(state): State<AppState>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let Some(index) = local_index_from_uri(request.uri()) else {
        return next.run(request).await;
    };
    if assert_identifier(index, "index name").is_err() || !should_passthrough(&state, index) {
        return next.run(request).await;
    }
    proxy_request(&state, request).await
}

fn local_index_from_uri(uri: &Uri) -> Option<&str> {
    let first = uri.path().trim_start_matches('/').split('/').next()?;
    if first.is_empty() || first.starts_with('_') {
        None
    } else {
        Some(first)
    }
}

fn should_passthrough(state: &AppState, index: &str) -> bool {
    let Some(config) = &state.passthrough else {
        return false;
    };
    if config.patterns.is_empty() {
        return false;
    }
    config
        .patterns
        .iter()
        .any(|pattern| match_pattern(pattern, index))
}

async fn fallback(State(state): State<AppState>, request: Request<Body>) -> Response {
    let path = request.uri().path();
    if is_cluster_passthrough_path(path) {
        if state.passthrough.is_some() {
            return proxy_request(&state, request).await;
        }
        return api_error(StatusCode::NOT_IMPLEMENTED, "passthrough not configured");
    }

    if let Some(index) = local_index_from_uri(request.uri()) {
        if let Err(error) = assert_identifier(index, "index name") {
            return api_error(StatusCode::BAD_REQUEST, &error);
        }
    }
    api_error(StatusCode::NOT_FOUND, "endpoint not found")
}

fn is_cluster_passthrough_path(path: &str) -> bool {
    path == "/_cluster/state"
        || path == "/_nodes"
        || path == "/_tasks"
        || path == "/_alias"
        || path == "/_snapshot"
        || path.starts_with("/_snapshot/")
        || path == "/_ilm"
        || path.starts_with("/_ilm/")
        || path == "/_security"
        || path.starts_with("/_security/")
        || path == "/_cat"
        || path.starts_with("/_cat/")
}

async fn proxy_request(state: &AppState, request: Request<Body>) -> Response {
    let Some(config) = &state.passthrough else {
        return api_error(StatusCode::NOT_IMPLEMENTED, "passthrough not configured");
    };
    let (parts, body) = request.into_parts();
    let bytes = match to_bytes(body, MAX_PROXY_BODY).await {
        Ok(bytes) => bytes,
        Err(error) => return api_error(StatusCode::BAD_REQUEST, &error.to_string()),
    };
    proxy_parts(state, config, parts.method, parts.uri, parts.headers, bytes).await
}

async fn proxy_parts(
    state: &AppState,
    config: &PassthroughConfig,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let mut target = config.base_url.clone();
    target.set_path(uri.path());
    target.set_query(uri.query());

    let mut request = state.http_client.request(method, target).body(body);
    for (name, value) in &headers {
        if name == HOST || name == CONTENT_LENGTH {
            continue;
        }
        request = request.header(name, value);
    }
    let response = match request.send().await {
        Ok(response) => response,
        Err(error) => return api_error(StatusCode::BAD_GATEWAY, &error.to_string()),
    };
    let status = response.status();
    let headers = response.headers().clone();
    let body = match response.bytes().await {
        Ok(body) => body,
        Err(error) => return api_error(StatusCode::BAD_GATEWAY, &error.to_string()),
    };
    (status, headers, body).into_response()
}
