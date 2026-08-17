use crate::query::assert_identifier;
use serde_json::{Map, Value};
use std::collections::BTreeMap;

/// Mapped field name to Elasticsearch type, for one index.
///
/// This is derived from the PostgreSQL catalog rather than from the in-memory
/// metadata, so it stays correct across restarts.
pub type FieldTypes = BTreeMap<String, String>;

/// Elasticsearch field type to the PostgreSQL column type it is stored in.
///
/// A mapped field becomes a real typed column; anything not in the mapping
/// falls back to the residual `document JSONB` column.
const TYPE_MAP: [(&str, &str); 15] = [
    ("binary", "TEXT"),
    ("boolean", "BOOLEAN"),
    ("byte", "SMALLINT"),
    // Spelled as the catalog reports it, so column types round-trip literally.
    ("date", "TIMESTAMP WITH TIME ZONE"),
    ("double", "DOUBLE PRECISION"),
    ("float", "REAL"),
    ("geo_point", "JSONB"),
    ("half_float", "REAL"),
    ("integer", "INTEGER"),
    ("ip", "TEXT"),
    ("keyword", "TEXT"),
    ("long", "BIGINT"),
    ("object", "JSONB"),
    ("short", "SMALLINT"),
    ("text", "TEXT"),
];

/// PostgreSQL `information_schema.data_type` to the Elasticsearch type reported
/// for it. Several Elasticsearch types share a column type, so this is the
/// representative choice rather than an exact inverse of `TYPE_MAP`.
const REVERSE_TYPE_MAP: [(&str, &str); 9] = [
    ("bigint", "long"),
    ("boolean", "boolean"),
    ("double precision", "double"),
    ("integer", "integer"),
    ("jsonb", "object"),
    ("real", "float"),
    ("smallint", "short"),
    ("text", "keyword"),
    ("timestamp with time zone", "date"),
];

/// PostgreSQL truncates identifiers beyond this many bytes.
const MAX_IDENTIFIER_LENGTH: usize = 63;

/// Alias prefix for the `_source` reassembly columns. Reserved as a field-name
/// prefix so a mapped column can never shadow one of those output labels.
pub const SOURCE_COLUMN_PREFIX: &str = "_espg_col_";

/// Format used to render `date` columns back into `_source`, matching the
/// Elasticsearch `strict_date_optional_time` default.
const DATE_OUTPUT_FORMAT: &str = r#"YYYY-MM-DD"T"HH24:MI:SS.MS"Z""#;

/// The PostgreSQL column type backing an Elasticsearch field type.
pub fn postgres_type(field_type: &str) -> Option<&'static str> {
    TYPE_MAP
        .iter()
        .find(|(name, _)| *name == field_type)
        .map(|(_, sql)| *sql)
}

/// The Elasticsearch type reported for a PostgreSQL column type.
pub fn elasticsearch_type(data_type: &str) -> Option<&'static str> {
    REVERSE_TYPE_MAP
        .iter()
        .find(|(name, _)| *name == data_type)
        .map(|(_, es)| *es)
}

/// SQL for binding a text parameter into a typed column.
///
/// Every parameter in this codebase is bound as text, so the conversion happens
/// in SQL: `($3::text)::bigint`. The inner cast keeps the parameter type
/// unambiguous for the PostgreSQL planner.
pub fn param_expression(field_type: &str, position: usize) -> String {
    match postgres_type(field_type) {
        Some("TEXT") | None => format!("${position}::text"),
        Some(sql_type) => format!("(${position}::text)::{sql_type}"),
    }
}

/// SQL for reading a mapped column back as text, for `_source` reassembly.
pub fn read_expression(field: &str, field_type: &str) -> String {
    let column = crate::query::quote_identifier(field);
    match field_type {
        "date" => format!("to_char({column} AT TIME ZONE 'UTC', '{DATE_OUTPUT_FORMAT}')"),
        _ => format!("{column}::text"),
    }
}

/// Convert a column value, read back as text, into the JSON it came from.
pub fn json_from_text(field_type: &str, text: &str) -> Value {
    match field_type {
        "byte" | "integer" | "long" | "short" => text
            .parse::<i64>()
            .ok()
            .map(Value::from)
            .unwrap_or_else(|| Value::String(text.to_owned())),
        "double" | "float" | "half_float" => text
            .parse::<f64>()
            .ok()
            .and_then(serde_json::Number::from_f64)
            .map(Value::Number)
            .unwrap_or_else(|| Value::String(text.to_owned())),
        "boolean" => match text {
            "true" | "t" => Value::Bool(true),
            "false" | "f" => Value::Bool(false),
            _ => Value::String(text.to_owned()),
        },
        "geo_point" | "object" => {
            serde_json::from_str(text).unwrap_or_else(|_| Value::String(text.to_owned()))
        }
        _ => Value::String(text.to_owned()),
    }
}

/// Mapping-level keys that sit beside `properties` rather than describing a
/// field. They are accepted and ignored so that bodies such as
/// `{"dynamic": "strict"}` are not mistaken for a field named `dynamic`.
const MAPPING_LEVEL_KEYS: [&str; 7] = [
    "_meta",
    "_source",
    "date_detection",
    "dynamic",
    "dynamic_templates",
    "numeric_detection",
    "runtime",
];

/// Extract and validate the `properties` object of a `mappings` body.
///
/// Accepts the Elasticsearch shape `{"properties": {...}}` as well as a bare
/// properties object, which some clients send to `PUT /:index/_mapping`.
/// A missing or empty `mappings` body yields an empty mapping.
pub fn parse_mapping_properties(mappings: &Value) -> Result<Map<String, Value>, String> {
    let Some(object) = mappings.as_object() else {
        return Err("mappings must be an object".to_owned());
    };
    if object.is_empty() {
        return Ok(Map::new());
    }

    let mut validated = Map::new();
    match object.get("properties") {
        Some(properties) => {
            let properties = properties
                .as_object()
                .ok_or_else(|| "mappings.properties must be an object".to_owned())?;
            for (field, definition) in properties {
                let definition = validate_field(field, definition)?;
                let _ = validated.insert(field.clone(), Value::Object(definition));
            }
        }
        // Bare properties object: skip mapping-level keys. A field that shares a
        // name with one of them has to be sent inside `properties`.
        None => {
            for (field, definition) in object {
                if MAPPING_LEVEL_KEYS.contains(&field.as_str()) {
                    continue;
                }
                let definition = validate_field(field, definition)?;
                let _ = validated.insert(field.clone(), Value::Object(definition));
            }
        }
    }
    Ok(validated)
}

/// Merge incoming properties into an existing mapping.
///
/// Elasticsearch allows new fields to be added to a live mapping but rejects a
/// type change on an existing field. Redefining a field identically is a no-op.
/// Types are compared by backing column, so a change that keeps the same column
/// type (`keyword` to `text`) is allowed while one that would not is rejected.
pub fn merge_mapping_properties(
    existing: &Map<String, Value>,
    incoming: &Map<String, Value>,
) -> Result<Map<String, Value>, String> {
    let mut merged = existing.clone();
    for (field, definition) in incoming {
        if let Some(current) = merged.get(field) {
            let current_type = field_type(current);
            let incoming_type = field_type(definition);
            if postgres_type(current_type) != postgres_type(incoming_type) {
                return Err(format!(
                    "mapper [{field}] cannot be changed from type [{current_type}] to [{incoming_type}]"
                ));
            }
        }
        let _ = merged.insert(field.clone(), definition.clone());
    }
    Ok(merged)
}

/// Validate one field definition, rejecting subfields.
fn validate_field(field: &str, definition: &Value) -> Result<Map<String, Value>, String> {
    assert_identifier(field, "mapping field")
        .map_err(|_| format!("invalid mapping field name [{field}]"))?;
    if field.starts_with(SOURCE_COLUMN_PREFIX) {
        return Err(format!(
            "mapping field name [{field}] uses the reserved prefix [{SOURCE_COLUMN_PREFIX}]"
        ));
    }
    let Some(object) = definition.as_object() else {
        return Err(format!("mapping for field [{field}] must be an object"));
    };
    if object.contains_key("properties") || object.contains_key("fields") {
        return Err(format!(
            "mapping for field [{field}] uses subfields, which are not supported"
        ));
    }
    let Some(field_type) = object.get("type") else {
        return Err(format!("mapping for field [{field}] is missing [type]"));
    };
    let Some(field_type) = field_type.as_str() else {
        return Err(format!("mapping type for field [{field}] must be a string"));
    };
    if postgres_type(field_type).is_none() {
        return Err(format!(
            "unsupported mapping type [{field_type}] for field [{field}]"
        ));
    }
    Ok(object.clone())
}

/// Operator class enabling trigram matching on a `text` column.
const TRIGRAM_OPERATOR_CLASS: &str = "gin_trgm_ops";

/// PostgreSQL extension providing [`TRIGRAM_OPERATOR_CLASS`].
pub const TRIGRAM_EXTENSION: &str = "pg_trgm";

/// How a mapped column is indexed.
pub struct IndexSpec {
    pub method: &'static str,
    pub operator_class: Option<&'static str>,
}

/// The index to build for a mapped column.
///
/// `text` is Elasticsearch's prose type, and `match` on it compiles to
/// `ILIKE '%...%'`, which a btree cannot serve at all — and a btree entry is
/// capped at 2704 bytes, so long prose would fail on write. A trigram GIN index
/// serves that `ILIKE` and has no such cap.
///
/// `keyword`, `ip`, and `binary` share the `TEXT` column type but hold short
/// exact-match values, so they keep btree: trigram GIN supports neither `=` nor
/// `ORDER BY`. `JSONB` takes plain GIN for the same size reason as `text`.
pub fn index_spec(field_type: &str) -> IndexSpec {
    match field_type {
        "text" => IndexSpec {
            method: "GIN",
            operator_class: Some(TRIGRAM_OPERATOR_CLASS),
        },
        _ => match postgres_type(field_type) {
            Some("JSONB") => IndexSpec {
                method: "GIN",
                operator_class: None,
            },
            _ => IndexSpec {
                method: "BTREE",
                operator_class: None,
            },
        },
    }
}

/// Whether any mapped field needs the trigram extension installed.
pub fn requires_trigram(properties: &Map<String, Value>) -> bool {
    properties
        .values()
        .any(|definition| index_spec(field_type(definition)).operator_class.is_some())
}

/// Index name for a mapped column, clamped to PostgreSQL's identifier limit.
pub fn index_name(index: &str, field: &str) -> String {
    let name = format!("{index}_{field}_idx");
    match name.char_indices().nth(MAX_IDENTIFIER_LENGTH) {
        Some((cutoff, _)) => name.get(..cutoff).unwrap_or(&name).to_owned(),
        None => name,
    }
}

/// `CREATE INDEX` statements for a validated set of properties.
pub fn index_statements(index: &str, properties: &Map<String, Value>) -> Vec<String> {
    let table = crate::query::quote_identifier(index);
    let mut statements = Vec::new();
    for (field, definition) in properties {
        let field_type = field_type(definition);
        if postgres_type(field_type).is_none() {
            continue;
        }
        let spec = index_spec(field_type);
        let method = spec.method;
        let column = match spec.operator_class {
            Some(operator_class) => {
                format!("{} {operator_class}", crate::query::quote_identifier(field))
            }
            None => crate::query::quote_identifier(field),
        };
        statements.push(format!(
            "CREATE INDEX IF NOT EXISTS {} ON {table} USING {method} ({column})",
            crate::query::quote_identifier(&index_name(index, field))
        ));
    }
    statements
}

/// Column definitions (`"name" TYPE`) for a validated set of properties.
pub fn column_definitions(properties: &Map<String, Value>) -> Vec<String> {
    let mut columns = Vec::new();
    for (field, definition) in properties {
        if let Some(sql_type) = postgres_type(field_type(definition)) {
            columns.push(format!(
                "{} {sql_type}",
                crate::query::quote_identifier(field)
            ));
        }
    }
    columns
}

fn field_type(definition: &Value) -> &str {
    definition
        .as_object()
        .and_then(|object| object.get("type"))
        .and_then(Value::as_str)
        .unwrap_or("object")
}

#[cfg(test)]
mod tests {
    use super::{merge_mapping_properties, parse_mapping_properties};
    use serde_json::{json, Map, Value};

    fn properties(raw: Value) -> Map<String, Value> {
        parse_mapping_properties(&raw).unwrap()
    }

    #[test]
    fn parses_properties_wrapper_and_bare_object() {
        let wrapped = properties(json!({"properties": {"title": {"type": "text"}}}));
        let bare = properties(json!({"title": {"type": "text"}}));
        assert_eq!(wrapped, bare);
        assert_eq!(
            wrapped.get("title").and_then(|field| field.get("type")),
            Some(&json!("text"))
        );
    }

    #[test]
    fn empty_mappings_yield_empty_properties() {
        assert!(properties(json!({})).is_empty());
        assert!(properties(json!({"properties": {}})).is_empty());
    }

    #[test]
    fn field_definitions_are_validated() {
        assert!(parse_mapping_properties(&json!({"title": "text"})).is_err());
        assert!(parse_mapping_properties(&json!({"title": {}})).is_err());
        assert!(parse_mapping_properties(&json!({"title": {"type": 7}})).is_err());
        assert!(parse_mapping_properties(&json!({"title": {"type": "nope"}})).is_err());
        assert!(parse_mapping_properties(&json!({"bad-field": {"type": "text"}})).is_err());
        assert!(parse_mapping_properties(&json!("text")).is_err());
    }

    #[test]
    fn mapping_level_keys_are_ignored_in_bare_form() {
        assert!(properties(json!({"dynamic": "strict"})).is_empty());
        let mixed = properties(json!({"dynamic": "strict", "title": {"type": "text"}}));
        assert_eq!(mixed.len(), 1);
        assert!(mixed.contains_key("title"));
    }

    #[test]
    fn subfields_are_rejected() {
        let nested =
            json!({"author": {"type": "object", "properties": {"name": {"type": "text"}}}});
        assert!(parse_mapping_properties(&nested).is_err());
        let multi = json!({"title": {"type": "text", "fields": {"raw": {"type": "keyword"}}}});
        assert!(parse_mapping_properties(&multi).is_err());
    }

    #[test]
    fn extra_field_options_are_preserved() {
        let parsed = properties(json!({"created_at": {"type": "date", "format": "epoch_millis"}}));
        assert_eq!(
            parsed
                .get("created_at")
                .and_then(|field| field.get("format")),
            Some(&json!("epoch_millis"))
        );
    }

    #[test]
    fn merge_adds_new_fields_and_keeps_existing() {
        let existing = properties(json!({"title": {"type": "text"}}));
        let incoming = properties(json!({"views": {"type": "long"}}));
        let merged = merge_mapping_properties(&existing, &incoming).unwrap();
        assert_eq!(merged.len(), 2);
        assert!(merged.contains_key("title"));
        assert!(merged.contains_key("views"));
    }

    #[test]
    fn merge_rejects_type_change_but_allows_identical_redefinition() {
        let existing = properties(json!({"title": {"type": "text"}}));
        let same = properties(json!({"title": {"type": "text"}}));
        assert!(merge_mapping_properties(&existing, &same).is_ok());

        let conflicting = properties(json!({"title": {"type": "long"}}));
        let error = merge_mapping_properties(&existing, &conflicting).unwrap_err();
        assert!(error.contains("cannot be changed from type [text] to [long]"));
    }

    #[test]
    fn merge_allows_changes_that_keep_the_same_column_type() {
        let existing = properties(json!({"title": {"type": "text"}}));
        let widened = properties(json!({"title": {"type": "keyword"}}));
        assert!(merge_mapping_properties(&existing, &widened).is_ok());
    }

    #[test]
    fn every_supported_type_has_a_column_type() {
        for (field_type, _) in super::TYPE_MAP {
            assert!(super::postgres_type(field_type).is_some());
        }
    }

    #[test]
    fn column_types_round_trip_to_elasticsearch_types() {
        for (data_type, field_type) in super::REVERSE_TYPE_MAP {
            let column = super::postgres_type(field_type)
                .expect("reverse-mapped type must be supported")
                .to_lowercase();
            assert_eq!(
                column, data_type,
                "{field_type} should map back to {column}"
            );
        }
    }

    #[test]
    fn every_mapped_column_gets_an_index() {
        let parsed = properties(json!({
            "title": {"type": "text"},
            "views": {"type": "long"},
            "meta": {"type": "object"}
        }));
        let statements = super::index_statements("books", &parsed);
        assert_eq!(statements.len(), parsed.len());
        assert!(statements.contains(&String::from(
            r#"CREATE INDEX IF NOT EXISTS "books_views_idx" ON "books" USING BTREE ("views")"#
        )));
        // Prose takes a trigram GIN: it serves the ILIKE that `match` compiles
        // to, and has no btree tuple-size limit.
        assert!(statements.contains(&String::from(
            r#"CREATE INDEX IF NOT EXISTS "books_title_idx" ON "books" USING GIN ("title" gin_trgm_ops)"#
        )));
        // jsonb takes GIN; a btree would reject oversized values on write.
        assert!(statements.contains(&String::from(
            r#"CREATE INDEX IF NOT EXISTS "books_meta_idx" ON "books" USING GIN ("meta")"#
        )));
    }

    #[test]
    fn keyword_keeps_btree_while_text_takes_trigram() {
        // Both are TEXT columns, but only `text` needs trigram matching;
        // trigram GIN supports neither `=` nor ORDER BY, which keyword wants.
        assert_eq!(super::index_spec("keyword").method, "BTREE");
        assert_eq!(super::index_spec("ip").method, "BTREE");
        assert_eq!(super::index_spec("text").method, "GIN");
        assert_eq!(
            super::index_spec("text").operator_class,
            Some("gin_trgm_ops")
        );
    }

    #[test]
    fn only_text_fields_require_the_trigram_extension() {
        assert!(super::requires_trigram(&properties(
            json!({"title": {"type": "text"}})
        )));
        assert!(!super::requires_trigram(&properties(
            json!({"tag": {"type": "keyword"}, "views": {"type": "long"}})
        )));
    }

    #[test]
    fn index_names_stay_within_the_identifier_limit() {
        let field = "f".repeat(80);
        let parsed = properties(json!({ field.clone(): {"type": "long"} }));
        let statements = super::index_statements("books", &parsed);
        let statement = statements.first().cloned().unwrap_or_default();
        let name = statement.split('"').nth(1).unwrap_or_default().to_owned();
        assert_eq!(name.len(), super::MAX_IDENTIFIER_LENGTH);
    }

    #[test]
    fn columns_are_declared_for_mapped_fields() {
        let parsed = properties(json!({"title": {"type": "text"}, "views": {"type": "long"}}));
        let columns = super::column_definitions(&parsed);
        assert_eq!(columns, vec!["\"title\" TEXT", "\"views\" BIGINT"]);
    }

    #[test]
    fn parameters_convert_text_bindings_into_the_column_type() {
        assert_eq!(super::param_expression("long", 3), "($3::text)::BIGINT");
        assert_eq!(super::param_expression("keyword", 1), "$1::text");
    }

    #[test]
    fn column_text_round_trips_back_into_json() {
        assert_eq!(super::json_from_text("long", "42"), json!(42));
        assert_eq!(super::json_from_text("double", "1.5"), json!(1.5));
        assert_eq!(super::json_from_text("boolean", "t"), json!(true));
        assert_eq!(super::json_from_text("keyword", "hello"), json!("hello"));
        assert_eq!(
            super::json_from_text("object", r#"{"a":1}"#),
            json!({"a": 1})
        );
    }
}
