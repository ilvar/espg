use crate::query::assert_identifier;
use serde_json::{Map, Value};

/// Field types accepted in a mapping definition.
///
/// Mappings are compatibility metadata: documents are stored as `JSONB` and are
/// not coerced to these types. The list exists so that clients get an
/// Elasticsearch-shaped rejection for typos instead of silently storing a
/// mapping that never matches anything.
const SUPPORTED_TYPES: [&str; 15] = [
    "binary",
    "boolean",
    "byte",
    "date",
    "double",
    "float",
    "geo_point",
    "half_float",
    "integer",
    "ip",
    "keyword",
    "long",
    "object",
    "short",
    "text",
];

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
pub fn merge_mapping_properties(
    existing: &Map<String, Value>,
    incoming: &Map<String, Value>,
) -> Result<Map<String, Value>, String> {
    let mut merged = existing.clone();
    for (field, definition) in incoming {
        if let Some(current) = merged.get(field) {
            let current_type = field_type(current);
            let incoming_type = field_type(definition);
            if current_type != incoming_type {
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
    if !SUPPORTED_TYPES.contains(&field_type) {
        return Err(format!(
            "unsupported mapping type [{field_type}] for field [{field}]"
        ));
    }
    Ok(object.clone())
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

        let conflicting = properties(json!({"title": {"type": "keyword"}}));
        let error = merge_mapping_properties(&existing, &conflicting).unwrap_err();
        assert!(error.contains("cannot be changed from type [text] to [keyword]"));
    }
}
