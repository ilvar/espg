use crate::mapping::{param_expression, read_expression, FieldTypes};
use serde_json::{Map, Value};

#[derive(Clone, Debug, PartialEq)]
pub struct SearchPlan {
    pub where_clause: String,
    pub values: Vec<String>,
    pub sort_clause: String,
    pub size: i64,
    pub from: i64,
    pub aggregation: Option<Aggregation>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Aggregation {
    pub name: String,
    pub sql: String,
    pub kind: AggregationKind,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AggregationKind {
    Terms,
    Histogram,
    DateHistogram,
    Avg,
    Min,
    Max,
    Sum,
    Stats,
    Cardinality,
}

#[derive(Clone, Debug, PartialEq)]
pub struct MultiSearchQuery {
    pub index: String,
    pub body: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct BulkOperation {
    pub id: Option<String>,
    pub source: Map<String, Value>,
}

pub fn build_search_plan(
    body: &Map<String, Value>,
    table_name: &str,
    types: &FieldTypes,
) -> Result<SearchPlan, String> {
    let mut values = Vec::new();
    let where_clause = build_where_clause(body.get("query"), &mut values, types)?;
    let sort_clause = build_sort_clause(body.get("sort"), types)?;
    let size = to_i64(body.get("size"), 10);
    let from = to_i64(body.get("from"), 0);
    let aggregation = build_aggregation(body, table_name, &where_clause, types)?;

    Ok(SearchPlan {
        where_clause,
        values,
        sort_clause,
        size,
        from,
        aggregation,
    })
}

pub fn build_where_clause(
    query: Option<&Value>,
    values: &mut Vec<String>,
    types: &FieldTypes,
) -> Result<String, String> {
    let clause = build_query_clause(query, values, types)?;
    if clause.is_empty() {
        Ok(String::new())
    } else {
        Ok(format!("WHERE {clause}"))
    }
}

fn build_query_clause(
    query: Option<&Value>,
    values: &mut Vec<String>,
    types: &FieldTypes,
) -> Result<String, String> {
    let Some(query_map) = query.and_then(Value::as_object) else {
        return Ok(String::new());
    };
    if query_map.is_empty() {
        return Ok(String::new());
    }

    if let Some(term) = query_map.get("term").and_then(Value::as_object) {
        if let Some((field, value)) = term.iter().next() {
            assert_identifier(field, "term field")?;
            values.push(value_to_string(value));
            return Ok(format!(
                "{} = {}",
                field_expression(field, types),
                field_param(field, types, values.len())
            ));
        }
    }

    if let Some(match_query) = query_map.get("match").and_then(Value::as_object) {
        if let Some((field, value)) = match_query.iter().next() {
            assert_identifier(field, "match field")?;
            values.push(format!("%{}%", value_to_string(value)));
            return Ok(format!(
                "{} ILIKE ${}",
                field_text_expression(field, types),
                values.len()
            ));
        }
    }

    if let Some(terms) = query_map.get("terms").and_then(Value::as_object) {
        if let Some((field, value)) = terms.iter().next() {
            assert_identifier(field, "terms field")?;
            let Some(list) = value.as_array() else {
                return Ok(String::new());
            };
            let mut placeholders = Vec::new();
            for item in list {
                values.push(value_to_string(item));
                placeholders.push(field_param(field, types, values.len()));
            }
            if placeholders.is_empty() {
                return Ok(String::new());
            }
            return Ok(format!(
                "{} IN ({})",
                field_expression(field, types),
                placeholders.join(", ")
            ));
        }
    }

    if let Some(range) = query_map.get("range").and_then(Value::as_object) {
        if let Some((field, value)) = range.iter().next() {
            assert_identifier(field, "range field")?;
            let Some(constraints) = value.as_object() else {
                return Ok(String::new());
            };
            let mut clauses = Vec::new();
            if let Some(gte) = constraints.get("gte") {
                values.push(value_to_string(gte));
                clauses.push(format!(
                    "{} >= {}",
                    field_expression(field, types),
                    field_param(field, types, values.len())
                ));
            }
            if let Some(lte) = constraints.get("lte") {
                values.push(value_to_string(lte));
                clauses.push(format!(
                    "{} <= {}",
                    field_expression(field, types),
                    field_param(field, types, values.len())
                ));
            }
            if !clauses.is_empty() {
                return Ok(clauses.join(" AND "));
            }
        }
    }

    if let Some(bool_query) = query_map.get("bool").and_then(Value::as_object) {
        let mut clauses = Vec::new();
        append_boolean_clauses(bool_query.get("must"), values, false, &mut clauses, types)?;
        append_boolean_clauses(bool_query.get("filter"), values, false, &mut clauses, types)?;
        append_boolean_clauses(
            bool_query.get("must_not"),
            values,
            true,
            &mut clauses,
            types,
        )?;

        if let Some(should) = bool_query.get("should").and_then(Value::as_array) {
            let mut should_clauses = Vec::new();
            for condition in should {
                let clause = build_query_clause(Some(condition), values, types)?;
                if !clause.is_empty() {
                    should_clauses.push(clause);
                }
            }
            if !should_clauses.is_empty() {
                clauses.push(format!("({})", should_clauses.join(" OR ")));
            }
        }

        if !clauses.is_empty() {
            return Ok(clauses.join(" AND "));
        }
    }

    Ok(String::new())
}

fn append_boolean_clauses(
    value: Option<&Value>,
    values: &mut Vec<String>,
    negate: bool,
    clauses: &mut Vec<String>,
    types: &FieldTypes,
) -> Result<(), String> {
    let Some(items) = value.and_then(Value::as_array) else {
        return Ok(());
    };
    for condition in items {
        let clause = build_query_clause(Some(condition), values, types)?;
        if clause.is_empty() {
            continue;
        }
        if negate {
            clauses.push(format!("NOT ({clause})"));
        } else {
            clauses.push(clause);
        }
    }
    Ok(())
}

pub fn build_sort_clause(sort: Option<&Value>, types: &FieldTypes) -> Result<String, String> {
    let Some(sort) = sort else {
        return Ok("ORDER BY created_at DESC".to_owned());
    };

    let mut parts = Vec::new();
    match sort {
        Value::Array(entries) => {
            for entry in entries {
                let clause = parse_sort_entry(entry, types)?;
                if !clause.is_empty() {
                    parts.push(clause);
                }
            }
        }
        Value::Object(entries) => {
            for (field, direction) in entries {
                let clause = parse_sort_field(field, direction, types)?;
                if !clause.is_empty() {
                    parts.push(clause);
                }
            }
        }
        Value::String(field) => parts.push(parse_sort_field(
            field,
            &Value::String("asc".to_owned()),
            types,
        )?),
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }

    if parts.is_empty() {
        Ok("ORDER BY created_at DESC".to_owned())
    } else {
        Ok(format!("ORDER BY {}", parts.join(", ")))
    }
}

fn parse_sort_entry(entry: &Value, types: &FieldTypes) -> Result<String, String> {
    match entry {
        Value::String(field) => parse_sort_field(field, &Value::String("asc".to_owned()), types),
        Value::Object(entries) => {
            let Some((field, direction)) = entries.iter().next() else {
                return Ok(String::new());
            };
            parse_sort_field(field, direction, types)
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::Array(_) => Ok(String::new()),
    }
}

fn parse_sort_field(field: &str, direction: &Value, types: &FieldTypes) -> Result<String, String> {
    let order = match direction {
        Value::String(value) if value.eq_ignore_ascii_case("desc") => "DESC",
        Value::Object(options)
            if options
                .get("order")
                .and_then(Value::as_str)
                .is_some_and(|value| value.eq_ignore_ascii_case("desc")) =>
        {
            "DESC"
        }
        Value::Null
        | Value::Bool(_)
        | Value::Number(_)
        | Value::String(_)
        | Value::Array(_)
        | Value::Object(_) => "ASC",
    };

    if field == "_id" || field == "id" {
        return Ok(format!("id {order}"));
    }
    assert_identifier(field, "sort field")?;
    Ok(format!("{} {order}", field_expression(field, types)))
}

fn build_aggregation(
    body: &Map<String, Value>,
    table_name: &str,
    where_clause: &str,
    types: &FieldTypes,
) -> Result<Option<Aggregation>, String> {
    let Some(aggs) = body
        .get("aggs")
        .or_else(|| body.get("aggregations"))
        .and_then(Value::as_object)
    else {
        return Ok(None);
    };
    let Some((name, body)) = aggs.iter().next() else {
        return Ok(None);
    };
    let Some(agg_body) = body.as_object() else {
        return Ok(None);
    };

    if let Some(terms) = agg_body.get("terms").and_then(Value::as_object) {
        let field = required_field(terms, "terms aggregation")?;
        let limit = to_i64(terms.get("size"), 10);
        let key_expression = field_text_expression(&field, types);
        let sql = format!(
            "SELECT {key_expression} AS key, COUNT(*)::bigint AS doc_count FROM {table_name} {where_clause} GROUP BY key ORDER BY doc_count DESC LIMIT {limit}"
        );
        return Ok(Some(Aggregation {
            name: name.clone(),
            sql,
            kind: AggregationKind::Terms,
        }));
    }

    if let Some(histogram) = agg_body.get("histogram").and_then(Value::as_object) {
        let field = required_field(histogram, "histogram aggregation")?;
        let interval = histogram
            .get("interval")
            .and_then(number_as_f64)
            .ok_or_else(|| "histogram aggregation requires interval".to_owned())?;
        if interval <= 0.0 {
            return Err("histogram aggregation requires positive interval".to_owned());
        }
        let numeric = field_numeric_expression(&field, types);
        let sql = format!(
            "SELECT (floor({numeric} / {interval}) * {interval})::double precision AS key, COUNT(*)::bigint AS doc_count FROM {table_name} {where_clause} GROUP BY key ORDER BY key ASC"
        );
        return Ok(Some(Aggregation {
            name: name.clone(),
            sql,
            kind: AggregationKind::Histogram,
        }));
    }

    if let Some(histogram) = agg_body.get("date_histogram").and_then(Value::as_object) {
        let field = required_field(histogram, "date_histogram aggregation")?;
        let interval = histogram
            .get("calendar_interval")
            .and_then(Value::as_str)
            .unwrap_or("day");
        if !matches!(
            interval,
            "minute" | "hour" | "day" | "week" | "month" | "year"
        ) {
            return Err("unsupported calendar_interval".to_owned());
        }
        let timestamp = field_timestamp_expression(&field, types);
        let sql = format!(
            r#"SELECT (EXTRACT(EPOCH FROM date_trunc('{interval}', {timestamp})) * 1000)::double precision AS key, to_char(date_trunc('{interval}', {timestamp}) AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS key_as_string, COUNT(*)::bigint AS doc_count FROM {table_name} {where_clause} GROUP BY 1, 2 ORDER BY 1 ASC"#
        );
        return Ok(Some(Aggregation {
            name: name.clone(),
            sql,
            kind: AggregationKind::DateHistogram,
        }));
    }

    for (key, kind, sql_fn) in [
        ("avg", AggregationKind::Avg, "AVG"),
        ("min", AggregationKind::Min, "MIN"),
        ("max", AggregationKind::Max, "MAX"),
        ("sum", AggregationKind::Sum, "SUM"),
    ] {
        if let Some(metric) = agg_body.get(key).and_then(Value::as_object) {
            let field = required_field(metric, &format!("{key} aggregation"))?;
            let numeric = field_numeric_expression(&field, types);
            let sql = format!(
                "SELECT {sql_fn}({numeric})::double precision AS value FROM {table_name} {where_clause}"
            );
            return Ok(Some(Aggregation {
                name: name.clone(),
                sql,
                kind,
            }));
        }
    }

    if let Some(stats) = agg_body.get("stats").and_then(Value::as_object) {
        let field = required_field(stats, "stats aggregation")?;
        let numeric = field_numeric_expression(&field, types);
        let text = field_text_expression(&field, types);
        let sql = format!(
            "SELECT COUNT({text})::bigint AS count, MIN({numeric})::double precision AS min, MAX({numeric})::double precision AS max, AVG({numeric})::double precision AS avg, SUM({numeric})::double precision AS sum FROM {table_name} {where_clause}"
        );
        return Ok(Some(Aggregation {
            name: name.clone(),
            sql,
            kind: AggregationKind::Stats,
        }));
    }

    if let Some(cardinality) = agg_body.get("cardinality").and_then(Value::as_object) {
        let field = required_field(cardinality, "cardinality aggregation")?;
        let text = field_text_expression(&field, types);
        let sql = format!(
            "SELECT COUNT(DISTINCT {text})::bigint AS value FROM {table_name} {where_clause}"
        );
        return Ok(Some(Aggregation {
            name: name.clone(),
            sql,
            kind: AggregationKind::Cardinality,
        }));
    }

    Ok(None)
}

fn required_field(body: &Map<String, Value>, label: &str) -> Result<String, String> {
    let field = body
        .get("field")
        .and_then(Value::as_str)
        .ok_or_else(|| format!("{label} requires field"))?;
    assert_identifier(field, &format!("{label} field"))?;
    Ok(field.to_owned())
}

pub fn parse_bulk(body: &[u8]) -> Result<Vec<BulkOperation>, String> {
    let text = std::str::from_utf8(body).map_err(|error| format!("bulk parse error: {error}"))?;
    let lines = non_empty_lines(text);
    if !lines.len().is_multiple_of(2) {
        return Err("invalid bulk payload; expected action and source".to_owned());
    }

    let mut operations = Vec::new();
    for pair in lines.chunks_exact(2) {
        let action_line = pair
            .first()
            .ok_or_else(|| "invalid bulk payload; expected action and source".to_owned())?;
        let source_line = pair
            .get(1)
            .ok_or_else(|| "invalid bulk payload; expected action and source".to_owned())?;
        let action: Value =
            serde_json::from_str(action_line).map_err(|_| "invalid bulk action json".to_owned())?;
        let index_action = action
            .get("index")
            .and_then(Value::as_object)
            .ok_or_else(|| "invalid bulk action; expected index action".to_owned())?;
        let source: Map<String, Value> =
            serde_json::from_str(source_line).map_err(|_| "invalid bulk source json".to_owned())?;
        let id = index_action
            .get("_id")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned);
        operations.push(BulkOperation { id, source });
    }
    Ok(operations)
}

pub fn parse_multi_search(body: &[u8]) -> Result<Vec<MultiSearchQuery>, String> {
    let text =
        std::str::from_utf8(body).map_err(|error| format!("msearch parse error: {error}"))?;
    let lines = non_empty_lines(text);
    if !lines.len().is_multiple_of(2) {
        return Err("invalid msearch payload; expected header and body".to_owned());
    }

    let mut queries = Vec::new();
    for pair in lines.chunks_exact(2) {
        let header_line = pair
            .first()
            .ok_or_else(|| "invalid msearch payload; expected header and body".to_owned())?;
        let body_line = pair
            .get(1)
            .ok_or_else(|| "invalid msearch payload; expected header and body".to_owned())?;
        let header: Value = serde_json::from_str(header_line)
            .map_err(|_| "invalid msearch header json".to_owned())?;
        let body: Map<String, Value> =
            serde_json::from_str(body_line).map_err(|_| "invalid msearch body json".to_owned())?;
        let index = header
            .get("index")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        queries.push(MultiSearchQuery { index, body });
    }
    Ok(queries)
}

fn non_empty_lines(text: &str) -> Vec<&str> {
    text.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect()
}

/// SQL for a field's value in its native type.
///
/// A mapped field is a real column; anything else is read out of the residual
/// `document` JSONB, where every value is text.
fn field_expression(field: &str, types: &FieldTypes) -> String {
    match types.get(field) {
        Some(_) => quote_identifier(field),
        None => format!("document ->> '{field}'"),
    }
}

/// SQL for a field's value rendered as text.
fn field_text_expression(field: &str, types: &FieldTypes) -> String {
    match types.get(field) {
        Some(field_type) => read_expression(field, field_type),
        None => format!("document ->> '{field}'"),
    }
}

/// SQL for a field's value as `double precision`.
fn field_numeric_expression(field: &str, types: &FieldTypes) -> String {
    match types.get(field).map(String::as_str) {
        Some("double" | "float" | "half_float") => quote_identifier(field),
        Some(_) => format!("{}::double precision", quote_identifier(field)),
        None => format!("(document ->> '{field}')::double precision"),
    }
}

/// SQL for a field's value as `timestamptz`.
fn field_timestamp_expression(field: &str, types: &FieldTypes) -> String {
    match types.get(field).map(String::as_str) {
        Some("date") => quote_identifier(field),
        Some(_) => format!("({}::text)::timestamptz", quote_identifier(field)),
        None => format!("(document ->> '{field}')::timestamptz"),
    }
}

/// SQL for a bound parameter compared against a field.
///
/// Parameters are always bound as text, so a mapped column needs the value
/// converted to the column type before comparison.
fn field_param(field: &str, types: &FieldTypes, position: usize) -> String {
    match types.get(field) {
        Some(field_type) => param_expression(field_type, position),
        None => format!("${position}"),
    }
}

pub fn assert_identifier(value: &str, label: &str) -> Result<(), String> {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return Err(format!("invalid {label}"));
    };
    if !(first.is_ascii_alphabetic() || first == '_') {
        return Err(format!("invalid {label}"));
    }
    if chars.any(|character| !(character.is_ascii_alphanumeric() || character == '_')) {
        return Err(format!("invalid {label}"));
    }
    Ok(())
}

pub fn quote_identifier(value: &str) -> String {
    format!("\"{value}\"")
}

pub fn match_pattern(pattern: &str, value: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    let parts = pattern.split('*').collect::<Vec<&str>>();
    if parts.len() == 1 {
        return pattern == value;
    }
    let Some(first) = parts.first() else {
        return false;
    };
    let Some(last) = parts.last() else {
        return false;
    };
    if !value.starts_with(first) || !value.ends_with(last) {
        return false;
    }

    let mut remaining = value;
    for part in parts {
        if part.is_empty() {
            continue;
        }
        let Some(position) = remaining.find(part) else {
            return false;
        };
        let start = position.saturating_add(part.len());
        let Some(next) = remaining.get(start..) else {
            return false;
        };
        remaining = next;
    }
    true
}

fn value_to_string(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::Array(_) | Value::Object(_) => {
            value.to_string()
        }
    }
}

fn number_as_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Number(number) => number.as_f64(),
        Value::String(text) => text.parse::<f64>().ok(),
        Value::Null | Value::Bool(_) | Value::Array(_) | Value::Object(_) => None,
    }
}

fn to_i64(value: Option<&Value>, fallback: i64) -> i64 {
    let Some(value) = value else {
        return fallback;
    };
    match value {
        Value::Number(number) => number.as_i64().unwrap_or(fallback),
        Value::String(text) => text.parse::<i64>().unwrap_or(fallback),
        Value::Null | Value::Bool(_) | Value::Array(_) | Value::Object(_) => fallback,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        assert_identifier, build_search_plan, match_pattern, parse_bulk, parse_multi_search,
        FieldTypes,
    };
    use serde_json::Value;

    #[test]
    fn identifier_rules_match_postgres_table_contract() {
        assert!(assert_identifier("books_2026", "index name").is_ok());
        assert!(assert_identifier("logs-prod", "index name").is_err());
        assert!(assert_identifier("9books", "index name").is_err());
    }

    #[test]
    fn wildcard_patterns_match_in_order() {
        assert!(match_pattern("logs-*", "logs-prod"));
        assert!(match_pattern("*-archive", "logs-archive"));
        assert!(match_pattern("a*b*c", "a-1-b-2-c"));
        assert!(!match_pattern("a*b*c", "a-1-c-2-b"));
    }

    #[test]
    fn search_plan_supports_bool_sort_and_terms() {
        let body: Value = serde_json::from_str(
            r#"{
                "query":{"bool":{"must":[{"term":{"status":"published"}}],"must_not":[{"term":{"hidden":true}}]}},
                "sort":[{"created_at":"desc"}],
                "from":2,
                "size":25
            }"#,
        )
        .unwrap();
        let object = body.as_object().unwrap();
        let plan = build_search_plan(object, "\"books\"", &FieldTypes::new()).unwrap();
        assert!(plan.where_clause.contains("status"));
        assert!(plan.where_clause.contains("NOT"));
        assert_eq!(plan.values, vec!["published", "true"]);
        assert_eq!(plan.size, 25);
        assert_eq!(plan.from, 2);
    }

    fn typed(field: &str, field_type: &str) -> FieldTypes {
        let mut types = FieldTypes::new();
        let _ = types.insert(field.to_owned(), field_type.to_owned());
        types
    }

    fn plan_for(raw: &str, types: &FieldTypes) -> super::SearchPlan {
        let body: Value = serde_json::from_str(raw).unwrap();
        let object = body.as_object().unwrap();
        build_search_plan(object, "\"books\"", types).unwrap()
    }

    #[test]
    fn mapped_fields_query_their_column_and_unmapped_fields_use_jsonb() {
        let query = r#"{"query":{"range":{"views":{"gte":9}}}}"#;

        let mapped = plan_for(query, &typed("views", "long"));
        assert!(
            mapped
                .where_clause
                .contains(r#""views" >= ($1::text)::BIGINT"#),
            "{}",
            mapped.where_clause
        );

        let unmapped = plan_for(query, &FieldTypes::new());
        assert!(
            unmapped.where_clause.contains("document ->> 'views' >= $1"),
            "{}",
            unmapped.where_clause
        );
    }

    #[test]
    fn sorting_a_mapped_field_orders_by_the_column() {
        let plan = plan_for(r#"{"sort":[{"views":"desc"}]}"#, &typed("views", "long"));
        assert_eq!(plan.sort_clause, r#"ORDER BY "views" DESC"#);
    }

    #[test]
    fn numeric_aggregations_skip_the_jsonb_cast_for_mapped_fields() {
        let plan = plan_for(
            r#"{"aggs":{"average":{"avg":{"field":"views"}}}}"#,
            &typed("views", "long"),
        );
        let sql = plan.aggregation.map(|aggregation| aggregation.sql);
        let sql = sql.unwrap_or_default();
        assert!(sql.contains(r#"AVG("views"::double precision)"#), "{sql}");
        assert!(!sql.contains("document ->>"), "{sql}");
    }

    #[test]
    fn matching_a_mapped_field_compares_it_as_text() {
        let plan = plan_for(
            r#"{"query":{"match":{"views":7}}}"#,
            &typed("views", "long"),
        );
        assert!(
            plan.where_clause.contains(r#""views"::text ILIKE $1"#),
            "{}",
            plan.where_clause
        );
    }

    #[test]
    fn bulk_requires_action_source_pairs() {
        let parsed = parse_bulk(b"{\"index\":{\"_id\":\"1\"}}\n{\"title\":\"hello\"}\n").unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(
            parsed.first().and_then(|item| item.id.as_deref()),
            Some("1")
        );
        assert!(parse_bulk(b"{\"index\":{}}\n").is_err());
    }

    #[test]
    fn msearch_parses_header_body_pairs() {
        let parsed = parse_multi_search(
            b"{\"index\":\"books\"}\n{\"query\":{\"term\":{\"status\":\"new\"}}}\n",
        )
        .unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(
            parsed.first().map(|query| query.index.as_str()),
            Some("books")
        );
    }
}
