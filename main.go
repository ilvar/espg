package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type dbPool interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type server struct {
	pool dbPool
}

type bulkOperation struct {
	ID     string
	Source map[string]any
}

type aggQuery struct {
	Name string
	SQL  string
	Type string
}

var (
	identifierPattern = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)
	allowedIntervals  = map[string]struct{}{
		"minute": {},
		"hour":   {},
		"day":    {},
		"week":   {},
		"month":  {},
		"year":   {},
	}
)

func main() {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, buildConnString())
	if err != nil {
		log.Fatalf("failed to connect to postgres: %v", err)
	}
	defer pool.Close()

	srv := &server{pool: pool}
	app := fiber.New(fiber.Config{
		DisableStartupMessage: true,
	})
	srv.registerRoutes(app)

	port := getEnv("PORT", "3000")
	log.Printf("espg listening on %s", port)
	if err := app.Listen(":" + port); err != nil {
		log.Fatalf("server error: %v", err)
	}
}

func buildConnString() string {
	if databaseURL := os.Getenv("DATABASE_URL"); databaseURL != "" {
		return databaseURL
	}

	host := getEnv("PGHOST", "localhost")
	port := getEnv("PGPORT", "5432")
	user := getEnv("PGUSER", "postgres")
	password := os.Getenv("PGPASSWORD")
	database := getEnv("PGDATABASE", "postgres")
	poolSize := getEnv("PGPOOL_SIZE", "10")

	return fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s pool_max_conns=%s", host, port, user, password, database, poolSize)
}

func getEnv(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}

func (s *server) registerRoutes(app *fiber.App) {
	app.Get("/", func(c *fiber.Ctx) error {
		return s.handleRoot(c)
	})
	app.Get("/_cluster/health", func(c *fiber.Ctx) error {
		return s.handleClusterHealth(c)
	})
	app.Head("/:index", func(c *fiber.Ctx) error {
		index := c.Params("index")
		if err := assertIdentifier(index, "index name"); err != nil {
			return writeError(c, fiber.StatusBadRequest, err.Error())
		}
		return s.handleHeadIndex(c, index)
	})
	app.Get("/:index", func(c *fiber.Ctx) error {
		index := c.Params("index")
		if err := assertIdentifier(index, "index name"); err != nil {
			return writeError(c, fiber.StatusBadRequest, err.Error())
		}
		return s.handleGetIndex(c, index)
	})
	app.Put("/:index", func(c *fiber.Ctx) error {
		index := c.Params("index")
		if err := assertIdentifier(index, "index name"); err != nil {
			return writeError(c, fiber.StatusBadRequest, err.Error())
		}
		return s.handleCreateIndex(c, index)
	})
	app.Post("/:index/_doc", func(c *fiber.Ctx) error {
		index := c.Params("index")
		if err := assertIdentifier(index, "index name"); err != nil {
			return writeError(c, fiber.StatusBadRequest, err.Error())
		}
		return s.handleCreateDocument(c, index)
	})
	app.Put("/:index/_doc/:id", func(c *fiber.Ctx) error {
		index := c.Params("index")
		if err := assertIdentifier(index, "index name"); err != nil {
			return writeError(c, fiber.StatusBadRequest, err.Error())
		}
		return s.handleUpsertDocument(c, index, c.Params("id"))
	})
	app.Get("/:index/_doc/:id", func(c *fiber.Ctx) error {
		index := c.Params("index")
		if err := assertIdentifier(index, "index name"); err != nil {
			return writeError(c, fiber.StatusBadRequest, err.Error())
		}
		return s.handleGetDocument(c, index, c.Params("id"))
	})
	app.Delete("/:index/_doc/:id", func(c *fiber.Ctx) error {
		index := c.Params("index")
		if err := assertIdentifier(index, "index name"); err != nil {
			return writeError(c, fiber.StatusBadRequest, err.Error())
		}
		return s.handleDeleteDocument(c, index, c.Params("id"))
	})
	app.Delete("/:index", func(c *fiber.Ctx) error {
		index := c.Params("index")
		if err := assertIdentifier(index, "index name"); err != nil {
			return writeError(c, fiber.StatusBadRequest, err.Error())
		}
		return s.handleDeleteIndex(c, index)
	})
	app.Post("/:index/_delete_by_query", func(c *fiber.Ctx) error {
		index := c.Params("index")
		if err := assertIdentifier(index, "index name"); err != nil {
			return writeError(c, fiber.StatusBadRequest, err.Error())
		}
		return s.handleDeleteByQuery(c, index)
	})
	app.Post("/:index/_bulk", func(c *fiber.Ctx) error {
		index := c.Params("index")
		if err := assertIdentifier(index, "index name"); err != nil {
			return writeError(c, fiber.StatusBadRequest, err.Error())
		}
		return s.handleBulk(c, index)
	})
	app.Post("/:index/_search", func(c *fiber.Ctx) error {
		index := c.Params("index")
		if err := assertIdentifier(index, "index name"); err != nil {
			return writeError(c, fiber.StatusBadRequest, err.Error())
		}
		return s.handleSearch(c, index)
	})
}

func (s *server) handleRoot(c *fiber.Ctx) error {
	return writeJSON(c, fiber.StatusOK, map[string]any{
		"name":         "espg",
		"cluster_name": "espg",
		"version": map[string]any{
			"number":         "8.0.0",
			"build_flavor":   "default",
			"build_type":     "docker",
			"build_snapshot": true,
		},
		"tagline": "You Know, for Search",
	})
}

func (s *server) handleClusterHealth(c *fiber.Ctx) error {
	return writeJSON(c, fiber.StatusOK, map[string]any{
		"cluster_name":                     "espg",
		"status":                           "green",
		"number_of_nodes":                  1,
		"active_primary_shards":            0,
		"active_shards":                    0,
		"relocating_shards":                0,
		"initializing_shards":              0,
		"unassigned_shards":                0,
		"delayed_unassigned_shards":        0,
		"number_of_pending_tasks":          0,
		"number_of_in_flight_fetch":        0,
		"task_max_waiting_in_queue_millis": 0,
		"active_shards_percent_as_number":  100,
	})
}

func (s *server) handleHeadIndex(c *fiber.Ctx, index string) error {
	exists, err := s.indexExists(c.Context(), index)
	if err != nil {
		return writeError(c, fiber.StatusInternalServerError, err.Error())
	}
	if !exists {
		return c.SendStatus(fiber.StatusNotFound)
	}
	return c.SendStatus(fiber.StatusOK)
}

func (s *server) handleGetIndex(c *fiber.Ctx, index string) error {
	exists, err := s.indexExists(c.Context(), index)
	if err != nil {
		return writeError(c, fiber.StatusInternalServerError, err.Error())
	}
	if !exists {
		return writeError(c, fiber.StatusNotFound, "index not found")
	}
	return writeJSON(c, fiber.StatusOK, map[string]any{
		index: map[string]any{
			"aliases": map[string]any{},
			"mappings": map[string]any{
				"properties": map[string]any{},
			},
			"settings": map[string]any{
				"index": map[string]any{
					"number_of_shards":   "1",
					"number_of_replicas": "0",
				},
			},
		},
	})
}

func (s *server) handleCreateIndex(c *fiber.Ctx, index string) error {
	ctx := c.Context()
	identifier := quoteIdentifier(index)
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id TEXT PRIMARY KEY,
			document JSONB NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		);
	`, identifier)

	if _, err := s.pool.Exec(ctx, query); err != nil {
		return writeError(c, fiber.StatusInternalServerError, err.Error())
	}

	return writeJSON(c, fiber.StatusOK, map[string]any{
		"acknowledged": true,
		"index":        index,
	})
}

func (s *server) handleCreateDocument(c *fiber.Ctx, index string) error {
	if ok, err := s.indexExists(c.Context(), index); err != nil {
		return writeError(c, fiber.StatusInternalServerError, err.Error())
	} else if !ok {
		return writeError(c, fiber.StatusNotFound, "index not found")
	}

	var document map[string]any
	if err := parseDocumentBody(c, &document); err != nil {
		return writeError(c, fiber.StatusBadRequest, err.Error())
	}

	id := fmt.Sprintf("%d", time.Now().UnixNano())
	inserted, err := s.upsertDocument(c.Context(), index, id, document)
	if err != nil {
		return writeError(c, fiber.StatusInternalServerError, err.Error())
	}

	status := fiber.StatusCreated
	result := "created"
	if !inserted {
		status = fiber.StatusOK
		result = "updated"
	}

	return writeJSON(c, status, map[string]any{
		"_index": index,
		"_id":    id,
		"result": result,
	})
}

func (s *server) handleUpsertDocument(c *fiber.Ctx, index string, id string) error {
	if ok, err := s.indexExists(c.Context(), index); err != nil {
		return writeError(c, fiber.StatusInternalServerError, err.Error())
	} else if !ok {
		return writeError(c, fiber.StatusNotFound, "index not found")
	}

	var document map[string]any
	if err := parseDocumentBody(c, &document); err != nil {
		return writeError(c, fiber.StatusBadRequest, err.Error())
	}

	inserted, err := s.upsertDocument(c.Context(), index, id, document)
	if err != nil {
		return writeError(c, fiber.StatusInternalServerError, err.Error())
	}

	status := fiber.StatusCreated
	result := "created"
	if !inserted {
		status = fiber.StatusOK
		result = "updated"
	}

	return writeJSON(c, status, map[string]any{
		"_index": index,
		"_id":    id,
		"result": result,
	})
}

func (s *server) handleGetDocument(c *fiber.Ctx, index string, id string) error {
	if ok, err := s.indexExists(c.Context(), index); err != nil {
		return writeError(c, fiber.StatusInternalServerError, err.Error())
	} else if !ok {
		return writeError(c, fiber.StatusNotFound, "index not found")
	}

	identifier := quoteIdentifier(index)
	query := fmt.Sprintf("SELECT document FROM %s WHERE id = $1", identifier)
	var document map[string]any
	if err := s.pool.QueryRow(c.Context(), query, id).Scan(&document); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return writeJSON(c, fiber.StatusNotFound, map[string]any{
				"_index": index,
				"_id":    id,
				"found":  false,
			})
		}
		return writeError(c, fiber.StatusInternalServerError, err.Error())
	}

	return writeJSON(c, fiber.StatusOK, map[string]any{
		"_index":  index,
		"_id":     id,
		"found":   true,
		"_source": document,
	})
}

func (s *server) handleDeleteDocument(c *fiber.Ctx, index string, id string) error {
	if ok, err := s.indexExists(c.Context(), index); err != nil {
		return writeError(c, fiber.StatusInternalServerError, err.Error())
	} else if !ok {
		return writeError(c, fiber.StatusNotFound, "index not found")
	}

	identifier := quoteIdentifier(index)
	query := fmt.Sprintf("DELETE FROM %s WHERE id = $1", identifier)
	commandTag, err := s.pool.Exec(c.Context(), query, id)
	if err != nil {
		return writeError(c, fiber.StatusInternalServerError, err.Error())
	}
	if commandTag.RowsAffected() == 0 {
		return writeJSON(c, fiber.StatusNotFound, map[string]any{
			"_index": index,
			"_id":    id,
			"result": "not_found",
		})
	}

	return writeJSON(c, fiber.StatusOK, map[string]any{
		"_index": index,
		"_id":    id,
		"result": "deleted",
	})
}

func (s *server) handleDeleteIndex(c *fiber.Ctx, index string) error {
	ctx := c.Context()
	identifier := quoteIdentifier(index)
	query := fmt.Sprintf("DROP TABLE IF EXISTS %s;", identifier)

	if _, err := s.pool.Exec(ctx, query); err != nil {
		return writeError(c, fiber.StatusInternalServerError, err.Error())
	}

	return writeJSON(c, fiber.StatusOK, map[string]any{
		"acknowledged": true,
		"index":        index,
	})
}

func (s *server) handleDeleteByQuery(c *fiber.Ctx, index string) error {
	if ok, err := s.indexExists(c.Context(), index); err != nil {
		return writeError(c, fiber.StatusInternalServerError, err.Error())
	} else if !ok {
		return writeError(c, fiber.StatusNotFound, "index not found")
	}

	var body map[string]any
	if len(c.Body()) > 0 {
		decoder := json.NewDecoder(strings.NewReader(string(c.Body())))
		decoder.UseNumber()
		if err := decoder.Decode(&body); err != nil && !errors.Is(err, io.EOF) {
			return writeError(c, fiber.StatusBadRequest, "invalid json body")
		}
	}

	values := []any{}
	whereClause, err := buildWhereClause(body["query"], &values)
	if err != nil {
		return writeError(c, fiber.StatusBadRequest, err.Error())
	}

	identifier := quoteIdentifier(index)
	query := fmt.Sprintf("DELETE FROM %s %s", identifier, whereClause)
	commandTag, err := s.pool.Exec(c.Context(), query, values...)
	if err != nil {
		return writeError(c, fiber.StatusInternalServerError, err.Error())
	}

	return writeJSON(c, fiber.StatusOK, map[string]any{
		"deleted": commandTag.RowsAffected(),
	})
}

func (s *server) handleBulk(c *fiber.Ctx, index string) error {
	body := c.Body()
	if len(body) == 0 && c.Request().Header.ContentLength() > 0 {
		return writeError(c, fiber.StatusBadRequest, "unable to read request body")
	}

	operations, err := parseBulk(body)
	if err != nil {
		return writeError(c, fiber.StatusBadRequest, err.Error())
	}

	if len(operations) == 0 {
		return writeJSON(c, fiber.StatusOK, map[string]any{
			"errors": false,
			"items":  []any{},
		})
	}

	values := make([]any, 0, len(operations)*2)
	placeholders := make([]string, 0, len(operations))
	for i, op := range operations {
		values = append(values, op.ID, op.Source)
		base := i*2 + 1
		placeholders = append(placeholders, fmt.Sprintf("($%d, $%d)", base, base+1))
	}

	identifier := quoteIdentifier(index)
	query := fmt.Sprintf(`
		INSERT INTO %s (id, document)
		VALUES %s
		ON CONFLICT (id) DO UPDATE SET document = EXCLUDED.document
	`, identifier, strings.Join(placeholders, ", "))

	if _, err := s.pool.Exec(c.Context(), query, values...); err != nil {
		return writeError(c, fiber.StatusInternalServerError, err.Error())
	}

	items := make([]map[string]any, 0, len(operations))
	for _, op := range operations {
		items = append(items, map[string]any{
			"index": map[string]any{
				"_id":    op.ID,
				"status": 201,
			},
		})
	}

	return writeJSON(c, fiber.StatusOK, map[string]any{
		"errors": false,
		"items":  items,
	})
}

func (s *server) handleSearch(c *fiber.Ctx, index string) error {
	if ok, err := s.indexExists(c.Context(), index); err != nil {
		return writeError(c, fiber.StatusInternalServerError, err.Error())
	} else if !ok {
		return writeError(c, fiber.StatusNotFound, "index not found")
	}

	var body map[string]any
	if len(c.Body()) > 0 {
		decoder := json.NewDecoder(strings.NewReader(string(c.Body())))
		decoder.UseNumber()
		if err := decoder.Decode(&body); err != nil && !errors.Is(err, io.EOF) {
			return writeError(c, fiber.StatusBadRequest, "invalid json body")
		}
	}

	values := []any{}
	whereClause, err := buildWhereClause(body["query"], &values)
	if err != nil {
		return writeError(c, fiber.StatusBadRequest, err.Error())
	}

	size := toInt(body["size"], 10)
	from := toInt(body["from"], 0)

	identifier := quoteIdentifier(index)
	query := fmt.Sprintf(`
		SELECT id, document
		FROM %s
		%s
		ORDER BY created_at DESC
		LIMIT %d
		OFFSET %d
	`, identifier, whereClause, size, from)

	rows, err := s.pool.Query(c.Context(), query, values...)
	if err != nil {
		return writeError(c, fiber.StatusInternalServerError, err.Error())
	}
	defer rows.Close()

	hits := []map[string]any{}
	for rows.Next() {
		var id string
		var document map[string]any
		if err := rows.Scan(&id, &document); err != nil {
			return writeError(c, fiber.StatusInternalServerError, err.Error())
		}
		hits = append(hits, map[string]any{
			"_id":     id,
			"_source": document,
		})
	}
	if rows.Err() != nil {
		return writeError(c, fiber.StatusInternalServerError, rows.Err().Error())
	}

	aggregations := map[string]any{}
	aggQuery, err := buildAggs(body, identifier, whereClause)
	if err != nil {
		return writeError(c, fiber.StatusBadRequest, err.Error())
	}

	if aggQuery != nil {
		aggRows, err := s.pool.Query(c.Context(), aggQuery.SQL, values...)
		if err != nil {
			return writeError(c, fiber.StatusInternalServerError, err.Error())
		}
		defer aggRows.Close()

		buckets := []map[string]any{}
		for aggRows.Next() {
			var key any
			var count int
			if err := aggRows.Scan(&key, &count); err != nil {
				return writeError(c, fiber.StatusInternalServerError, err.Error())
			}

			bucket := map[string]any{"doc_count": count}
			switch aggQuery.Type {
			case "terms":
				bucket["key"] = key
			case "histogram":
				bucket["key"] = key
			case "date_histogram":
				switch value := key.(type) {
				case time.Time:
					bucket["key_as_string"] = value.UTC().Format(time.RFC3339)
					bucket["key"] = value.UTC().UnixMilli()
				default:
					bucket["key"] = nil
					bucket["key_as_string"] = nil
				}
			}
			buckets = append(buckets, bucket)
		}
		if aggRows.Err() != nil {
			return writeError(c, fiber.StatusInternalServerError, aggRows.Err().Error())
		}

		aggregations[aggQuery.Name] = map[string]any{"buckets": buckets}
	}

	return writeJSON(c, fiber.StatusOK, map[string]any{
		"hits": map[string]any{
			"total": len(hits),
			"hits":  hits,
		},
		"aggregations": aggregations,
	})
}

func (s *server) indexExists(ctx context.Context, index string) (bool, error) {
	var exists bool
	if err := s.pool.QueryRow(ctx, "SELECT to_regclass($1) IS NOT NULL", fmt.Sprintf("public.%s", index)).Scan(&exists); err != nil {
		return false, err
	}
	return exists, nil
}

func (s *server) upsertDocument(ctx context.Context, index string, id string, document map[string]any) (bool, error) {
	identifier := quoteIdentifier(index)
	query := fmt.Sprintf(`
		INSERT INTO %s (id, document)
		VALUES ($1, $2)
		ON CONFLICT (id) DO UPDATE SET document = EXCLUDED.document
		RETURNING (xmax = 0)
	`, identifier)
	var inserted bool
	if err := s.pool.QueryRow(ctx, query, id, document).Scan(&inserted); err != nil {
		return false, err
	}
	return inserted, nil
}

func parseDocumentBody(c *fiber.Ctx, target *map[string]any) error {
	if len(c.Body()) == 0 {
		return errors.New("document body required")
	}
	decoder := json.NewDecoder(strings.NewReader(string(c.Body())))
	decoder.UseNumber()
	if err := decoder.Decode(target); err != nil {
		return errors.New("invalid json body")
	}
	if len(*target) == 0 {
		return errors.New("document body required")
	}
	return nil
}

func parseBulk(body []byte) ([]bulkOperation, error) {
	scanner := bufio.NewScanner(strings.NewReader(string(body)))
	scanner.Buffer(make([]byte, 0, 64*1024), 10*1024*1024)
	lines := []string{}
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		lines = append(lines, line)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("bulk parse error: %w", err)
	}

	operations := []bulkOperation{}
	for i := 0; i < len(lines); i += 2 {
		if i+1 >= len(lines) {
			return nil, errors.New("invalid bulk payload; expected action and source")
		}
		var action map[string]map[string]any
		if err := json.Unmarshal([]byte(lines[i]), &action); err != nil {
			return nil, errors.New("invalid bulk action json")
		}

		indexAction, ok := action["index"]
		if !ok {
			return nil, errors.New("invalid bulk action; expected index action")
		}

		var source map[string]any
		if err := json.Unmarshal([]byte(lines[i+1]), &source); err != nil {
			return nil, errors.New("invalid bulk source json")
		}

		idValue, ok := indexAction["_id"].(string)
		if !ok || idValue == "" {
			idValue = fmt.Sprintf("%d", time.Now().UnixNano())
		}

		operations = append(operations, bulkOperation{ID: idValue, Source: source})
	}

	return operations, nil
}

func buildWhereClause(query any, values *[]any) (string, error) {
	if query == nil {
		return "", nil
	}

	queryMap, ok := query.(map[string]any)
	if !ok || len(queryMap) == 0 {
		return "", nil
	}

	if term, ok := queryMap["term"].(map[string]any); ok {
		for field, value := range term {
			if err := assertIdentifier(field, "term field"); err != nil {
				return "", err
			}
			*values = append(*values, fmt.Sprint(value))
			return fmt.Sprintf("WHERE document ->> '%s' = $%d", field, len(*values)), nil
		}
	}

	if match, ok := queryMap["match"].(map[string]any); ok {
		for field, value := range match {
			if err := assertIdentifier(field, "match field"); err != nil {
				return "", err
			}
			*values = append(*values, fmt.Sprintf("%%%s%%", value))
			return fmt.Sprintf("WHERE document ->> '%s' ILIKE $%d", field, len(*values)), nil
		}
	}

	if rangeValue, ok := queryMap["range"].(map[string]any); ok {
		for field, value := range rangeValue {
			if err := assertIdentifier(field, "range field"); err != nil {
				return "", err
			}
			constraints, ok := value.(map[string]any)
			if !ok {
				continue
			}
			clauses := []string{}
			if gte, ok := constraints["gte"]; ok {
				*values = append(*values, fmt.Sprint(gte))
				clauses = append(clauses, fmt.Sprintf("document ->> '%s' >= $%d", field, len(*values)))
			}
			if lte, ok := constraints["lte"]; ok {
				*values = append(*values, fmt.Sprint(lte))
				clauses = append(clauses, fmt.Sprintf("document ->> '%s' <= $%d", field, len(*values)))
			}
			if len(clauses) > 0 {
				return "WHERE " + strings.Join(clauses, " AND "), nil
			}
		}
	}

	if boolQuery, ok := queryMap["bool"].(map[string]any); ok {
		must, ok := boolQuery["must"].([]any)
		if !ok {
			return "", nil
		}
		clauses := []string{}
		for _, condition := range must {
			clause, err := buildWhereClause(condition, values)
			if err != nil {
				return "", err
			}
			if clause != "" {
				clauses = append(clauses, strings.TrimPrefix(clause, "WHERE "))
			}
		}
		if len(clauses) > 0 {
			return "WHERE " + strings.Join(clauses, " AND "), nil
		}
	}

	return "", nil
}

func buildAggs(body map[string]any, tableName string, whereClause string) (*aggQuery, error) {
	aggs := body["aggs"]
	if aggs == nil {
		aggs = body["aggregations"]
	}

	aggMap, ok := aggs.(map[string]any)
	if !ok || len(aggMap) == 0 {
		return nil, nil
	}

	var name string
	var aggBody map[string]any
	for key, value := range aggMap {
		aggBody, _ = value.(map[string]any)
		name = key
		break
	}
	if aggBody == nil {
		return nil, nil
	}

	if terms, ok := aggBody["terms"].(map[string]any); ok {
		field, ok := terms["field"].(string)
		if !ok {
			return nil, errors.New("terms aggregation requires field")
		}
		if err := assertIdentifier(field, "terms field"); err != nil {
			return nil, err
		}
		limit := 10
		if sizeValue, ok := terms["size"]; ok {
			limit = toInt(sizeValue, 10)
		}
		sql := fmt.Sprintf(`
			SELECT document ->> '%s' AS key, COUNT(*)::int AS doc_count
			FROM %s
			%s
			GROUP BY key
			ORDER BY doc_count DESC
			LIMIT %d
		`, field, tableName, whereClause, limit)
		return &aggQuery{Name: name, SQL: sql, Type: "terms"}, nil
	}

	if histogram, ok := aggBody["histogram"].(map[string]any); ok {
		field, ok := histogram["field"].(string)
		if !ok {
			return nil, errors.New("histogram aggregation requires field")
		}
		if err := assertIdentifier(field, "histogram field"); err != nil {
			return nil, err
		}
		intervalValue, ok := histogram["interval"]
		if !ok {
			return nil, errors.New("histogram aggregation requires interval")
		}
		interval := float64(0)
		switch value := intervalValue.(type) {
		case json.Number:
			if parsed, err := value.Float64(); err == nil {
				interval = parsed
			}
		case float64:
			interval = value
		case int:
			interval = float64(value)
		case int64:
			interval = float64(value)
		}
		if interval <= 0 {
			return nil, errors.New("histogram aggregation requires positive interval")
		}
		sql := fmt.Sprintf(`
			SELECT (floor((document ->> '%s')::numeric / %g) * %g)::numeric AS key, COUNT(*)::int AS doc_count
			FROM %s
			%s
			GROUP BY key
			ORDER BY key ASC
		`, field, interval, interval, tableName, whereClause)
		return &aggQuery{Name: name, SQL: sql, Type: "histogram"}, nil
	}

	if histogram, ok := aggBody["date_histogram"].(map[string]any); ok {
		field, ok := histogram["field"].(string)
		if !ok {
			return nil, errors.New("date_histogram aggregation requires field")
		}
		if err := assertIdentifier(field, "date_histogram field"); err != nil {
			return nil, err
		}
		interval, _ := histogram["calendar_interval"].(string)
		if interval == "" {
			interval = "day"
		}
		if _, ok := allowedIntervals[interval]; !ok {
			return nil, errors.New("unsupported calendar_interval")
		}

		sql := fmt.Sprintf(`
			SELECT date_trunc('%s', (document ->> '%s')::timestamptz) AS key, COUNT(*)::int AS doc_count
			FROM %s
			%s
			GROUP BY key
			ORDER BY key ASC
		`, interval, field, tableName, whereClause)
		return &aggQuery{Name: name, SQL: sql, Type: "date_histogram"}, nil
	}

	return nil, nil
}

func assertIdentifier(value string, label string) error {
	if !identifierPattern.MatchString(value) {
		return fmt.Errorf("invalid %s", label)
	}
	return nil
}

func quoteIdentifier(value string) string {
	return fmt.Sprintf(`"%s"`, value)
}

func toInt(value any, fallback int) int {
	switch v := value.(type) {
	case json.Number:
		if number, err := v.Int64(); err == nil {
			return int(number)
		}
	case float64:
		return int(v)
	case int:
		return v
	case int64:
		return int(v)
	case string:
		if parsed, err := strconv.Atoi(v); err == nil {
			return parsed
		}
	}
	return fallback
}

func writeJSON(c *fiber.Ctx, status int, payload any) error {
	c.Status(status)
	return c.JSON(payload)
}

func writeError(c *fiber.Ctx, status int, message string) error {
	return writeJSON(c, status, map[string]any{"error": message})
}
