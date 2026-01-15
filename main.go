package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
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
	pool       dbPool
	indexStore *indexStore
	pass       *passthroughConfig
	httpClient *http.Client
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

type indexMetadata struct {
	Mappings map[string]any
	Settings map[string]any
}

type indexStore struct {
	mu    sync.RWMutex
	items map[string]indexMetadata
}

type passthroughConfig struct {
	baseURL  *url.URL
	patterns []string
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

	srv := &server{
		pool:       pool,
		indexStore: newIndexStore(),
		pass:       newPassthroughConfig(),
		httpClient: &http.Client{Timeout: 30 * time.Second},
	}
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
	app.Get("/_cluster/state", func(c *fiber.Ctx) error {
		return s.handleClusterPassthrough(c)
	})
	app.Get("/_nodes", func(c *fiber.Ctx) error {
		return s.handleClusterPassthrough(c)
	})
	app.Get("/_tasks", func(c *fiber.Ctx) error {
		return s.handleClusterPassthrough(c)
	})
	app.All("/_snapshot/*", func(c *fiber.Ctx) error {
		return s.handleClusterPassthrough(c)
	})
	app.All("/_ilm/*", func(c *fiber.Ctx) error {
		return s.handleClusterPassthrough(c)
	})
	app.All("/_security/*", func(c *fiber.Ctx) error {
		return s.handleClusterPassthrough(c)
	})
	app.Get("/_alias", func(c *fiber.Ctx) error {
		return s.handleClusterPassthrough(c)
	})
	app.All("/_cat/*", func(c *fiber.Ctx) error {
		return s.handleClusterPassthrough(c)
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
	app.Get("/:index/_mapping", func(c *fiber.Ctx) error {
		index := c.Params("index")
		if err := assertIdentifier(index, "index name"); err != nil {
			return writeError(c, fiber.StatusBadRequest, err.Error())
		}
		return s.handleGetMapping(c, index)
	})
	app.Put("/:index/_mapping", func(c *fiber.Ctx) error {
		index := c.Params("index")
		if err := assertIdentifier(index, "index name"); err != nil {
			return writeError(c, fiber.StatusBadRequest, err.Error())
		}
		return s.handlePutMapping(c, index)
	})
	app.Get("/:index/_settings", func(c *fiber.Ctx) error {
		index := c.Params("index")
		if err := assertIdentifier(index, "index name"); err != nil {
			return writeError(c, fiber.StatusBadRequest, err.Error())
		}
		return s.handleGetSettings(c, index)
	})
	app.Put("/:index/_settings", func(c *fiber.Ctx) error {
		index := c.Params("index")
		if err := assertIdentifier(index, "index name"); err != nil {
			return writeError(c, fiber.StatusBadRequest, err.Error())
		}
		return s.handlePutSettings(c, index)
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
	app.Post("/:index/_update/:id", func(c *fiber.Ctx) error {
		index := c.Params("index")
		if err := assertIdentifier(index, "index name"); err != nil {
			return writeError(c, fiber.StatusBadRequest, err.Error())
		}
		return s.handleUpdateDocument(c, index, c.Params("id"))
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
	app.Post("/:index/_update_by_query", func(c *fiber.Ctx) error {
		index := c.Params("index")
		if err := assertIdentifier(index, "index name"); err != nil {
			return writeError(c, fiber.StatusBadRequest, err.Error())
		}
		return s.handleUpdateByQuery(c, index)
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
	app.Get("/:index/_count", func(c *fiber.Ctx) error {
		index := c.Params("index")
		if err := assertIdentifier(index, "index name"); err != nil {
			return writeError(c, fiber.StatusBadRequest, err.Error())
		}
		return s.handleCount(c, index)
	})
	app.Post("/:index/_count", func(c *fiber.Ctx) error {
		index := c.Params("index")
		if err := assertIdentifier(index, "index name"); err != nil {
			return writeError(c, fiber.StatusBadRequest, err.Error())
		}
		return s.handleCount(c, index)
	})
	app.Post("/:index/_mget", func(c *fiber.Ctx) error {
		index := c.Params("index")
		if err := assertIdentifier(index, "index name"); err != nil {
			return writeError(c, fiber.StatusBadRequest, err.Error())
		}
		return s.handleMultiGet(c, index)
	})
	app.Post("/_mget", func(c *fiber.Ctx) error {
		return s.handleMultiGet(c, "")
	})
	app.Post("/:index/_msearch", func(c *fiber.Ctx) error {
		index := c.Params("index")
		if err := assertIdentifier(index, "index name"); err != nil {
			return writeError(c, fiber.StatusBadRequest, err.Error())
		}
		return s.handleMultiSearch(c, index)
	})
	app.Post("/_msearch", func(c *fiber.Ctx) error {
		return s.handleMultiSearch(c, "")
	})
	app.All("/:index/*", func(c *fiber.Ctx) error {
		index := c.Params("index")
		if err := assertIdentifier(index, "index name"); err != nil {
			return writeError(c, fiber.StatusBadRequest, err.Error())
		}
		if !s.shouldPassthroughIndex(index) {
			return writeError(c, fiber.StatusNotFound, "endpoint not found")
		}
		return s.handlePassthrough(c)
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
	if s.shouldPassthroughIndex(index) {
		return s.handlePassthrough(c)
	}
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
	if s.shouldPassthroughIndex(index) {
		return s.handlePassthrough(c)
	}
	exists, err := s.indexExists(c.Context(), index)
	if err != nil {
		return writeError(c, fiber.StatusInternalServerError, err.Error())
	}
	if !exists {
		return writeError(c, fiber.StatusNotFound, "index not found")
	}
	mapping, settings := s.indexStore.get(index)
	return writeJSON(c, fiber.StatusOK, map[string]any{
		index: map[string]any{
			"aliases": map[string]any{},
			"mappings": map[string]any{
				"properties": mapping,
			},
			"settings": map[string]any{
				"index": map[string]any{
					"number_of_shards":   settings["number_of_shards"],
					"number_of_replicas": settings["number_of_replicas"],
				},
			},
		},
	})
}

func (s *server) handleCreateIndex(c *fiber.Ctx, index string) error {
	if s.shouldPassthroughIndex(index) {
		return s.handlePassthrough(c)
	}
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
	s.indexStore.ensure(index)

	return writeJSON(c, fiber.StatusOK, map[string]any{
		"acknowledged": true,
		"index":        index,
	})
}

func (s *server) handleCreateDocument(c *fiber.Ctx, index string) error {
	if s.shouldPassthroughIndex(index) {
		return s.handlePassthrough(c)
	}
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
	if s.shouldPassthroughIndex(index) {
		return s.handlePassthrough(c)
	}
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
	if s.shouldPassthroughIndex(index) {
		return s.handlePassthrough(c)
	}
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
	if s.shouldPassthroughIndex(index) {
		return s.handlePassthrough(c)
	}
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
	if s.shouldPassthroughIndex(index) {
		return s.handlePassthrough(c)
	}
	ctx := c.Context()
	identifier := quoteIdentifier(index)
	query := fmt.Sprintf("DROP TABLE IF EXISTS %s;", identifier)

	if _, err := s.pool.Exec(ctx, query); err != nil {
		return writeError(c, fiber.StatusInternalServerError, err.Error())
	}
	s.indexStore.delete(index)

	return writeJSON(c, fiber.StatusOK, map[string]any{
		"acknowledged": true,
		"index":        index,
	})
}

func (s *server) handleDeleteByQuery(c *fiber.Ctx, index string) error {
	if s.shouldPassthroughIndex(index) {
		return s.handlePassthrough(c)
	}
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
	if s.shouldPassthroughIndex(index) {
		return s.handlePassthrough(c)
	}
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
	if s.shouldPassthroughIndex(index) {
		return s.handlePassthrough(c)
	}
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

	result, err := s.executeSearch(c.Context(), index, body)
	if err != nil {
		if respErr, ok := err.(responseError); ok {
			return writeError(c, respErr.Status, respErr.Message)
		}
		return writeError(c, fiber.StatusInternalServerError, err.Error())
	}

	return writeJSON(c, fiber.StatusOK, map[string]any{
		"hits": map[string]any{
			"total": result.Total,
			"hits":  result.Hits,
		},
		"aggregations": result.Aggregations,
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

func (s *server) handleGetMapping(c *fiber.Ctx, index string) error {
	if s.shouldPassthroughIndex(index) {
		return s.handlePassthrough(c)
	}
	if ok, err := s.indexExists(c.Context(), index); err != nil {
		return writeError(c, fiber.StatusInternalServerError, err.Error())
	} else if !ok {
		return writeError(c, fiber.StatusNotFound, "index not found")
	}
	mapping, _ := s.indexStore.get(index)
	return writeJSON(c, fiber.StatusOK, map[string]any{
		index: map[string]any{
			"mappings": map[string]any{
				"properties": mapping,
			},
		},
	})
}

func (s *server) handlePutMapping(c *fiber.Ctx, index string) error {
	if s.shouldPassthroughIndex(index) {
		return s.handlePassthrough(c)
	}
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
	properties := map[string]any{}
	if props, ok := body["properties"].(map[string]any); ok {
		properties = props
	}
	s.indexStore.setMapping(index, properties)
	return writeJSON(c, fiber.StatusOK, map[string]any{
		"acknowledged": true,
	})
}

func (s *server) handleGetSettings(c *fiber.Ctx, index string) error {
	if s.shouldPassthroughIndex(index) {
		return s.handlePassthrough(c)
	}
	if ok, err := s.indexExists(c.Context(), index); err != nil {
		return writeError(c, fiber.StatusInternalServerError, err.Error())
	} else if !ok {
		return writeError(c, fiber.StatusNotFound, "index not found")
	}
	_, settings := s.indexStore.get(index)
	return writeJSON(c, fiber.StatusOK, map[string]any{
		index: map[string]any{
			"settings": map[string]any{
				"index": settings,
			},
		},
	})
}

func (s *server) handlePutSettings(c *fiber.Ctx, index string) error {
	if s.shouldPassthroughIndex(index) {
		return s.handlePassthrough(c)
	}
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
	settings := map[string]any{}
	if indexSettings, ok := body["index"].(map[string]any); ok {
		settings = indexSettings
	}
	s.indexStore.setSettings(index, settings)
	return writeJSON(c, fiber.StatusOK, map[string]any{
		"acknowledged": true,
	})
}

func (s *server) handleUpdateDocument(c *fiber.Ctx, index string, id string) error {
	if s.shouldPassthroughIndex(index) {
		return s.handlePassthrough(c)
	}
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

	doc, ok := body["doc"].(map[string]any)
	if !ok || len(doc) == 0 {
		return writeError(c, fiber.StatusBadRequest, "update requires doc")
	}
	docAsUpsert, _ := body["doc_as_upsert"].(bool)
	upsertBody, _ := body["upsert"].(map[string]any)

	identifier := quoteIdentifier(index)
	updateQuery := fmt.Sprintf("UPDATE %s SET document = document || $1 WHERE id = $2 RETURNING id", identifier)
	var updatedID string
	err := s.pool.QueryRow(c.Context(), updateQuery, doc, id).Scan(&updatedID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			if docAsUpsert || len(upsertBody) > 0 {
				insertDoc := doc
				if len(insertDoc) == 0 {
					insertDoc = upsertBody
				}
				inserted, err := s.upsertDocument(c.Context(), index, id, insertDoc)
				if err != nil {
					return writeError(c, fiber.StatusInternalServerError, err.Error())
				}
				result := "updated"
				status := fiber.StatusOK
				if inserted {
					result = "created"
					status = fiber.StatusCreated
				}
				return writeJSON(c, status, map[string]any{
					"_index": index,
					"_id":    id,
					"result": result,
				})
			}
			return writeJSON(c, fiber.StatusNotFound, map[string]any{
				"_index": index,
				"_id":    id,
				"result": "not_found",
			})
		}
		return writeError(c, fiber.StatusInternalServerError, err.Error())
	}

	return writeJSON(c, fiber.StatusOK, map[string]any{
		"_index": index,
		"_id":    id,
		"result": "updated",
	})
}

func (s *server) handleUpdateByQuery(c *fiber.Ctx, index string) error {
	if s.shouldPassthroughIndex(index) {
		return s.handlePassthrough(c)
	}
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

	doc, ok := body["doc"].(map[string]any)
	if !ok || len(doc) == 0 {
		return writeError(c, fiber.StatusBadRequest, "update_by_query requires doc")
	}

	values := []any{doc}
	whereClause, err := buildWhereClause(body["query"], &values)
	if err != nil {
		return writeError(c, fiber.StatusBadRequest, err.Error())
	}

	identifier := quoteIdentifier(index)
	query := fmt.Sprintf("UPDATE %s SET document = document || $1 %s", identifier, whereClause)
	commandTag, err := s.pool.Exec(c.Context(), query, values...)
	if err != nil {
		return writeError(c, fiber.StatusInternalServerError, err.Error())
	}

	return writeJSON(c, fiber.StatusOK, map[string]any{
		"updated": commandTag.RowsAffected(),
	})
}

func (s *server) handleCount(c *fiber.Ctx, index string) error {
	if s.shouldPassthroughIndex(index) {
		return s.handlePassthrough(c)
	}
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
	query := fmt.Sprintf("SELECT COUNT(*)::int FROM %s %s", identifier, whereClause)
	var count int
	if err := s.pool.QueryRow(c.Context(), query, values...).Scan(&count); err != nil {
		return writeError(c, fiber.StatusInternalServerError, err.Error())
	}

	return writeJSON(c, fiber.StatusOK, map[string]any{
		"count": count,
		"_shards": map[string]any{
			"total":      1,
			"successful": 1,
			"skipped":    0,
			"failed":     0,
		},
	})
}

func (s *server) handleMultiGet(c *fiber.Ctx, index string) error {
	if index != "" && s.shouldPassthroughIndex(index) {
		return s.handlePassthrough(c)
	}
	var body map[string]any
	if len(c.Body()) > 0 {
		decoder := json.NewDecoder(strings.NewReader(string(c.Body())))
		decoder.UseNumber()
		if err := decoder.Decode(&body); err != nil && !errors.Is(err, io.EOF) {
			return writeError(c, fiber.StatusBadRequest, "invalid json body")
		}
	}

	var requests []map[string]any
	if docs, ok := body["docs"].([]any); ok {
		for _, doc := range docs {
			if docMap, ok := doc.(map[string]any); ok {
				requests = append(requests, docMap)
			}
		}
	} else if ids, ok := body["ids"].([]any); ok {
		for _, id := range ids {
			requests = append(requests, map[string]any{"_id": fmt.Sprint(id)})
		}
	}

	if len(requests) == 0 {
		return writeError(c, fiber.StatusBadRequest, "mget requires docs or ids")
	}

	responses := make([]map[string]any, 0, len(requests))
	grouped := map[string][]string{}
	order := make([]string, 0, len(requests))
	for _, req := range requests {
		reqIndex := index
		if reqIndex == "" {
			if indexValue, ok := req["_index"].(string); ok {
				reqIndex = indexValue
			}
		}
		if reqIndex == "" {
			return writeError(c, fiber.StatusBadRequest, "mget requires index")
		}
		if err := assertIdentifier(reqIndex, "index name"); err != nil {
			return writeError(c, fiber.StatusBadRequest, err.Error())
		}
		id := fmt.Sprint(req["_id"])
		grouped[reqIndex] = append(grouped[reqIndex], id)
		order = append(order, reqIndex+"::"+id)
	}

	foundDocs := map[string]map[string]map[string]any{}
	for reqIndex, ids := range grouped {
		if ok, err := s.indexExists(c.Context(), reqIndex); err != nil {
			return writeError(c, fiber.StatusInternalServerError, err.Error())
		} else if !ok {
			foundDocs[reqIndex] = map[string]map[string]any{}
			continue
		}
		identifier := quoteIdentifier(reqIndex)
		query := fmt.Sprintf("SELECT id, document FROM %s WHERE id = ANY($1)", identifier)
		rows, err := s.pool.Query(c.Context(), query, ids)
		if err != nil {
			return writeError(c, fiber.StatusInternalServerError, err.Error())
		}
		docs := map[string]map[string]any{}
		for rows.Next() {
			var id string
			var doc map[string]any
			if err := rows.Scan(&id, &doc); err != nil {
				rows.Close()
				return writeError(c, fiber.StatusInternalServerError, err.Error())
			}
			docs[id] = doc
		}
		rows.Close()
		foundDocs[reqIndex] = docs
	}

	for _, key := range order {
		parts := strings.SplitN(key, "::", 2)
		reqIndex := parts[0]
		id := parts[1]
		docs := foundDocs[reqIndex]
		doc, ok := docs[id]
		if ok {
			responses = append(responses, map[string]any{
				"_index":  reqIndex,
				"_id":     id,
				"found":   true,
				"_source": doc,
			})
		} else {
			responses = append(responses, map[string]any{
				"_index": reqIndex,
				"_id":    id,
				"found":  false,
			})
		}
	}

	return writeJSON(c, fiber.StatusOK, map[string]any{
		"docs": responses,
	})
}

func (s *server) handleMultiSearch(c *fiber.Ctx, index string) error {
	if index != "" && s.shouldPassthroughIndex(index) {
		return s.handlePassthrough(c)
	}
	body := c.Body()
	if len(body) == 0 && c.Request().Header.ContentLength() > 0 {
		return writeError(c, fiber.StatusBadRequest, "unable to read request body")
	}
	queries, err := parseMultiSearch(body)
	if err != nil {
		return writeError(c, fiber.StatusBadRequest, err.Error())
	}
	responses := make([]map[string]any, 0, len(queries))
	for _, query := range queries {
		targetIndex := index
		if query.Index != "" {
			targetIndex = query.Index
		}
		if targetIndex == "" {
			responses = append(responses, map[string]any{
				"error": map[string]any{
					"type":   "index_missing_exception",
					"reason": "index missing",
				},
			})
			continue
		}
		if err := assertIdentifier(targetIndex, "index name"); err != nil {
			responses = append(responses, map[string]any{
				"error": map[string]any{
					"type":   "invalid_index_name_exception",
					"reason": err.Error(),
				},
			})
			continue
		}
		if ok, err := s.indexExists(c.Context(), targetIndex); err != nil {
			return writeError(c, fiber.StatusInternalServerError, err.Error())
		} else if !ok {
			responses = append(responses, map[string]any{
				"error": map[string]any{
					"type":   "index_not_found_exception",
					"reason": "index not found",
				},
			})
			continue
		}
		result, err := s.executeSearch(c.Context(), targetIndex, query.Body)
		if err != nil {
			if respErr, ok := err.(responseError); ok {
				responses = append(responses, map[string]any{
					"error": map[string]any{
						"type":   "search_phase_execution_exception",
						"reason": respErr.Message,
					},
					"status": respErr.Status,
				})
				continue
			}
			return writeError(c, fiber.StatusInternalServerError, err.Error())
		}
		responses = append(responses, map[string]any{
			"hits": map[string]any{
				"total": result.Total,
				"hits":  result.Hits,
			},
			"aggregations": result.Aggregations,
		})
	}

	return writeJSON(c, fiber.StatusOK, map[string]any{
		"responses": responses,
	})
}

func (s *server) handleClusterPassthrough(c *fiber.Ctx) error {
	return s.handlePassthrough(c)
}

func (s *server) handlePassthrough(c *fiber.Ctx) error {
	if s.pass == nil || s.pass.baseURL == nil {
		return writeError(c, fiber.StatusNotImplemented, "passthrough not configured")
	}
	target := s.pass.baseURL.ResolveReference(&url.URL{Path: string(c.Request().URI().Path()), RawQuery: string(c.Request().URI().QueryString())})
	req, err := http.NewRequestWithContext(context.Background(), c.Method(), target.String(), strings.NewReader(string(c.Body())))
	if err != nil {
		return writeError(c, fiber.StatusInternalServerError, err.Error())
	}
	c.Request().Header.VisitAll(func(k, v []byte) {
		req.Header.Set(string(k), string(v))
	})
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return writeError(c, fiber.StatusBadGateway, err.Error())
	}
	defer resp.Body.Close()

	c.Status(resp.StatusCode)
	for key, values := range resp.Header {
		for _, value := range values {
			c.Set(key, value)
		}
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return writeError(c, fiber.StatusBadGateway, err.Error())
	}
	return c.Send(body)
}

func (s *server) shouldPassthroughIndex(index string) bool {
	if s.pass == nil || s.pass.baseURL == nil {
		return false
	}
	if len(s.pass.patterns) == 0 {
		return false
	}
	for _, pattern := range s.pass.patterns {
		if matchPattern(pattern, index) {
			return true
		}
	}
	return false
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
	clause, err := buildQueryClause(query, values)
	if err != nil || clause == "" {
		return "", err
	}
	return "WHERE " + clause, nil
}

func buildQueryClause(query any, values *[]any) (string, error) {
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
			return fmt.Sprintf("document ->> '%s' = $%d", field, len(*values)), nil
		}
	}

	if match, ok := queryMap["match"].(map[string]any); ok {
		for field, value := range match {
			if err := assertIdentifier(field, "match field"); err != nil {
				return "", err
			}
			*values = append(*values, fmt.Sprintf("%%%s%%", value))
			return fmt.Sprintf("document ->> '%s' ILIKE $%d", field, len(*values)), nil
		}
	}

	if terms, ok := queryMap["terms"].(map[string]any); ok {
		for field, value := range terms {
			if err := assertIdentifier(field, "terms field"); err != nil {
				return "", err
			}
			list, ok := value.([]any)
			if !ok {
				return "", nil
			}
			placeholders := make([]string, 0, len(list))
			for _, item := range list {
				*values = append(*values, fmt.Sprint(item))
				placeholders = append(placeholders, fmt.Sprintf("$%d", len(*values)))
			}
			if len(placeholders) == 0 {
				return "", nil
			}
			return fmt.Sprintf("document ->> '%s' IN (%s)", field, strings.Join(placeholders, ", ")), nil
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
				return strings.Join(clauses, " AND "), nil
			}
		}
	}

	if boolQuery, ok := queryMap["bool"].(map[string]any); ok {
		clauses := []string{}
		if must, ok := boolQuery["must"].([]any); ok {
			for _, condition := range must {
				clause, err := buildQueryClause(condition, values)
				if err != nil {
					return "", err
				}
				if clause != "" {
					clauses = append(clauses, clause)
				}
			}
		}
		if filter, ok := boolQuery["filter"].([]any); ok {
			for _, condition := range filter {
				clause, err := buildQueryClause(condition, values)
				if err != nil {
					return "", err
				}
				if clause != "" {
					clauses = append(clauses, clause)
				}
			}
		}
		if mustNot, ok := boolQuery["must_not"].([]any); ok {
			for _, condition := range mustNot {
				clause, err := buildQueryClause(condition, values)
				if err != nil {
					return "", err
				}
				if clause != "" {
					clauses = append(clauses, fmt.Sprintf("NOT (%s)", clause))
				}
			}
		}
		if should, ok := boolQuery["should"].([]any); ok {
			shouldClauses := []string{}
			for _, condition := range should {
				clause, err := buildQueryClause(condition, values)
				if err != nil {
					return "", err
				}
				if clause != "" {
					shouldClauses = append(shouldClauses, clause)
				}
			}
			if len(shouldClauses) > 0 {
				clauses = append(clauses, fmt.Sprintf("(%s)", strings.Join(shouldClauses, " OR ")))
			}
		}
		if len(clauses) > 0 {
			return strings.Join(clauses, " AND "), nil
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

	if avg, ok := aggBody["avg"].(map[string]any); ok {
		field, ok := avg["field"].(string)
		if !ok {
			return nil, errors.New("avg aggregation requires field")
		}
		if err := assertIdentifier(field, "avg field"); err != nil {
			return nil, err
		}
		sql := fmt.Sprintf("SELECT AVG((document ->> '%s')::numeric) AS value FROM %s %s", field, tableName, whereClause)
		return &aggQuery{Name: name, SQL: sql, Type: "avg"}, nil
	}

	if min, ok := aggBody["min"].(map[string]any); ok {
		field, ok := min["field"].(string)
		if !ok {
			return nil, errors.New("min aggregation requires field")
		}
		if err := assertIdentifier(field, "min field"); err != nil {
			return nil, err
		}
		sql := fmt.Sprintf("SELECT MIN((document ->> '%s')::numeric) AS value FROM %s %s", field, tableName, whereClause)
		return &aggQuery{Name: name, SQL: sql, Type: "min"}, nil
	}

	if max, ok := aggBody["max"].(map[string]any); ok {
		field, ok := max["field"].(string)
		if !ok {
			return nil, errors.New("max aggregation requires field")
		}
		if err := assertIdentifier(field, "max field"); err != nil {
			return nil, err
		}
		sql := fmt.Sprintf("SELECT MAX((document ->> '%s')::numeric) AS value FROM %s %s", field, tableName, whereClause)
		return &aggQuery{Name: name, SQL: sql, Type: "max"}, nil
	}

	if sum, ok := aggBody["sum"].(map[string]any); ok {
		field, ok := sum["field"].(string)
		if !ok {
			return nil, errors.New("sum aggregation requires field")
		}
		if err := assertIdentifier(field, "sum field"); err != nil {
			return nil, err
		}
		sql := fmt.Sprintf("SELECT SUM((document ->> '%s')::numeric) AS value FROM %s %s", field, tableName, whereClause)
		return &aggQuery{Name: name, SQL: sql, Type: "sum"}, nil
	}

	if stats, ok := aggBody["stats"].(map[string]any); ok {
		field, ok := stats["field"].(string)
		if !ok {
			return nil, errors.New("stats aggregation requires field")
		}
		if err := assertIdentifier(field, "stats field"); err != nil {
			return nil, err
		}
		sql := fmt.Sprintf(`
			SELECT
				COUNT(document ->> '%s')::int AS count,
				MIN((document ->> '%s')::numeric) AS min,
				MAX((document ->> '%s')::numeric) AS max,
				AVG((document ->> '%s')::numeric) AS avg,
				SUM((document ->> '%s')::numeric) AS sum
			FROM %s
			%s
		`, field, field, field, field, field, tableName, whereClause)
		return &aggQuery{Name: name, SQL: sql, Type: "stats"}, nil
	}

	if cardinality, ok := aggBody["cardinality"].(map[string]any); ok {
		field, ok := cardinality["field"].(string)
		if !ok {
			return nil, errors.New("cardinality aggregation requires field")
		}
		if err := assertIdentifier(field, "cardinality field"); err != nil {
			return nil, err
		}
		sql := fmt.Sprintf("SELECT COUNT(DISTINCT document ->> '%s')::int AS value FROM %s %s", field, tableName, whereClause)
		return &aggQuery{Name: name, SQL: sql, Type: "cardinality"}, nil
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

func newIndexStore() *indexStore {
	return &indexStore{
		items: make(map[string]indexMetadata),
	}
}

func (s *indexStore) ensure(index string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.items[index]; ok {
		return
	}
	s.items[index] = indexMetadata{
		Mappings: map[string]any{},
		Settings: map[string]any{
			"number_of_shards":   "1",
			"number_of_replicas": "0",
		},
	}
}

func (s *indexStore) get(index string) (map[string]any, map[string]any) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	item, ok := s.items[index]
	if !ok {
		return map[string]any{}, map[string]any{
			"number_of_shards":   "1",
			"number_of_replicas": "0",
		}
	}
	return item.Mappings, item.Settings
}

func (s *indexStore) setMapping(index string, mapping map[string]any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	item, ok := s.items[index]
	if !ok {
		item = indexMetadata{
			Mappings: map[string]any{},
			Settings: map[string]any{
				"number_of_shards":   "1",
				"number_of_replicas": "0",
			},
		}
	}
	item.Mappings = mapping
	s.items[index] = item
}

func (s *indexStore) setSettings(index string, settings map[string]any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	item, ok := s.items[index]
	if !ok {
		item = indexMetadata{
			Mappings: map[string]any{},
			Settings: map[string]any{
				"number_of_shards":   "1",
				"number_of_replicas": "0",
			},
		}
	}
	for key, value := range settings {
		item.Settings[key] = value
	}
	s.items[index] = item
}

func (s *indexStore) delete(index string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.items, index)
}

func newPassthroughConfig() *passthroughConfig {
	rawURL := os.Getenv("PASSTHROUGH_URL")
	if rawURL == "" {
		return nil
	}
	parsed, err := url.Parse(rawURL)
	if err != nil {
		log.Printf("invalid PASSTHROUGH_URL: %v", err)
		return nil
	}
	patterns := []string{}
	if rawPatterns := os.Getenv("PASSTHROUGH_INDICES"); rawPatterns != "" {
		for _, pattern := range strings.Split(rawPatterns, ",") {
			pattern = strings.TrimSpace(pattern)
			if pattern != "" {
				patterns = append(patterns, pattern)
			}
		}
	}
	return &passthroughConfig{baseURL: parsed, patterns: patterns}
}

func matchPattern(pattern string, value string) bool {
	if pattern == "*" {
		return true
	}
	parts := strings.Split(pattern, "*")
	if len(parts) == 1 {
		return pattern == value
	}
	if !strings.HasPrefix(value, parts[0]) {
		return false
	}
	if !strings.HasSuffix(value, parts[len(parts)-1]) {
		return false
	}
	current := value
	for _, part := range parts {
		if part == "" {
			continue
		}
		idx := strings.Index(current, part)
		if idx == -1 {
			return false
		}
		current = current[idx+len(part):]
	}
	return true
}

type searchResult struct {
	Hits         []map[string]any
	Total        int
	Aggregations map[string]any
}

func (s *server) executeSearch(ctx context.Context, index string, body map[string]any) (*searchResult, error) {
	values := []any{}
	whereClause, err := buildWhereClause(body["query"], &values)
	if err != nil {
		return nil, writeErrorMessage(fiber.StatusBadRequest, err.Error())
	}

	size := toInt(body["size"], 10)
	from := toInt(body["from"], 0)
	sortClause, err := buildSortClause(body["sort"])
	if err != nil {
		return nil, writeErrorMessage(fiber.StatusBadRequest, err.Error())
	}

	identifier := quoteIdentifier(index)
	query := fmt.Sprintf(`
		SELECT id, document
		FROM %s
		%s
		%s
		LIMIT %d
		OFFSET %d
	`, identifier, whereClause, sortClause, size, from)

	rows, err := s.pool.Query(ctx, query, values...)
	if err != nil {
		return nil, writeErrorMessage(fiber.StatusInternalServerError, err.Error())
	}
	defer rows.Close()

	hits := []map[string]any{}
	for rows.Next() {
		var id string
		var document map[string]any
		if err := rows.Scan(&id, &document); err != nil {
			return nil, writeErrorMessage(fiber.StatusInternalServerError, err.Error())
		}
		hits = append(hits, map[string]any{
			"_id":     id,
			"_source": document,
		})
	}
	if rows.Err() != nil {
		return nil, writeErrorMessage(fiber.StatusInternalServerError, rows.Err().Error())
	}

	aggregations := map[string]any{}
	aggQuery, err := buildAggs(body, identifier, whereClause)
	if err != nil {
		return nil, writeErrorMessage(fiber.StatusBadRequest, err.Error())
	}

	if aggQuery != nil {
		switch aggQuery.Type {
		case "terms", "histogram", "date_histogram":
			aggRows, err := s.pool.Query(ctx, aggQuery.SQL, values...)
			if err != nil {
				return nil, writeErrorMessage(fiber.StatusInternalServerError, err.Error())
			}
			defer aggRows.Close()

			buckets := []map[string]any{}
			for aggRows.Next() {
				var key any
				var count int
				if err := aggRows.Scan(&key, &count); err != nil {
					return nil, writeErrorMessage(fiber.StatusInternalServerError, err.Error())
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
				return nil, writeErrorMessage(fiber.StatusInternalServerError, aggRows.Err().Error())
			}

			aggregations[aggQuery.Name] = map[string]any{"buckets": buckets}
		case "avg", "min", "max", "sum":
			var value any
			if err := s.pool.QueryRow(ctx, aggQuery.SQL, values...).Scan(&value); err != nil {
				return nil, writeErrorMessage(fiber.StatusInternalServerError, err.Error())
			}
			aggregations[aggQuery.Name] = map[string]any{"value": value}
		case "cardinality":
			var value int
			if err := s.pool.QueryRow(ctx, aggQuery.SQL, values...).Scan(&value); err != nil {
				return nil, writeErrorMessage(fiber.StatusInternalServerError, err.Error())
			}
			aggregations[aggQuery.Name] = map[string]any{"value": value}
		case "stats":
			var count int
			var min any
			var max any
			var avg any
			var sum any
			if err := s.pool.QueryRow(ctx, aggQuery.SQL, values...).Scan(&count, &min, &max, &avg, &sum); err != nil {
				return nil, writeErrorMessage(fiber.StatusInternalServerError, err.Error())
			}
			aggregations[aggQuery.Name] = map[string]any{
				"count": count,
				"min":   min,
				"max":   max,
				"avg":   avg,
				"sum":   sum,
			}
		}
	}

	return &searchResult{
		Hits:         hits,
		Total:        len(hits),
		Aggregations: aggregations,
	}, nil
}

func buildSortClause(sort any) (string, error) {
	if sort == nil {
		return "ORDER BY created_at DESC", nil
	}
	parts := []string{}
	switch value := sort.(type) {
	case []any:
		for _, entry := range value {
			clause, err := parseSortEntry(entry)
			if err != nil {
				return "", err
			}
			if clause != "" {
				parts = append(parts, clause)
			}
		}
	case map[string]any:
		for field, direction := range value {
			clause, err := parseSortField(field, direction)
			if err != nil {
				return "", err
			}
			if clause != "" {
				parts = append(parts, clause)
			}
		}
	case string:
		clause, err := parseSortField(value, "asc")
		if err != nil {
			return "", err
		}
		if clause != "" {
			parts = append(parts, clause)
		}
	}
	if len(parts) == 0 {
		return "ORDER BY created_at DESC", nil
	}
	return "ORDER BY " + strings.Join(parts, ", "), nil
}

func parseSortEntry(entry any) (string, error) {
	switch v := entry.(type) {
	case string:
		return parseSortField(v, "asc")
	case map[string]any:
		for field, direction := range v {
			return parseSortField(field, direction)
		}
	}
	return "", nil
}

func parseSortField(field string, direction any) (string, error) {
	order := "ASC"
	switch value := direction.(type) {
	case string:
		if strings.EqualFold(value, "desc") {
			order = "DESC"
		}
	case map[string]any:
		if ord, ok := value["order"].(string); ok && strings.EqualFold(ord, "desc") {
			order = "DESC"
		}
	}
	if field == "_id" || field == "id" {
		return fmt.Sprintf("id %s", order), nil
	}
	if err := assertIdentifier(field, "sort field"); err != nil {
		return "", err
	}
	return fmt.Sprintf("document ->> '%s' %s", field, order), nil
}

type multiSearchQuery struct {
	Index string
	Body  map[string]any
}

func parseMultiSearch(body []byte) ([]multiSearchQuery, error) {
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
		return nil, fmt.Errorf("msearch parse error: %w", err)
	}

	queries := []multiSearchQuery{}
	for i := 0; i < len(lines); i += 2 {
		if i+1 >= len(lines) {
			return nil, errors.New("invalid msearch payload; expected header and body")
		}
		var header map[string]any
		if err := json.Unmarshal([]byte(lines[i]), &header); err != nil {
			return nil, errors.New("invalid msearch header json")
		}
		var body map[string]any
		if err := json.Unmarshal([]byte(lines[i+1]), &body); err != nil {
			return nil, errors.New("invalid msearch body json")
		}
		index := ""
		if indexValue, ok := header["index"].(string); ok {
			index = indexValue
		}
		queries = append(queries, multiSearchQuery{Index: index, Body: body})
	}
	return queries, nil
}

type responseError struct {
	Status  int
	Message string
}

func (r responseError) Error() string {
	return r.Message
}

func writeErrorMessage(status int, message string) error {
	return responseError{Status: status, Message: message}
}
