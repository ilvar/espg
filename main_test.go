package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type mockRow struct {
	scan func(dest ...any) error
}

func (m mockRow) Scan(dest ...any) error {
	return m.scan(dest...)
}

type mockRows struct {
	values [][]any
	idx    int
	err    error
}

func (m *mockRows) Close()                        {}
func (m *mockRows) Err() error                    { return m.err }
func (m *mockRows) CommandTag() pgconn.CommandTag { return pgconn.CommandTag{} }
func (m *mockRows) FieldDescriptions() []pgconn.FieldDescription {
	return nil
}
func (m *mockRows) Next() bool {
	if m.idx >= len(m.values) {
		return false
	}
	m.idx++
	return true
}
func (m *mockRows) Scan(dest ...any) error {
	row := m.values[m.idx-1]
	for i := range dest {
		switch d := dest[i].(type) {
		case *string:
			*d = row[i].(string)
		case *map[string]any:
			*d = row[i].(map[string]any)
		case *any:
			*d = row[i]
		case *int:
			*d = row[i].(int)
		default:
			return errors.New("unsupported scan type")
		}
	}
	return nil
}
func (m *mockRows) Values() ([]any, error) {
	if m.idx == 0 || m.idx > len(m.values) {
		return nil, errors.New("no current row")
	}
	return m.values[m.idx-1], nil
}
func (m *mockRows) RawValues() [][]byte { return nil }
func (m *mockRows) Conn() *pgx.Conn     { return nil }

type mockPool struct {
	execFn     func(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	queryFn    func(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	queryRowFn func(ctx context.Context, sql string, args ...any) pgx.Row
}

func (m *mockPool) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	return m.execFn(ctx, sql, args...)
}

func (m *mockPool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return m.queryFn(ctx, sql, args...)
}

func (m *mockPool) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return m.queryRowFn(ctx, sql, args...)
}

func setupApp(pool dbPool) *fiber.App {
	app := fiber.New()
	srv := &server{
		pool:       pool,
		indexStore: newIndexStore(),
		httpClient: &http.Client{Timeout: 30 * time.Second},
	}
	srv.registerRoutes(app)
	return app
}

func decodeBody(t *testing.T, resp *http.Response) map[string]any {
	t.Helper()
	var payload map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return payload
}

func TestRootEndpoint(t *testing.T) {
	pool := &mockPool{}
	app := setupApp(pool)
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	payload := decodeBody(t, resp)
	if payload["name"] != "espg" {
		t.Fatalf("expected name espg")
	}
}

func TestClusterHealthEndpoint(t *testing.T) {
	pool := &mockPool{}
	app := setupApp(pool)
	req := httptest.NewRequest(http.MethodGet, "/_cluster/health", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	payload := decodeBody(t, resp)
	if payload["status"] != "green" {
		t.Fatalf("expected green status")
	}
}

func TestCreateDocument(t *testing.T) {
	pool := &mockPool{}
	pool.queryRowFn = func(ctx context.Context, sql string, args ...any) pgx.Row {
		switch {
		case strings.Contains(sql, "to_regclass"):
			return mockRow{scan: func(dest ...any) error {
				*dest[0].(*bool) = true
				return nil
			}}
		case strings.Contains(sql, "RETURNING (xmax = 0)"):
			return mockRow{scan: func(dest ...any) error {
				*dest[0].(*bool) = true
				return nil
			}}
		default:
			return mockRow{scan: func(dest ...any) error { return nil }}
		}
	}
	pool.execFn = func(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
		return pgconn.CommandTag{}, nil
	}
	pool.queryFn = func(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
		return &mockRows{}, nil
	}

	app := setupApp(pool)
	req := httptest.NewRequest(http.MethodPost, "/books/_doc", strings.NewReader(`{"title":"Hello"}`))
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201, got %d", resp.StatusCode)
	}
	payload := decodeBody(t, resp)
	if payload["result"] != "created" {
		t.Fatalf("expected created result")
	}
}

func TestGetDocumentNotFound(t *testing.T) {
	pool := &mockPool{}
	pool.queryRowFn = func(ctx context.Context, sql string, args ...any) pgx.Row {
		switch {
		case strings.Contains(sql, "to_regclass"):
			return mockRow{scan: func(dest ...any) error {
				*dest[0].(*bool) = true
				return nil
			}}
		case strings.Contains(sql, "SELECT document"):
			return mockRow{scan: func(dest ...any) error { return pgx.ErrNoRows }}
		default:
			return mockRow{scan: func(dest ...any) error { return nil }}
		}
	}
	pool.execFn = func(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
		return pgconn.CommandTag{}, nil
	}
	pool.queryFn = func(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
		return &mockRows{}, nil
	}

	app := setupApp(pool)
	req := httptest.NewRequest(http.MethodGet, "/books/_doc/abc", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", resp.StatusCode)
	}
	payload := decodeBody(t, resp)
	if found, ok := payload["found"].(bool); !ok || found {
		t.Fatalf("expected found false")
	}
}

func TestHeadIndexNotFound(t *testing.T) {
	pool := &mockPool{}
	pool.queryRowFn = func(ctx context.Context, sql string, args ...any) pgx.Row {
		return mockRow{scan: func(dest ...any) error {
			*dest[0].(*bool) = false
			return nil
		}}
	}
	pool.execFn = func(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
		return pgconn.CommandTag{}, nil
	}
	pool.queryFn = func(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
		return &mockRows{}, nil
	}

	app := setupApp(pool)
	req := httptest.NewRequest(http.MethodHead, "/books", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", resp.StatusCode)
	}
}
