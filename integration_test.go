//go:build integration

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func requireDatabaseURL(t *testing.T) string {
	t.Helper()
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		t.Skip("DATABASE_URL not set")
	}
	return url
}

func TestIntegrationIndexLifecycle(t *testing.T) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, requireDatabaseURL(t))
	if err != nil {
		t.Fatalf("connect to database: %v", err)
	}
	defer pool.Close()

	app := setupApp(pool)
	index := fmt.Sprintf("books_%d", time.Now().UnixNano())

	createReq := httptest.NewRequest(http.MethodPut, "/"+index, nil)
	createResp, err := app.Test(createReq)
	if err != nil {
		t.Fatalf("create index: %v", err)
	}
	if createResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", createResp.StatusCode)
	}

	defer func() {
		deleteReq := httptest.NewRequest(http.MethodDelete, "/"+index, nil)
		_, _ = app.Test(deleteReq)
	}()

	upsertBody := strings.NewReader(`{"title":"Hello","status":"published"}`)
	upsertReq := httptest.NewRequest(http.MethodPut, "/"+index+"/_doc/1", upsertBody)
	upsertResp, err := app.Test(upsertReq)
	if err != nil {
		t.Fatalf("upsert document: %v", err)
	}
	if upsertResp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201, got %d", upsertResp.StatusCode)
	}

	searchBody := strings.NewReader(`{"query":{"term":{"status":"published"}}}`)
	searchReq := httptest.NewRequest(http.MethodPost, "/"+index+"/_search", searchBody)
	searchResp, err := app.Test(searchReq)
	if err != nil {
		t.Fatalf("search request: %v", err)
	}
	if searchResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", searchResp.StatusCode)
	}

	var payload map[string]any
	if err := json.NewDecoder(searchResp.Body).Decode(&payload); err != nil {
		t.Fatalf("decode search response: %v", err)
	}

	hits := payload["hits"].(map[string]any)
	if hits["total"].(float64) != 1 {
		t.Fatalf("expected 1 hit, got %v", hits["total"])
	}
}
