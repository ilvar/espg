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
		t.Fatalf("create index (%s): %v", createReq.URL.Path, err)
	}
	if createResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 for %s, got %d", createReq.URL.Path, createResp.StatusCode)
	}

	defer func() {
		deleteReq := httptest.NewRequest(http.MethodDelete, "/"+index, nil)
		_, _ = app.Test(deleteReq)
	}()

	upsertBody := strings.NewReader(`{"title":"Hello","status":"published"}`)
	upsertReq := httptest.NewRequest(http.MethodPut, "/"+index+"/_doc/1", upsertBody)
	upsertResp, err := app.Test(upsertReq)
	if err != nil {
		t.Fatalf("upsert document (%s): %v", upsertReq.URL.Path, err)
	}
	if upsertResp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 for %s, got %d", upsertReq.URL.Path, upsertResp.StatusCode)
	}

	searchBody := strings.NewReader(`{"query":{"term":{"status":"published"}}}`)
	searchReq := httptest.NewRequest(http.MethodPost, "/"+index+"/_search", searchBody)
	searchResp, err := app.Test(searchReq)
	if err != nil {
		t.Fatalf("search request (%s): %v", searchReq.URL.Path, err)
	}
	if searchResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 for %s, got %d", searchReq.URL.Path, searchResp.StatusCode)
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

func TestIntegrationIndexOperations(t *testing.T) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, requireDatabaseURL(t))
	if err != nil {
		t.Fatalf("connect to database: %v", err)
	}
	defer pool.Close()

	app := setupApp(pool)
	index := fmt.Sprintf("orders_%d", time.Now().UnixNano())

	createBody := strings.NewReader(`{"mappings":{"properties":{"status":{"type":"keyword"}}}}`)
	createReq := httptest.NewRequest(http.MethodPut, "/"+index, createBody)
	createResp, err := app.Test(createReq)
	if err != nil {
		t.Fatalf("create index (%s): %v", createReq.URL.Path, err)
	}
	if createResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 for %s, got %d", createReq.URL.Path, createResp.StatusCode)
	}

	defer func() {
		deleteReq := httptest.NewRequest(http.MethodDelete, "/"+index, nil)
		_, _ = app.Test(deleteReq)
	}()

	bulkPayload := strings.NewReader(`{"index":{"_id":"1"}}` + "\n" +
		`{"status":"new","count":5,"category":"alpha","created_at":"2024-01-01T00:00:00Z"}` + "\n" +
		`{"index":{"_id":"2"}}` + "\n" +
		`{"status":"processing","count":15,"category":"beta","created_at":"2024-01-02T00:00:00Z"}` + "\n" +
		`{"index":{"_id":"3"}}` + "\n" +
		`{"status":"new","count":25,"category":"alpha","created_at":"2024-01-03T00:00:00Z"}` + "\n")
	bulkReq := httptest.NewRequest(http.MethodPost, "/"+index+"/_bulk", bulkPayload)
	bulkResp, err := app.Test(bulkReq)
	if err != nil {
		t.Fatalf("bulk request (%s): %v", bulkReq.URL.Path, err)
	}
	if bulkResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 for %s, got %d", bulkReq.URL.Path, bulkResp.StatusCode)
	}
	bulkBody := decodeBody(t, bulkResp)
	if errors, ok := bulkBody["errors"].(bool); !ok || errors {
		t.Fatalf("expected bulk errors false")
	}

	updateBody := strings.NewReader(`{"status":"updated","count":20,"category":"beta","created_at":"2024-01-02T12:00:00Z"}`)
	updateReq := httptest.NewRequest(http.MethodPut, "/"+index+"/_doc/2", updateBody)
	updateResp, err := app.Test(updateReq)
	if err != nil {
		t.Fatalf("update document (%s): %v", updateReq.URL.Path, err)
	}
	if updateResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 for %s, got %d", updateReq.URL.Path, updateResp.StatusCode)
	}

	deleteReq := httptest.NewRequest(http.MethodDelete, "/"+index+"/_doc/1", nil)
	deleteResp, err := app.Test(deleteReq)
	if err != nil {
		t.Fatalf("delete document (%s): %v", deleteReq.URL.Path, err)
	}
	if deleteResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 for %s, got %d", deleteReq.URL.Path, deleteResp.StatusCode)
	}

	deleteByQueryBody := strings.NewReader(`{"query":{"term":{"status":"new"}}}`)
	deleteByQueryReq := httptest.NewRequest(http.MethodPost, "/"+index+"/_delete_by_query", deleteByQueryBody)
	deleteByQueryResp, err := app.Test(deleteByQueryReq)
	if err != nil {
		t.Fatalf("delete by query (%s): %v", deleteByQueryReq.URL.Path, err)
	}
	if deleteByQueryResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 for %s, got %d", deleteByQueryReq.URL.Path, deleteByQueryResp.StatusCode)
	}
	deleteByQueryBodyPayload := decodeBody(t, deleteByQueryResp)
	if deleted, ok := deleteByQueryBodyPayload["deleted"].(float64); !ok || deleted != 1 {
		t.Fatalf("expected delete by query to delete 1 document, got %v", deleteByQueryBodyPayload["deleted"])
	}

	termsAggBody := strings.NewReader(`{"aggs":{"by_category":{"terms":{"field":"category","size":10}}}}`)
	termsAggReq := httptest.NewRequest(http.MethodPost, "/"+index+"/_search", termsAggBody)
	termsAggResp, err := app.Test(termsAggReq)
	if err != nil {
		t.Fatalf("terms agg (%s): %v", termsAggReq.URL.Path, err)
	}
	if termsAggResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 for %s, got %d", termsAggReq.URL.Path, termsAggResp.StatusCode)
	}
	termsAggPayload := decodeBody(t, termsAggResp)
	aggs := termsAggPayload["aggregations"].(map[string]any)
	termsBuckets := aggs["by_category"].(map[string]any)["buckets"].([]any)
	if len(termsBuckets) != 1 {
		t.Fatalf("expected 1 terms bucket, got %d", len(termsBuckets))
	}

	histogramAggBody := strings.NewReader(`{"aggs":{"by_count":{"histogram":{"field":"count","interval":10}}}}`)
	histogramAggReq := httptest.NewRequest(http.MethodPost, "/"+index+"/_search", histogramAggBody)
	histogramAggResp, err := app.Test(histogramAggReq)
	if err != nil {
		t.Fatalf("histogram agg (%s): %v", histogramAggReq.URL.Path, err)
	}
	if histogramAggResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 for %s, got %d", histogramAggReq.URL.Path, histogramAggResp.StatusCode)
	}
	histogramPayload := decodeBody(t, histogramAggResp)
	histogramBuckets := histogramPayload["aggregations"].(map[string]any)["by_count"].(map[string]any)["buckets"].([]any)
	if len(histogramBuckets) != 1 {
		t.Fatalf("expected 1 histogram bucket, got %d", len(histogramBuckets))
	}

	dateHistogramBody := strings.NewReader(`{"aggs":{"by_day":{"date_histogram":{"field":"created_at","calendar_interval":"day"}}}}`)
	dateHistogramReq := httptest.NewRequest(http.MethodPost, "/"+index+"/_search", dateHistogramBody)
	dateHistogramResp, err := app.Test(dateHistogramReq)
	if err != nil {
		t.Fatalf("date histogram agg (%s): %v", dateHistogramReq.URL.Path, err)
	}
	if dateHistogramResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 for %s, got %d", dateHistogramReq.URL.Path, dateHistogramResp.StatusCode)
	}
	dateHistogramPayload := decodeBody(t, dateHistogramResp)
	dateBuckets := dateHistogramPayload["aggregations"].(map[string]any)["by_day"].(map[string]any)["buckets"].([]any)
	if len(dateBuckets) != 1 {
		t.Fatalf("expected 1 date histogram bucket, got %d", len(dateBuckets))
	}

	deleteIndexReq := httptest.NewRequest(http.MethodDelete, "/"+index, nil)
	deleteIndexResp, err := app.Test(deleteIndexReq)
	if err != nil {
		t.Fatalf("delete index (%s): %v", deleteIndexReq.URL.Path, err)
	}
	if deleteIndexResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 for %s, got %d", deleteIndexReq.URL.Path, deleteIndexResp.StatusCode)
	}
}
