#!/usr/bin/env bash
set -euo pipefail

compose_command=(docker compose)

log() {
  printf '\n==> %s\n' "$1"
}

cleanup() {
  log "Stopping containers"
  "${compose_command[@]}" down -v
}
trap cleanup EXIT

log "Starting Postgres container"
"${compose_command[@]}" up -d db

log "Waiting for Postgres to be ready"
for _ in {1..20}; do
  if "${compose_command[@]}" exec -T db pg_isready -U postgres >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

log "Running integration tests (go test -v -tags=integration ./...)"
DATABASE_URL="postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable" \
  go test -v -tags=integration ./...
