#!/usr/bin/env bash
set -euo pipefail

compose_command=(docker compose)

cleanup() {
  "${compose_command[@]}" down -v
}
trap cleanup EXIT

"${compose_command[@]}" up -d db

for _ in {1..20}; do
  if "${compose_command[@]}" exec -T db pg_isready -U postgres >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

DATABASE_URL="postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable" \
  go test -tags=integration ./...
