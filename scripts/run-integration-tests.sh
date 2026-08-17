#!/usr/bin/env bash
set -euo pipefail

compose=(docker compose)
cleanup() {
  "${compose[@]}" down -v >/dev/null 2>&1 || true
}
trap cleanup EXIT

"${compose[@]}" up -d --build

for _ in {1..60}; do
  if curl --fail --silent http://localhost:3000/ >/dev/null; then
    break
  fi
  sleep 2
done

curl --fail --silent http://localhost:3000/ | grep -q '"name":"espg"'
curl --fail --silent -X PUT http://localhost:3000/ci_books | grep -q '"acknowledged":true'
curl --fail --silent -X PUT \
  -H 'content-type: application/json' \
  --data '{"title":"Hello","status":"published"}' \
  http://localhost:3000/ci_books/_doc/1 | grep -Eq '"result":"created"|"result":"updated"'
curl --fail --silent -X POST \
  -H 'content-type: application/json' \
  --data '{"query":{"term":{"status":"published"}}}' \
  http://localhost:3000/ci_books/_search | grep -q '"total":1'
