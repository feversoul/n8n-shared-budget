#!/bin/sh
# test/stack.sh up|down|logs|kill|start [postgres]   the test stack by hand
cd "$(dirname "$0")/.."
action=$1; shift
files="-f docker-compose.yml -f test/docker-compose.yml"
[ "${1:-}" = postgres ] && files="$files -f test/docker-compose.postgres.yml"
compose="docker compose --env-file test/test.env $files -p budget-test"
case "$action" in
  up) $compose up -d ;;
  down) $compose down -v ;;
  logs) $compose logs --tail "${TAIL:-40}" n8n ;;
  kill) $compose kill n8n ;;
  start) $compose start n8n ;;
esac
