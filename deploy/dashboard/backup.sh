#!/bin/sh
set -eu
cd "$(dirname "$0")"
destination="/backups/dashboard-$(date -u +%Y%m%dT%H%M%SZ).sqlite"
docker compose exec -T api python -m chatlog_keeper.dashboard.cli backup "$destination"
