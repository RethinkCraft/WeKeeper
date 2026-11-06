#!/bin/sh
# Run by the installed system timer. Existing certificates stay active on failure.
set -eu
cd "$(dirname "$0")"
docker compose --profile certificate run --rm certbot renew --quiet
docker compose exec -T nginx nginx -t
docker compose exec -T nginx nginx -s reload
