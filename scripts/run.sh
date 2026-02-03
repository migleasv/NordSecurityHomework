#!/usr/bin/env bash
set -euo pipefail

docker compose up -d parser
docker compose run --rm scraper
docker compose down
