$ErrorActionPreference = "Stop"

docker compose up -d parser
docker compose run --rm scraper
docker compose down
