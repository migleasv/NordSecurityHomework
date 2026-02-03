**Overview**
This project scrapes https://books.toscrape.com and produces a `books.json` file with structured book data. The scraper fetches listing and product pages, while a separate gRPC parser service extracts structured fields from each product HTML.


**Structure**
Component | Type | Responsibility
---|---|---
Parser | Service | Long-running gRPC service. Parses a single book HTML page into structured data
Scraper | Batch job (CronJob in Kubernetes) | Crawls pages, fetches HTML, sends each page to the parser


**Output Schema**
Each item in `books.json` follows:
```json
{
  "name": "",
  "availability": "",
  "upc": "",
  "price_excl_tax": 0.0,
  "tax": 0.0
}
```

**Responsibilities**
Scraper (application/main.py):
- Discover listing pages and product URLs
- Fetch HTML with concurrency control and timeouts
- Coordinate batching, deduplication, and persistence
- Handle network failures and keep the pipeline moving

Parser (parser_service.py):
- Parse a single product page HTML
- Extract and validate required fields
- Normalize values (e.g., prices)
- Return a typed `Book` via gRPC or a clear parse error

**Requirements**
- Python 3.10+
- `pip install -r requirements.txt` (or install packages listed below)

Packages:
- `aiohttp`
- `beautifulsoup4`
- `grpcio`
- `grpcio-tools`

**Generate gRPC Stubs**
```powershell
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. book_parser.proto
```

**Run the Parser Service**
```powershell
python parser_service.py
```

**Run the Scraper**
```powershell
python application/main.py
```

**Quick gRPC Smoke Test**
```powershell
python test_client.py
```

**Docker**
Build images:
```powershell
docker build -t book-parser:1.0 -f Dockerfile.parser .
docker build -t book-scraper:1.0 -f Dockerfile.scraper .
```
Compose:
```powershell
docker compose up --build
or
./scripts/run.ps1
```

```bash
./scripts/run.sh
```

**Kubernetes (Minikube)**
Build images directly in Minikube:
```powershell
minikube image build -t book-parser:1.0 -f Dockerfile.parser .
minikube image build -t book-scraper:1.0 -f Dockerfile.scraper .
```

Apply manifests/ deploy:
```powershell
kubectl apply -f k8s/all.yaml
```

Run the scraper manually (one-off job):
```powershell
kubectl -n book-app create job --from=cronjob/scraper scraper-manual
```

Suspend/resume the CronJob:
```powershell
kubectl -n book-app patch cronjob scraper -p "{\"spec\":{\"suspend\":true}}"
kubectl -n book-app patch cronjob scraper -p "{\"spec\":{\"suspend\":false}}"
```

**Notes**
- Start `parser_service.py` before running the scraper.
- The parser writes parsed items to `parsed_books.json` in the repo root.
- The parser rejects duplicate UPCs with `ALREADY_EXISTS`. If `test_client.py` is run multiple times, duplicate message is printed.
- The scraper skips a book if the parser returns a gRPC error.
- The scraper retries HTTP fetches and gRPC parse calls a few times with exponential backoff to improve resilience.

**Possible Issues And Improvements**
- GRPC calls don't set explicit per-request deadlines; adding deadlines would improve failure recovery.
- Errors from `fetch_text` and batch parsing are mostly skipped without logging, makig it difficult to debug.
- Duplicate tracking is split between `books.json` and `parsed_books.json`, and the parser’s in-memory `seen_upcs` is lost on restart.
- The parser writes to disk on every request, which can be slow under load; batching or buffering could improve throughput.
- The scraper uses an insecure gRPC channel with no authorization.
