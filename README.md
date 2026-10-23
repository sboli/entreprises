# Entreprises

A Go HTTP service with embedded DuckDB, a persistent registry snapshot, and automatic refresh every seven days. Runs as one container without a separate database or scheduler.

## Data

The supplied `StockEtablissement_utf8.csv` header contains SIREN/SIRET, trade names, addresses, and activity codes. **It does not contain company legal names.** The importer therefore downloads both official ZIPs and joins them by SIREN:

- Establishments: <https://www.data.gouv.fr/api/1/datasets/r/0651fb76-bcf3-4f6a-a38d-bc04fa708576>
- Legal units: <https://www.data.gouv.fr/api/1/datasets/r/825f4199-cadd-486c-ac46-a65a8ea1a047>

These are stable resource redirect URLs from the [official Sirene dataset](https://www.data.gouv.fr/datasets/base-sirene-des-entreprises-et-de-leurs-etablissements-siren-siret). The upstream stocks are monthly snapshots; downloading weekly does not make the source update weekly. The publisher announces that CSV stocks will be retired during the second half of 2027; that will require changing the importer to Parquet.

Only fully public records (`statutDiffusion… = O` in **both** datasets) are published. Partially/non-public records are excluded entirely. `[ND]` and empty CSV values become NULL. Identifiers remain strings, preserving leading zeros. All public establishments are stored, including closed ones; search defaults to active establishments in active legal units.

ZIP responses stream to disk, follow redirects, and have size and time limits. Only the expected CSV member is extracted to a fixed path. DuckDB reads the CSVs directly with all fields as strings and can spill temporary work to the data volume. Schema validation and a unique SIRET index must succeed, and the resulting registry must contain at least one record before publication.

## Run

```sh
docker compose up --build -d
curl 'http://localhost:8080/api/companies?q=carrefour'
curl 'http://localhost:8080/api/companies/12345678900012'
curl 'http://localhost:8080/api/status'
```

The initial startup downloads and imports both stocks automatically. `/readyz` and search return 503 until that succeeds; `/healthz` remains 200 during the import. Subsequent refreshes build a separate snapshot while the last successful snapshot continues serving requests. Publication uses an atomically replaced `CURRENT` manifest. Failures preserve the old snapshot and retry after an hour. The successful import timestamp is stored in DuckDB, so restarts preserve the seven-day deadline. Temporary files are removed after success/failure; interrupted import artifacts are cleaned on startup. Old snapshots are deleted after in-flight queries finish.

Set `API_KEY` to require `Authorization: Bearer <key>` on all `/api/*` requests. Health/readiness probes remain unauthenticated. With no key, the API is accessible to anyone who can reach it. Compose binds only to loopback.

For a local CSV import without downloading anything (both files are required):

```sh
go build -o entreprises .
DATA_DIR=./data ./entreprises import \
  --establishments ./StockEtablissement_utf8.csv \
  --legal-units /path/to/StockUniteLegale_utf8.csv
DATA_DIR=./data ./entreprises
```

The import command takes the same volume lock as the server: stop the server before an offline import. A local import also starts a fresh seven-day interval. Data files are excluded from Git and from the Docker build context.

## HTTP API

| Route | Response |
| --- | --- |
| `GET /api/companies?q=…` | `{ "companies": [...], "next_cursor": "…" or null }` |
| `GET /api/companies/{siret}` | `{ "company": {...} }`; 404 if absent or excluded |
| `GET /api/pappers-search?q=…` | Compatibility alias for company search |
| `GET /api/pappers-company?siret=…` | Compatibility alias for company details |
| `GET /api/status` | Snapshot, row count, refresh state, last success/error |
| `GET /healthz` | Process liveness |
| `GET /readyz` | Whether a published snapshot is available |

Search accepts a name, legal name, trade name, legal-unit alias, acronym, SIREN (9 digits), or SIRET (14 digits). Whitespace inside numeric identifiers is removed. Name matching is case- and accent-insensitive literal substring matching; punctuation and SQL wildcard characters are literal. Minimum name length: three characters. Maximum query size: 256 bytes.

- `limit`: 1–100, default 50.
- `cursor`: omit (or `*`) for the first page; pass `next_cursor` for later pages. Preserve the query, limit, and active flag. If the registry changes, a stale cursor returns 409; restart from page one.
- `active=false`: include closed establishments and inactive legal units. Default: active only.
- Name results contain one entry per establishment, sorted by SIRET. Branches can therefore appear separately. A SIREN lookup returns one establishment, preferring its headquarters. A SIRET lookup returns exactly that establishment.
- Detail lookup includes closed establishments. Identifiers are checked for digit count; no checksum validation is imposed on historical registry data.

Example company:

```json
{
  "name": "Étoile Boutique",
  "legal_name": "SOCIETE ETOILE",
  "trade_name": "Étoile Boutique",
  "siret": "01234567800001",
  "siren": "012345678",
  "tva_id": "",
  "address": "7 RUE DU PORT",
  "postal_code": "97110",
  "city": "Pointe-à-Pitre",
  "legal_form": "5710",
  "activity": "47.11A",
  "headquarters": true,
  "active": true,
  "details_loaded": true
}
```

`legal_form` and `activity` are registry codes. The source does not provide a verified VAT identifier, activity labels, directors, revenue, contacts, or websites; those values are not invented. `tva_id` stays empty and `pappers_raw` is omitted.

## GitHub Actions / GHCR

[`.github/workflows/image.yml`](.github/workflows/image.yml) runs Go race tests and vet, builds the container, and runs the fixture smoke tests. Pull requests validate without publishing. Pushes to `main`, version tags (`v*`), and manual workflow runs publish AMD64 and ARM64 images to `ghcr.io/sboli/entreprises` (the workflow derives the name from the repository).

Tags include `latest` on the default branch, `main` for branch builds, `sha-<full commit SHA>`, and the version number for semantic version tags such as `v1.2.3` → `1.2.3`. Authentication uses the built-in `GITHUB_TOKEN` with `packages: write`; no registry password secret is needed. Actions are pinned to commit SHAs. Publication only occurs after the checks and smoke test succeed. Each architecture also runs the Go tests inside the Docker build.

Use `ghcr.io/sboli/entreprises:latest` to run the published image, or pin a commit tag/digest. Private images require registry authentication.

## Configuration

| Environment variable | Default |
| --- | --- |
| `DATA_DIR` | `/data` |
| `LISTEN_ADDR` | `:8080` |
| `API_KEY` | empty (authentication disabled) |
| `ESTABLISHMENTS_URL` | supplied stable resource URL |
| `LEGAL_UNITS_URL` | companion stable resource URL |
| `REFRESH_INTERVAL` | `168h` (seven days after successful completion) |
| `RETRY_INTERVAL` | `1h` after a failed refresh |
| `REFRESH_TIMEOUT` | `12h` for the entire refresh |
| `QUERY_TIMEOUT` | `15s` including database connection wait |
| `DUCKDB_MEMORY_LIMIT` | `2GB` (Compose sets `4GB`) |
| `DUCKDB_THREADS` | `2` |
| `QUERY_CONCURRENCY` | `4` (excess requests return 429) |
| `MAX_DOWNLOAD_BYTES` | `10737418240` (10 GiB per ZIP) |
| `MAX_EXTRACT_BYTES` | `42949672960` (40 GiB per CSV) |

Each HTTP download also has a six-hour timeout. HTTP query values are bound as SQL parameters. The API opens the published database read-only with external access disabled; import has a separate connection with local file access. Resource limits bound concurrency rather than request rate; use your ingress for rate limiting if needed.

## Verification

```sh
go test -race ./...
go vet ./...
go build ./...
```

Tests use tiny synthetic CSVs and ZIPs, a real embedded DuckDB, and a local HTTP server. They exercise redirect/download/extraction/import/publication, legal/trade-name search, leading-zero identifiers, address fields, privacy and active-state filtering, cursor pagination and invalidation, bearer authentication, persistent restart loading, and preserving the current snapshot after bad imports/download failures. They do not download, inspect, or import the supplied large archive or full CSV.

For a real container smoke test (Docker must be running):

```sh
docker build -t entreprises:local-test .
python3 scripts/smoke-test.py
```

This creates only small synthetic stocks, serves them from a temporary local HTTP server, and starts a non-root container with a read-only root filesystem. It checks automatic ZIP download/import, pagination, authentication, a scheduled refresh, preserving the old snapshot on failure, retry recovery, restart persistence, the volume lock, and graceful shutdown. Its container and volume are removed afterward. It never downloads or reads the full registry files.
