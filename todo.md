# Postmark Project TODO Checklist

> Use this as a living checklist. Tick each item when you have implemented *code + tests* and merged to `main`.

---

## Phase 0 — Project Bootstrap

* [x] **0.1** Create `pyproject.toml` with installable `postmark` package structure.
* [x] **0.2** Add dev dependencies and lock file via `uv`.
* [x] **0.3** Configure `.pre‑commit‑config.yaml` (black, isort, mypy, pytest).
* [x] **0.4** Set up initial GitHub Actions CI skeleton.

## Phase 1 — Storage Layer Foundation

### SQLite

* [x] **1.1** Implement `postmark.storage.sqlite.create_db()` & `upgrade()`.
* [x] **1.2** Add migration system and `schema_version` table.

### Parquet

* [ ] **1.3** Create `postmark/storage/columnar.py` with `write_parquet` / `read_parquet`.
* [ ] **1.4** Partition files by `YYYY‑MM` folder, ZSTD compression.

### S3 Attachments

* [ ] **1.5** Build S3 wrapper (`S3Client`) targeting local MinIO.
* [ ] **1.6** Verify upload and retrieval in integration test.

## Phase 2 — Email Model & Parsing

* [ ] **2.1** Define `Email` dataclass.
* [ ] **2.2** Implement `parse_eml(path)` (headers, body, attachments).
* [ ] **2.3** Implement `normalise_body(text)` for whitespace & quote stripping.

## Phase 3 — Deduplication & Threading

* [ ] **3.1** Create `BloomFilter` utility.
* [ ] **3.2** Add `fuzzy_equal` function (Levenshtein ratio).
* [ ] **3.3** Combine into `should_store` helper.
* [ ] **3.4** Implement thread detection logic (`detect_thread`).

## Phase 4 — Backfill CLI

### Skeleton

* [ ] **4.1** Scaffold `postmark‑backfill` Click command with `--dry‑run`.
* [ ] **4.2** Parse `.tgz` archive and count `.eml` files.

### Write Path

* [ ] **4.3** Integrate parse → dedup → thread flow.
* [ ] **4.4** Batch insert emails into SQLite (1000/tx).
* [ ] **4.5** Append rows to Parquet in 10 k chunks.
* [ ] **4.6** Upload attachments to S3 under `{uuid}/{mime_path}/filename`.
* [ ] **4.7** Implement dead‑letter queue & `--fail‑fast`.

## Phase 5 — Sync Service

* [ ] **5.1** Implement Gmail poller stub (`poll(last_history_id)`).
* [ ] **5.2** Create `postmark‑sync` CLI loop (30 s interval, 1 h overlap).
* [ ] **5.3** Persist sync\_state in SQLite.
* [ ] **5.4** Add IMAP fallback retrieval.

## Phase 6 — MCP API Service

* [ ] **6.1** Create FastAPI app shell (`postmark/mcp/api.py`).
* [ ] **6.2** Implement `/health` endpoint.
* [ ] **6.3** Add `/search` with pagination (LIKE query).
* [ ] **6.4** Add `/get_email` & `/get_thread` endpoints.
* [ ] **6.5** Implement body truncation & attachments toggle.

## Phase 7 — Observability

* [ ] **7.1** Add Prometheus middleware (counter + histogram).
* [ ] **7.2** Expose `/metrics` endpoint.
* [ ] **7.3** Expose `/ready` probe (DB & S3 checks).
* [ ] **7.4** Enable structured JSON logging with correlation IDs.

## Phase 8 — Tooling & Utilities

* [ ] **8.1** Build `postmark‑reindex` CLI to rebuild `normalized_hash`.
* [ ] **8.2** Create `postmark‑add‑test` anonymiser utility.

## Phase 9 — Containerisation & CI/CD

* [ ] **9.1** Write production `Dockerfile` (slim Python base).
* [ ] **9.2** Draft `docker‑compose.yml` for dev (mcp, minio, prometheus).
* [ ] **9.3** Configure full GitHub Actions workflow (lint, test, build, push).
* [ ] **9.4** Update README & docs/badges.

## Validation & Documentation

* [ ] **10.1** Ensure all unit & integration tests pass (`pytest`).
* [ ] **10.2** Run end‑to‑end backfill + sync smoke test.
* [ ] **10.3** Document API endpoints & development workflow.
* [ ] **10.4** Publish first semantic release and version tag.

---

### Legend

* **Bold numbers** match steps from the refined plan.
* Mark tasks complete by changing `[ ]` to `[x]`.
* Keep this file in the repo root so progress is visible in PRs.
