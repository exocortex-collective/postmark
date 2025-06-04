Below is a **three‑pass planning sequence** that begins with a full‑scale blueprint, then refines it twice until every unit of work is “just right” for safe, test‑driven development.

---

## 1 ⟡ Blueprint (First‑pass, high‑level)

| Phase                     | Outcome                                         | Major Work‑streams                                                                                                                                                 |
| ------------------------- | ----------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **0 — Project Bootstrap** | Reproducible dev env & repo skeleton            | • `uv` virtual‑env & dependency pinning<br>• `pre‑commit`, `black`, `isort`, `mypy`, `pytest` scaffolding<br>• Architecture Decision Records (ADR) template & docs |
| **1 — Storage Layer**     | Persist, read, and query data                   | • **SQLite** schema + migrations for `emails`, `attachments`, etc.<br>• **Parquet** writer/reader with ZSTD & yyyy‑mm partitioning<br>• **S3** attachment helper   |
| **2 — Core Processing**   | Parse, dedup, thread, write                     | • RFC822 parser & normaliser<br>• Bloom‑filter & fuzzy‑match dedup<br>• Thread detection logic                                                                     |
| **3 — Bulk Backfill CLI** | `postmark‑backfill` streams .tgz → DB + Parquet | • Batch ingest (1 k/tx, checkpoints)<br>• Dead‑letter queue                                                                                                        |
| **4 — Sync Service**      | `postmark‑sync` keeps store ≤ 30 s behind Gmail | • Gmail API poll, IMAP fallback<br>• Overlap & retry logic                                                                                                         |
| **5 — MCP Service**       | `postmark‑mcp` exposes query API                | • Search / get / thread endpoints<br>• Pagination, body‑truncation                                                                                                 |
| **6 — Observability**     | Metrics, health, logs                           | • Prometheus counters/timers<br>• `/health`, `/ready`, structured JSON logs                                                                                        |
| **7 — Tooling & Reindex** | Extra CLIs for ops & tests                      | • `postmark‑reindex`, `postmark‑add‑test`                                                                                                                          |
| **8 — Delivery**          | Containerise, CI, docs                          | • Dockerfiles, `docker‑compose`, GitHub Actions, semantic releases                                                                                                 |

---

## 2 ⟡ Iterative Chunks (Second‑pass)

1. **Bootstrap**

   * 1.1 Repo initialiser script
   * 1.2 CI skeleton
2. **Storage Layer Foundation**

   * 2.1 `sqlite.py` connection + schema create
   * 2.2 Parquet helper (write one row‑group)
   * 2.3 S3 wrapper with local MinIO
3. **Email Model & Parser**

   * 3.1 RFC822 loader → dataclass
   * 3.2 Body/HTML normaliser
4. **Dedup & Threading**

   * 4.1 Bloom filter util
   * 4.2 Fuzzy match function
   * 4.3 Thread detector
5. **Backfill CLI – skeleton**

   * 5.1 Argparse + loop
   * 5.2 Read .tgz manifest
6. **Backfill CLI – writers**

   * 6.1 SQLite batch insert
   * 6.2 Parquet batch write
   * 6.3 Attachment upload stub
7. **Sync Service**

   * 7.1 Gmail delta poller
   * 7.2 IMAP fallback
8. **MCP Service**

   * 8.1 FastAPI server shell
   * 8.2 `/search` endpoint
   * 8.3 `/get_email`, `/get_thread`
9. **Observability**

   * 9.1 Prometheus middleware
   * 9.2 Health probes
10. **Tooling & Docs**

    * 10.1 `reindex` command
    * 10.2 `add‑test` anonymiser

---

## 3 ⟡ Right‑Sized Steps (Third‑pass, ready for code)

Each step is tuned for a single test‑driven development cycle (≈ 15–40 LOC plus tests).

| #      | Step                                                                                     | Key Tests                         |
| ------ | ---------------------------------------------------------------------------------------- | --------------------------------- |
| **1**  | Create `pyproject.toml`, install `uv`, run `uv pip install -e \".[dev]\"`                | `pytest -q` exits 0               |
| **2**  | Add `pre‑commit` with black/isort/mypy hooks                                             | Git hook formats a sample file    |
| **3**  | Implement `postmark.storage.sqlite.create_db()` that creates **only** the `emails` table | Assert table exists & schema hash |
| **4**  | Implement migration version table & idempotent `upgrade()`                               | Run twice without error           |
| **5**  | Write `columnar.write_parquet(df, path)` with unit test on 10‑row DataFrame              | File exists & row‑count == 10     |
| **6**  | Implement `s3.put_bytes(key, bytes_)` against local MinIO (`docker‑compose`)             | Object listed via boto3           |
| **7**  | Create `email.parser.parse_eml(path)` → Email dataclass                                  | Fixtures for plain & html mails   |
| **8**  | Implement `normalise_body(text)` stripping quotes, whitespace, encodings                 | Golden‑master strings             |
| **9**  | Add `dedup.BloomFilter(cap, fp_rate)` + `add`/`__contains__`                             | False‑negatives 0; fp ≈ spec      |
| **10** | Implement `dedup.fuzzy_equal(a, b, threshold)`                                           | 95 % vs 90 % cases                |
| **11** | Implement `threading.detect(email, prev_emails)` using headers only                      | Thread id matches fixture         |
| **12** | CLI scaffold `postmark‑backfill` with `click` and `--dry‑run` option                     | `--help` shows sub‑commands       |
| **13** | Add batch SQLite insert (1000 rows) with transaction & rollback test                     | Rows persisted or none            |
| **14** | Add Parquet write per 10 k emails (mock DF)                                              | Parquet file created              |
| **15** | Attachments: detect, hash, upload dummy                                                  | S3 key pattern matches spec       |
| **16** | Finish backfill happy‑path flow end‑to‑end (fixture .tgz)                                | Emails in DB & Parquet            |
| **17** | `sync.gmail.poll()` returns list of new message\_ids (mocked)                            | Handles empty diff                |
| **18** | IMAP fallback retrieves by UID                                                           | Integration test w/ green‑mail    |
| **19** | `postmark‑sync` service loop with overlap window                                         | Duplicate avoidance test          |
| **20** | FastAPI skeleton with `/health`                                                          | Returns 200                       |
| **21** | Implement `/search` returning stub result                                                | Response schema validated         |
| **22** | Wire SQLite search query with LIKE & date filter                                         | Result count correct              |
| **23** | Add body‑trunc & attachments flag                                                        | Truncation length assert          |
| **24** | Implement `/get_email` & `/get_thread`                                                   | Correct joins / ordering          |
| **25** | Prometheus counter middleware                                                            | Increments on each API hit        |
| **26** | Add `/metrics` & `/ready`                                                                | Text format parse                 |
| **27** | `postmark‑reindex` rebuilds normalised\_hash                                             | CLI runs & updates count          |
| **28** | `postmark‑add‑test` minimal anonymiser                                                   | Produces .eml with same error     |
| **29** | Dockerfile + docker‑compose for dev services                                             | `compose up` health‑checks pass   |
| **30** | GitHub Actions: lint, test, build image                                                  | CI green on main                  |

---

> **Next:** follow the code‑generation prompts (not included here) to implement each step in order, ensuring every deliverable is integrated and tested.
