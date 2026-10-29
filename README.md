# Banking Transactions ETL Pipeline

An incremental, idempotent ETL pipeline that turns messy retail-banking source files into a clean, analytics-ready star schema — with data-quality checks, quarantine of bad records, FX conversion from a live API, and a full audit trail of every run.

Built with **Python, pandas and SQLite**, tested with **pytest**, and checked on every push with **GitHub Actions**.

---

## Why this project

Real upstream feeds are never clean. Core banking systems, card processors and CRMs send files with mixed date formats, currency symbols in numeric fields, duplicates, late corrections and records that reference customers who don't exist. This project shows how a data engineer handles that reliably:

- **Nothing is silently dropped** – every rejected row goes to a quarantine file with the exact rule(s) it failed.
- **Re-running is safe** – files are fingerprinted (SHA-256), so the same file is never loaded twice, and loads are upserts.
- **Runs are all-or-nothing** – each run is one database transaction; a failure rolls everything back and the files are retried next time.
- **Everything is observable** – run log, per-rule data-quality metrics and a markdown report for every run.

## Architecture

```
 data/raw/                          etl_pipeline.py                               data/warehouse.db (SQLite)
 ─────────                          ───────────────                               ──────────────────────────
 YYYY-MM-DD_transactions.csv ─┐
 YYYY-MM-DD_customers.json  ──┼─► EXTRACT ─► TRANSFORM ─► VALIDATE ─┬─► LOAD ──►  dim_customer      (SCD type 1)
 Frankfurter FX API (ECB)  ───┘   (new files  (clean,     (10+ rules)│  (upsert)   fact_transaction
                                   only)       dedupe,               │             dim_fx_rate
                                               FX → GBP)             │             etl_run / etl_file_manifest / dq_result
                                                                     │             v_daily_summary, v_spend_by_category,
                                                                     │             v_customer_activity  (views)
                                                                     └─► data/quarantine/<run_id>_<entity>.csv
                                                                         reports/run_<run_id>.md
```

## What the pipeline does

| Stage | Details |
|---|---|
| **Extract** | Discovers daily CSV/JSON files, skips any already processed (by file hash), re-processes a file if its content changed. Fetches daily GBP→USD/EUR rates from the free [Frankfurter](https://frankfurter.dev) ECB API, falling back to static rates if the API is unreachable (or with `--offline`). |
| **Transform** | Parses `£1,234.50`-style amounts, ISO and UK day-first timestamps, normalises case/whitespace (`" gbp"` → `GBP`, `DR` → `debit`), removes exact duplicates, keeps the latest version of re-sent transactions and customers, converts every amount to GBP using the latest rate on or before the transaction date, flags transactions ≥ £10,000. |
| **Validate** | Rule-based checks with severities: `error` rules quarantine the row, `warn` rules are only recorded. Pass rates are stored per run in `dq_result`. |
| **Load** | `INSERT … ON CONFLICT DO UPDATE` upserts into a star schema with foreign keys. Customer updates only overwrite if newer (`updated_at`), so out-of-order feeds can't regress data. |
| **Observe** | `etl_run` (status, row counts, FX source, error), `etl_file_manifest`, `dq_result`, a log file and a per-run markdown report. |

### Data-quality rules

| Entity | Rule | Severity |
|---|---|---|
| Transaction | `txn_id_present`, `customer_exists`, `timestamp_valid`, `timestamp_not_future`, `amount_present`, `amount_positive`, `currency_supported`, `txn_type_valid` | error → quarantine |
| Transaction | `channel_valid`, `status_valid` | warn |
| Customer | `customer_id_present`, `customer_id_format`, `updated_at_valid` | error → quarantine |
| Customer | `email_valid`, `dob_valid_adult`, `segment_known` | warn |

## Example results

From the run sequence above (14 days, then 7 more):

| Run | Files | Rows read | Rows loaded | Rows quarantined |
|---|---|---|---|---|
| 1 – initial load | 16 | 11,917 | 11,297 | 420 |
| 2 – re-run | 0 | 0 | 0 | 0 |
| 3 – incremental | 8 | 5,726 | 5,423 | 220 |

The remaining difference between rows read and loaded is duplicates and superseded versions (e.g. a transaction first sent as `pending` and re-sent as `completed`), which are resolved rather than rejected. A full per-rule breakdown for the first run is in [`docs/sample_run_report.md`](docs/sample_run_report.md).

Sample of the quarantine file – the raw record is kept exactly as received, plus the reason:

```
txn_id,customer_id,txn_timestamp,amount,currency,...,dq_failures
T20260901000003,C99999,2026-09-01T17:41:50,38.93,GBP,...,customer_exists
T20260901000004,C00432,2026-09-01T14:38:40,N/A,GBP,...,amount_present
T20260901000034,C00216,2026-09-01T08:44:58,,GBP,...,amount_present
```

## Querying the warehouse

```bash
sqlite3 data/warehouse.db
```

```sql
-- Daily debits vs credits in GBP
SELECT * FROM v_daily_summary;

-- Top customers by net outflow
SELECT * FROM v_customer_activity ORDER BY net_flow_gbp LIMIT 10;

-- Data-quality trend across runs
SELECT run_id, rule, failed_rows, pass_rate FROM dq_result WHERE entity = 'transaction';
```

## Tests

`pytest etl_pipeline.py -v` runs 7 tests (kept in the same file, at the bottom):

- amount parsing (currency symbols, thousands separators, nulls)
- mixed timestamp formats – including a regression test for pandas' `dayfirst=True` silently reading `2026-10-03` as 10 March
- validation quarantines the right rows for the right reasons
- FX conversion uses the latest rate on or before the transaction date
- end-to-end: idempotent re-runs, identical regenerated files skipped, only new files processed, no orphans or non-positive amounts in the fact table
- a late-arriving update overwrites a transaction's status instead of duplicating it
- a failure mid-run rolls back every change and records the run as `failed`

## Project structure

```
banking-etl-pipeline/
├── etl_pipeline.py            # the whole pipeline: generator, ETL, DQ rules, CLI, report, tests
├── docs/sample_run_report.md  # example output of one run
├── environment.yml            # conda environment
├── requirements.txt           # pip equivalent (used by CI)
├── .github/workflows/ci.yml   # runs tests + a smoke run on every push
└── .gitignore                 # data/, reports/, logs/ are generated, not committed
```

## Design decisions

- **SQLite** keeps the project zero-setup; the SQL (upserts, views, foreign keys) maps directly to PostgreSQL or Snowflake.
- **Quarantine instead of drop** – in a bank, a rejected transaction is something an operations team needs to investigate, so the original record and failure reason are preserved.
- **Synthetic data** – no real customer data is used. The generator is seeded, so results are reproducible, and it deliberately injects the problems seen in real feeds.
- **Single file** – the whole pipeline is readable top to bottom; in production each section (extract, transform, rules, load) would become its own module, and scheduling would move to Airflow or a similar orchestrator.

## Possible extensions

- Orchestrate with Airflow/Prefect and schedule daily runs
- Swap SQLite for PostgreSQL in Docker
- Use Great Expectations or pandera for the rule definitions
- Add SCD type 2 history to `dim_customer`
- Stream transactions in with Kafka instead of daily files
