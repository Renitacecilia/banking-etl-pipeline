"""
Banking Transactions ETL Pipeline
=================================

An end-to-end, incremental ETL pipeline for retail-banking data:

    EXTRACT    daily transaction CSVs + customer JSON snapshots (+ live FX rates API)
    TRANSFORM  type coercion, standardisation, de-duplication, FX conversion to GBP
    VALIDATE   rule-based data-quality checks; failing rows are quarantined, not lost
    LOAD       idempotent upserts into a SQLite star schema (fact + dimensions)
    OBSERVE    run log, per-rule data-quality metrics, file manifest, markdown run report

Usage
-----
    python etl_pipeline.py generate --days 14        # create messy raw source files
    python etl_pipeline.py run                       # run the pipeline (incremental)
    python etl_pipeline.py run --offline             # skip the FX API, use fallback rates
    python etl_pipeline.py report                    # print warehouse + data-quality summary
    python etl_pipeline.py reset                     # delete the warehouse and outputs
    pytest etl_pipeline.py -v                        # run the test-suite (bottom of file)
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
import shutil
import sqlite3
import sys
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Callable

import numpy as np
import pandas as pd

try:  # requests is only needed for the live FX call; the pipeline degrades gracefully without it
    import requests
except ImportError:  # pragma: no cover
    requests = None

log = logging.getLogger("etl")

# --------------------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent

SUPPORTED_CURRENCIES = ("GBP", "USD", "EUR")
FALLBACK_FX = {"USD": 1.27, "EUR": 1.17}  # units of currency per 1 GBP, used if the API is unreachable
FX_API_URL = "https://api.frankfurter.dev/v1"  # free ECB reference rates, no API key needed
CHANNELS = ("card", "online", "branch", "atm", "transfer")
TXN_TYPES = ("debit", "credit")
STATUSES = ("completed", "pending", "reversed")
SEGMENTS = ("retail", "premier", "business")
LARGE_TXN_GBP = 10_000  # flag (not reject) unusually large transactions for review
EMAIL_RE = r"^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$"
CUSTOMER_ID_RE = r"^C\d{5}$"


@dataclass
class Config:
    base_dir: Path = BASE_DIR
    offline: bool = False

    @property
    def raw_dir(self) -> Path:
        return self.base_dir / "data" / "raw"

    @property
    def quarantine_dir(self) -> Path:
        return self.base_dir / "data" / "quarantine"

    @property
    def report_dir(self) -> Path:
        return self.base_dir / "reports"

    @property
    def db_path(self) -> Path:
        return self.base_dir / "data" / "warehouse.db"


def _utcnow() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


# --------------------------------------------------------------------------------------
# 1. Synthetic source data (deliberately messy, like real upstream feeds)
# --------------------------------------------------------------------------------------
FIRST_NAMES = ["Olivia", "Amelia", "Isla", "Ava", "Mia", "Noah", "Oliver", "George", "Arthur", "Leo",
               "Priya", "Mohammed", "Chen", "Fatima", "Lucas", "Sofia", "Jack", "Grace", "Aisha", "Daniel"]
LAST_NAMES = ["Smith", "Jones", "Taylor", "Brown", "Williams", "Wilson", "Johnson", "Davies", "Patel", "Khan",
              "Evans", "Thomas", "Roberts", "Walker", "Wright", "Thompson", "Okafor", "Nguyen", "Murphy", "Hughes"]
MERCHANTS = {
    "groceries": ["Tesco", "Sainsbury's", "Asda", "Aldi"],
    "transport": ["TfL", "Trainline", "Uber"],
    "dining": ["Pret A Manger", "Nando's", "Costa Coffee"],
    "retail": ["Amazon", "John Lewis", "Argos"],
    "utilities": ["British Gas", "Thames Water", "BT"],
    "travel": ["British Airways", "Booking.com", "easyJet"],
    "entertainment": ["Netflix", "Spotify", "Odeon"],
}


def _make_customers(rng: np.random.Generator, ids: range, as_of: date) -> pd.DataFrame:
    rows = []
    for i in ids:
        first, last = str(rng.choice(FIRST_NAMES)), str(rng.choice(LAST_NAMES))
        dob = date(1950, 1, 1) + timedelta(days=int(rng.integers(0, 365 * 55)))
        joined = as_of - timedelta(days=int(rng.integers(30, 3650)))
        rows.append({
            "customer_id": f"C{i:05d}",
            "full_name": f"{first} {last}",
            "email": f"{first}.{last}{i}@example.com".lower().replace("'", ""),
            "date_of_birth": dob.isoformat(),
            "country": "GB" if rng.random() < 0.85 else str(rng.choice(["IE", "FR", "US", "DE"])),
            "segment": str(rng.choice(SEGMENTS, p=[0.75, 0.15, 0.10])),
            "joined_on": joined.isoformat(),
            "updated_at": f"{as_of.isoformat()}T00:00:00",
        })
    return pd.DataFrame(rows)


def _dirty_customers(rng: np.random.Generator, df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    n = len(df)

    def pick(frac: float) -> np.ndarray:
        return rng.random(n) < frac

    m = pick(0.06)
    df.loc[m, "full_name"] = "  " + df.loc[m, "full_name"].str.lower().str.replace(" ", "   ") + " "
    m = pick(0.03)
    df.loc[m, "email"] = df.loc[m, "email"].str.split("@").str[0] + "@"           # invalid email
    m = pick(0.06)
    df.loc[m, "date_of_birth"] = pd.to_datetime(df.loc[m, "date_of_birth"]).dt.strftime("%d/%m/%Y")
    m = pick(0.01)
    df.loc[m, "date_of_birth"] = "31/02/1990"                                        # impossible date
    m = pick(0.10)
    df.loc[m, "segment"] = " " + df.loc[m, "segment"].str.upper()
    m = pick(0.05)
    df.loc[m, "country"] = df.loc[m, "country"].str.lower()
    # a couple of records with no key at all
    df.loc[rng.choice(n, size=min(2, n), replace=False), "customer_id"] = ""
    # stale duplicates: older versions of some customers arrive in the same feed
    stale = df.sample(frac=0.01, random_state=int(rng.integers(1e9))).copy()
    stale["updated_at"] = "2020-01-01T00:00:00"
    stale["email"] = "old.address@example.com"
    return pd.concat([df, stale], ignore_index=True).sample(frac=1, random_state=1)


def _make_transactions(rng: np.random.Generator, day: date, customer_ids: list[str], n: int) -> pd.DataFrame:
    cats = list(MERCHANTS)
    rows = []
    for i in range(1, n + 1):
        txn_type = "credit" if rng.random() < 0.12 else "debit"
        if txn_type == "credit":
            cat, merchant = "income", "Transfer In"
            amount = round(float(rng.lognormal(6.5, 0.8)), 2)
            channel = "transfer"
        else:
            cat = str(rng.choice(cats))
            merchant = str(rng.choice(MERCHANTS[cat]))
            amount = round(float(rng.lognormal(3.2, 1.0)), 2)
            channel = str(rng.choice(CHANNELS[:4], p=[0.6, 0.3, 0.04, 0.06]))
        ts = datetime.combine(day, datetime.min.time()) + timedelta(seconds=int(rng.integers(0, 86400)))
        rows.append({
            "txn_id": f"T{day:%Y%m%d}{i:06d}",
            "customer_id": str(rng.choice(customer_ids)),
            "txn_timestamp": ts.isoformat(timespec="seconds"),
            "amount": f"{amount:.2f}",
            "currency": str(rng.choice(SUPPORTED_CURRENCIES, p=[0.85, 0.08, 0.07])),
            "txn_type": txn_type,
            "channel": channel,
            "merchant": merchant,
            "merchant_category": cat,
            "status": "pending" if rng.random() < 0.05 else ("reversed" if rng.random() < 0.01 else "completed"),
        })
    return pd.DataFrame(rows)


def _dirty_transactions(rng: np.random.Generator, df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    n = len(df)

    def pick(frac: float) -> np.ndarray:
        return rng.random(n) < frac

    m = pick(0.30)  # second timestamp format from a legacy system (UK day-first)
    df.loc[m, "txn_timestamp"] = pd.to_datetime(df.loc[m, "txn_timestamp"]).dt.strftime("%d/%m/%Y %H:%M:%S")
    m = pick(0.10) & (df["currency"] == "GBP").to_numpy()
    df.loc[m, "amount"] = df.loc[m, "amount"].map(lambda a: f"£{float(a):,.2f}")
    df.loc[pick(0.01), "amount"] = ""
    df.loc[pick(0.005), "amount"] = "N/A"
    m = pick(0.005)
    df.loc[m, "amount"] = "-" + df.loc[m, "amount"].str.lstrip("£")
    m = pick(0.001)
    df.loc[m, "amount"] = [f"{x:.2f}" for x in rng.uniform(10_500, 25_000, int(m.sum()))]  # large txns
    m = pick(0.10)
    df.loc[m, "currency"] = " " + df.loc[m, "currency"].str.lower()
    df.loc[pick(0.003), "currency"] = "XXX"
    m = pick(0.10)
    df.loc[m, "channel"] = df.loc[m, "channel"].str.upper()
    m = pick(0.05)
    df.loc[m, "txn_type"] = df.loc[m, "txn_type"].map({"debit": "DR", "credit": "CR"})
    df.loc[pick(0.005), "customer_id"] = "C99999"                                    # orphan: unknown customer
    future = (pd.Timestamp(df["txn_timestamp"].iloc[0][:10]) + pd.Timedelta(days=400)).isoformat()
    df.loc[pick(0.003), "txn_timestamp"] = future                                    # timestamp in the future
    df.loc[pick(0.003), "txn_id"] = ""
    df.loc[pick(0.002), "txn_timestamp"] = "not a date"
    dupes = df.sample(frac=0.01, random_state=int(rng.integers(1e9)))                # exact duplicate rows
    return pd.concat([df, dupes], ignore_index=True)


def generate_raw_data(cfg: Config, start: date, days: int, n_customers: int = 500,
                      txns_per_day: int = 800, seed: int = 42) -> list[Path]:
    """Write messy daily source files into data/raw/. Deterministic for a given seed + date,
    so regenerating the same dates produces byte-identical files (useful for idempotency tests)."""
    if start + timedelta(days=days - 1) >= date.today():
        log.warning("Some generated days are today or later - their transactions will fail the "
                    "timestamp_not_future check. Use an earlier --start for a clean demo.")
    cfg.raw_dir.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []

    # Base customer snapshot (always regenerated identically from the seed)
    base_rng = np.random.default_rng(seed)
    customers = _make_customers(base_rng, range(1, n_customers + 1), start)
    snapshot_path = cfg.raw_dir / f"{start.isoformat()}_customers.json"
    snapshot_path.write_text(_dirty_customers(base_rng, customers).to_json(orient="records", indent=1))
    written.append(snapshot_path)
    customer_ids = customers["customer_id"].tolist()

    prev_pending: pd.DataFrame | None = None
    for d in range(days):
        day = start + timedelta(days=d)
        rng = np.random.default_rng(seed + day.toordinal())

        # Every 7th day the CRM sends a delta: some updated customers + some brand-new ones
        if d > 0 and d % 7 == 0:
            updated = customers.sample(n=min(25, len(customers)), random_state=day.toordinal()).copy()
            updated["segment"] = "premier"
            updated["email"] = updated["email"].str.replace("@example.com", "@newmail.co.uk", regex=False)
            updated["updated_at"] = f"{day.isoformat()}T06:00:00"
            first_new = len(customer_ids) + 1
            new = _make_customers(rng, range(first_new, first_new + 10), day)
            new["updated_at"] = f"{day.isoformat()}T06:00:00"
            customer_ids += new["customer_id"].tolist()
            customers = pd.concat([customers, new], ignore_index=True)
            delta_path = cfg.raw_dir / f"{day.isoformat()}_customers.json"
            delta_path.write_text(pd.concat([updated, new]).to_json(orient="records", indent=1))
            written.append(delta_path)

        txns = _make_transactions(rng, day, customer_ids, txns_per_day)
        # Late-arriving corrections: yesterday's pending transactions re-sent as completed
        if prev_pending is not None and len(prev_pending):
            resent = prev_pending.head(5).copy()
            resent["status"] = "completed"
            txns = pd.concat([txns, resent], ignore_index=True)
        prev_pending = txns[(txns["status"] == "pending") & txns["txn_id"].str.contains(f"{day:%Y%m%d}")]

        path = cfg.raw_dir / f"{day.isoformat()}_transactions.csv"
        _dirty_transactions(rng, txns).to_csv(path, index=False)
        written.append(path)

    log.info("Generated %d raw files in %s", len(written), cfg.raw_dir)
    return written


# --------------------------------------------------------------------------------------
# 2. Warehouse schema
# --------------------------------------------------------------------------------------
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id    TEXT PRIMARY KEY,
    full_name      TEXT,
    email          TEXT,
    email_valid    INTEGER,
    date_of_birth  TEXT,
    country        TEXT,
    segment        TEXT,
    joined_on      TEXT,
    updated_at     TEXT,
    source_file    TEXT,
    run_id         TEXT,
    loaded_at      TEXT
);

CREATE TABLE IF NOT EXISTS dim_fx_rate (
    rate_date      TEXT,
    currency       TEXT,
    rate_per_gbp   REAL,
    source         TEXT,
    PRIMARY KEY (rate_date, currency)
);

CREATE TABLE IF NOT EXISTS fact_transaction (
    txn_id             TEXT PRIMARY KEY,
    customer_id        TEXT NOT NULL REFERENCES dim_customer(customer_id),
    txn_ts             TEXT NOT NULL,
    txn_date           TEXT NOT NULL,
    amount             REAL NOT NULL,
    currency           TEXT NOT NULL,
    fx_rate_per_gbp    REAL NOT NULL,
    amount_gbp         REAL NOT NULL,
    signed_amount_gbp  REAL NOT NULL,
    txn_type           TEXT NOT NULL,
    channel            TEXT,
    merchant           TEXT,
    merchant_category  TEXT,
    status             TEXT,
    is_large_txn       INTEGER,
    source_file        TEXT,
    run_id             TEXT,
    loaded_at          TEXT
);
CREATE INDEX IF NOT EXISTS ix_fact_customer ON fact_transaction(customer_id);
CREATE INDEX IF NOT EXISTS ix_fact_date ON fact_transaction(txn_date);

CREATE TABLE IF NOT EXISTS etl_file_manifest (
    file_name      TEXT PRIMARY KEY,
    file_hash      TEXT NOT NULL,
    rows_read      INTEGER,
    run_id         TEXT,
    processed_at   TEXT
);

CREATE TABLE IF NOT EXISTS etl_run (
    run_id            TEXT PRIMARY KEY,
    started_at        TEXT,
    finished_at       TEXT,
    status            TEXT,
    files_processed   INTEGER,
    rows_read         INTEGER,
    rows_loaded       INTEGER,
    rows_quarantined  INTEGER,
    fx_source         TEXT,
    error             TEXT
);

CREATE TABLE IF NOT EXISTS dq_result (
    run_id        TEXT,
    entity        TEXT,
    rule          TEXT,
    severity      TEXT,
    failed_rows   INTEGER,
    total_rows    INTEGER,
    pass_rate     REAL,
    PRIMARY KEY (run_id, entity, rule)
);

CREATE VIEW IF NOT EXISTS v_daily_summary AS
SELECT txn_date,
       COUNT(*)                                                             AS txn_count,
       ROUND(SUM(CASE WHEN txn_type = 'debit'  THEN amount_gbp ELSE 0 END), 2) AS debits_gbp,
       ROUND(SUM(CASE WHEN txn_type = 'credit' THEN amount_gbp ELSE 0 END), 2) AS credits_gbp,
       SUM(is_large_txn)                                                    AS large_txns
FROM fact_transaction
WHERE status = 'completed'
GROUP BY txn_date;

CREATE VIEW IF NOT EXISTS v_spend_by_category AS
SELECT merchant_category,
       COUNT(*)                 AS txn_count,
       ROUND(SUM(amount_gbp), 2) AS spend_gbp,
       ROUND(AVG(amount_gbp), 2) AS avg_txn_gbp
FROM fact_transaction
WHERE txn_type = 'debit' AND status = 'completed'
GROUP BY merchant_category;

CREATE VIEW IF NOT EXISTS v_customer_activity AS
SELECT c.customer_id, c.segment, c.country,
       COUNT(f.txn_id)                        AS txn_count,
       ROUND(SUM(f.signed_amount_gbp), 2)     AS net_flow_gbp,
       MAX(f.txn_date)                        AS last_txn_date
FROM dim_customer c
LEFT JOIN fact_transaction f
       ON f.customer_id = c.customer_id AND f.status = 'completed'
GROUP BY c.customer_id, c.segment, c.country;
"""


def connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(SCHEMA_SQL)
    return conn


def _records(df: pd.DataFrame) -> list[tuple]:
    """DataFrame -> list of tuples with NaN/NaT/NA converted to None for sqlite."""
    obj = df.astype(object)
    return list(obj.where(obj.notna(), None).itertuples(index=False, name=None))


def _upsert_sql(table: str, cols: list[str], keys: list[str], only_if_newer: str | None = None) -> str:
    placeholders = ", ".join("?" for _ in cols)
    updates = ", ".join(f"{c} = excluded.{c}" for c in cols if c not in keys)
    sql = (f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({placeholders}) "
           f"ON CONFLICT ({', '.join(keys)}) DO UPDATE SET {updates}")
    if only_if_newer:  # protects against older records overwriting newer ones (out-of-order feeds)
        sql += f" WHERE excluded.{only_if_newer} >= {table}.{only_if_newer}"
    return sql


# --------------------------------------------------------------------------------------
# 3. Extract
# --------------------------------------------------------------------------------------
def _file_hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def discover_new_files(conn: sqlite3.Connection, raw_dir: Path) -> list[tuple[Path, str]]:
    """Return files that are new or whose content changed since they were last processed."""
    seen = dict(conn.execute("SELECT file_name, file_hash FROM etl_file_manifest").fetchall())
    candidates = sorted(list(raw_dir.glob("*_customers.json")) + list(raw_dir.glob("*_transactions.csv")))
    new: list[tuple[Path, str]] = []
    for path in candidates:
        digest = _file_hash(path)
        if seen.get(path.name) == digest:
            continue
        if path.name in seen:
            log.warning("%s has changed since it was last loaded - reprocessing", path.name)
        new.append((path, digest))
    return new


def extract_customers(path: Path) -> pd.DataFrame:
    df = pd.DataFrame(json.loads(path.read_text()))
    return df.astype(str).assign(source_file=path.name)


def extract_transactions(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    return df.assign(source_file=path.name)


def fetch_fx_rates(start: date, end: date, offline: bool = False) -> tuple[pd.DataFrame, str]:
    """Daily GBP -> USD/EUR rates. Tries the Frankfurter (ECB) API, falls back to static rates.
    Returns a long DataFrame [rate_date, currency, rate_per_gbp, source] and the source used."""
    others = [c for c in SUPPORTED_CURRENCIES if c != "GBP"]
    start = start - timedelta(days=7)  # look back so weekends/holidays at the start still get a rate
    frame: pd.DataFrame | None = None
    source = "fallback"

    if not offline and requests is not None:
        try:
            resp = requests.get(f"{FX_API_URL}/{start}..{end}",
                                params={"base": "GBP", "symbols": ",".join(others)}, timeout=10)
            resp.raise_for_status()
            rates = resp.json()["rates"]
            frame = pd.DataFrame([(d, ccy, float(r)) for d, m in rates.items() for ccy, r in m.items()],
                                 columns=["rate_date", "currency", "rate_per_gbp"])
            source = "frankfurter_api"
            log.info("Fetched %d FX rates from the API", len(frame))
        except Exception as exc:  # network, HTTP or payload errors all fall back
            log.warning("FX API unavailable (%s) - using fallback rates", type(exc).__name__)

    if frame is None or frame.empty:
        days = pd.date_range(start, end, freq="D").strftime("%Y-%m-%d")
        frame = pd.DataFrame([(d, c, FALLBACK_FX[c]) for d in days for c in others],
                             columns=["rate_date", "currency", "rate_per_gbp"])
        source = "fallback"

    gbp = pd.DataFrame({"rate_date": frame["rate_date"].unique(), "currency": "GBP", "rate_per_gbp": 1.0})
    frame = pd.concat([frame, gbp], ignore_index=True).assign(source=source)
    return frame, source


# --------------------------------------------------------------------------------------
# 4. Transform
# --------------------------------------------------------------------------------------
NULL_TOKENS = {"", "nan", "none", "null", "n/a", "na"}


def _clean_str(s: pd.Series) -> pd.Series:
    s = s.astype(str).str.strip().str.replace(r"\s+", " ", regex=True)
    return s.where(~s.str.lower().isin(NULL_TOKENS))


def parse_amount(s: pd.Series) -> pd.Series:
    """'£1,234.50' -> 1234.5 ; '' / 'N/A' -> NaN ; keeps the sign so negatives can be rejected."""
    txt = s.astype(str).str.strip().str.replace(r"[£$€,\s]", "", regex=True)
    return pd.to_numeric(txt.where(~txt.str.lower().isin(NULL_TOKENS)), errors="coerce")


def parse_datetime(s: pd.Series) -> pd.Series:
    """Handles ISO-8601 and UK day-first formats in the same column; unparseable -> NaT.
    Formats are tried explicitly: pandas' format='mixed' + dayfirst=True would silently
    misread ISO dates such as 2026-10-03 as 10 March."""
    s = _clean_str(s)
    out = pd.to_datetime(s, format="ISO8601", errors="coerce")
    for fmt in ("%d/%m/%Y %H:%M:%S", "%d/%m/%Y %H:%M", "%d/%m/%Y"):
        todo = out.isna() & s.notna()
        if not todo.any():
            break
        out.loc[todo] = pd.to_datetime(s[todo], format=fmt, errors="coerce")
    return out


def clean_customers(raw: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame(index=raw.index)
    df["customer_id"] = _clean_str(raw["customer_id"]).str.upper()
    df["full_name"] = _clean_str(raw["full_name"]).str.title()
    df["email"] = _clean_str(raw["email"]).str.lower()
    df["email_valid"] = df["email"].fillna("").str.match(EMAIL_RE).astype(int)
    dob = parse_datetime(raw["date_of_birth"])
    df["date_of_birth"] = dob.dt.strftime("%Y-%m-%d")
    df["country"] = _clean_str(raw["country"]).str.upper()
    df["segment"] = _clean_str(raw["segment"]).str.lower()
    df["joined_on"] = parse_datetime(raw["joined_on"]).dt.strftime("%Y-%m-%d")
    df["updated_at"] = parse_datetime(raw["updated_at"]).dt.strftime("%Y-%m-%dT%H:%M:%S")
    df["source_file"] = raw["source_file"]
    df["_dob"] = dob
    return df


def clean_transactions(raw: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame(index=raw.index)
    df["txn_id"] = _clean_str(raw["txn_id"]).str.upper()
    df["customer_id"] = _clean_str(raw["customer_id"]).str.upper()
    df["txn_ts"] = parse_datetime(raw["txn_timestamp"])
    df["amount"] = parse_amount(raw["amount"])
    df["currency"] = _clean_str(raw["currency"]).str.upper()
    df["txn_type"] = _clean_str(raw["txn_type"]).str.lower().replace({"dr": "debit", "cr": "credit"})
    df["channel"] = _clean_str(raw["channel"]).str.lower()
    df["merchant"] = _clean_str(raw["merchant"])
    df["merchant_category"] = _clean_str(raw["merchant_category"]).str.lower()
    df["status"] = _clean_str(raw["status"]).str.lower()
    df["source_file"] = raw["source_file"]
    return df


def apply_fx(df: pd.DataFrame, fx: pd.DataFrame) -> pd.DataFrame:
    """Convert every amount to GBP using the latest rate on or before the transaction date."""
    df = df.copy()
    df["txn_date"] = df["txn_ts"].dt.normalize().astype("datetime64[ns]")
    rates = fx[["rate_date", "currency", "rate_per_gbp"]].copy()
    rates["rate_date"] = pd.to_datetime(rates["rate_date"]).astype("datetime64[ns]")
    merged = pd.merge_asof(
        df.reset_index().sort_values("txn_date"),
        rates.sort_values("rate_date"),
        left_on="txn_date", right_on="rate_date", by="currency", direction="backward",
    ).set_index("index").loc[df.index]
    missing = merged["rate_per_gbp"].isna()
    if missing.any():  # no earlier rate known: use the static fallback rather than drop the row
        merged.loc[missing, "rate_per_gbp"] = merged.loc[missing, "currency"].map({**FALLBACK_FX, "GBP": 1.0})
    df["fx_rate_per_gbp"] = merged["rate_per_gbp"].astype(float)
    df["amount_gbp"] = (df["amount"] / df["fx_rate_per_gbp"]).round(2)
    df["signed_amount_gbp"] = np.where(df["txn_type"] == "debit", -df["amount_gbp"], df["amount_gbp"])
    df["is_large_txn"] = (df["amount_gbp"] >= LARGE_TXN_GBP).astype(int)
    df["txn_date"] = df["txn_date"].dt.strftime("%Y-%m-%d")
    df["txn_ts"] = df["txn_ts"].dt.strftime("%Y-%m-%dT%H:%M:%S")
    return df


# --------------------------------------------------------------------------------------
# 5. Data-quality rules
# --------------------------------------------------------------------------------------
@dataclass(frozen=True)
class Rule:
    name: str
    passes: Callable[[pd.DataFrame], pd.Series]  # True where the row passes
    severity: str = "error"                       # "error" -> quarantine, "warn" -> record only


def customer_rules() -> list[Rule]:
    today = pd.Timestamp.today().normalize()
    return [
        Rule("customer_id_present", lambda d: d["customer_id"].notna()),
        Rule("customer_id_format", lambda d: d["customer_id"].fillna("").str.match(CUSTOMER_ID_RE)),
        Rule("updated_at_valid", lambda d: d["updated_at"].notna()),
        Rule("email_valid", lambda d: d["email_valid"] == 1, "warn"),
        Rule("dob_valid_adult", lambda d: d["_dob"].notna() & (d["_dob"] <= today - pd.DateOffset(years=18)), "warn"),
        Rule("segment_known", lambda d: d["segment"].isin(SEGMENTS), "warn"),
    ]


def transaction_rules(known_customers: set[str]) -> list[Rule]:
    now = pd.Timestamp.now()
    return [
        Rule("txn_id_present", lambda d: d["txn_id"].notna()),
        Rule("customer_exists", lambda d: d["customer_id"].isin(known_customers)),
        Rule("timestamp_valid", lambda d: d["txn_ts"].notna()),
        Rule("timestamp_not_future", lambda d: d["txn_ts"].isna() | (d["txn_ts"] <= now)),
        Rule("amount_present", lambda d: d["amount"].notna()),
        Rule("amount_positive", lambda d: d["amount"].isna() | (d["amount"] > 0)),
        Rule("currency_supported", lambda d: d["currency"].isin(SUPPORTED_CURRENCIES)),
        Rule("txn_type_valid", lambda d: d["txn_type"].isin(TXN_TYPES)),
        Rule("channel_valid", lambda d: d["channel"].isin(CHANNELS), "warn"),
        Rule("status_valid", lambda d: d["status"].isin(STATUSES), "warn"),
    ]


def validate(df: pd.DataFrame, rules: list[Rule], entity: str) -> tuple[pd.Series, list[dict]]:
    """Returns (failure reasons per row - empty string means the row is clean, metrics)."""
    reasons = pd.Series("", index=df.index, dtype=object)
    metrics = []
    for rule in rules:
        ok = rule.passes(df).fillna(False).astype(bool)
        failed = int((~ok).sum())
        metrics.append({"entity": entity, "rule": rule.name, "severity": rule.severity,
                        "failed_rows": failed, "total_rows": len(df),
                        "pass_rate": round(1 - failed / len(df), 4) if len(df) else 1.0})
        if rule.severity == "error":
            reasons[~ok] = reasons[~ok] + rule.name + ";"
    return reasons.str.rstrip(";"), metrics


def _quarantine(cfg: Config, raw: pd.DataFrame, reasons: pd.Series, run_id: str, entity: str) -> int:
    bad = reasons[reasons != ""]
    if bad.empty:
        return 0
    cfg.quarantine_dir.mkdir(parents=True, exist_ok=True)
    out = raw.loc[bad.index].assign(dq_failures=bad, run_id=run_id)
    out.to_csv(cfg.quarantine_dir / f"{run_id}_{entity}.csv", index=False)
    return len(out)


# --------------------------------------------------------------------------------------
# 6. Pipeline orchestration
# --------------------------------------------------------------------------------------
CUSTOMER_COLS = ["customer_id", "full_name", "email", "email_valid", "date_of_birth", "country",
                 "segment", "joined_on", "updated_at", "source_file", "run_id", "loaded_at"]
FACT_COLS = ["txn_id", "customer_id", "txn_ts", "txn_date", "amount", "currency", "fx_rate_per_gbp",
             "amount_gbp", "signed_amount_gbp", "txn_type", "channel", "merchant", "merchant_category",
             "status", "is_large_txn", "source_file", "run_id", "loaded_at"]


@dataclass
class RunSummary:
    run_id: str
    status: str = "running"
    files_processed: int = 0
    rows_read: int = 0
    rows_loaded: int = 0
    rows_quarantined: int = 0
    fx_source: str | None = None
    error: str | None = None
    metrics: list[dict] = field(default_factory=list)


def _process_customers(conn, cfg, files, s: RunSummary) -> None:
    raw = pd.concat([extract_customers(p) for p, _ in files], ignore_index=True)
    s.rows_read += len(raw)
    clean = clean_customers(raw)
    reasons, metrics = validate(clean, customer_rules(), "customer")
    s.rows_quarantined += _quarantine(cfg, raw, reasons, s.run_id, "customer")

    valid = clean[reasons == ""].sort_values("updated_at")
    deduped = valid.drop_duplicates("customer_id", keep="last")  # latest version of each customer wins
    metrics.append({"entity": "customer", "rule": "superseded_versions_dropped", "severity": "info",
                    "failed_rows": len(valid) - len(deduped), "total_rows": len(valid), "pass_rate": None})
    s.metrics += metrics

    rows = deduped.assign(run_id=s.run_id, loaded_at=_utcnow())[CUSTOMER_COLS]
    conn.executemany(_upsert_sql("dim_customer", CUSTOMER_COLS, ["customer_id"], only_if_newer="updated_at"),
                     _records(rows))
    s.rows_loaded += len(rows)
    log.info("Customers: read=%d loaded=%d quarantined=%d", len(raw), len(rows), int((reasons != "").sum()))


def _process_transactions(conn, cfg, files, s: RunSummary) -> None:
    raw = pd.concat([extract_transactions(p) for p, _ in files], ignore_index=True)
    s.rows_read += len(raw)

    exact_dupes = raw.duplicated()
    raw = raw[~exact_dupes]
    clean = clean_transactions(raw)

    known = {r[0] for r in conn.execute("SELECT customer_id FROM dim_customer")}
    reasons, metrics = validate(clean, transaction_rules(known), "transaction")
    metrics.insert(0, {"entity": "transaction", "rule": "exact_duplicates_removed", "severity": "info",
                       "failed_rows": int(exact_dupes.sum()), "total_rows": len(exact_dupes), "pass_rate": None})
    s.rows_quarantined += _quarantine(cfg, raw, reasons, s.run_id, "transaction")

    valid = clean[reasons == ""]
    # The same txn_id can arrive twice (e.g. pending, then completed). Files are processed in date
    # order, so the last occurrence is the most recent state of that transaction.
    deduped = valid[~valid["txn_id"].duplicated(keep="last")]
    metrics.append({"entity": "transaction", "rule": "superseded_versions_dropped", "severity": "info",
                    "failed_rows": len(valid) - len(deduped), "total_rows": len(valid), "pass_rate": None})
    s.metrics += metrics
    if deduped.empty:
        return

    fx, s.fx_source = fetch_fx_rates(deduped["txn_ts"].min().date(), deduped["txn_ts"].max().date(), cfg.offline)
    fx_cols = ["rate_date", "currency", "rate_per_gbp", "source"]
    conn.executemany(_upsert_sql("dim_fx_rate", fx_cols, ["rate_date", "currency"]), _records(fx[fx_cols]))

    enriched = apply_fx(deduped, fx).assign(run_id=s.run_id, loaded_at=_utcnow())[FACT_COLS]
    conn.executemany(_upsert_sql("fact_transaction", FACT_COLS, ["txn_id"]), _records(enriched))
    s.rows_loaded += len(enriched)
    log.info("Transactions: read=%d loaded=%d quarantined=%d", len(raw) + int(exact_dupes.sum()),
             len(enriched), int((reasons != "").sum()))


def run_pipeline(cfg: Config) -> RunSummary:
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:6]
    s = RunSummary(run_id=run_id)
    started = _utcnow()
    conn = connect(cfg.db_path)
    log.info("Run %s started", run_id)
    try:
        files = discover_new_files(conn, cfg.raw_dir)
        if not files:
            log.info("No new or changed source files - nothing to do")
            s.status = "no_new_data"
            return s

        # Customers are loaded before transactions so newly-onboarded customers exist first
        cust_files = [f for f in files if f[0].name.endswith("_customers.json")]
        txn_files = [f for f in files if f[0].name.endswith("_transactions.csv")]

        with conn:  # one atomic transaction: either everything from this run lands, or nothing does
            if cust_files:
                _process_customers(conn, cfg, cust_files, s)
            if txn_files:
                _process_transactions(conn, cfg, txn_files, s)
            conn.executemany(
                "INSERT INTO etl_file_manifest VALUES (?, ?, ?, ?, ?) ON CONFLICT(file_name) DO UPDATE SET "
                "file_hash = excluded.file_hash, rows_read = excluded.rows_read, run_id = excluded.run_id, "
                "processed_at = excluded.processed_at",
                [(p.name, h, _count_rows(p), run_id, _utcnow()) for p, h in files])
            conn.executemany(
                "INSERT INTO dq_result VALUES (?, ?, ?, ?, ?, ?, ?)",
                [(run_id, m["entity"], m["rule"], m["severity"], m["failed_rows"], m["total_rows"], m["pass_rate"])
                 for m in s.metrics])
        s.files_processed = len(files)
        s.status = "success"
        return s
    except Exception as exc:
        s.status, s.error = "failed", f"{type(exc).__name__}: {exc}"
        log.exception("Run %s failed - all changes rolled back", run_id)
        raise
    finally:
        with conn:
            conn.execute("INSERT INTO etl_run VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                         (run_id, started, _utcnow(), s.status, s.files_processed, s.rows_read,
                          s.rows_loaded, s.rows_quarantined, s.fx_source, s.error))
        conn.close()
        if s.status == "success":
            write_run_report(cfg, s)
        log.info("Run %s finished: %s", run_id, s.status)


def _count_rows(path: Path) -> int:
    if path.suffix == ".json":
        return len(json.loads(path.read_text()))
    return max(sum(1 for _ in path.open()) - 1, 0)


# --------------------------------------------------------------------------------------
# 7. Reporting
# --------------------------------------------------------------------------------------
def _md_table(df: pd.DataFrame) -> str:
    cols = list(df.columns)
    lines = ["| " + " | ".join(cols) + " |", "|" + "|".join("---" for _ in cols) + "|"]
    for row in df.itertuples(index=False):
        lines.append("| " + " | ".join("" if pd.isna(v) else str(v) for v in row) + " |")
    return "\n".join(lines)


def write_run_report(cfg: Config, s: RunSummary) -> Path:
    cfg.report_dir.mkdir(parents=True, exist_ok=True)
    dq = pd.DataFrame(s.metrics)
    if not dq.empty:
        dq["pass_rate"] = dq["pass_rate"].map(lambda v: "" if pd.isna(v) else f"{v:.2%}")
    text = "\n".join([
        f"# ETL run report - `{s.run_id}`", "",
        f"- **Status:** {s.status}",
        f"- **Files processed:** {s.files_processed}",
        f"- **Rows read:** {s.rows_read:,}",
        f"- **Rows loaded:** {s.rows_loaded:,}",
        f"- **Rows quarantined:** {s.rows_quarantined:,}",
        f"- **FX source:** {s.fx_source}", "",
        "## Data-quality checks", "",
        _md_table(dq) if not dq.empty else "_No checks run._", "",
    ])
    path = cfg.report_dir / f"run_{s.run_id}.md"
    path.write_text(text)
    return path


def print_report(cfg: Config) -> None:
    if not cfg.db_path.exists():
        print("No warehouse yet - run `python etl_pipeline.py run` first.")
        return
    conn = sqlite3.connect(cfg.db_path)
    q = lambda sql: pd.read_sql(sql, conn)  # noqa: E731
    pd.set_option("display.width", 140)
    print("\n== Warehouse row counts ==")
    print(q("""SELECT 'dim_customer' AS table_name, COUNT(*) AS rows FROM dim_customer
               UNION ALL SELECT 'fact_transaction', COUNT(*) FROM fact_transaction
               UNION ALL SELECT 'dim_fx_rate', COUNT(*) FROM dim_fx_rate""").to_string(index=False))
    print("\n== Recent ETL runs ==")
    print(q("""SELECT run_id, status, files_processed, rows_read, rows_loaded, rows_quarantined, fx_source
               FROM etl_run ORDER BY started_at DESC LIMIT 5""").to_string(index=False))
    print("\n== Daily summary (completed transactions, GBP) ==")
    print(q("SELECT * FROM v_daily_summary ORDER BY txn_date").to_string(index=False))
    print("\n== Spend by category (GBP) ==")
    print(q("SELECT * FROM v_spend_by_category ORDER BY spend_gbp DESC").to_string(index=False))
    print("\n== Data-quality failures across all runs ==")
    print(q("""SELECT entity, rule, severity, SUM(failed_rows) AS failed_rows, SUM(total_rows) AS checked_rows
               FROM dq_result GROUP BY entity, rule, severity HAVING SUM(failed_rows) > 0
               ORDER BY entity, failed_rows DESC""").to_string(index=False))
    conn.close()


def reset(cfg: Config) -> None:
    for p in (cfg.db_path,):
        p.unlink(missing_ok=True)
    for d in (cfg.quarantine_dir, cfg.report_dir):
        shutil.rmtree(d, ignore_errors=True)
    print("Warehouse, quarantine and reports removed (raw files kept).")


# --------------------------------------------------------------------------------------
# 8. CLI
# --------------------------------------------------------------------------------------
def _setup_logging(base_dir: Path) -> None:
    (base_dir / "logs").mkdir(exist_ok=True)
    fmt = "%(asctime)s | %(levelname)-7s | %(message)s"
    logging.basicConfig(level=logging.INFO, format=fmt, handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(base_dir / "logs" / "etl.log"),
    ])


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Banking transactions ETL pipeline")
    sub = parser.add_subparsers(dest="command", required=True)

    g = sub.add_parser("generate", help="create messy synthetic source files")
    g.add_argument("--start", type=date.fromisoformat,
                   default=date.today() - timedelta(days=14), help="first day (YYYY-MM-DD)")
    g.add_argument("--days", type=int, default=14)
    g.add_argument("--customers", type=int, default=500)
    g.add_argument("--txns-per-day", type=int, default=800)
    g.add_argument("--seed", type=int, default=42)

    r = sub.add_parser("run", help="run the ETL pipeline over new source files")
    r.add_argument("--offline", action="store_true", help="skip the FX API and use fallback rates")

    sub.add_parser("report", help="print warehouse and data-quality summary")
    sub.add_parser("reset", help="delete warehouse, quarantine and reports")

    args = parser.parse_args(argv)
    cfg = Config(offline=getattr(args, "offline", False))
    _setup_logging(cfg.base_dir)

    if args.command == "generate":
        generate_raw_data(cfg, args.start, args.days, args.customers, args.txns_per_day, args.seed)
    elif args.command == "run":
        s = run_pipeline(cfg)
        print(f"\nRun {s.run_id}: {s.status} | files={s.files_processed} read={s.rows_read:,} "
              f"loaded={s.rows_loaded:,} quarantined={s.rows_quarantined:,} fx={s.fx_source}")
    elif args.command == "report":
        print_report(cfg)
    elif args.command == "reset":
        reset(cfg)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


# --------------------------------------------------------------------------------------
# 9. Tests  (run with:  pytest etl_pipeline.py -v)
# --------------------------------------------------------------------------------------
def _tmp_cfg(tmp_path: Path) -> Config:
    return Config(base_dir=tmp_path, offline=True)


def test_parse_amount_handles_symbols_commas_and_nulls():
    s = pd.Series(["£1,234.50", "12", "", "N/A", "-5.00", "abc"])
    out = parse_amount(s)
    assert out.iloc[0] == 1234.5 and out.iloc[1] == 12 and out.iloc[4] == -5
    assert out.iloc[[2, 3, 5]].isna().all()


def test_parse_datetime_mixed_formats_are_day_first():
    out = parse_datetime(pd.Series(["2026-10-03T14:00:00", "03/10/2026 14:00:00", "not a date"]))
    assert out.iloc[0] == out.iloc[1] == pd.Timestamp("2026-10-03 14:00:00")
    assert pd.isna(out.iloc[2])


def test_validation_quarantines_bad_rows():
    raw = pd.DataFrame({
        "txn_id": ["T1", "", "T3", "T4"], "customer_id": ["C00001", "C00001", "C99999", "C00001"],
        "txn_timestamp": ["2026-01-01T10:00:00"] * 4, "amount": ["10", "10", "10", "-3"],
        "currency": ["gbp", "GBP", "GBP", "GBP"], "txn_type": ["DR", "debit", "debit", "debit"],
        "channel": ["card"] * 4, "merchant": ["Tesco"] * 4, "merchant_category": ["groceries"] * 4,
        "status": ["completed"] * 4, "source_file": ["f"] * 4,
    })
    reasons, _ = validate(clean_transactions(raw), transaction_rules({"C00001"}), "transaction")
    assert reasons.iloc[0] == ""                      # lowercase currency + 'DR' are fixed, not rejected
    assert "txn_id_present" in reasons.iloc[1]
    assert "customer_exists" in reasons.iloc[2]
    assert "amount_positive" in reasons.iloc[3]


def test_fx_conversion_uses_latest_prior_rate():
    df = pd.DataFrame({"txn_ts": pd.to_datetime(["2026-01-05 09:00", "2026-01-04 09:00"]),
                       "currency": ["USD", "GBP"], "amount": [127.0, 50.0], "txn_type": ["debit", "credit"]})
    fx = pd.DataFrame({"rate_date": ["2026-01-02", "2026-01-02"], "currency": ["USD", "GBP"],
                       "rate_per_gbp": [1.27, 1.0]})
    out = apply_fx(df, fx)
    assert out["amount_gbp"].tolist() == [100.0, 50.0]
    assert out["signed_amount_gbp"].tolist() == [-100.0, 50.0]


def test_end_to_end_run_is_idempotent_and_incremental(tmp_path):
    cfg = _tmp_cfg(tmp_path)
    start = date.today() - timedelta(days=10)
    generate_raw_data(cfg, start, days=3, n_customers=50, txns_per_day=100)

    first = run_pipeline(cfg)
    assert first.status == "success" and first.rows_loaded > 0 and first.rows_quarantined > 0
    conn = sqlite3.connect(cfg.db_path)
    n_fact = conn.execute("SELECT COUNT(*) FROM fact_transaction").fetchone()[0]

    # Re-running with no new files changes nothing
    assert run_pipeline(cfg).status == "no_new_data"
    assert conn.execute("SELECT COUNT(*) FROM fact_transaction").fetchone()[0] == n_fact

    # Regenerating the same days yields identical files -> still skipped
    generate_raw_data(cfg, start, days=3, n_customers=50, txns_per_day=100)
    assert run_pipeline(cfg).status == "no_new_data"

    # New days arrive -> only the new files are processed
    generate_raw_data(cfg, start, days=5, n_customers=50, txns_per_day=100)
    third = run_pipeline(cfg)
    assert third.status == "success" and third.files_processed == 2  # only the 2 new daily files
    assert conn.execute("SELECT COUNT(*) FROM fact_transaction").fetchone()[0] > n_fact
    # no orphans, no non-positive amounts, everything converted to GBP
    assert conn.execute("""SELECT COUNT(*) FROM fact_transaction f LEFT JOIN dim_customer c
                           USING (customer_id) WHERE c.customer_id IS NULL""").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM fact_transaction WHERE amount <= 0 OR amount_gbp IS NULL").fetchone()[0] == 0
    conn.close()


def test_late_arriving_update_overwrites_status(tmp_path):
    cfg = _tmp_cfg(tmp_path)
    cfg.raw_dir.mkdir(parents=True)
    cols = "txn_id,customer_id,txn_timestamp,amount,currency,txn_type,channel,merchant,merchant_category,status"
    (cfg.raw_dir / "2026-01-01_customers.json").write_text(json.dumps([{
        "customer_id": "C00001", "full_name": "a b", "email": "a@b.com", "date_of_birth": "1990-01-01",
        "country": "GB", "segment": "retail", "joined_on": "2020-01-01", "updated_at": "2026-01-01T00:00:00"}]))
    (cfg.raw_dir / "2026-01-01_transactions.csv").write_text(
        f"{cols}\nT1,C00001,2026-01-01T10:00:00,10,GBP,debit,card,Tesco,groceries,pending\n")
    run_pipeline(cfg)
    (cfg.raw_dir / "2026-01-02_transactions.csv").write_text(
        f"{cols}\nT1,C00001,2026-01-01T10:00:00,10,GBP,debit,card,Tesco,groceries,completed\n")
    run_pipeline(cfg)
    conn = sqlite3.connect(cfg.db_path)
    assert conn.execute("SELECT COUNT(*), MAX(status) FROM fact_transaction").fetchone() == (1, "completed")
    conn.close()


def test_failed_run_rolls_back(tmp_path, monkeypatch):
    cfg = _tmp_cfg(tmp_path)
    generate_raw_data(cfg, date.today() - timedelta(days=5), days=1, n_customers=20, txns_per_day=20)

    def boom(*_a, **_k):
        raise RuntimeError("simulated failure")

    monkeypatch.setattr(sys.modules[__name__], "apply_fx", boom)
    try:
        run_pipeline(cfg)
    except RuntimeError:
        pass
    conn = sqlite3.connect(cfg.db_path)
    assert conn.execute("SELECT COUNT(*) FROM dim_customer").fetchone()[0] == 0      # customers rolled back too
    assert conn.execute("SELECT COUNT(*) FROM etl_file_manifest").fetchone()[0] == 0  # files will be retried
    assert conn.execute("SELECT status FROM etl_run").fetchone()[0] == "failed"
    conn.close()
