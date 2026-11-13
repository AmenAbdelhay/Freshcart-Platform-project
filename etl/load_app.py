"""
Daily Production -> Bronze sync (incremental).

Per table:
  - first run, or the source got a NEW COLUMN (e.g. orders.promocode)
        -> one full reload, so old rows also get the new column's values
  - otherwise
        -> rows whose primary key is missing in the warehouse (any age)
        -> rows changed since the warehouse watermark (needs `updated_at`)

Everything is UPSERTed by primary key, so re-running never creates duplicates.
One transaction per table. Rows deleted in the source are not removed.
"""
import os
import logging
import tempfile
from datetime import timedelta
from pathlib import Path

import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env")
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

SOURCE = {
    "host": os.getenv("PROD_HOST", "localhost"),
    "port": int(os.getenv("PROD_PORT", "5433")),
    "dbname": os.getenv("PROD_DB", "freshcart_source"),
    "user": os.getenv("PROD_USER", "analytics_reader"),
    "password": os.getenv("PROD_PASSWORD"),
}
WAREHOUSE = {
    "host": os.getenv("WAREHOUSE_HOST", "localhost"),
    "port": int(os.getenv("WAREHOUSE_PORT", "5435")),
    "dbname": os.getenv("WAREHOUSE_DB", "postgres"),
    "user": os.getenv("WAREHOUSE_USER", "fresh_wh"),
    "password": os.getenv("WAREHOUSE_PASSWORD"),
}

SOURCE_SCHEMA = "app"
TARGET_SCHEMA = "bronze_app"
TABLES = ["stores", "customers", "products", "product_prices", "orders", "order_items", "payments"]

UPDATED_COL = "updated_at"      # column used to detect changed rows
LOOKBACK = timedelta(hours=1)   # safety overlap for the watermark (upsert is idempotent)
KEY_BATCH = 5000                # missing keys fetched per query

ident = sql.Identifier


def join(names):
    return sql.SQL(", ").join(ident(n) for n in names)


def columns(src, table):
    """[(column, source type)] - enums/domains/composites become text."""
    with src.cursor() as cur:
        cur.execute("""
            SELECT a.attname,
                   CASE WHEN t.typtype IN ('e','d','c') THEN 'text'
                        WHEN t.typcategory = 'A' AND et.typtype IN ('e','d','c') THEN 'text[]'
                        ELSE format_type(a.atttypid, a.atttypmod) END
            FROM pg_attribute a
            JOIN pg_class c ON c.oid = a.attrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_type t ON t.oid = a.atttypid
            LEFT JOIN pg_type et ON t.typcategory = 'A' AND et.oid = t.typelem
            WHERE n.nspname = %s AND c.relname = %s AND a.attnum > 0 AND NOT a.attisdropped
            ORDER BY a.attnum
        """, (SOURCE_SCHEMA, table))
        return cur.fetchall()


def primary_key(src, table):
    with src.cursor() as cur:
        cur.execute("""
            SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY (i.indkey)
            WHERE i.indrelid = (quote_ident(%s) || '.' || quote_ident(%s))::regclass AND i.indisprimary
            ORDER BY array_position(i.indkey::int2[], a.attnum)
        """, (SOURCE_SCHEMA, table))
        return [r[0] for r in cur.fetchall()]


def copy_rows(src, query, params, cur, dest, cols):
    """Stream a source query straight into a warehouse table (via a temp file)."""
    with tempfile.TemporaryFile() as buf, src.cursor() as s:
        s.copy_expert(s.mogrify("COPY (" + query.as_string(src) + ") TO STDOUT", params).decode(), buf)
        buf.seek(0)
        cur.copy_expert(sql.SQL("COPY {} ({}) FROM STDIN").format(dest, cols).as_string(cur.connection), buf)


def sync_table(src, wh, table):
    defs = columns(src, table)
    pk = primary_key(src, table)
    if not defs:
        raise ValueError(f"Source table {SOURCE_SCHEMA}.{table} not found")
    if not pk:
        raise ValueError(f"{SOURCE_SCHEMA}.{table} has no primary key (needed to avoid duplicates)")

    types = dict(defs)
    names = list(types)
    cols, pk_cols = join(names), join(pk)
    src_tbl = sql.SQL("{}.{}").format(ident(SOURCE_SCHEMA), ident(table))
    target = sql.SQL("{}.{}").format(ident(TARGET_SCHEMA), ident(table))
    stg = ident("_stg")
    has_updated = UPDATED_COL in types

    with wh.cursor() as cur:
        # --- target table: a new table or a new source column means "full reload"
        cur.execute("SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema = %s AND table_name = %s", (TARGET_SCHEMA, table))
        full = not set(names) <= {r[0] for r in cur.fetchall()}

        cur.execute(sql.SQL("CREATE TABLE IF NOT EXISTS {} ()").format(target))
        for n in names:
            cur.execute(sql.SQL("ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} {}").format(
                target, ident(n), sql.SQL(types[n])))
        cur.execute(sql.SQL("CREATE UNIQUE INDEX IF NOT EXISTS {} ON {} ({})").format(
            ident(f"ux_{table}_pk"), target, pk_cols))
        cur.execute(sql.SQL("CREATE TEMP TABLE {} (LIKE {}) ON COMMIT DROP").format(stg, target))

        if full:
            copy_rows(src, sql.SQL("SELECT {} FROM {}").format(cols, src_tbl), None, cur, stg, cols)
        else:
            # (1) MISSING rows: compare primary keys only, then fetch just those rows
            keys = ident("_src_keys")
            cur.execute(sql.SQL("CREATE TEMP TABLE {} ON COMMIT DROP AS SELECT {} FROM {} WITH NO DATA")
                        .format(keys, pk_cols, target))
            copy_rows(src, sql.SQL("SELECT {} FROM {}").format(pk_cols, src_tbl), None, cur, keys, pk_cols)

            on = sql.SQL(" AND ").join(sql.SQL("t.{c} = k.{c}").format(c=ident(n)) for n in pk)
            cur.execute(sql.SQL("SELECT k.* FROM {} k WHERE NOT EXISTS (SELECT 1 FROM {} t WHERE {})")
                        .format(keys, target, on))
            missing = cur.fetchall()

            unnest = sql.SQL(", ").join(sql.SQL("%s::{}[]").format(sql.SQL(types[n])) for n in pk)
            by_key = sql.SQL("SELECT {} FROM {} WHERE ({}) IN (SELECT * FROM unnest({}))").format(
                cols, src_tbl, pk_cols, unnest)
            for i in range(0, len(missing), KEY_BATCH):
                arrays = [list(c) for c in zip(*missing[i:i + KEY_BATCH])]
                copy_rows(src, by_key, arrays, cur, stg, cols)

            # (2) CHANGED rows: only rows touched since the warehouse watermark
            if has_updated:
                cur.execute(sql.SQL("SELECT max({}) FROM {}").format(ident(UPDATED_COL), target))
                watermark = cur.fetchone()[0]
                if watermark is not None:
                    changed = sql.SQL("SELECT {} FROM {} WHERE {} >= %s").format(
                        cols, src_tbl, ident(UPDATED_COL))
                    copy_rows(src, changed, (watermark - LOOKBACK,), cur, stg, cols)

        # --- upsert (DISTINCT ON removes overlap between the fetches)
        non_pk = [n for n in names if n not in pk]
        action = (sql.SQL("DO UPDATE SET {}").format(sql.SQL(", ").join(
            sql.SQL("{c} = EXCLUDED.{c}").format(c=ident(n)) for n in non_pk))
            if non_pk else sql.SQL("DO NOTHING"))
        cur.execute(sql.SQL(
            "WITH up AS (INSERT INTO {t} ({c}) SELECT DISTINCT ON ({pk}) {c} FROM {s} ORDER BY {pk} "
            "ON CONFLICT ({pk}) {a} RETURNING (xmax = 0) AS ins) "
            "SELECT count(*) FILTER (WHERE ins), count(*) FILTER (WHERE NOT ins) FROM up"
        ).format(t=target, c=cols, pk=pk_cols, s=stg, a=action))
        inserted, updated = cur.fetchone()

    wh.commit()
    src.rollback()
    logging.info("%s: inserted=%s | updated=%s%s%s", table, inserted, updated,
                 " (FULL RELOAD: new table/column)" if full else "",
                 "" if has_updated else " (no updated_at: only new rows tracked)")


def main():
    for name in ("PROD_PASSWORD", "WAREHOUSE_PASSWORD"):
        if not os.getenv(name):
            raise ValueError(f"Missing {name} in .env")

    src = psycopg2.connect(**SOURCE, connect_timeout=10)
    src.set_session(readonly=True)
    wh = psycopg2.connect(**WAREHOUSE, connect_timeout=10, options="-c lock_timeout=30000")
    try:
        with wh.cursor() as cur:
            cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(ident(TARGET_SCHEMA)))
        wh.commit()

        for table in TABLES:
            logging.info("Syncing %s ...", table)
            try:
                sync_table(src, wh, table)
            except Exception:
                wh.rollback()
                src.rollback()
                logging.exception("Failed syncing %s", table)
                raise
        logging.info("Production to Bronze sync completed successfully.")
    finally:
        src.close()
        wh.close()


if __name__ == "__main__":
    main()