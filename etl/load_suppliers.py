"""
Load supplier CSV files (source_files/suppliers/<supplier>/*.csv) into bronze_suppliers.

  fresh/*.csv   -> bronze_suppliers.fresh
  general/*.csv -> bronze_suppliers.general

  - Ledger table (file_path + file_hash): NEW file -> load | CHANGED -> replace its rows | SAME -> skip.
    Rows + ledger entry are saved in ONE transaction.
  - Headers are mapped through ALLOWED (any case / spaces / underscores / old or new name).
    Unknown headers are ignored with a warning. Columns with no data are not created.
  - Types are INFERRED from ALL files of a table (integer / bigint / numeric / date, else text).
    If they differ from the table: empty it, rebuild it, reload ALL its files.
  - Rows with more values than the header (e.g. "11;33") are glued into the last column ("11.33").
    Exact duplicate rows inside a file are dropped.

Extra columns: _source_file, _file_hash, _stock_date (date from the file name), _loaded_at.

Run:  python load_suppliers.py             (normal)
      python load_suppliers.py --rebuild   (force a full reload)
"""
import csv
import datetime
import hashlib
import io
import logging
import os
import re
import sys
import tempfile
from pathlib import Path

import psycopg2
from dotenv import load_dotenv
from psycopg2 import sql

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ---------------------------------------------------------------- config
WAREHOUSE = {
    "host": os.getenv("WAREHOUSE_HOST", "localhost"),
    "port": int(os.getenv("WAREHOUSE_PORT", "5435")),
    "dbname": os.getenv("WAREHOUSE_DB", "postgres"),
    "user": os.getenv("WAREHOUSE_USER", "fresh_wh"),
    "password": os.getenv("WAREHOUSE_PASSWORD"),
}

SUPPLIERS_DIR = Path(os.getenv("SUPPLIERS_DIR", ROOT / "source_files" / "suppliers"))
SCHEMA = "bronze_suppliers"
LEDGER = "etl_loaded_files"
META_COLS = ["_source_file", "_file_hash", "_stock_date", "_loaded_at"]

# final column -> header names that feed it (compared without case / spaces / _ / -)
QTY = ["qtyonhands", "qtyonhand", "quantityonhand", "quantityonhands", "qty", "quantity"]
COST = ["unitcost", "costperunit", "cost"]

ALLOWED = {
    "fresh": {
        "supplier": ["supplier"],
        "product_id": ["product", "productid"],
        "store_id": ["store", "storeid"],
        "stock_date": ["date", "stockdate"],
        "QtyOnHands": QTY,
        "reorder_level": ["reorderlevel", "minqty", "minimumqty"],
        "UnitCost": COST,
    },
    "general": {
        "Supplier": ["supplier"],
        "Product": ["product", "productid"],
        "Store": ["store", "storeid"],
        "Date": ["date", "stockdate"],
        "QtyOnHands": QTY,
        "MinQty": ["minqty", "reorderlevel", "minimumqty"],
        "UnitCost": COST,
    },
}
# A supplier folder not listed above is loaded as-is (all of its columns).


# ---------------------------------------------------------------- helpers
def loose(name):
    """Lower-case, letters and digits only."""
    return re.sub(r"[^a-z0-9]", "", name.lower())


def stock_date(name):
    """Find 2026-10-17 or 20261017 in the file name."""
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})|(\d{4})(\d{2})(\d{2})", name)
    try:
        return datetime.date(*[int(g) for g in m.groups() if g]) if m else None
    except ValueError:
        return None


def file_hash(path):
    """SHA-256 of the file content (read in 1 MB chunks)."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def qualified(table):
    return sql.SQL("{}.{}").format(sql.Identifier(SCHEMA), sql.Identifier(table))


def read_csv(path):
    """Return (header, rows, glued). Detects encoding + delimiter; every row has len(header) values."""
    raw = path.read_bytes()
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        text = raw.decode("cp1252")

    delimiter = max([",", ";", "\t", "|"], key=text.split("\n", 1)[0].count)
    data = [r for r in csv.reader(io.StringIO(text, newline=""), delimiter=delimiter) if r]
    if not data:
        return [], [], 0

    header = [h.strip() for h in data[0]]
    n, rows, glued = len(header), [], 0
    for r in data[1:]:
        if len(r) > n:
            extra = r[n - 1:]
            if any(v.strip() for v in extra[1:]):          # real extra value, e.g. "11;33"
                r, glued = r[:n - 1] + [".".join(extra)], glued + 1
            else:                                          # only trailing empty fields
                r = r[:n]
        rows.append(r + [None] * (n - len(r)))
    return header, rows, glued


def shape(table, header, rows, keep=None):
    """Map headers to final columns. Returns (cols, rows, ignored_headers, duplicates_dropped)."""
    spec = ALLOWED.get(table)
    lookup = {loose(a): canon for canon, names in (spec or {}).items() for a in names + [canon]}

    positions, ignored = {}, []                            # final column -> source positions
    for i, h in enumerate(header):
        canon = lookup.get(loose(h)) if spec else (h or None)
        if canon:
            positions.setdefault(canon, []).append(i)
        else:
            ignored.append(h or f"#{i + 1}")
    if keep is not None:
        positions = {c: p for c, p in positions.items() if c in keep}

    out, seen, dups = [], set(), 0
    for r in rows:
        # two headers feeding one column (e.g. Qty + QtyOnHands): take the first non-empty value
        vals = tuple(next((r[i].strip() for i in idx if r[i] and r[i].strip()), None)
                     for idx in positions.values())
        if not any(vals):
            continue                                       # blank line
        if vals in seen:
            dups += 1
            continue
        seen.add(vals)
        out.append(vals)
    return list(positions), out, ignored, dups


# ---------------------------------------------------------------- type inference
def kind(v):
    """Type of one value: int / bigint / numeric / date / text."""
    if re.fullmatch(r"-?(0|[1-9]\d*)", v):                 # no leading zeros (007 stays text)
        return "int" if abs(int(v)) < 2**31 else "bigint"
    if re.fullmatch(r"-?\d+[.,]\d+", v):
        return "numeric"
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", v):
        try:
            datetime.date.fromisoformat(v)
            return "date"
        except ValueError:
            pass
    return "text"


def pg_type(kinds):
    """Combine the kinds seen in one column into one Postgres type."""
    if "text" in kinds:
        return "text"
    if kinds <= {"int", "bigint"}:
        return "bigint" if "bigint" in kinds else "integer"
    if kinds <= {"int", "bigint", "numeric"}:
        return "numeric"
    return "date" if kinds == {"date"} else "text"


def infer_types(table, files):
    """{column: postgres type} from ALL files of the table. Columns without data are left out."""
    kinds = {}
    for p in files:
        header, rows, _ = read_csv(p)
        cols, rows, _, _ = shape(table, header, rows)
        for i, c in enumerate(cols):
            kinds.setdefault(c, set()).update(kind(r[i]) for r in rows if r[i])
    return {c: pg_type(k) for c, k in kinds.items() if k}


def sync_table(wh, table, types, rebuild):
    """Make the table match `types`. If not: empty it, rebuild columns, forget its files in the ledger."""
    target = qualified(table)
    with wh.cursor() as cur:
        cur.execute(sql.SQL("""
            CREATE TABLE IF NOT EXISTS {} (
                _source_file TEXT, _file_hash TEXT, _stock_date DATE,
                _loaded_at TIMESTAMPTZ DEFAULT now())
        """).format(target))
        cur.execute("SELECT column_name, data_type FROM information_schema.columns "
                    "WHERE table_schema = %s AND table_name = %s", (SCHEMA, table))
        current = {c: t for c, t in cur.fetchall() if c not in META_COLS}
        if current != types or rebuild:
            cur.execute(sql.SQL("TRUNCATE {}").format(target))
            for c in current:
                cur.execute(sql.SQL("ALTER TABLE {} DROP COLUMN {}").format(target, sql.Identifier(c)))
            for c, t in types.items():                     # t comes from pg_type(), never from the files
                cur.execute(sql.SQL("ALTER TABLE {} ADD COLUMN {} {}").format(
                    target, sql.Identifier(c), sql.SQL(t)))
            cur.execute(sql.SQL("DELETE FROM {} WHERE table_name = %s").format(qualified(LEDGER)), (table,))
            logging.warning("%s: structure/types changed -> full reload with types %s", table, types)
    wh.commit()


# ---------------------------------------------------------------- load one file
def load_file(wh, table, rel_path, path, digest, types):
    header, raw_rows, glued = read_csv(path)
    cols, rows, ignored, dups = shape(table, header, raw_rows, keep=types)
    day = stock_date(path.name)

    if day is None:
        logging.warning("%s: no date in file name - _stock_date will be NULL", rel_path)
    if ignored:
        logging.warning("%s: ignored columns not in ALLOWED: %s", rel_path, ignored)
    if glued:
        logging.warning("%s: %s rows had extra values - glued into the last column", rel_path, glued)
    if dups:
        logging.warning("%s: %s duplicate rows dropped", rel_path, dups)

    dec = [i for i, c in enumerate(cols) if types[c] == "numeric"]   # decimal comma -> dot
    target = qualified(table)
    with wh.cursor() as cur:
        cur.execute(sql.SQL("DELETE FROM {} WHERE _source_file = %s").format(target), (rel_path,))
        if rows and cols:
            with tempfile.TemporaryFile("w+", newline="", encoding="utf-8") as buf:
                writer = csv.writer(buf)
                for r in rows:
                    r = list(r)
                    for i in dec:
                        r[i] = r[i] and r[i].replace(",", ".")
                    writer.writerow([rel_path, digest, day] + r)
                buf.seek(0)
                names = sql.SQL(", ").join(
                    sql.Identifier(c) for c in ["_source_file", "_file_hash", "_stock_date"] + cols)
                cur.copy_expert(sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT csv)").format(
                    target, names).as_string(wh), buf)

        cur.execute(sql.SQL("""
            INSERT INTO {} (file_path, table_name, file_hash, row_count)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (file_path) DO UPDATE
               SET file_hash = EXCLUDED.file_hash, row_count = EXCLUDED.row_count, loaded_at = now()
        """).format(qualified(LEDGER)), (rel_path, table, digest, len(rows)))

    wh.commit()  # rows + ledger together
    return len(rows)


# ---------------------------------------------------------------- main
def main():
    rebuild = "--rebuild" in sys.argv
    if not WAREHOUSE["password"]:
        raise ValueError("Missing WAREHOUSE_PASSWORD in .env")
    if not SUPPLIERS_DIR.is_dir():
        raise FileNotFoundError(f"Suppliers folder not found: {SUPPLIERS_DIR}")

    wh = psycopg2.connect(**WAREHOUSE, connect_timeout=10)
    new_count = changed_count = errors = 0
    folders = {re.sub(r"\W+", "_", p.name.lower()).strip("_") or "files": p
               for p in sorted(SUPPLIERS_DIR.iterdir()) if p.is_dir()}

    try:
        with wh.cursor() as cur:
            cur.execute("SELECT pg_advisory_lock(hashtext('load_suppliers'))")  # no parallel runs
            cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(SCHEMA)))
            cur.execute(sql.SQL("""
                CREATE TABLE IF NOT EXISTS {} (
                    file_path TEXT PRIMARY KEY, table_name TEXT, file_hash TEXT,
                    row_count BIGINT, loaded_at TIMESTAMPTZ DEFAULT now())
            """).format(qualified(LEDGER)))
        wh.commit()

        for table, folder in folders.items():
            files = sorted(p for p in folder.rglob("*") if p.is_file() and p.suffix.lower() == ".csv")
            try:
                types = infer_types(table, files)
                sync_table(wh, table, types, rebuild)
            except Exception:
                wh.rollback()
                errors += 1
                logging.exception("%s: could not prepare table", table)
                continue

            with wh.cursor() as cur:
                cur.execute(sql.SQL("SELECT file_path, file_hash FROM {} WHERE table_name = %s").format(
                    qualified(LEDGER)), (table,))
                loaded = dict(cur.fetchall())
            wh.commit()

            for path in files:
                rel, digest = path.relative_to(SUPPLIERS_DIR).as_posix(), file_hash(path)
                if rel not in loaded:
                    status = "new"
                elif digest != loaded[rel]:
                    status = "changed"
                else:
                    continue  # already loaded, same content
                try:
                    n = load_file(wh, table, rel, path, digest, types)
                    new_count += status == "new"
                    changed_count += status == "changed"
                    logging.info("%s file %s: %s rows -> %s.%s", status.upper(), rel, n, SCHEMA, table)
                except Exception:
                    wh.rollback()
                    errors += 1
                    logging.exception("Failed loading %s (will retry next run)", rel)

            with wh.cursor() as cur:
                cur.execute(sql.SQL("""
                    SELECT _stock_date, count(DISTINCT _source_file) FROM {}
                    GROUP BY 1 HAVING count(DISTINCT _source_file) > 1
                """).format(qualified(table)))
                for day, n in cur.fetchall():
                    logging.warning("%s: %s files share stock date %s - check for double data", table, n, day)
            wh.commit()

        logging.info("Done. new=%s | changed=%s | failed=%s", new_count, changed_count, errors)
    finally:
        wh.close()

    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main()