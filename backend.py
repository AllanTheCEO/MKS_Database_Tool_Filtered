# backend.py
from typing import List
from db import search_serial_numbers_contains as _db_search_serials, search_test_stands_contains as _db_search_teststand
from db import bronze_paths_for_serial as _db_bronze
import db
from pathlib import Path
import io
import zipfile
import polars as pl


def search_serials(prefix: str = "", limit: int = 50, test_stands: List[str] | None = None) -> List[str]:
    q = (prefix or "").strip()
    if not q:
        return []
    return _db_search_serials(q, limit, test_stands)

def search_teststand(prefix: str = "", limit: int = 50) -> List[str]:
    q = (prefix or "").strip()
    if not q:
        return []
    return _db_search_teststand(q, limit)
    

def bronze_paths_for_serial(serial: str, limit: int = 500) -> List[str]:
    s = (serial or "").strip()
    if not s:
        return []
    return _db_bronze(s, limit)


def filter_bronze_by_testtype(bronze_list: List[str], type_list: List[str]) -> List[str]:
    if not bronze_list:
        return []
    if not type_list:
        return bronze_list
    return db.filter_by_testtype(bronze_list, type_list)


def zip_csv_from_parquets(paths: List[str]) -> bytes:
    """Read multiple parquet files and return a ZIP archive of CSVs as bytes."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as z:
        name_counts = {}
        for raw in paths:
            p = str(raw).strip()
            if not p:
                continue
            try:
                df = pl.read_parquet(p)
            except Exception as e:
                # Include a tiny error note instead of failing the whole archive
                err_name = f"FAILED_{Path(p).name}.txt"
                z.writestr(err_name, f"Failed to read parquet {p}: {e}\n")
                continue

            # Generate a mostly-unique CSV name within the ZIP
            base = Path(p).with_suffix(".csv").name
            n = name_counts.get(base, 0)
            name_counts[base] = n + 1
            if n:
                stem = Path(base).stem
                base = f"{stem}_{n}.csv"

            s_buf = io.StringIO()
            df.write_csv(s_buf)
            csv_bytes = s_buf.getvalue().encode("utf-8")
            z.writestr(base, csv_bytes)

    return buf.getvalue()
