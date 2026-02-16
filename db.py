# db.py
import pyodbc
import polars as pl
from pyarrow import parquet as pq
from typing import List
from pathlib import Path
from functools import lru_cache

SERVER   = "server_here"
DATABASE = "database_name_here"
DRIVER   = "driver_name_here"

def _connect():
    return pyodbc.connect(
        f"DRIVER={{{DRIVER}}};SERVER={SERVER};DATABASE={DATABASE};"
        "Trusted_Connection=Yes;Encrypt=Yes;TrustServerCertificate=Yes;",
        timeout=5,
    )

def search_serial_numbers_contains(substring: str, limit: int = 50, test_stands: List[str] = None) -> List[str]:
    q = (substring or "").strip()
    if not q:
        return []

    like_param = f"%{q}%"

    # Base WHERE: match the serial substring
    where_clauses = ["serial_number LIKE ?"]
    params: list[object] = [limit, like_param]

    # Optional: restrict to selected test stands
    if test_stands:
        placeholders = ",".join("?" for _ in test_stands)
        where_clauses.append(f"test_stand IN ({placeholders})")
        params.extend(test_stands)

    sql = f"""
        SELECT TOP (?)
            serial_number,
            test_stand
        FROM dbo.SilverFiles
        WHERE {' AND '.join(where_clauses)}
        ORDER BY serial_number, test_stand
    """

    with _connect() as cn:
        cur = cn.cursor()
        rows = cur.execute(sql, tuple(params)).fetchall()

    labels = []
    labels.extend(f"{serial} ({stand})" for serial, stand in rows)
    return labels

def search_test_stands_contains(substring: str, limit: int = 50) -> List[str]:
    q = (substring or "").strip()
    if not q:
        return []
    sql = """
        SELECT DISTINCT TOP (?)
            test_stand
        FROM dbo.SilverFiles
        WHERE test_stand LIKE ?
        ORDER BY test_stand
    """
    like_param = f"%{q}%"
    with _connect() as cn:
        cur = cn.cursor()
        rows = cur.execute(sql, (limit, like_param)).fetchall()
    return [r[0] for r in rows]

def _split_serial_label(label: str) -> tuple[str, str | None]:
    s = (label or "").strip()
    if not s:
        return "", None
    if " (" not in s:
        return s, None
    serial, stand = s.rsplit(" (", 1)
    stand = stand.rstrip(")")
    return serial, stand or None

def bronze_paths_for_serial_uncached(serial: str, limit: int = 500) -> List[str]:
    """
    Given an exact serial, find its Silver file(s) then read their Bronze references.
    Expects a column like 'bronze_path' in the Silver parquet; tries a couple fallbacks.
    Returns a de-duplicated, sorted list.
    """
    serial = (serial or "").strip()
    if not serial:
        return []
    
    serial_number, stand = _split_serial_label(serial)

    # 1) Get Silver parquet paths for the serial
    if stand:
        sql = """
            SELECT file_path
            FROM dbo.SilverFiles
            WHERE serial_number = ? AND test_stand = ?
            ORDER BY file_path
        """
        params = (serial_number, stand)
    else:
        sql = """
            SELECT file_path
            FROM dbo.SilverFiles
            WHERE serial_number = ?
            ORDER BY file_path
        """
        params = (serial_number,)
        
    silver_paths: List[str] = []
    with _connect() as cn:
        cur = cn.cursor()
        silver_paths = [row[0] for row in cur.execute(sql, params).fetchall()]

    if not silver_paths:
        return []

    # 2) Collect bronze paths from the parquets
    bronze: list[str] = []
    for fp in silver_paths:
        try:
            # Try the common column name first (fast, column-pruned)
            df = pl.read_parquet(fp, columns=["bronze_path"])
            bronze.extend(df.get_column("bronze_path").drop_nulls().to_list())
            continue
        except Exception:
            # Fallback: read schema & look for a plausible column
            try:
                df_all = pl.read_parquet(fp)
                cand = None
                for c in df_all.columns:
                    lc = c.lower()
                    if "bronze" in lc and ("path" in lc or "file" in lc or "ref" in lc):
                        cand = c
                        break
                if cand:
                    bronze.extend(df_all.get_column(cand).drop_nulls().to_list())
            except Exception as e:
                print(f"[WARN] Could not read bronze refs from {fp}: {e}")

    # 3) Dedup & cap
    dedup = sorted({str(p) for p in bronze})
    return dedup


@lru_cache(maxsize = 1024)
def bronze_paths_for_serial_cached(serial: str) -> tuple[str, ...]:
    print(bronze_paths_for_serial_cached.cache_info().currsize)
    return tuple(bronze_paths_for_serial_uncached(serial))

def bronze_paths_for_serial(serial: str, limit: int = 500) -> List[str]:
    return list(bronze_paths_for_serial_cached(serial))[:limit]

def filter_by_testtype(bronze_list: List[str], type_list: List[str]) -> List[str]:
    types = ",".join("?" for _ in type_list)
    paths = ",".join("?" for _ in bronze_list)

    if not paths:
        return []
    
    sql = f"""
        SELECT file_path
        FROM dbo.BronzeFiles
        WHERE file_path IN ({paths})
        AND (
            test_type IN ({types})
            OR test_type = 'None'
            )
        ORDER BY file_path
    """

    with _connect() as cn:
        cur = cn.cursor()
        rows = cur.execute(sql, *bronze_list, *type_list).fetchall()

    return [row[0] for row in rows]


def parquet_to_csv(path: Path):
    df = pl.read_parquet(str(path), engine="pyarrow")
    df.to_csv(str(path), index=False)

