from pathlib import Path
import sys
import polars as pl
from pyarrow import parquet as pq

# Resolve the silver folder next to this script
SILVER_ROOT = (Path(__file__).parent / "silver_parquets").resolve()

DEFAULT_SERIALS = ["tests_here"]

def read_silver_df(serial: str, silver_root: Path = SILVER_ROOT) -> pl.DataFrame:
    fp = silver_root / f"{serial}.parquet"
    if not fp.exists():
        raise FileNotFoundError(f"Missing silver file for {serial}: {fp}")
    return pl.read_parquet(fp)

def maybe_print_metadata(fp: Path) -> None:
    try:
        meta = pq.ParquetFile(fp).metadata.metadata or {}
        sn = meta.get(b"Serial Number")
        if sn:
            print(f"  metadata Serial Number: {sn.decode()}")
    except Exception:
        pass  # metadata is optional

def maybe_print_testtype(fp: Path) -> None:
    try:
        meta = pq.ParquetFile(fp).metadata.metadata or {}
        sn = meta.get(b"Test Type")
        if sn:
            print(f"  metadata Test Type: {sn.decode()}")
    except Exception:
        print("No metadata found")

def main() -> None:
    serials = sys.argv[1:] if len(sys.argv) > 1 else DEFAULT_SERIALS
    print(f"Silver root: {SILVER_ROOT}")

    for sn in serials:
        fp = SILVER_ROOT / f"{sn}.parquet"
        try:
            df = read_silver_df(sn)
            print(f"\n=== {sn} ===")
            print(f"  file: {fp}")
            maybe_print_metadata(fp)
            print(df)  # print first 5 rows
        except FileNotFoundError as e:
            print(f"[SKIP] {e}")
        except Exception as e:
            print(f"[ERROR] Failed to read {fp}: {e}")

if __name__ == "__main__":
    main()
    bronze = "bronze_path_here"
    maybe_print_testtype(bronze)
    