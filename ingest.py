import sys
import os
import pyarrow as pa
import pyarrow.csv as csv
from pyarrow import parquet as pq
from pathlib import Path
import polars as pl
import ingest_SQL

test_stand_list = ["test_stand_list_here"]

BASE_DIR = Path(__file__).resolve().parent
BRONZE_ROOT = BASE_DIR / "parquets"
SILVER_ROOT = BASE_DIR / "silver_parquets"

def build_silver(serial_dir: str, bronze_list: list[Path], test_stand: str) -> Path:
    files = []
    
    for path in bronze_list:
        files.append(str(path))

    schema={"bronze_path": pl.Utf8,
            "serial_number": pl.Utf8
            }
    columns = {"bronze_path": files,
               "serial_number": serial_dir
               }
    
    df = pl.DataFrame(columns, schema)
    out_path = SILVER_ROOT / test_stand / f"{serial_dir}.parquet"
    out_path.parent.mkdir(parents=True, exist_ok=True) 
    table = df.to_arrow()
    meta = dict(table.schema.metadata or {})
    meta[b"Serial Number"] = serial_dir.encode()
    meta[b"Test Stand"] = test_stand.encode()
    table = table.replace_schema_metadata(meta)
    pq.write_table(table, out_path, compression="zstd")
    return out_path

def read_serial_number(path: str | Path) -> str | None:
    kv = pq.ParquetFile(path).metadata.metadata or {}
    v = kv.get(b"Serial Number")
    return v.decode() if v is not None else None

def write_bronze_parquets(in_dir: Path, test_stand: str, bronze_root: Path = BRONZE_ROOT) -> list[Path]:
    bronze_root.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []

    for csv_path in in_dir.glob("*.csv"):
        if csv_path.suffix.lower() == ".csv":
            try:
                df = pl.read_csv(csv_path, ignore_errors=True, truncate_ragged_lines=True)
                
                test_type = "None"
                if "example_test_here" in str(csv_path):
                    test_type = "example_test_here"
                elif "example_test_here" in str(csv_path):
                    test_type = "example_test_here"
                table = df.to_arrow()
                schema = table.schema
                existing_meta = schema.metadata or {}
                new_meta = dict(existing_meta)
                new_meta[b"example_test_here"] = test_type.encode("utf-8")
                table = table.replace_schema_metadata(new_meta)
            except Exception as e:
                print(f"[WARN] Failed to read CSV: {csv_path} -> {e}")
                continue


            parquet_path = bronze_root / test_stand / f"{csv_path.stem}.parquet"
            parquet_path.parent.mkdir(parents=True, exist_ok=True) 
            try:
                pq.write_table(table, parquet_path, compression="zstd")
                written.append(parquet_path)
            except Exception as e:
                print(f"[WARN] Failed to write Parquet: {parquet_path} -> {e}")
                continue
    return written

def main() -> None:
    for test_stand in test_stand_list:
        in_dir = Path(rf"\\{test_stand}path_here")
        for dir in os.listdir(in_dir):
            silver_path = Path("silver_parquets") / Path(f"{dir}.parquet")
            metadata = pq.ParquetFile(silver_path).metadata if silver_path.exists() else None
            stand = metadata.metadata.get(b"Test Stand").decode() if metadata and metadata.metadata else "UNKNOWN"
            if os.path.isdir(Path(in_dir / dir)):
                if silver_path.exists() and stand == test_stand:
                    print(f"[SKIP] Already loaded files for {dir}: {silver_path}")
                else:
                    build_silver(dir, write_bronze_parquets(Path(in_dir / dir), test_stand), test_stand)
        print(f"Completed parquet files for: {test_stand}")
    ingest_SQL.main()

if __name__ == "__main__":
    main()