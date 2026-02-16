# load_silver_paths.py
from pathlib import Path
from pyarrow import parquet as pq 
import pyodbc

# --- config ---
SERVER   = "server_name_here"
DATABASE = "database_name_here"
DRIVER   = "driver_name_here"
SILVER_ROOT = r"silver_parquets"
BRONZE_ROOT = r"parquets"


def get_connection():
    return pyodbc.connect(
        f"DRIVER={{{DRIVER}}};SERVER={SERVER};DATABASE={DATABASE};"
        "Trusted_Connection=Yes;Encrypt=Yes;TrustServerCertificate=Yes;"
    )

def ensure_tables(cur):
    # Silver files
    cur.execute("""
    IF OBJECT_ID(N'dbo.SilverFiles', N'U') IS NULL
        CREATE TABLE dbo.SilverFiles(
            file_path     NVARCHAR(400) NOT NULL PRIMARY KEY,
            serial_number NVARCHAR(128) NULL,
            test_stand    NVARCHAR(128) NULL
        );
    """)
    cur.execute("""
    IF COL_LENGTH('dbo.SilverFiles','serial_number') IS NULL
        ALTER TABLE dbo.SilverFiles ADD serial_number NVARCHAR(128) NULL;
    IF COL_LENGTH('dbo.SilverFiles','test_stand') IS NULL
        ALTER TABLE dbo.SilverFiles ADD test_stand NVARCHAR(128) NULL;
    """)

    # Bronze files
    cur.execute("""
    IF OBJECT_ID(N'dbo.BronzeFiles', N'U') IS NULL
        CREATE TABLE dbo.BronzeFiles(
            file_path NVARCHAR(400) NOT NULL PRIMARY KEY,
            test_type NVARCHAR(128) NULL
        );
    """)
    cur.execute("""
    IF COL_LENGTH('dbo.BronzeFiles','test_type') IS NULL
        ALTER TABLE dbo.BronzeFiles ADD test_type NVARCHAR(128) NULL;
    """)

    
def read_silver_metadata(path: Path):
    try:
        pf = pq.ParquetFile(path)
        md = pf.metadata.metadata or {}
        sn = md.get(b"Serial Number")
        ts = md.get(b"Test Stand")
        serial = sn.decode() if sn else None
        test_stand = ts.decode() if ts else None
    except Exception as e:
        print(f"[WARN] Could not read metadata from {path.name}: {e}")
        serial = None
        test_stand = None
    return serial, test_stand


def read_bronze_metadata(path: Path):
    try:
        pf = pq.ParquetFile(path)
        md = pf.metadata.metadata or {}
        tt = md.get(b"Test Type")
        test_type = tt.decode() if tt else None
    except Exception as e:
        print(f"[WARN] Could not read metadata from {path.name}: {e}")
        test_type = None
    return test_type

def upsert_silver(cur, silver_paths):
    check_sql  = "SELECT serial_number, test_stand FROM dbo.SilverFiles WHERE file_path = ?"
    insert_sql = "INSERT INTO dbo.SilverFiles(file_path, serial_number, test_stand) VALUES (?, ?, ?)"
    update_sql = "UPDATE dbo.SilverFiles SET serial_number = ?, test_stand = ? WHERE file_path = ?"

    new_count = update_count = skip_count = 0

    for p in silver_paths:
        p = Path(p)
        fp = str(p.resolve())
        serial, test_stand = read_silver_metadata(p)

        row = cur.execute(check_sql, fp).fetchone()
        if row is None:
            cur.execute(insert_sql, fp, serial, test_stand)
            new_count += 1
        else:
            current_serial, current_stand = row
            if current_serial == serial and current_stand == test_stand:
                skip_count += 1
            else:
                cur.execute(update_sql, serial, test_stand, fp)
                update_count += 1

    return new_count, update_count, skip_count


def upsert_bronze(cur, bronze_paths):
    check_sql  = "SELECT test_type FROM dbo.BronzeFiles WHERE file_path = ?"
    insert_sql = "INSERT INTO dbo.BronzeFiles(file_path, test_type) VALUES (?, ?)"
    update_sql = "UPDATE dbo.BronzeFiles SET test_type = ? WHERE file_path = ?"

    new_count = update_count = skip_count = 0

    for p in bronze_paths:
        p = Path(p)
        fp = str(p.resolve())
        test_type = read_bronze_metadata(p)

        row = cur.execute(check_sql, fp).fetchone()
        if row is None:
            cur.execute(insert_sql, fp, test_type)
            new_count += 1
        else:
            (current_type,) = row
            if current_type == test_type:
                skip_count += 1
            else:
                cur.execute(update_sql, test_type, fp)
                update_count += 1

    return new_count, update_count, skip_count


# ---------- original behavior preserved ----------

def main(silver_root: str = SILVER_ROOT, bronze_root: str = BRONZE_ROOT):

    cn = get_connection()
    cur = cn.cursor()

    ensure_tables(cur)
    cn.commit()

    silver_paths = sorted(Path(silver_root).rglob("*.parquet"))
    bronze_paths = sorted(Path(bronze_root).rglob("*.parquet"))

    new_s, upd_s, skip_s = upsert_silver(cur, silver_paths)
    new_b, upd_b, skip_b = upsert_bronze(cur, bronze_paths)

    cn.commit()
    cur.close()
    cn.close()

    print(f"[OK] inserted {new_s}, updated {upd_s}, skipped {skip_s}")
    print(f"[OK] bronze: inserted {new_b}, updated {upd_b}, skipped {skip_b}")


if __name__ == "__main__":
    main()
