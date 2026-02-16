import os
from pathlib import Path
from ingest import build_silver, read_serial_number, write_bronze_parquets
from ingest_SQL import get_connection, ensure_tables, upsert_silver, upsert_bronze


def ingest_stage():
    in_dir = "input_directory_here"
    silver_list = []
    bronze_all = []
    for test_stand_dir in os.listdir(in_dir):
        test_stand = str(test_stand_dir)
        stand_path = os.path.join(in_dir, test_stand)
        if not os.path.isdir(stand_path):
            continue
        for serial_dir in os.listdir(stand_path):
            serial_path = os.path.join(stand_path, serial_dir)
            if not os.path.isdir(serial_path):
                continue

            serial_path = Path(serial_path)
            bronze_list = write_bronze_parquets(serial_path, test_stand)
            silver = build_silver(serial_dir, bronze_list, test_stand)
            silver_list.append(silver)
            bronze_all.extend(bronze_list)

            

    cn = get_connection()
    cur = cn.cursor()

    ensure_tables(cur)
    new_s, upd_s, skip_s = upsert_silver(cur, silver_list)
    new_b, upd_b, skip_b = upsert_bronze(cur, bronze_all)

    cn.commit()
    cur.close()
    cn.close()

    #for object in os.listdir(in_dir):
    #    shutil.rmtree(os.path.join(in_dir, object), ignore_errors=True)

def main():
    ingest_stage()

if __name__ == "__main__":
    main()