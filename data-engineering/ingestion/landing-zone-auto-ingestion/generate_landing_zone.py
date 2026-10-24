"""Write a small sample landing zone to a local folder, ready to upload to Object Storage.

Each top-level folder is one table. Run it in three steps to see the workflows behave incrementally:

    # 1. the initial landing zone
    python generate_landing_zone.py initial landing

    # 2. later: a new day of sales arrives in an existing table folder
    python generate_landing_zone.py new-day landing --day 2026-10-21

    # 3. later still: a brand-new table folder appears
    python generate_landing_zone.py new-folder landing

The initial landing zone:

    landing/
      sales/sale_date=2026-10-18/part-00000.parquet   partitioned by sale_date (3 days, 2 files each)
      customers/part-00000.parquet
      products/part-00000.parquet
      Web-Events/part-00000.parquet                   the name needs sanitizing -> table web_events
      _staging/part-00000.parquet                     hidden folder -> skipped
      docs/notes.txt                                  no Parquet files -> skipped

Each step only adds files, so uploading the same local folder again uploads just the new ones.
"""
import argparse
import datetime
import os
import random

import pyarrow as pa
import pyarrow.parquet as pq

COUNTRIES = ["BR", "US", "MX", "DE"]
FIRST_SALES_DAYS = ["2026-10-18", "2026-10-19", "2026-10-20"]
FILES_PER_DAY, ROWS_PER_FILE = 2, 100


def write_parquet(path, columns):
    """Write one Parquet file, creating its folder if needed."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    pq.write_table(pa.table(columns), path)
    print("wrote", path)


def utc_timestamps(values):
    # Write timestamps WITH a timezone (UTC). Without one, Spark 3.5 reads the column as
    # TIMESTAMP_NTZ instead of TIMESTAMP.
    return pa.array(values, pa.timestamp("us", tz="UTC"))


def first_order_id(day):
    """Order ids are unique per day and readable: 2026-10-18 -> 20261018000, 20261018001, ..."""
    return int(day.replace("-", "")) * 1000


def write_sales_day(root, day):
    """sales/sale_date=<day>/part-NNNNN.parquet - the folder name is the partition value."""
    random.seed(day)
    start = datetime.datetime.fromisoformat(day).replace(tzinfo=datetime.timezone.utc)
    for file_number in range(FILES_PER_DAY):
        start_id = first_order_id(day) + file_number * ROWS_PER_FILE
        order_ids = list(range(start_id, start_id + ROWS_PER_FILE))
        write_parquet(os.path.join(root, "sales", f"sale_date={day}", f"part-{file_number:05d}.parquet"), {
            "order_id":    pa.array(order_ids, pa.int64()),
            "customer_id": pa.array([random.randint(1, 50) for _ in order_ids], pa.int32()),
            "product_id":  pa.array([random.randint(1, 20) for _ in order_ids], pa.int32()),
            "quantity":    pa.array([random.randint(1, 5) for _ in order_ids], pa.int32()),
            "amount":      pa.array([round(random.uniform(5, 500), 2) for _ in order_ids], pa.float64()),
            "order_ts":    utc_timestamps([start + datetime.timedelta(seconds=random.randint(0, 86399))
                                           for _ in order_ids]),
        })


def write_initial(root):
    for day in FIRST_SALES_DAYS:
        write_sales_day(root, day)          # seeds its own random numbers, one seed per day

    random.seed(42)
    write_parquet(os.path.join(root, "customers", "part-00000.parquet"), {
        "customer_id": pa.array(range(1, 51), pa.int32()),
        "name":        [f"Customer {n}" for n in range(1, 51)],
        "country":     [random.choice(COUNTRIES) for _ in range(50)],
        "signup_date": pa.array([datetime.date(2026, 1, 1) + datetime.timedelta(days=n) for n in range(50)]),
    })

    write_parquet(os.path.join(root, "products", "part-00000.parquet"), {
        "product_id": pa.array(range(1, 21), pa.int32()),
        "name":       [f"Product {n}" for n in range(1, 21)],
        "category":   [random.choice(["books", "games", "music"]) for _ in range(20)],
        "price":      pa.array([round(random.uniform(5, 100), 2) for _ in range(20)], pa.float64()),
    })

    # A folder whose name is not a valid table name: the setup workflow maps it to web_events.
    start = datetime.datetime(2026, 10, 18, tzinfo=datetime.timezone.utc)
    write_parquet(os.path.join(root, "Web-Events", "part-00000.parquet"), {
        "event_id":    pa.array(range(1, 301), pa.int64()),
        "customer_id": pa.array([random.randint(1, 50) for _ in range(300)], pa.int32()),
        "page":        [random.choice(["/home", "/cart", "/checkout"]) for _ in range(300)],
        "event_ts":    utc_timestamps([start + datetime.timedelta(minutes=n) for n in range(300)]),
    })

    # Two folders the setup workflow must skip.
    write_parquet(os.path.join(root, "_staging", "part-00000.parquet"), {"id": pa.array([1, 2, 3], pa.int64())})
    os.makedirs(os.path.join(root, "docs"), exist_ok=True)
    with open(os.path.join(root, "docs", "notes.txt"), "w") as notes:
        notes.write("Not data: this folder holds no Parquet files, so it is not a table.\n")
    print("wrote", os.path.join(root, "docs", "notes.txt"))


def write_new_folder(root):
    """returns/ - a table folder that did not exist when the setup workflow first ran."""
    random.seed(7)
    start = datetime.datetime(2026, 10, 19, tzinfo=datetime.timezone.utc)
    write_parquet(os.path.join(root, "returns", "part-00000.parquet"), {
        "return_id":   pa.array(range(1, 31), pa.int64()),
        "order_id":    pa.array([first_order_id(FIRST_SALES_DAYS[0]) + n for n in range(30)], pa.int64()),
        "reason":      [random.choice(["damaged", "late", "wrong item"]) for _ in range(30)],
        "returned_at": utc_timestamps([start + datetime.timedelta(hours=n) for n in range(30)]),
    })


parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
parser.add_argument("step", choices=["initial", "new-day", "new-folder"])
parser.add_argument("root", help="local folder to write into, e.g. landing")
parser.add_argument("--day", default="2026-10-21", help="the sales day to add, for new-day (YYYY-MM-DD)")
args = parser.parse_args()

if args.step == "initial":
    write_initial(args.root)
elif args.step == "new-day":
    write_sales_day(args.root, args.day)
else:
    write_new_folder(args.root)
