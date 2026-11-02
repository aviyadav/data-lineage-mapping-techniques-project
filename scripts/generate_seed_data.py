#!/usr/bin/env python3
"""
Generate realistic seed data for the data lineage demo project.

Default output:
  - seed_data/raw/raw_customers.csv  -> 100 rows
  - seed_data/raw/raw_orders.csv     -> 15000 rows

The generated orders intentionally include a small number of:
  - missing customer_id values
  - cancelled orders
  - negative amounts
  - duplicate order_id values

These anomalies help demonstrate:
  - transformation filtering
  - data quality failures
  - lineage-aware contamination propagation
  - impact scoring
"""

import argparse
import csv
import random
import sys
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent

# Supports both:
#   project/scripts/generate_seed_data.py
#   project/generate_seed_data.py
BASE_DIR = SCRIPT_DIR.parent if SCRIPT_DIR.name == "scripts" else SCRIPT_DIR

DEFAULT_OUT_DIR = BASE_DIR / "seed_data" / "raw"


FIRST_NAMES = [
    "Ada",
    "Grace",
    "Alan",
    "Katherine",
    "Edsger",
    "Barbara",
    "Donald",
    "Margaret",
    "John",
    "Ann",
    "Tim",
    "Radia",
    "Vint",
    "Frances",
    "Jean",
    "Ken",
    "Linus",
    "Dennis",
    "Guido",
    "Bjarne",
    "James",
    "Mary",
    "Robert",
    "Patricia",
    "Michael",
    "Jennifer",
    "William",
    "Linda",
    "David",
    "Elizabeth",
    "Richard",
    "Susan",
    "Joseph",
    "Jessica",
    "Thomas",
    "Sarah",
    "Charles",
    "Karen",
    "Christopher",
    "Nancy",
]

LAST_NAMES = [
    "Lovelace",
    "Hopper",
    "Turing",
    "Johnson",
    "Dijkstra",
    "Liskov",
    "Knuth",
    "Hamilton",
    "McCarthy",
    "Bayer",
    "Berners-Lee",
    "Perlman",
    "Cerf",
    "Allen",
    "Sammet",
    "Thompson",
    "Torvalds",
    "Ritchie",
    "Rossum",
    "Stroustrup",
    "Smith",
    "Jones",
    "Brown",
    "Wilson",
    "Moore",
    "Taylor",
    "Anderson",
    "Thomas",
    "Jackson",
    "White",
    "Harris",
    "Martin",
    "Garcia",
    "Martinez",
    "Robinson",
    "Clark",
    "Rodriguez",
    "Lewis",
    "Lee",
    "Walker",
]

REGIONS = ["North", "South", "East", "West"]

PRODUCT_IDS = [f"P{i:03d}" for i in range(1, 121)]

ORDER_STATUSES = ["PLACED", "SHIPPED", "DELIVERED", "CANCELLED"]

CUSTOMER_HEADER = [
    "customer_id",
    "name",
    "email",
    "region",
    "signup_date",
]

ORDER_HEADER = [
    "order_id",
    "customer_id",
    "product_id",
    "amount",
    "order_timestamp",
    "status",
]


def _random_datetime(rng: random.Random, start: datetime, end: datetime) -> datetime:
    """Return a random datetime between start and end."""
    total_seconds = int((end - start).total_seconds())
    offset_seconds = rng.randint(0, total_seconds)
    return start + timedelta(seconds=offset_seconds)


def generate_customers(count: int, rng: random.Random) -> list:
    """
    Generate raw customer rows.

    Columns:
      customer_id, name, email, region, signup_date
    """
    rows = []

    signup_start = datetime(2023, 1, 1)

    for customer_id in range(1, count + 1):
        first_name = rng.choice(FIRST_NAMES)
        last_name = rng.choice(LAST_NAMES)

        name = f"{first_name} {last_name}"

        # Append customer_id to reduce accidental duplicate emails.
        email = f"{first_name.lower()}.{last_name.lower()}{customer_id}@example.com"

        region = rng.choice(REGIONS)

        signup_date = (
            signup_start + timedelta(days=rng.randint(0, 1095))
        ).date().isoformat()

        rows.append(
            [
                customer_id,
                name,
                email,
                region,
                signup_date,
            ]
        )

    return rows


def generate_orders(
    customer_count: int,
    order_count: int,
    rng: random.Random,
) -> list:
    """
    Generate raw order rows.

    Columns:
      order_id, customer_id, product_id, amount, order_timestamp, status

    Intentional anomalies:
      - ~2% missing customer_id
      - ~0.5% negative amount
      - additional cancelled orders beyond normal status distribution
      - approximately 100 duplicate order_id rows for larger datasets
    """

    start_time = datetime(2026, 1, 1, 0, 0, 0)
    end_time = datetime(2026, 10, 31, 23, 59, 59)

    # For 15,000 orders this produces 100 duplicate order rows.
    intended_duplicates = max(1, min(100, order_count // 150))

    unique_count = order_count - intended_duplicates

    base_order_id = 10000
    next_order_id = base_order_id

    rows = []
    clean_order_ids = []

    for _ in range(unique_count):
        order_id = next_order_id
        next_order_id += 1

        customer_id = rng.randint(1, customer_count)
        product_id = rng.choice(PRODUCT_IDS)
        amount = round(rng.uniform(5.0, 1200.0), 2)
        status = rng.choices(
            ORDER_STATUSES,
            weights=[20, 25, 45, 10],
        )[0]

        order_timestamp = _random_datetime(rng, start_time, end_time)

        # Inject controlled anomalies.
        anomaly = rng.random()

        if anomaly < 0.02:
            # Missing customer_id.
            customer_id = ""
        elif anomaly < 0.025:
            # Negative amount.
            amount = -round(rng.uniform(1.0, 250.0), 2)
        elif anomaly < 0.03:
            # Force cancelled.
            status = "CANCELLED"

        row = [
            order_id,
            customer_id,
            product_id,
            f"{amount:.2f}",
            order_timestamp.strftime("%Y-%m-%dT%H:%M:%S"),
            status,
        ]

        rows.append(row)

        # Track rows that would survive the staging filter:
        #   customer_id IS NOT NULL
        #   status <> 'CANCELLED'
        #   amount >= 0
        if customer_id != "" and status != "CANCELLED" and amount >= 0:
            clean_order_ids.append(order_id)

    # If there are not enough clean IDs to duplicate, add extra clean rows.
    duplicate_count = min(intended_duplicates, len(clean_order_ids))
    extra_needed = intended_duplicates - duplicate_count

    for _ in range(extra_needed):
        order_id = next_order_id
        next_order_id += 1

        customer_id = rng.randint(1, customer_count)
        product_id = rng.choice(PRODUCT_IDS)
        amount = round(rng.uniform(5.0, 1200.0), 2)
        status = rng.choices(
            ["PLACED", "SHIPPED", "DELIVERED"],
            weights=[30, 30, 40],
        )[0]

        order_timestamp = _random_datetime(rng, start_time, end_time)

        row = [
            order_id,
            customer_id,
            product_id,
            f"{amount:.2f}",
            order_timestamp.strftime("%Y-%m-%dT%H:%M:%S"),
            status,
        ]

        rows.append(row)
        clean_order_ids.append(order_id)

    # Inject duplicate order_id rows.
    # These duplicates are intentionally valid rows so they survive staging
    # and trigger the uniqueness quality check downstream.
    for original_order_id in rng.sample(clean_order_ids, duplicate_count):
        customer_id = rng.randint(1, customer_count)
        product_id = rng.choice(PRODUCT_IDS)
        amount = round(rng.uniform(5.0, 1200.0), 2)
        status = rng.choices(
            ["PLACED", "SHIPPED", "DELIVERED"],
            weights=[30, 30, 40],
        )[0]

        order_timestamp = _random_datetime(rng, start_time, end_time)

        rows.append(
            [
                original_order_id,
                customer_id,
                product_id,
                f"{amount:.2f}",
                order_timestamp.strftime("%Y-%m-%dT%H:%M:%S"),
                status,
            ]
        )

    rng.shuffle(rows)

    if len(rows) != order_count:
        raise RuntimeError(
            f"Generated {len(rows)} orders, expected {order_count}."
        )

    return rows


def write_csv(path: Path, header: list, rows: list) -> None:
    """Write rows to a CSV file."""
    path.parent.mkdir(parents=True, exist_ok=True)

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)


def print_summary(customers: list, orders: list) -> None:
    """Print basic statistics about generated data."""
    print("\nSeed data summary")
    print("-" * 60)
    print(f"Customers generated: {len(customers)}")
    print(f"Orders generated:    {len(orders)}")

    missing_customer = sum(1 for row in orders if row[1] == "")
    cancelled = sum(1 for row in orders if row[5] == "CANCELLED")
    negative_amount = sum(1 for row in orders if float(row[3]) < 0)

    order_id_counts = Counter(row[0] for row in orders)
    duplicate_order_ids = sum(1 for count in order_id_counts.values() if count > 1)
    rows_with_duplicate_order_ids = sum(
        count for count in order_id_counts.values() if count > 1
    )

    print(f"Orders with missing customer_id: {missing_customer}")
    print(f"Cancelled orders:                {cancelled}")
    print(f"Negative amount orders:          {negative_amount}")
    print(f"Duplicate order_id values:       {duplicate_order_ids}")
    print(f"Rows involved in duplicates:     {rows_with_duplicate_order_ids}")
    print("-" * 60)


def maybe_upload_to_rustfs(customer_path: Path, order_path: Path) -> None:
    """
    Optional helper to upload generated files directly to RustFS.

    This assumes the project modules are importable:
      - config.py
      - storage.py
    """
    sys.path.insert(0, str(BASE_DIR))

    from config import RUSTFS_BUCKET_RAW
    from storage import ensure_bucket, upload_file

    ensure_bucket(RUSTFS_BUCKET_RAW)

    upload_file(customer_path, RUSTFS_BUCKET_RAW, key=customer_path.name)
    upload_file(order_path, RUSTFS_BUCKET_RAW, key=order_path.name)

    print(f"Uploaded {customer_path.name} to rustfs://{RUSTFS_BUCKET_RAW}/")
    print(f"Uploaded {order_path.name} to rustfs://{RUSTFS_BUCKET_RAW}/")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate seed CSV files for the data lineage demo."
    )

    parser.add_argument(
        "--customers",
        type=int,
        default=100,
        help="Number of customer rows to generate. Default: 100",
    )

    parser.add_argument(
        "--orders",
        type=int,
        default=15000,
        help="Number of order rows to generate. Default: 15000",
    )

    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducibility. Default: 42",
    )

    parser.add_argument(
        "--out-dir",
        type=str,
        default=str(DEFAULT_OUT_DIR),
        help="Output directory for CSV files.",
    )

    parser.add_argument(
        "--upload",
        action="store_true",
        help="Optionally upload generated files to RustFS using project config.",
    )

    args = parser.parse_args()

    rng = random.Random(args.seed)

    customers = generate_customers(args.customers, rng)
    orders = generate_orders(args.customers, args.orders, rng)

    out_dir = Path(args.out_dir)

    customer_path = out_dir / "raw_customers.csv"
    order_path = out_dir / "raw_orders.csv"

    write_csv(customer_path, CUSTOMER_HEADER, customers)
    write_csv(order_path, ORDER_HEADER, orders)

    print(f"Wrote customers to: {customer_path}")
    print(f"Wrote orders to:    {order_path}")

    print_summary(customers, orders)

    if args.upload:
        maybe_upload_to_rustfs(customer_path, order_path)


if __name__ == "__main__":
    main()