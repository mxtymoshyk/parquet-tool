"""Generate sample Parquet files for screenshots and schema-diff demos.

Outputs two files in this directory:
- orders_v1.parquet: original schema (8 columns, nested address struct, tags list)
- orders_v2.parquet: evolved schema (added shipping_status, dropped tags,
  promoted total_amount float32 -> float64, renamed customer_email -> email,
  added discount struct)

Run:
    python examples/generate_samples.py
"""

from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

random.seed(42)

OUT_DIR = Path(__file__).resolve().parent
N_ROWS = 25_000

CITIES = ["Kyiv", "Lviv", "Odesa", "Warsaw", "Berlin", "Vienna", "Prague", "Krakow"]
COUNTRIES = ["UA", "PL", "DE", "AT", "CZ"]
STATUSES_V1 = ["pending", "paid", "shipped", "cancelled", "refunded"]
SHIP_STATUSES = ["awaiting", "in_transit", "delivered", "returned"]
TAGS_POOL = ["express", "gift", "fragile", "bulk", "subscription", "promo", "vip"]


def _rand_dt(start: datetime, span_days: int) -> datetime:
    return start + timedelta(
        days=random.randint(0, span_days),
        seconds=random.randint(0, 86_400),
    )


def _addresses(n: int) -> list[dict]:
    return [
        {
            "street": f"{random.randint(1, 999)} {random.choice(['Main', 'Oak', 'Pine', 'Maple'])} St",
            "city": random.choice(CITIES),
            "country": random.choice(COUNTRIES),
            "postcode": f"{random.randint(10000, 99999)}",
        }
        for _ in range(n)
    ]


def build_v1() -> pa.Table:
    base = datetime(2025, 1, 1, tzinfo=timezone.utc)
    order_ids = [f"ORD-{i:07d}" for i in range(N_ROWS)]
    customer_ids = [random.randint(1000, 9999) for _ in range(N_ROWS)]
    emails = [f"user{cid}@example.com" for cid in customer_ids]
    created = [_rand_dt(base, 180) for _ in range(N_ROWS)]
    totals = [round(random.uniform(5.0, 1500.0), 2) for _ in range(N_ROWS)]
    qty = [random.randint(1, 12) for _ in range(N_ROWS)]
    statuses = [random.choice(STATUSES_V1) for _ in range(N_ROWS)]
    tags = [random.sample(TAGS_POOL, k=random.randint(0, 3)) for _ in range(N_ROWS)]
    addresses = _addresses(N_ROWS)

    schema = pa.schema(
        [
            ("order_id", pa.string()),
            ("customer_id", pa.int32()),
            ("customer_email", pa.string()),
            ("created_at", pa.timestamp("us", tz="UTC")),
            ("total_amount", pa.float32()),
            ("quantity", pa.int16()),
            ("status", pa.string()),
            ("tags", pa.list_(pa.string())),
            (
                "shipping_address",
                pa.struct(
                    [
                        ("street", pa.string()),
                        ("city", pa.string()),
                        ("country", pa.string()),
                        ("postcode", pa.string()),
                    ]
                ),
            ),
        ]
    )

    return pa.table(
        {
            "order_id": order_ids,
            "customer_id": customer_ids,
            "customer_email": emails,
            "created_at": created,
            "total_amount": totals,
            "quantity": qty,
            "status": statuses,
            "tags": tags,
            "shipping_address": addresses,
        },
        schema=schema,
    )


def build_v2() -> pa.Table:
    """Schema-evolved variant for diff demos."""
    base = datetime(2025, 7, 1, tzinfo=timezone.utc)
    order_ids = [f"ORD-{i:07d}" for i in range(N_ROWS)]
    customer_ids = [random.randint(1000, 9999) for _ in range(N_ROWS)]
    emails = [f"user{cid}@example.com" for cid in customer_ids]
    created = [_rand_dt(base, 120) for _ in range(N_ROWS)]
    totals = [round(random.uniform(5.0, 1500.0), 2) for _ in range(N_ROWS)]
    qty = [random.randint(1, 12) for _ in range(N_ROWS)]
    statuses = [random.choice(STATUSES_V1) for _ in range(N_ROWS)]
    ship_statuses = [random.choice(SHIP_STATUSES) for _ in range(N_ROWS)]
    addresses = _addresses(N_ROWS)
    discounts = [
        {
            "code": random.choice(["SAVE10", "WELCOME", "VIP25", None]),
            "amount": round(random.uniform(0.0, 50.0), 2),
        }
        for _ in range(N_ROWS)
    ]

    schema = pa.schema(
        [
            ("order_id", pa.string()),
            ("customer_id", pa.int64()),
            ("email", pa.string()),
            ("created_at", pa.timestamp("us", tz="UTC")),
            ("total_amount", pa.float64()),
            ("quantity", pa.int16()),
            ("status", pa.string()),
            ("shipping_status", pa.string()),
            (
                "shipping_address",
                pa.struct(
                    [
                        ("street", pa.string()),
                        ("city", pa.string()),
                        ("country", pa.string()),
                        ("postcode", pa.string()),
                    ]
                ),
            ),
            (
                "discount",
                pa.struct(
                    [
                        ("code", pa.string()),
                        ("amount", pa.float64()),
                    ]
                ),
            ),
        ]
    )

    return pa.table(
        {
            "order_id": order_ids,
            "customer_id": customer_ids,
            "email": emails,
            "created_at": created,
            "total_amount": totals,
            "quantity": qty,
            "status": statuses,
            "shipping_status": ship_statuses,
            "shipping_address": addresses,
            "discount": discounts,
        },
        schema=schema,
    )


def write(table: pa.Table, name: str) -> Path:
    path = OUT_DIR / name
    pq.write_table(
        table,
        path,
        compression="snappy",
        row_group_size=5_000,
        use_dictionary=True,
    )
    return path


def main() -> None:
    v1 = build_v1()
    v2 = build_v2()
    p1 = write(v1, "orders_v1.parquet")
    p2 = write(v2, "orders_v2.parquet")
    for p in (p1, p2):
        meta = pq.read_metadata(p)
        print(f"{p.name}: {meta.num_rows} rows, {meta.num_row_groups} row groups, {p.stat().st_size:,} bytes")


if __name__ == "__main__":
    main()
