import random
from datetime import date, datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq
import pytest


@pytest.fixture(scope="session")
def qapp():
    from PyQt6.QtWidgets import QApplication

    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


@pytest.fixture
def small_table():
    return pa.table(
        {
            "name": ["Alice", "Bob", "Charlie", "Diana", None],
            "age": [30, 25, 35, 28, None],
            "city": ["New York", "Boston", "New York", "Chicago", "Boston"],
            "score": [95.5, 87.3, None, 92.1, 78.0],
        }
    )


@pytest.fixture
def small_parquet(tmp_path):
    table = pa.table(
        {
            "id": pa.array([1, 2, 3, 4, 5], type=pa.int64()),
            "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
            "value": [10.0, 20.0, 30.0, 40.0, 50.0],
        }
    )
    path = str(tmp_path / "small.parquet")
    pq.write_table(table, path, row_group_size=2)
    return path


@pytest.fixture
def small_parquet_dir(tmp_path):
    t1 = pa.table(
        {
            "id": pa.array([1, 2, 3], type=pa.int64()),
            "name": ["Alice", "Bob", "Charlie"],
            "value": [10.0, 20.0, 30.0],
        }
    )
    t2 = pa.table(
        {
            "id": pa.array([4, 5, 6], type=pa.int64()),
            "name": ["Diana", "Eve", "Frank"],
            "value": [40.0, 50.0, 60.0],
        }
    )
    d = tmp_path / "parquet_dir"
    d.mkdir()
    pq.write_table(t1, str(d / "part1.parquet"), row_group_size=2)
    pq.write_table(t2, str(d / "part2.parquet"), row_group_size=2)
    return str(d)


@pytest.fixture
def nested_parquet(tmp_path):
    struct_type = pa.struct(
        [
            ("street", pa.string()),
            ("zip", pa.int32()),
        ]
    )
    table = pa.table(
        {
            "id": [1, 2],
            "address": pa.array(
                [{"street": "123 Main", "zip": 10001}, {"street": "456 Oak", "zip": 20002}],
                type=struct_type,
            ),
            "tags": pa.array([["a", "b"], ["c"]], type=pa.list_(pa.string())),
        }
    )
    path = str(tmp_path / "nested.parquet")
    pq.write_table(table, path)
    return path


@pytest.fixture(scope="session")
def synthetic_healthcare_parquet(tmp_path_factory):
    rng = random.Random(42)
    n = 100

    first_names = ["Alex", "Jordan", "Taylor", "Morgan", "Casey", "Riley", "Quinn", "Avery"]
    last_names = ["Smith", "Johnson", "Lee", "Brown", "Davis", "Miller", "Wilson", "Moore"]
    cities = ["Springfield", "Riverside", "Lakeside", "Hillview", "Parkdale"]
    states = ["CA", "NY", "TX", "FL", "WA"]
    diagnosis_codes = ["E11.9", "I10", "J45.909", "M54.5", "F41.1"]

    name_struct = pa.struct(
        [
            ("Prefix", pa.string()),
            ("First", pa.string()),
            ("Middle", pa.string()),
            ("Last", pa.string()),
            ("Suffix", pa.string()),
        ]
    )
    address_struct = pa.struct(
        [
            ("Line1", pa.string()),
            ("City", pa.string()),
            ("State", pa.string()),
            ("Postal_Code", pa.string()),
            ("Created_Timestamp", pa.timestamp("us", tz="UTC")),
        ]
    )
    diagnosis_struct = pa.struct([("Code", pa.string()), ("Description", pa.string())])

    names = pa.array(
        [
            {
                "Prefix": "",
                "First": rng.choice(first_names),
                "Middle": "",
                "Last": rng.choice(last_names),
                "Suffix": "",
            }
            for _ in range(n)
        ],
        type=name_struct,
    )
    addresses = pa.array(
        [
            [
                {
                    "Line1": f"{rng.randint(1, 9999)} Main St",
                    "City": rng.choice(cities),
                    "State": rng.choice(states),
                    "Postal_Code": f"{rng.randint(10000, 99999)}",
                    "Created_Timestamp": datetime.now(timezone.utc),
                }
            ]
            for _ in range(n)
        ],
        type=pa.list_(address_struct),
    )
    diagnoses = pa.array(
        [
            [
                {"Code": rng.choice(diagnosis_codes), "Description": "Synthetic"}
                for _ in range(rng.randint(0, 3))
            ]
            for _ in range(n)
        ],
        type=pa.list_(diagnosis_struct),
    )

    table = pa.table(
        {
            "Member_Key": [f"MK{rng.randint(100000, 999999)}" for _ in range(n)],
            "Member_ID": [f"M{i:08d}" for i in range(n)],
            "Birth_Date": [date(1970 + rng.randint(0, 50), 1, 1) for _ in range(n)],
            "Gender": [rng.choice(["M", "F", "U"]) for _ in range(n)],
            "Active": [rng.choice([True, False]) for _ in range(n)],
            "Name": names,
            "Addresses": addresses,
            "Diagnoses": diagnoses,
        }
    )

    path = str(tmp_path_factory.mktemp("synth") / "healthcare.parquet")
    pq.write_table(table, path, row_group_size=25)
    return path
