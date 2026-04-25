import pyarrow as pa
import pytest

from parquet_tool.gui_utils import FilterSpec
from parquet_tool.parquet_backend import (
    ParquetDirectory,
    ParquetFile,
    _compute_value_distribution,
    build_column_mask,
    build_composite_mask,
    build_search_mask,
    filtered_scan,
)

# -- build_search_mask --


class TestBuildSearchMask:
    def test_basic_search(self, small_table):
        mask = build_search_mask(small_table, "alice")
        assert mask is not None
        result = small_table.filter(mask)
        assert result.num_rows == 1
        assert result.column("name")[0].as_py() == "Alice"

    def test_search_across_columns(self, small_table):
        mask = build_search_mask(small_table, "New York")
        result = small_table.filter(mask)
        assert result.num_rows == 2

    def test_case_insensitive(self, small_table):
        mask = build_search_mask(small_table, "ALICE")
        result = small_table.filter(mask)
        assert result.num_rows == 1

    def test_no_match(self, small_table):
        mask = build_search_mask(small_table, "zzzzz")
        result = small_table.filter(mask)
        assert result.num_rows == 0

    def test_specific_columns(self, small_table):
        mask = build_search_mask(small_table, "30", columns=["age"])
        result = small_table.filter(mask)
        assert result.num_rows == 1

    def test_nonexistent_column_ignored(self, small_table):
        mask = build_search_mask(small_table, "Alice", columns=["no_such_col"])
        assert mask is None

    def test_empty_table(self):
        table = pa.table({"col": pa.array([], type=pa.string())})
        mask = build_search_mask(table, "test")
        result = table.filter(mask)
        assert result.num_rows == 0


# -- build_column_mask --


class TestBuildColumnMask:
    def test_contains_mode(self, small_table):
        mask = build_column_mask(small_table, "name", "ali", mode="contains")
        result = small_table.filter(mask)
        assert result.num_rows == 1

    def test_exact_mode(self, small_table):
        mask = build_column_mask(small_table, "name", "Alice", mode="exact")
        result = small_table.filter(mask)
        assert result.num_rows == 1

    def test_exact_mode_no_partial(self, small_table):
        mask = build_column_mask(small_table, "name", "Ali", mode="exact")
        result = small_table.filter(mask)
        assert result.num_rows == 0

    def test_regex_mode(self, small_table):
        mask = build_column_mask(small_table, "name", "^[AB]", mode="regex")
        result = small_table.filter(mask)
        assert result.num_rows == 2  # Alice, Bob

    def test_greater_than(self, small_table):
        mask = build_column_mask(small_table, "age", "29", mode=">")
        result = small_table.filter(mask)
        assert result.num_rows == 2  # 30, 35

    def test_greater_equal(self, small_table):
        mask = build_column_mask(small_table, "age", "30", mode=">=")
        result = small_table.filter(mask)
        assert result.num_rows == 2  # 30, 35

    def test_less_than(self, small_table):
        mask = build_column_mask(small_table, "age", "28", mode="<")
        result = small_table.filter(mask)
        assert result.num_rows == 1  # 25

    def test_less_equal(self, small_table):
        mask = build_column_mask(small_table, "age", "28", mode="<=")
        result = small_table.filter(mask)
        assert result.num_rows == 2  # 25, 28

    def test_between(self, small_table):
        mask = build_column_mask(small_table, "age", "26", mode="between", value2="31")
        result = small_table.filter(mask)
        assert result.num_rows == 2  # 28, 30

    def test_invalid_numeric(self, small_table):
        mask = build_column_mask(small_table, "name", "not_a_number", mode=">")
        assert mask is None

    def test_nonexistent_column(self, small_table):
        mask = build_column_mask(small_table, "nope", "val")
        assert mask is None

    def test_default_mode_is_contains(self, small_table):
        mask = build_column_mask(small_table, "city", "york")
        result = small_table.filter(mask)
        assert result.num_rows == 2


# -- build_composite_mask --


class TestBuildCompositeMask:
    def test_and_join(self, small_table):
        specs = [
            FilterSpec("city", "contains", "New York"),
            FilterSpec("age", ">", "31"),
        ]
        mask = build_composite_mask(small_table, specs, "AND")
        result = small_table.filter(mask)
        assert result.num_rows == 1  # Charlie (35, New York)

    def test_or_join(self, small_table):
        specs = [
            FilterSpec("name", "exact", "Alice"),
            FilterSpec("name", "exact", "Bob"),
        ]
        mask = build_composite_mask(small_table, specs, "OR")
        result = small_table.filter(mask)
        assert result.num_rows == 2

    def test_empty_specs(self, small_table):
        mask = build_composite_mask(small_table, [], "AND")
        assert mask is None

    def test_single_spec(self, small_table):
        specs = [FilterSpec("name", "contains", "Alice")]
        mask = build_composite_mask(small_table, specs, "AND")
        result = small_table.filter(mask)
        assert result.num_rows == 1


# -- filtered_scan --


class TestFilteredScan:
    def test_basic_scan(self, small_parquet):
        pf = ParquetFile(small_parquet)

        def mask_fn(table):
            return build_column_mask(table, "name", "Alice")

        table, total = filtered_scan(pf.read_row_group, pf.num_row_groups, mask_fn, 0, 100)
        assert total == 1
        assert table is not None

    def test_progress_callback(self, small_parquet):
        pf = ParquetFile(small_parquet)
        progress_calls = []

        def mask_fn(table):
            # match-all mask
            return build_column_mask(table, "name", "", mode="regex")

        def progress_cb(cur, total):
            progress_calls.append((cur, total))

        filtered_scan(
            pf.read_row_group,
            pf.num_row_groups,
            mask_fn,
            0,
            100,
            progress_cb=progress_cb,
        )
        assert len(progress_calls) == pf.num_row_groups

    def test_cancellation(self, small_parquet):
        pf = ParquetFile(small_parquet)

        def mask_fn(table):
            return build_column_mask(table, "name", "", mode="regex")

        table, total = filtered_scan(
            pf.read_row_group,
            pf.num_row_groups,
            mask_fn,
            0,
            100,
            cancelled_fn=lambda: True,
        )
        assert table is None
        assert total == 0

    def test_offset_and_limit(self, small_parquet):
        pf = ParquetFile(small_parquet)

        def mask_fn(table):
            return build_column_mask(table, "name", "", mode="regex")

        table, total = filtered_scan(
            pf.read_row_group,
            pf.num_row_groups,
            mask_fn,
            1,
            2,
        )
        assert table.num_rows == 2
        assert total == 5

    def test_early_exit_optimization(self, small_parquet):
        """After page is full, remaining row groups should be counted cheaply."""
        pf = ParquetFile(small_parquet)

        def mask_fn(table):
            return build_column_mask(table, "name", "", mode="regex")

        # limit=2 means page full after first row group (size=2)
        table, total = filtered_scan(
            pf.read_row_group,
            pf.num_row_groups,
            mask_fn,
            0,
            2,
        )
        assert table.num_rows == 2
        assert total == 5  # all 5 rows counted even though page has only 2


# -- _compute_value_distribution --


class TestComputeValueDistribution:
    def test_basic(self):
        col = pa.chunked_array([pa.array(["a", "b", "a", "c", "a"])])
        result = _compute_value_distribution(col, top_n=2)
        assert len(result) == 2
        assert result[0]["value"] == "a"
        assert result[0]["count"] == 3
        assert result[0]["percentage"] == pytest.approx(60.0)

    def test_top_n_limits(self):
        col = pa.chunked_array([pa.array([1, 2, 3, 4, 5])])
        result = _compute_value_distribution(col, top_n=3)
        assert len(result) == 3

    def test_empty_column(self):
        col = pa.chunked_array([pa.array([], type=pa.string())])
        result = _compute_value_distribution(col)
        assert result == []


# -- ParquetFile --


class TestParquetFile:
    def test_init(self, small_parquet):
        pf = ParquetFile(small_parquet)
        assert pf.num_rows == 5
        assert pf.num_row_groups == 3  # row_group_size=2, 5 rows -> 3 groups
        assert len(pf.schema.names) == 3
        assert pf.file_size > 0

    def test_read_row_group(self, small_parquet):
        pf = ParquetFile(small_parquet)
        table = pf.read_row_group(0)
        assert table.num_rows == 2
        assert "id" in table.column_names

    def test_read_row_group_with_columns(self, small_parquet):
        pf = ParquetFile(small_parquet)
        table = pf.read_row_group(0, columns=["id"])
        assert table.num_columns == 1

    def test_read_range(self, small_parquet):
        pf = ParquetFile(small_parquet)
        table = pf.read_range(0, 3)
        assert table.num_rows == 3

    def test_read_range_offset(self, small_parquet):
        pf = ParquetFile(small_parquet)
        table = pf.read_range(2, 2)
        assert table.num_rows == 2
        assert table.column("id")[0].as_py() == 3

    def test_read_range_beyond_end(self, small_parquet):
        pf = ParquetFile(small_parquet)
        table = pf.read_range(100, 10)
        assert table.num_rows == 0

    def test_get_schema_info(self, small_parquet):
        pf = ParquetFile(small_parquet)
        info = pf.get_schema_info()
        assert len(info) == 3
        assert info[0]["name"] == "id"
        assert "type" in info[0]
        assert "nullable" in info[0]

    def test_get_file_metadata(self, small_parquet):
        pf = ParquetFile(small_parquet)
        meta = pf.get_file_metadata()
        assert meta["num_rows"] == 5
        assert meta["num_row_groups"] == 3
        assert "path" in meta
        assert "file_size" in meta

    def test_get_row_group_metadata(self, small_parquet):
        pf = ParquetFile(small_parquet)
        rg = pf.get_row_group_metadata(0)
        assert rg["num_rows"] == 2
        assert len(rg["columns"]) == 3
        assert "compression" in rg["columns"][0]

    def test_get_column_statistics(self, small_parquet):
        pf = ParquetFile(small_parquet)
        stats = pf.get_column_statistics("value")
        assert stats["count"] == 5
        assert stats["null_count"] == 0
        assert stats["min"] == 10.0
        assert stats["max"] == 50.0
        assert stats["mean"] == pytest.approx(30.0)

    def test_get_column_statistics_string(self, small_parquet):
        pf = ParquetFile(small_parquet)
        stats = pf.get_column_statistics("name")
        assert stats["count"] == 5
        assert stats["mean"] is None

    def test_get_value_distribution(self, small_parquet):
        pf = ParquetFile(small_parquet)
        dist = pf.get_value_distribution("name", top_n=3)
        assert len(dist) <= 3
        assert all("value" in d and "count" in d and "percentage" in d for d in dist)

    def test_search(self, small_parquet):
        pf = ParquetFile(small_parquet)
        table, total = pf.search("Alice")
        assert total == 1
        assert table.num_rows == 1

    def test_search_empty_query(self, small_parquet):
        pf = ParquetFile(small_parquet)
        table, total = pf.search("")
        assert total == 5

    def test_filter_column(self, small_parquet):
        pf = ParquetFile(small_parquet)
        table, total = pf.filter_column("name", "Bob")
        assert total == 1

    def test_filter_column_numeric(self, small_parquet):
        pf = ParquetFile(small_parquet)
        table, total = pf.filter_column("value", "25", mode=">")
        assert total == 3  # 30, 40, 50

    def test_filter_column_empty_value(self, small_parquet):
        pf = ParquetFile(small_parquet)
        table, total = pf.filter_column("name", "")
        assert total == 5

    def test_filter_multi(self, small_parquet):
        pf = ParquetFile(small_parquet)
        specs = [
            FilterSpec("name", "contains", "a"),  # Alice, Diana, Charlie
            FilterSpec("value", ">", "25"),  # 30, 40, 50
        ]
        table, total = pf.filter_multi(specs, "AND")
        assert total == 2  # Charlie(30), Diana(40)

    def test_filter_multi_empty(self, small_parquet):
        pf = ParquetFile(small_parquet)
        table, total = pf.filter_multi([], "AND")
        assert total == 5

    def test_nested_schema(self, nested_parquet):
        pf = ParquetFile(nested_parquet)
        info = pf.get_schema_info()
        addr = next(f for f in info if f["name"] == "address")
        assert len(addr["children"]) == 2  # street, zip

        tags = next(f for f in info if f["name"] == "tags")
        assert len(tags["children"]) == 1  # element


# -- ParquetDirectory --


class TestParquetDirectory:
    def test_init(self, small_parquet_dir):
        pd = ParquetDirectory(small_parquet_dir)
        assert pd.num_rows == 6
        assert len(pd.files) == 2
        assert pd.num_row_groups > 0

    def test_empty_directory(self, tmp_path):
        d = tmp_path / "empty"
        d.mkdir()
        with pytest.raises(ValueError, match="No valid parquet files"):
            ParquetDirectory(str(d))

    def test_read_range(self, small_parquet_dir):
        pd = ParquetDirectory(small_parquet_dir)
        table = pd.read_range(0, 6)
        assert table.num_rows == 6

    def test_read_range_across_files(self, small_parquet_dir):
        pd = ParquetDirectory(small_parquet_dir)
        table = pd.read_range(2, 3)
        assert table.num_rows == 3

    def test_search(self, small_parquet_dir):
        pd = ParquetDirectory(small_parquet_dir)
        table, total = pd.search("Eve")
        assert total == 1

    def test_filter_column(self, small_parquet_dir):
        pd = ParquetDirectory(small_parquet_dir)
        table, total = pd.filter_column("value", "35", mode=">")
        assert total == 3  # 40, 50, 60

    def test_filter_multi(self, small_parquet_dir):
        pd = ParquetDirectory(small_parquet_dir)
        specs = [FilterSpec("name", "contains", "a")]
        table, total = pd.filter_multi(specs, "AND")
        assert total > 0

    def test_get_schema_info(self, small_parquet_dir):
        pd = ParquetDirectory(small_parquet_dir)
        info = pd.get_schema_info()
        assert len(info) == 3

    def test_get_file_metadata(self, small_parquet_dir):
        pd = ParquetDirectory(small_parquet_dir)
        meta = pd.get_file_metadata()
        assert meta["num_rows"] == 6

    def test_get_column_statistics(self, small_parquet_dir):
        pd = ParquetDirectory(small_parquet_dir)
        stats = pd.get_column_statistics("value")
        assert stats["count"] == 6
        assert stats["min"] == 10.0
        assert stats["max"] == 60.0

    def test_get_value_distribution(self, small_parquet_dir):
        pd = ParquetDirectory(small_parquet_dir)
        dist = pd.get_value_distribution("name", top_n=3)
        assert len(dist) <= 3
