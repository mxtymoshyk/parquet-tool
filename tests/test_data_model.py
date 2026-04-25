import os
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from PyQt6.QtCore import QModelIndex, Qt

from parquet_tool.data_model import ParquetTableModel
from parquet_tool.gui_utils import FilterSpec
from parquet_tool.parquet_backend import ParquetFile


@pytest.fixture
def model(small_parquet, qapp):
    pf = ParquetFile(small_parquet)
    return ParquetTableModel(pf)


class TestParquetTableModel:
    def test_init(self, model):
        assert model.rowCount() == 5
        assert model.columnCount() == 3
        assert model.total_rows == 5

    def test_data_display_role(self, model):
        idx = model.index(0, 0)
        val = model.data(idx, Qt.ItemDataRole.DisplayRole)
        assert val == "1"

    def test_data_none_value(self, qapp):
        table = pa.table({"col": [None, "a"]})
        path = tempfile.mktemp(suffix=".parquet")
        pq.write_table(table, path)
        try:
            pf = ParquetFile(path)
            m = ParquetTableModel(pf)
            idx = m.index(0, 0)
            assert m.data(idx, Qt.ItemDataRole.DisplayRole) == ""
        finally:
            os.unlink(path)

    def test_data_alignment_numeric(self, model):
        # value column (float)
        idx = model.index(0, 2)
        align = model.data(idx, Qt.ItemDataRole.TextAlignmentRole)
        assert align is not None

    def test_data_invalid_index(self, model):
        assert model.data(QModelIndex(), Qt.ItemDataRole.DisplayRole) is None

    def test_header_horizontal(self, model):
        h = model.headerData(0, Qt.Orientation.Horizontal)
        assert h == "id"

    def test_header_vertical(self, model):
        h = model.headerData(0, Qt.Orientation.Vertical)
        assert h == "1"

    def test_raw_data(self, model):
        idx = model.index(0, 2)
        raw = model.raw_data(idx)
        assert raw == 10.0

    def test_raw_data_invalid(self, model):
        assert model.raw_data(QModelIndex()) is None

    def test_get_page_count(self, model):
        assert model.get_page_count() == 1  # 5 rows, 1000 per page

    def test_get_page_count_zero_rows(self, qapp, tmp_path):
        table = pa.table({"col": pa.array([], type=pa.string())})
        path = str(tmp_path / "empty.parquet")
        pq.write_table(table, path)
        pf = ParquetFile(path)
        m = ParquetTableModel(pf)
        assert m.get_page_count() == 1

    def test_set_page_out_of_bounds(self, model):
        model.set_page(999)
        assert model._current_page == 0  # unchanged

    def test_set_page_negative(self, model):
        model.set_page(-1)
        assert model._current_page == 0


class TestColumnVisibility:
    def test_set_visible_columns(self, model):
        model.set_visible_columns(["id", "name"])
        assert model.columnCount() == 2

    def test_data_after_hide(self, model):
        model.set_visible_columns(["name", "value"])
        idx = model.index(0, 0)
        assert model.data(idx, Qt.ItemDataRole.DisplayRole) == "Alice"

    def test_header_after_hide(self, model):
        model.set_visible_columns(["name"])
        assert model.headerData(0, Qt.Orientation.Horizontal) == "name"

    def test_sort_visible_column(self, model):
        model.set_visible_columns(["name", "value"])
        model.sort(0, Qt.SortOrder.AscendingOrder)
        idx = model.index(0, 0)
        assert model.data(idx, Qt.ItemDataRole.DisplayRole) == "Alice"


class TestSearch:
    def test_set_search(self, model):
        model.set_search("Alice")
        assert model.total_rows == 1
        assert model.rowCount() == 1

    def test_search_no_match(self, model):
        model.set_search("zzzzz")
        assert model.total_rows == 0
        assert model.rowCount() == 0

    def test_clear_filters(self, model):
        model.set_search("Alice")
        assert model.total_rows == 1
        model.clear_filters()
        assert model.total_rows == 5


class TestColumnFilter:
    def test_set_column_filter_spec(self, model):
        spec = FilterSpec("name", "contains", "Bob")
        model.set_column_filter(spec)
        assert model.total_rows == 1

    def test_set_column_filter_tuple(self, model):
        model.set_column_filter(("name", "Eve"))
        assert model.total_rows == 1

    def test_numeric_filter(self, model):
        spec = FilterSpec("value", ">", "25")
        model.set_column_filter(spec)
        assert model.total_rows == 3


class TestMultiFilter:
    def test_set_multi_filter(self, model):
        specs = [
            FilterSpec("name", "contains", "a"),
            FilterSpec("value", ">", "25"),
        ]
        model.set_multi_filter(specs, "AND")
        assert model.total_rows == 2  # Charlie(30), Diana(40)

    def test_multi_filter_or(self, model):
        specs = [
            FilterSpec("name", "exact", "Alice"),
            FilterSpec("name", "exact", "Eve"),
        ]
        model.set_multi_filter(specs, "OR")
        assert model.total_rows == 2

    def test_clear_multi_filter(self, model):
        specs = [FilterSpec("name", "contains", "a")]
        model.set_multi_filter(specs)
        model.clear_filters()
        assert model.total_rows == 5
        assert model._multi_filter is None


class TestSetPageData:
    def test_set_page_data(self, model):
        table = pa.table({"id": [99], "name": ["Test"], "value": [100.0]})
        model.set_page_data(table, 1)
        assert model.rowCount() == 1
        assert model.total_rows == 1


class TestExport:
    def test_export_to_csv(self, model, tmp_path):
        path = str(tmp_path / "export.csv")
        model.export_to_csv(path)
        with open(path) as f:
            lines = f.readlines()
        assert len(lines) == 6  # header + 5 rows

    def test_export_with_search(self, model, tmp_path):
        model.set_search("Alice")
        path = str(tmp_path / "export.csv")
        model.export_to_csv(path)
        with open(path) as f:
            lines = f.readlines()
        assert len(lines) == 2  # header + 1 row

    def test_export_with_column_filter(self, model, tmp_path):
        model.set_column_filter(FilterSpec("value", ">", "30"))
        path = str(tmp_path / "export.csv")
        model.export_to_csv(path)
        with open(path) as f:
            lines = f.readlines()
        assert len(lines) == 3  # header + 2 rows (40, 50)

    def test_export_with_multi_filter(self, model, tmp_path):
        specs = [
            FilterSpec("name", "exact", "Alice"),
            FilterSpec("name", "exact", "Bob"),
        ]
        model.set_multi_filter(specs, "OR")
        path = str(tmp_path / "export.csv")
        model.export_to_csv(path)
        with open(path) as f:
            lines = f.readlines()
        assert len(lines) == 3  # header + 2 rows


class TestNestedColumns:
    def test_nested_lazy_conversion(self, nested_parquet, qapp):
        pf = ParquetFile(nested_parquet)
        m = ParquetTableModel(pf)
        # struct and list columns should be flagged as nested
        assert any(m._nested_flags)
        assert m.rowCount() == 2

    def test_nested_data_access(self, nested_parquet, qapp):
        pf = ParquetFile(nested_parquet)
        m = ParquetTableModel(pf)
        # access nested struct column via raw_data
        addr_idx = m._column_names.index("address")
        idx = m.index(0, addr_idx)
        raw = m.raw_data(idx)
        assert isinstance(raw, dict)
        assert "street" in raw

    def test_nested_display_role(self, nested_parquet, qapp):
        pf = ParquetFile(nested_parquet)
        m = ParquetTableModel(pf)
        addr_idx = m._column_names.index("address")
        idx = m.index(0, addr_idx)
        val = m.data(idx, Qt.ItemDataRole.DisplayRole)
        assert "123 Main" in val

    def test_nested_list_access(self, nested_parquet, qapp):
        pf = ParquetFile(nested_parquet)
        m = ParquetTableModel(pf)
        tags_idx = m._column_names.index("tags")
        idx = m.index(0, tags_idx)
        raw = m.raw_data(idx)
        assert isinstance(raw, list)
        assert raw == ["a", "b"]


class TestSort:
    def test_sort_ascending(self, model):
        model.sort(2, Qt.SortOrder.AscendingOrder)  # sort by value
        idx = model.index(0, 2)
        assert model.data(idx, Qt.ItemDataRole.DisplayRole) == "10.0"

    def test_sort_descending(self, model):
        model.sort(2, Qt.SortOrder.DescendingOrder)  # sort by value
        idx = model.index(0, 2)
        assert model.data(idx, Qt.ItemDataRole.DisplayRole) == "50.0"

    def test_sort_empty_table(self, model):
        model.set_search("zzzzz")
        model.sort(0, Qt.SortOrder.AscendingOrder)  # should not raise
