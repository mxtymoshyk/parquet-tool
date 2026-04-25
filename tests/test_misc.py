"""Tests for filter_builder, nested_viewer, file_session, and gui_tabs."""

import json

from parquet_tool.file_session import FileSession
from parquet_tool.filter_builder import FilterBuilder, FilterConditionRow
from parquet_tool.gui_tabs import (
    populate_distribution,
    populate_stats,
)
from parquet_tool.gui_utils import FilterSpec
from parquet_tool.nested_viewer import NestedDataViewer

# -- FilterConditionRow --


class TestFilterConditionRow:
    def test_to_filter_spec(self, qapp):
        row = FilterConditionRow(["col1", "col2"])
        row.column_combo.setCurrentText("col1")
        row.mode_combo.setCurrentText(">")
        row.value_input.setText("10")
        spec = row.to_filter_spec()
        assert spec == FilterSpec("col1", ">", "10")

    def test_to_filter_spec_between(self, qapp):
        row = FilterConditionRow(["col1"])
        row.mode_combo.setCurrentText("between")
        row.value_input.setText("10")
        row.value_input2.setText("20")
        spec = row.to_filter_spec()
        assert spec.value2 == "20"

    def test_to_filter_spec_empty_value(self, qapp):
        row = FilterConditionRow(["col1"])
        spec = row.to_filter_spec()
        assert spec is None

    def test_between_toggles_input2(self, qapp):
        row = FilterConditionRow(["col1"])
        row.show()
        assert not row.value_input2.isVisible()
        row.mode_combo.setCurrentText("between")
        assert row.value_input2.isVisible()
        row.mode_combo.setCurrentText("contains")
        assert not row.value_input2.isVisible()
        row.hide()

    def test_remove_signal(self, qapp, qtbot):
        row = FilterConditionRow(["col1"])
        with qtbot.waitSignal(row.removed, timeout=1000) as blocker:
            row.remove_btn.click()
        assert blocker.args[0] is row


# -- FilterBuilder --


class TestFilterBuilder:
    def test_set_columns(self, qapp):
        fb = FilterBuilder()
        fb.set_columns(["a", "b", "c"])
        assert len(fb._rows) == 1
        assert fb._rows[0].column_combo.count() == 3

    def test_add_row(self, qapp):
        fb = FilterBuilder()
        fb.set_columns(["a"])
        fb._add_row()
        assert len(fb._rows) == 2

    def test_remove_row_keeps_minimum(self, qapp):
        fb = FilterBuilder()
        fb.set_columns(["a"])
        assert len(fb._rows) == 1
        fb._remove_row(fb._rows[0])
        assert len(fb._rows) == 1  # can't remove last

    def test_remove_row(self, qapp):
        fb = FilterBuilder()
        fb.set_columns(["a"])
        fb._add_row()
        assert len(fb._rows) == 2
        fb._remove_row(fb._rows[0])
        assert len(fb._rows) == 1

    def test_apply_emits_signal(self, qapp, qtbot):
        fb = FilterBuilder()
        fb.set_columns(["col1"])
        fb._rows[0].value_input.setText("test")
        with qtbot.waitSignal(fb.filterRequested, timeout=1000) as blocker:
            fb.apply_btn.click()
        specs, join = blocker.args
        assert len(specs) == 1
        assert join == "AND"

    def test_apply_skips_empty(self, qapp, qtbot):
        fb = FilterBuilder()
        fb.set_columns(["col1"])
        # don't set value
        with qtbot.assertNotEmitted(fb.filterRequested):
            fb.apply_btn.click()

    def test_clear_signal(self, qapp, qtbot):
        fb = FilterBuilder()
        fb.set_columns(["col1"])
        with qtbot.waitSignal(fb.cleared, timeout=1000):
            fb.clear_btn.click()
        assert len(fb._rows) == 1  # reset to one row


# -- NestedDataViewer --


class TestNestedDataViewer:
    def test_set_value_dict(self, qapp):
        viewer = NestedDataViewer()
        viewer.set_value({"key": "val", "num": 42}, "test_col")
        assert viewer.tree.topLevelItemCount() == 1
        root = viewer.tree.topLevelItem(0)
        assert root.childCount() == 2

    def test_set_value_list(self, qapp):
        viewer = NestedDataViewer()
        viewer.set_value(["a", "b", "c"], "tags")
        root = viewer.tree.topLevelItem(0)
        assert root.childCount() == 3

    def test_set_value_scalar(self, qapp):
        viewer = NestedDataViewer()
        viewer.set_value("hello", "col")
        assert viewer.tree.topLevelItemCount() == 1

    def test_set_value_none(self, qapp):
        viewer = NestedDataViewer()
        viewer.set_value(None, "col")
        assert viewer.tree.topLevelItemCount() == 1
        item = viewer.tree.topLevelItem(0)
        assert "null" in item.text(1)

    def test_set_value_nested(self, qapp):
        viewer = NestedDataViewer()
        viewer.set_value({"a": {"b": {"c": 1}}}, "deep")
        root = viewer.tree.topLevelItem(0)
        assert root.childCount() == 1

    def test_clear(self, qapp):
        viewer = NestedDataViewer()
        viewer.set_value({"key": "val"}, "col")
        viewer.clear()
        assert viewer.tree.topLevelItemCount() == 0
        assert viewer._raw_value is None

    def test_copy_json(self, qapp):
        from PyQt6.QtWidgets import QApplication

        viewer = NestedDataViewer()
        data = {"key": "val"}
        viewer.set_value(data, "col")
        viewer._copy_json()
        clipboard = QApplication.clipboard().text()
        assert json.loads(clipboard) == data


# -- FileSession --


class TestFileSession:
    def test_init(self):
        session = FileSession(None, None)
        assert session.pf is None
        assert session.model is None
        assert session.data_table_view is None

    def test_widget_refs(self, small_parquet, qapp):
        from parquet_tool.data_model import ParquetTableModel
        from parquet_tool.parquet_backend import ParquetFile

        pf = ParquetFile(small_parquet)
        model = ParquetTableModel(pf)
        session = FileSession(pf, model)
        assert session.pf is pf
        assert session.model is model
        session.data_table_view = "fake_widget"
        assert session.data_table_view == "fake_widget"


# -- populate_stats --


class TestPopulateStats:
    def test_basic(self, qapp):
        from PyQt6.QtWidgets import QLabel

        class FakeViewer:
            stats_labels = {
                "type": QLabel(),
                "count": QLabel(),
                "null_count": QLabel(),
                "valid_count": QLabel(),
                "unique_count": QLabel(),
                "min": QLabel(),
                "max": QLabel(),
                "mean": QLabel(),
            }

        viewer = FakeViewer()
        stats = {
            "type": "int64",
            "count": 100,
            "null_count": 5,
            "valid_count": 95,
            "unique_count": 90,
            "min": 1,
            "max": 100,
            "mean": 50.1234,
        }
        populate_stats(viewer, stats)
        assert viewer.stats_labels["type"].text() == "int64"
        assert "100" in viewer.stats_labels["count"].text()
        assert "5.0%" in viewer.stats_labels["null_count"].text()
        assert "50.1234" in viewer.stats_labels["mean"].text()

    def test_non_numeric_mean(self, qapp):
        from PyQt6.QtWidgets import QLabel

        class FakeViewer:
            stats_labels = {
                k: QLabel()
                for k in [
                    "type",
                    "count",
                    "null_count",
                    "valid_count",
                    "unique_count",
                    "min",
                    "max",
                    "mean",
                ]
            }

        viewer = FakeViewer()
        stats = {
            "type": "string",
            "count": 10,
            "null_count": 0,
            "valid_count": 10,
            "unique_count": 10,
            "min": "a",
            "max": "z",
            "mean": None,
        }
        populate_stats(viewer, stats)
        assert "N/A" in viewer.stats_labels["mean"].text()


# -- populate_distribution --


class TestPopulateDistribution:
    def test_basic(self, qapp):
        from PyQt6.QtWidgets import QTableWidget

        class FakeViewer:
            dist_table = QTableWidget()

        FakeViewer.dist_table.setColumnCount(3)
        viewer = FakeViewer()
        dist = [
            {"value": "alice", "count": 10, "percentage": 50.0},
            {"value": "bob", "count": 5, "percentage": 25.0},
        ]
        populate_distribution(viewer, dist)
        assert viewer.dist_table.rowCount() == 2
        assert viewer.dist_table.item(0, 0).text() == "alice"
        assert "50.0%" in viewer.dist_table.item(0, 2).text()
