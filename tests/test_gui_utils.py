from parquet_tool.gui_utils import (
    ColumnFilter,
    FilterSpec,
    PaginationBar,
    SearchBar,
    SettingsManager,
    ThemeManager,
    format_number,
    format_size,
)

# -- FilterSpec --


class TestFilterSpec:
    def test_defaults(self):
        spec = FilterSpec("col", "contains", "val")
        assert spec.column == "col"
        assert spec.mode == "contains"
        assert spec.value == "val"
        assert spec.value2 == ""

    def test_with_value2(self):
        spec = FilterSpec("col", "between", "10", "20")
        assert spec.value2 == "20"

    def test_equality(self):
        a = FilterSpec("col", "contains", "val")
        b = FilterSpec("col", "contains", "val")
        assert a == b

    def test_inequality(self):
        a = FilterSpec("col", "contains", "val")
        b = FilterSpec("col", "exact", "val")
        assert a != b


# -- format_size --


class TestFormatSize:
    def test_bytes(self):
        assert format_size(500) == "500.0 B"

    def test_kilobytes(self):
        assert format_size(1536) == "1.5 KB"

    def test_megabytes(self):
        assert format_size(2 * 1024 * 1024) == "2.0 MB"

    def test_gigabytes(self):
        assert format_size(3 * 1024**3) == "3.0 GB"

    def test_terabytes(self):
        assert format_size(1024**4) == "1.0 TB"

    def test_zero(self):
        assert format_size(0) == "0.0 B"


# -- format_number --


class TestFormatNumber:
    def test_small(self):
        assert format_number(42) == "42"

    def test_thousands(self):
        assert format_number(1234567) == "1,234,567"

    def test_zero(self):
        assert format_number(0) == "0"


# -- ThemeManager --


class TestThemeManager:
    def test_default_light(self):
        tm = ThemeManager()
        assert tm.is_dark is False

    def test_init_dark(self):
        tm = ThemeManager(is_dark=True)
        assert tm.is_dark is True

    def test_toggle(self, qapp):
        tm = ThemeManager()
        tm.toggle(qapp)
        assert tm.is_dark is True
        tm.toggle(qapp)
        assert tm.is_dark is False

    def test_apply_dark(self, qapp):
        tm = ThemeManager(is_dark=True)
        tm.apply(qapp)
        assert "background-color" in qapp.styleSheet()

    def test_apply_light(self, qapp):
        tm = ThemeManager(is_dark=False)
        tm.apply(qapp)
        assert qapp.styleSheet() == ""


# -- SettingsManager --


class TestSettingsManager:
    def test_default_is_dark(self):
        sm = SettingsManager()
        # may be True or False depending on prior tests; just check it works
        assert isinstance(sm.is_dark, bool)

    def test_set_is_dark(self):
        sm = SettingsManager()
        sm.is_dark = True
        assert sm.is_dark is True
        sm.is_dark = False
        assert sm.is_dark is False

    def test_recent_files_default(self):
        sm = SettingsManager()
        files = sm.recent_files
        assert isinstance(files, list)

    def test_recent_files_set(self, tmp_path):
        sm = SettingsManager()
        # use real paths so the existence check passes
        files = [str(tmp_path / f"{c}.parquet") for c in "abc"]
        for f in files:
            open(f, "w").close()
        sm.recent_files = files
        assert sm.recent_files == files

    def test_recent_files_truncated(self, tmp_path):
        sm = SettingsManager()
        paths = []
        for i in range(20):
            p = str(tmp_path / f"{i}.parquet")
            open(p, "w").close()
            paths.append(p)
        sm.recent_files = paths
        assert len(sm.recent_files) <= 10

    def test_recent_files_filters_nonexistent(self):
        sm = SettingsManager()
        sm.recent_files = ["/no/such/file.parquet"]
        assert sm.recent_files == []


# -- SearchBar --


class TestSearchBar:
    def test_search_signal(self, qapp, qtbot):
        bar = SearchBar()
        with qtbot.waitSignal(bar.searchRequested, timeout=1000) as blocker:
            bar.search_input.setText("test")
            bar.search_button.click()
        assert blocker.args == ["test"]

    def test_clear_signal(self, qapp, qtbot):
        bar = SearchBar()
        bar.search_input.setText("test")
        with qtbot.waitSignal(bar.cleared, timeout=1000):
            bar.clear_button.click()
        assert bar.search_input.text() == ""


# -- ColumnFilter --


class TestColumnFilter:
    def test_set_columns(self, qapp):
        cf = ColumnFilter()
        cf.set_columns(["a", "b", "c"])
        assert cf.column_combo.count() == 3

    def test_filter_signal(self, qapp, qtbot):
        cf = ColumnFilter()
        cf.set_columns(["col1"])
        cf.filter_input.setText("val")
        with qtbot.waitSignal(cf.filterRequested, timeout=1000) as blocker:
            cf.filter_button.click()
        spec = blocker.args[0]
        assert isinstance(spec, FilterSpec)
        assert spec.column == "col1"
        assert spec.mode == "contains"
        assert spec.value == "val"

    def test_filter_with_mode(self, qapp, qtbot):
        cf = ColumnFilter()
        cf.set_columns(["col1"])
        cf.mode_combo.setCurrentText(">")
        cf.filter_input.setText("10")
        with qtbot.waitSignal(cf.filterRequested, timeout=1000) as blocker:
            cf.filter_button.click()
        spec = blocker.args[0]
        assert spec.mode == ">"

    def test_between_shows_second_input(self, qapp):
        cf = ColumnFilter()
        cf.show()
        assert not cf.filter_input2.isVisible()
        cf.mode_combo.setCurrentText("between")
        assert cf.filter_input2.isVisible()
        cf.hide()

    def test_no_signal_when_empty(self, qapp, qtbot):
        cf = ColumnFilter()
        cf.set_columns(["col1"])
        # don't set filter_input text
        with qtbot.assertNotEmitted(cf.filterRequested):
            cf.filter_button.click()

    def test_clear_filter(self, qapp):
        cf = ColumnFilter()
        cf.filter_input.setText("val")
        cf.filter_input2.setText("val2")
        cf.clear_filter()
        assert cf.filter_input.text() == ""
        assert cf.filter_input2.text() == ""


# -- PaginationBar --


class TestPaginationBar:
    def test_update_state(self, qapp):
        bar = PaginationBar()
        bar.update_state(0, 5, 5000, 1000)
        assert "Page 1 of 5" in bar.page_label.text()
        assert bar.first_btn.isEnabled() is False
        assert bar.next_btn.isEnabled() is True

    def test_last_page(self, qapp):
        bar = PaginationBar()
        bar.update_state(4, 5, 5000, 1000)
        assert bar.next_btn.isEnabled() is False
        assert bar.prev_btn.isEnabled() is True

    def test_page_changed_signal(self, qapp, qtbot):
        bar = PaginationBar()
        bar.update_state(0, 5, 5000, 1000)
        with qtbot.waitSignal(bar.pageChanged, timeout=1000) as blocker:
            bar.next_btn.click()
        assert blocker.args == [1]

    def test_goto_row(self, qapp, qtbot):
        bar = PaginationBar()
        bar.update_state(0, 5, 5000, 1000)
        bar.goto_input.setText("2500")
        signals = []
        bar.pageChanged.connect(lambda p: signals.append(("page", p)))
        bar.rowHighlightRequested.connect(lambda r: signals.append(("row", r)))
        bar.goto_btn.click()
        assert ("page", 2) in signals  # row 2500 -> page 2 (0-indexed)
        assert ("row", 499) in signals  # row within page

    def test_goto_invalid(self, qapp):
        bar = PaginationBar()
        bar.update_state(0, 5, 5000, 1000)
        bar.goto_input.setText("abc")
        bar._on_goto()  # should not crash

    def test_goto_out_of_range(self, qapp):
        bar = PaginationBar()
        bar.update_state(0, 5, 5000, 1000)
        bar.goto_input.setText("99999")
        signals = []
        bar.pageChanged.connect(lambda p: signals.append(p))
        bar._on_goto()
        assert len(signals) == 0  # no signal emitted

    def test_no_rows(self, qapp):
        bar = PaginationBar()
        bar.update_state(0, 1, 0, 1000)
        assert "No rows" in bar.rows_label.text()
