from math import ceil

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pa_csv
from PyQt6.QtCore import QAbstractTableModel, QModelIndex, Qt

from .gui_utils import FilterSpec
from .parquet_backend import build_column_mask, build_composite_mask, build_search_mask

PAGE_SIZE = 1000


class ParquetTableModel(QAbstractTableModel):
    """Table model for displaying parquet data with pagination."""

    def __init__(self, parquet_file, parent=None):
        super().__init__(parent)
        self._pf = parquet_file
        self._column_names = list(parquet_file.schema.names)
        self._col_index = {name: i for i, name in enumerate(self._column_names)}
        self._visible_columns = list(self._column_names)
        self._current_page = 0
        self._current_table = None
        self._page_columns = []
        self._total_rows = parquet_file.num_rows
        self._search_query = ""
        self._column_filter = None  # FilterSpec
        self._multi_filter = None  # (list[FilterSpec], join_mode)

        self._load_page(0)

    @property
    def total_rows(self):
        return self._total_rows

    def rowCount(self, parent=QModelIndex()):
        return self._current_table.num_rows if self._current_table else 0

    def columnCount(self, parent=QModelIndex()):
        return len(self._visible_columns)

    def _cell_value(self, real_idx, row):
        """Get the Python value for a cell, converting Arrow scalars lazily."""
        col_data = self._page_columns[real_idx]
        if self._nested_flags[real_idx]:
            return col_data[row].as_py()
        return col_data[row]

    def data(self, index, role=Qt.ItemDataRole.DisplayRole):
        if not index.isValid():
            return None

        vis_col = self._visible_columns[index.column()]
        real_idx = self._col_index[vis_col]
        value = self._cell_value(real_idx, index.row())

        if role == Qt.ItemDataRole.DisplayRole:
            return "" if value is None else str(value)

        if role == Qt.ItemDataRole.TextAlignmentRole and isinstance(value, (int, float)):
            return int(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)

        return None

    def headerData(self, section, orientation, role=Qt.ItemDataRole.DisplayRole):
        if role != Qt.ItemDataRole.DisplayRole:
            return None

        if orientation == Qt.Orientation.Horizontal:
            return self._visible_columns[section]

        return str(self._current_page * PAGE_SIZE + section + 1)

    def raw_data(self, index):
        """Return the raw Python object for a cell (not stringified)."""
        if not index.isValid():
            return None
        vis_col = self._visible_columns[index.column()]
        real_idx = self._col_index[vis_col]
        return self._cell_value(real_idx, index.row())

    def set_visible_columns(self, visible):
        """Update which columns are shown. Pass list of column names."""
        self.beginResetModel()
        visible_set = set(visible)
        self._visible_columns = [c for c in self._column_names if c in visible_set]
        self.endResetModel()

    @property
    def column_names(self):
        return self._column_names

    @property
    def visible_columns(self):
        return self._visible_columns

    def sort(self, column, order):
        """Sort within current page using pyarrow compute."""
        if self._current_table is None or self._current_table.num_rows == 0:
            return

        col_name = self._visible_columns[column]
        sort_dir = "descending" if order == Qt.SortOrder.DescendingOrder else "ascending"

        try:
            indices = pc.sort_indices(self._current_table, sort_keys=[(col_name, sort_dir)])
            self._current_table = self._current_table.take(indices)
        except Exception:
            try:
                str_col = pc.cast(self._current_table.column(column), pa.string())
                temp = self._current_table.append_column("__sort__", str_col)
                indices = pc.sort_indices(temp, sort_keys=[("__sort__", sort_dir)])
                self._current_table = self._current_table.take(indices)
            except Exception:
                return

        self._update_page_columns()
        self.layoutChanged.emit()

    def set_page(self, page):
        """Navigate to a specific page."""
        total_pages = self.get_page_count()
        if page < 0 or page >= total_pages:
            return
        self._current_page = page
        self._load_page(page)

    def set_search(self, query):
        """Activate full-text search."""
        self._search_query = query
        self._column_filter = None
        self._current_page = 0
        self._load_page(0)

    def set_column_filter(self, spec):
        """Activate column filter with a FilterSpec."""
        if isinstance(spec, FilterSpec):
            self._column_filter = spec
        else:
            # backward compat: (column, value) tuple
            col, val = spec
            self._column_filter = FilterSpec(col, "contains", val)
        self._search_query = ""
        self._current_page = 0
        self._load_page(0)

    def set_multi_filter(self, filter_specs, join_mode="AND"):
        """Activate multi-condition filter."""
        self._multi_filter = (filter_specs, join_mode)
        self._search_query = ""
        self._column_filter = None
        self._current_page = 0
        self._load_page(0)

    def clear_filters(self):
        """Reset all search and filter state."""
        self._search_query = ""
        self._column_filter = None
        self._multi_filter = None
        self._current_page = 0
        self._total_rows = self._pf.num_rows
        self._load_page(0)

    def set_page_data(self, table, total_rows):
        """Receive pre-computed page data from a worker thread."""
        self.beginResetModel()
        self._current_table = table
        self._total_rows = total_rows
        self._update_page_columns()
        self.endResetModel()

    def get_page_count(self):
        if self._total_rows == 0:
            return 1
        return ceil(self._total_rows / PAGE_SIZE)

    def export_to_csv(self, file_path):
        """Export data (respecting current filters) to CSV via pyarrow native writer."""
        first = True
        with open(file_path, "wb") as f:
            for rg_idx in range(self._pf.num_row_groups):
                table = self._pf.read_row_group(rg_idx)
                filtered = self._apply_active_filter(table)
                if filtered is not None and filtered.num_rows > 0:
                    pa_csv.write_csv(
                        filtered,
                        f,
                        write_options=pa_csv.WriteOptions(include_header=first),
                    )
                    first = False

    # -- private --

    def _load_page(self, page):
        """Load data for the given page."""
        offset = page * PAGE_SIZE

        self.beginResetModel()

        if self._search_query:
            table, total = self._pf.search(self._search_query, offset=offset, limit=PAGE_SIZE)
            self._total_rows = total
        elif self._column_filter:
            spec = self._column_filter
            table, total = self._pf.filter_column(
                spec.column,
                spec.value,
                offset=offset,
                limit=PAGE_SIZE,
                mode=spec.mode,
                value2=spec.value2,
            )
            self._total_rows = total
        elif self._multi_filter:
            specs, join = self._multi_filter
            table, total = self._pf.filter_multi(
                specs,
                join,
                offset=offset,
                limit=PAGE_SIZE,
            )
            self._total_rows = total
        else:
            table = self._pf.read_range(offset, PAGE_SIZE)

        self._current_table = table
        self._update_page_columns()
        self.endResetModel()

    def _update_page_columns(self):
        """Convert current pyarrow table to column lists for fast cell access.

        Nested columns (list, struct, map) are kept as Arrow arrays to
        avoid the expensive to_pylist() conversion up front -- they get
        converted per-cell on access instead.
        """
        if self._current_table is None or self._current_table.num_rows == 0:
            self._page_columns = [[] for _ in self._column_names]
            self._nested_flags = [False] * len(self._column_names)
            return

        self._page_columns = []
        self._nested_flags = []
        for i in range(self._current_table.num_columns):
            col = self._current_table.column(i)
            field_type = self._current_table.schema.field(i).type
            is_nested = isinstance(field_type, (pa.ListType, pa.StructType, pa.MapType))
            self._nested_flags.append(is_nested)
            if is_nested:
                self._page_columns.append(col)
            else:
                self._page_columns.append(col.to_pylist())

    def _apply_active_filter(self, table):
        """Apply the currently active search/filter to a table. Used by export."""
        if self._search_query:
            mask = build_search_mask(table, self._search_query, self._column_names)
            return table.filter(mask) if mask is not None else None

        if self._column_filter:
            spec = self._column_filter
            mask = build_column_mask(table, spec.column, spec.value, spec.mode, spec.value2)
            return table.filter(mask) if mask is not None else None

        if self._multi_filter:
            specs, join = self._multi_filter
            mask = build_composite_mask(table, specs, join)
            return table.filter(mask) if mask is not None else None

        return table
