import pyarrow.csv as pa_csv
from PyQt6.QtCore import QThread, pyqtSignal

from .parquet_backend import build_column_mask, build_composite_mask, build_search_mask


class SearchWorker(QThread):
    """Run search/filter in background, reporting progress per row group."""

    progress = pyqtSignal(int, int)
    result = pyqtSignal(object, int)
    error = pyqtSignal(str)

    def __init__(self, pf, query=None, column_filter=None, offset=0, limit=1000):
        super().__init__()
        self._pf = pf
        self._query = query
        self._column_filter = column_filter
        self._offset = offset
        self._limit = limit
        self._cancelled = False

    def cancel(self):
        self._cancelled = True

    def run(self):
        try:
            if self._query:
                table, total = self._pf.search(
                    self._query,
                    offset=self._offset,
                    limit=self._limit,
                    progress_cb=self._report_progress,
                    cancelled_fn=lambda: self._cancelled,
                )
            elif self._column_filter:
                spec = self._column_filter
                table, total = self._pf.filter_column(
                    spec.column,
                    spec.value,
                    offset=self._offset,
                    limit=self._limit,
                    mode=spec.mode,
                    value2=spec.value2,
                    progress_cb=self._report_progress,
                    cancelled_fn=lambda: self._cancelled,
                )
            else:
                table = self._pf.read_range(self._offset, self._limit)
                total = self._pf.num_rows

            if not self._cancelled:
                self.result.emit(table, total)
        except Exception as e:
            if not self._cancelled:
                self.error.emit(str(e))
        finally:
            self._pf = None

    def _report_progress(self, current, total):
        self.progress.emit(current, total)


class MultiFilterWorker(QThread):
    """Run multi-condition filter in background."""

    progress = pyqtSignal(int, int)
    result = pyqtSignal(object, int)
    error = pyqtSignal(str)

    def __init__(self, pf, filter_specs, join_mode="AND", offset=0, limit=1000):
        super().__init__()
        self._pf = pf
        self._filter_specs = filter_specs
        self._join_mode = join_mode
        self._offset = offset
        self._limit = limit
        self._cancelled = False

    def cancel(self):
        self._cancelled = True

    def run(self):
        try:
            table, total = self._pf.filter_multi(
                self._filter_specs,
                self._join_mode,
                offset=self._offset,
                limit=self._limit,
                progress_cb=self._report_progress,
                cancelled_fn=lambda: self._cancelled,
            )
            if not self._cancelled:
                self.result.emit(table, total)
        except Exception as e:
            if not self._cancelled:
                self.error.emit(str(e))
        finally:
            self._pf = None

    def _report_progress(self, current, total):
        self.progress.emit(current, total)


class StatsWorker(QThread):
    """Compute column statistics in background."""

    progress = pyqtSignal(int, int)
    result = pyqtSignal(object)
    error = pyqtSignal(str)

    def __init__(self, pf, column_name):
        super().__init__()
        self._pf = pf
        self._column_name = column_name
        self._cancelled = False

    def cancel(self):
        self._cancelled = True

    def run(self):
        try:
            stats = self._pf.get_column_statistics(self._column_name)
            if not self._cancelled:
                self.result.emit(stats)
        except Exception as e:
            if not self._cancelled:
                self.error.emit(str(e))
        finally:
            self._pf = None


class DistributionWorker(QThread):
    """Compute value distribution in background."""

    result = pyqtSignal(object)
    error = pyqtSignal(str)

    def __init__(self, pf, column_name, top_n=20):
        super().__init__()
        self._pf = pf
        self._column_name = column_name
        self._top_n = top_n

    def run(self):
        try:
            dist = self._pf.get_value_distribution(self._column_name, self._top_n)
            self.result.emit(dist)
        except Exception as e:
            self.error.emit(str(e))
        finally:
            self._pf = None


class ExportWorker(QThread):
    """Export CSV in background with progress."""

    progress = pyqtSignal(int, int)
    result = pyqtSignal(str)
    error = pyqtSignal(str)

    def __init__(self, pf, file_path, search_query="", column_filter=None, multi_filter=None):
        super().__init__()
        self._pf = pf
        self._file_path = file_path
        self._search_query = search_query
        self._column_filter = column_filter
        self._multi_filter = multi_filter
        self._cancelled = False

    def cancel(self):
        self._cancelled = True

    def run(self):
        try:
            first = True
            total = self._pf.num_row_groups
            with open(self._file_path, "wb") as f:
                for rg_idx in range(total):
                    if self._cancelled:
                        break
                    table = self._pf.read_row_group(rg_idx)
                    filtered = self._apply_filter(table)
                    if filtered is not None and filtered.num_rows > 0:
                        pa_csv.write_csv(
                            filtered,
                            f,
                            write_options=pa_csv.WriteOptions(include_header=first),
                        )
                        first = False
                    self.progress.emit(rg_idx + 1, total)

            if not self._cancelled:
                self.result.emit(self._file_path)
        except Exception as e:
            if not self._cancelled:
                self.error.emit(str(e))
        finally:
            self._pf = None

    def _apply_filter(self, table):
        if self._search_query:
            mask = build_search_mask(table, self._search_query)
            return table.filter(mask) if mask is not None else None
        if self._column_filter:
            spec = self._column_filter
            mask = build_column_mask(table, spec.column, spec.value, spec.mode, spec.value2)
            return table.filter(mask) if mask is not None else None
        if self._multi_filter:
            specs, join_mode = self._multi_filter
            mask = build_composite_mask(table, specs, join_mode)
            return table.filter(mask) if mask is not None else None
        return table
