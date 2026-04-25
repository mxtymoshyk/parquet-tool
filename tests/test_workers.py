import os

import pytest

from parquet_tool.gui_utils import FilterSpec
from parquet_tool.parquet_backend import ParquetFile
from parquet_tool.workers import (
    DistributionWorker,
    ExportWorker,
    MultiFilterWorker,
    SearchWorker,
    StatsWorker,
)


@pytest.fixture
def pf_small(small_parquet):
    return ParquetFile(small_parquet)


def _run(qtbot, worker, *, timeout=5000):
    """Start a worker and wait for QThread.finished (post-run()) — race-free.

    Returns the args of the first worker.result emit, or () if none fired
    (e.g. when the worker was cancelled before emitting).
    """
    captured = []
    worker.result.connect(lambda *args: captured.append(args))
    with qtbot.waitSignal(worker.finished, timeout=timeout):
        worker.start()
    return captured[0] if captured else ()


class TestSearchWorker:
    def test_search_query(self, qapp, qtbot, pf_small):
        worker = SearchWorker(pf_small, query="Alice", offset=0, limit=100)
        table, total = _run(qtbot, worker)
        assert total == 1
        assert table.num_rows == 1

    def test_search_column_filter(self, qapp, qtbot, pf_small):
        spec = FilterSpec("name", "exact", "Bob")
        worker = SearchWorker(pf_small, column_filter=spec, offset=0, limit=100)
        _, total = _run(qtbot, worker)
        assert total == 1

    def test_search_no_filter(self, qapp, qtbot, pf_small):
        worker = SearchWorker(pf_small, offset=0, limit=100)
        _, total = _run(qtbot, worker)
        assert total == 5

    def test_search_progress(self, qapp, qtbot, pf_small):
        worker = SearchWorker(pf_small, query="Alice", offset=0, limit=100)
        progress_calls = []
        worker.progress.connect(lambda c, t: progress_calls.append((c, t)))
        _run(qtbot, worker)
        assert len(progress_calls) > 0

    def test_search_cancel(self, qapp, qtbot, pf_small):
        worker = SearchWorker(pf_small, query="Alice", offset=0, limit=100)
        worker.cancel()
        assert _run(qtbot, worker) == ()  # cancelled → result must be suppressed


class TestMultiFilterWorker:
    def test_basic(self, qapp, qtbot, pf_small):
        specs = [FilterSpec("name", "contains", "a")]
        worker = MultiFilterWorker(pf_small, specs, "AND", offset=0, limit=100)
        _, total = _run(qtbot, worker)
        assert total > 0

    def test_or_mode(self, qapp, qtbot, pf_small):
        specs = [
            FilterSpec("name", "exact", "Alice"),
            FilterSpec("name", "exact", "Bob"),
        ]
        worker = MultiFilterWorker(pf_small, specs, "OR", offset=0, limit=100)
        _, total = _run(qtbot, worker)
        assert total == 2


class TestStatsWorker:
    def test_basic(self, qapp, qtbot, pf_small):
        worker = StatsWorker(pf_small, "value")
        (stats,) = _run(qtbot, worker)
        assert stats["count"] == 5
        assert stats["mean"] == pytest.approx(30.0)

    def test_string_column(self, qapp, qtbot, pf_small):
        worker = StatsWorker(pf_small, "name")
        (stats,) = _run(qtbot, worker)
        assert stats["mean"] is None

    def test_cancel(self, qapp, qtbot, pf_small):
        worker = StatsWorker(pf_small, "value")
        worker.cancel()
        assert _run(qtbot, worker) == ()  # cancelled → result must be suppressed


class TestDistributionWorker:
    def test_basic(self, qapp, qtbot, pf_small):
        worker = DistributionWorker(pf_small, "name", top_n=3)
        (dist,) = _run(qtbot, worker)
        assert len(dist) <= 3
        assert all("value" in d for d in dist)


class TestExportWorker:
    def test_basic(self, qapp, qtbot, pf_small, tmp_path):
        path = str(tmp_path / "export.csv")
        worker = ExportWorker(pf_small, path)
        (out_path,) = _run(qtbot, worker)
        assert out_path == path
        assert os.path.exists(path)
        with open(path) as f:
            lines = f.readlines()
        assert len(lines) == 6  # header + 5 rows

    def test_with_search_query(self, qapp, qtbot, pf_small, tmp_path):
        path = str(tmp_path / "export.csv")
        worker = ExportWorker(pf_small, path, search_query="Alice")
        _run(qtbot, worker)
        with open(path) as f:
            lines = f.readlines()
        assert len(lines) == 2  # header + 1 row

    def test_with_column_filter(self, qapp, qtbot, pf_small, tmp_path):
        path = str(tmp_path / "export.csv")
        spec = FilterSpec("value", ">", "30")
        worker = ExportWorker(pf_small, path, column_filter=spec)
        _run(qtbot, worker)
        with open(path) as f:
            lines = f.readlines()
        assert len(lines) == 3  # header + 2 rows (40, 50)

    def test_progress(self, qapp, qtbot, pf_small, tmp_path):
        path = str(tmp_path / "export.csv")
        worker = ExportWorker(pf_small, path)
        progress_calls = []
        worker.progress.connect(lambda c, t: progress_calls.append((c, t)))
        _run(qtbot, worker)
        assert len(progress_calls) == pf_small.num_row_groups

    def test_cancel(self, qapp, qtbot, pf_small, tmp_path):
        path = str(tmp_path / "export.csv")
        worker = ExportWorker(pf_small, path)
        worker.cancel()
        assert _run(qtbot, worker) == ()  # cancelled → result must be suppressed
