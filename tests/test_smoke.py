"""Headless smoke test — package imports and viewer constructs under offscreen Qt."""

import os

import pytest


@pytest.fixture(autouse=True, scope="module")
def offscreen_qt():
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    yield


def test_package_imports():
    import parquet_tool

    assert parquet_tool.__version__


def test_viewer_constructs(qapp):
    from parquet_tool.gui_utils import SettingsManager, ThemeManager
    from parquet_tool.parquet_viewer import ParquetViewer

    settings = SettingsManager()
    theme = ThemeManager(is_dark=False)
    viewer = ParquetViewer(theme_manager=theme, settings=settings)
    assert viewer is not None
    viewer.close()


def test_viewer_opens_synthetic_file(qapp, synthetic_healthcare_parquet):
    from parquet_tool.gui_utils import SettingsManager, ThemeManager
    from parquet_tool.parquet_viewer import ParquetViewer

    settings = SettingsManager()
    theme = ThemeManager(is_dark=False)
    viewer = ParquetViewer(theme_manager=theme, settings=settings)
    viewer.open_file(synthetic_healthcare_parquet)

    assert viewer.file_tabs.count() >= 1
    viewer.close()
