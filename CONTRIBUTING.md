# Contributing

Thanks for considering a contribution!

## Dev setup

```bash
git clone https://github.com/maksymtymoshyk/parquet-tool.git
cd parquet-tool
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
pre-commit install
```

Python 3.9+ supported. PyQt6 needs system Qt libs on Linux:

```bash
sudo apt-get install libxkbcommon-x11-0 libxcb-cursor0 libegl1 libgl1
```

## Run tests

```bash
pytest                              # all tests
pytest --cov                        # with coverage
pytest tests/test_parquet_backend.py
pytest tests/test_data_model.py::TestSearch::test_set_search
```

GUI tests use `pytest-qt`. CI runs them headless via `QT_QPA_PLATFORM=offscreen`; you can do the same locally if your display is unavailable.

## Lint & format

```bash
ruff check .
ruff format .
pre-commit run --all-files
```

CI fails on lint/format errors. Pre-commit hooks fix most issues automatically.

## Branch / PR conventions

- Branch off `main`: `feat/<short-desc>`, `fix/<short-desc>`, `docs/<short-desc>`.
- One logical change per PR. Add tests for new behavior.
- Update `CHANGELOG.md` under `[Unreleased]`.
- PR title in imperative mood: "Add X", "Fix Y", "Refactor Z".

## Architecture

```
src/parquet_tool/
  main.py              entry point + QApplication setup
  parquet_viewer.py    QMainWindow: tabs, menus, drag-drop, status bar
  parquet_backend.py   pyarrow data layer: lazy load, metadata, search, filter
  data_model.py        QAbstractTableModel with pagination + column visibility
  gui_tabs.py          tab factories: data / schema / metadata / stats
  gui_utils.py         shared widgets, formatters, settings, theme
  workers.py           QThread workers: search, multi-filter, stats, export
  filter_builder.py    multi-condition AND/OR filter UI
  schema_diff.py       schema comparison dialog
  nested_viewer.py     tree viewer for nested struct/list/map
  file_session.py      per-file state container
```

## Release process

1. Bump `version` in `pyproject.toml` and `src/parquet_tool/__init__.py`.
2. Move `CHANGELOG.md` `[Unreleased]` items under a new `[X.Y.Z] - YYYY-MM-DD` heading.
3. Commit, tag: `git tag vX.Y.Z && git push --tags`.
4. The `release` workflow builds + publishes to PyPI via trusted publisher.

## Code of Conduct

This project follows the [Contributor Covenant](CODE_OF_CONDUCT.md). Be kind.
