# Changelog

All notable changes to this project are documented here. Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-04-25

### Added

- Initial public release.
- Paginated parquet data viewer with sortable columns and 1000 rows per page.
- Full-text search across all columns.
- Per-column filter with 8 modes (contains, exact, regex, numeric comparisons, between).
- Multi-condition AND/OR advanced filter builder.
- Schema inspector with nested type support (struct/list/map).
- File metadata + row group + per-column chunk inspector.
- Column statistics (count, nulls, unique, min, max, mean).
- Top-N value distribution per column.
- Schema diff dialog with color-coded changes.
- Streaming CSV export honoring active filters.
- Nested data tree viewer for struct/list/map cells.
- Multi-file tabs, drag-and-drop, persistent settings, dark/light theme.
- CLI entry point: `parquet-tool [path]`.
- pytest + pytest-qt test suite.
- GitHub Actions CI: tests on Linux/macOS/Windows × Python 3.9-3.12.
- PyPI release workflow.
