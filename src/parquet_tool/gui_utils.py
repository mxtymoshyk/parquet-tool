import os
from dataclasses import dataclass

from PyQt6.QtCore import QSettings, Qt, pyqtSignal
from PyQt6.QtWidgets import (
    QComboBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QWidget,
)

FILTER_MODES = ["contains", "exact", "regex", ">", ">=", "<", "<=", "between"]


@dataclass
class FilterSpec:
    """Specification for a single column filter."""

    column: str
    mode: str  # contains, exact, regex, >, >=, <, <=, between
    value: str
    value2: str = ""


def format_size(size_bytes):
    """Format bytes to human-readable string."""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} PB"


def format_number(n):
    """Format number with comma separators."""
    return f"{n:,}"


class SearchBar(QWidget):
    """Search input with search/clear buttons."""

    searchRequested = pyqtSignal(str)
    cleared = pyqtSignal()

    def __init__(self, parent=None):
        super().__init__(parent)
        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)

        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText("Search across all columns...")
        self.search_input.returnPressed.connect(self._on_search)

        self.search_button = QPushButton("Search")
        self.search_button.clicked.connect(self._on_search)

        self.clear_button = QPushButton("Clear")
        self.clear_button.clicked.connect(self._on_clear)

        layout.addWidget(self.search_input, 1)
        layout.addWidget(self.search_button)
        layout.addWidget(self.clear_button)

    def _on_search(self):
        self.searchRequested.emit(self.search_input.text())

    def _on_clear(self):
        self.search_input.clear()
        self.cleared.emit()


class ColumnFilter(QWidget):
    """Column selector + mode + value filter input."""

    filterRequested = pyqtSignal(object)  # FilterSpec
    cleared = pyqtSignal()

    def __init__(self, parent=None):
        super().__init__(parent)
        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)

        layout.addWidget(QLabel("Column:"))

        self.column_combo = QComboBox()
        self.column_combo.setMinimumWidth(150)
        layout.addWidget(self.column_combo)

        self.mode_combo = QComboBox()
        self.mode_combo.addItems(FILTER_MODES)
        self.mode_combo.setFixedWidth(90)
        self.mode_combo.currentTextChanged.connect(self._on_mode_changed)
        layout.addWidget(self.mode_combo)

        self.filter_input = QLineEdit()
        self.filter_input.setPlaceholderText("Filter value...")
        self.filter_input.returnPressed.connect(self._on_filter)
        layout.addWidget(self.filter_input, 1)

        self.filter_input2 = QLineEdit()
        self.filter_input2.setPlaceholderText("Upper bound...")
        self.filter_input2.setVisible(False)
        self.filter_input2.returnPressed.connect(self._on_filter)
        layout.addWidget(self.filter_input2, 1)

        self.filter_button = QPushButton("Filter")
        self.filter_button.clicked.connect(self._on_filter)
        layout.addWidget(self.filter_button)

    def set_columns(self, columns):
        self.column_combo.clear()
        self.column_combo.addItems(columns)

    def _on_mode_changed(self, mode):
        self.filter_input2.setVisible(mode == "between")

    def _on_filter(self):
        col = self.column_combo.currentText()
        val = self.filter_input.text()
        mode = self.mode_combo.currentText()
        if col and val:
            val2 = self.filter_input2.text() if mode == "between" else ""
            self.filterRequested.emit(FilterSpec(col, mode, val, val2))

    def clear_filter(self):
        self.filter_input.clear()
        self.filter_input2.clear()
        self.cleared.emit()


class PaginationBar(QWidget):
    """Page navigation with first/prev/next/last buttons and go-to input."""

    pageChanged = pyqtSignal(int)
    rowHighlightRequested = pyqtSignal(int)

    def __init__(self, parent=None):
        super().__init__(parent)
        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)

        self.first_btn = QPushButton("|<<")
        self.prev_btn = QPushButton("< Prev")
        self.page_label = QLabel("Page 0 of 0")
        self.page_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.next_btn = QPushButton("Next >")
        self.last_btn = QPushButton(">>|")
        self.rows_label = QLabel("")

        self.goto_input = QLineEdit()
        self.goto_input.setPlaceholderText("Row #")
        self.goto_input.setFixedWidth(80)
        self.goto_input.returnPressed.connect(self._on_goto)

        self.goto_btn = QPushButton("Go")
        self.goto_btn.setFixedWidth(36)
        self.goto_btn.clicked.connect(self._on_goto)

        self.first_btn.setFixedWidth(40)
        self.last_btn.setFixedWidth(40)
        self.prev_btn.setFixedWidth(60)
        self.next_btn.setFixedWidth(60)

        self.first_btn.clicked.connect(lambda: self._go_to(0))
        self.prev_btn.clicked.connect(lambda: self._go_to(self._current - 1))
        self.next_btn.clicked.connect(lambda: self._go_to(self._current + 1))
        self.last_btn.clicked.connect(lambda: self._go_to(self._total_pages - 1))

        layout.addWidget(self.first_btn)
        layout.addWidget(self.prev_btn)
        layout.addStretch()
        layout.addWidget(self.page_label)
        layout.addStretch()
        layout.addWidget(self.goto_input)
        layout.addWidget(self.goto_btn)
        layout.addWidget(self.next_btn)
        layout.addWidget(self.last_btn)
        layout.addWidget(self.rows_label)

        self._current = 0
        self._total_pages = 0
        self._page_size = 1000
        self._total_rows = 0

    def update_state(self, current_page, total_pages, total_rows, page_size):
        self._current = current_page
        self._total_pages = total_pages
        self._page_size = page_size
        self._total_rows = total_rows

        self.page_label.setText(f"Page {current_page + 1} of {total_pages}")

        start_row = current_page * page_size + 1
        end_row = min((current_page + 1) * page_size, total_rows)
        if total_rows > 0:
            self.rows_label.setText(
                f"Rows {format_number(start_row)}-{format_number(end_row)} "
                f"of {format_number(total_rows)}"
            )
        else:
            self.rows_label.setText("No rows")

        self.first_btn.setEnabled(current_page > 0)
        self.prev_btn.setEnabled(current_page > 0)
        self.next_btn.setEnabled(current_page < total_pages - 1)
        self.last_btn.setEnabled(current_page < total_pages - 1)

    def _go_to(self, page):
        if 0 <= page < self._total_pages:
            self._current = page
            self.pageChanged.emit(page)

    def _on_goto(self):
        text = self.goto_input.text().strip()
        if not text:
            return
        try:
            n = int(text)
        except ValueError:
            return
        if n < 1 or n > self._total_rows:
            return
        # treat input as 1-based row number
        page = (n - 1) // self._page_size
        row_in_page = (n - 1) % self._page_size
        self._go_to(page)
        self.rowHighlightRequested.emit(row_in_page)
        self.goto_input.clear()


class ThemeManager:
    """Manages dark/light theme switching."""

    DARK_STYLE = """
        QMainWindow, QWidget {
            background-color: #2b2b2b;
            color: #e0e0e0;
        }
        QTableView, QTableWidget {
            background-color: #1e1e1e;
            color: #e0e0e0;
            gridline-color: #404040;
            selection-background-color: #264f78;
        }
        QHeaderView::section {
            background-color: #383838;
            color: #e0e0e0;
            border: 1px solid #404040;
            padding: 4px;
        }
        QTreeWidget {
            background-color: #1e1e1e;
            color: #e0e0e0;
        }
        QTreeWidget::item:selected {
            background-color: #264f78;
        }
        QTabWidget::pane {
            border: 1px solid #404040;
        }
        QTabBar::tab {
            background-color: #383838;
            color: #e0e0e0;
            border: 1px solid #404040;
            padding: 6px 12px;
        }
        QTabBar::tab:selected {
            background-color: #2b2b2b;
            border-bottom: 2px solid #4a9eff;
        }
        QLineEdit, QComboBox, QSpinBox {
            background-color: #383838;
            color: #e0e0e0;
            border: 1px solid #555555;
            padding: 4px;
        }
        QPushButton {
            background-color: #383838;
            color: #e0e0e0;
            border: 1px solid #555555;
            padding: 4px 12px;
        }
        QPushButton:hover {
            background-color: #454545;
        }
        QPushButton:disabled {
            color: #666666;
        }
        QGroupBox {
            color: #e0e0e0;
            border: 1px solid #404040;
            margin-top: 6px;
            padding-top: 10px;
        }
        QGroupBox::title {
            subcontrol-origin: margin;
            padding: 0 3px;
        }
        QStatusBar {
            background-color: #383838;
            color: #e0e0e0;
        }
        QMenuBar {
            background-color: #383838;
            color: #e0e0e0;
        }
        QMenuBar::item:selected {
            background-color: #454545;
        }
        QMenu {
            background-color: #383838;
            color: #e0e0e0;
        }
        QMenu::item:selected {
            background-color: #264f78;
        }
        QSplitter::handle {
            background-color: #404040;
        }
    """

    LIGHT_STYLE = ""  # default fusion theme

    def __init__(self, is_dark=False):
        self.is_dark = is_dark

    def toggle(self, app):
        self.is_dark = not self.is_dark
        self.apply(app)

    def apply(self, app):
        app.setStyleSheet(self.DARK_STYLE if self.is_dark else self.LIGHT_STYLE)


class SettingsManager:
    """Persistent application settings via QSettings."""

    MAX_RECENT = 10

    def __init__(self):
        self._s = QSettings("parquet-tool", "ParquetTool")

    # -- theme --

    @property
    def is_dark(self):
        return self._s.value("theme/isDark", False, type=bool)

    @is_dark.setter
    def is_dark(self, value):
        self._s.setValue("theme/isDark", value)

    # -- recent files --

    @property
    def recent_files(self):
        import json

        val = self._s.value("recentFiles", "[]")
        if not val:
            return []
        try:
            result = json.loads(val)
            return [p for p in result if isinstance(p, str) and os.path.exists(p)]
        except (json.JSONDecodeError, TypeError):
            # migration from old format: single string or broken list
            if isinstance(val, str) and not val.startswith("["):
                return [val]
            return []

    @recent_files.setter
    def recent_files(self, paths):
        import json

        self._s.setValue("recentFiles", json.dumps(paths[: self.MAX_RECENT]))

    # -- window geometry / state --

    @property
    def window_geometry(self):
        return self._s.value("window/geometry")

    @window_geometry.setter
    def window_geometry(self, value):
        self._s.setValue("window/geometry", value)

    @property
    def window_state(self):
        return self._s.value("window/state")

    @window_state.setter
    def window_state(self, value):
        self._s.setValue("window/state", value)
