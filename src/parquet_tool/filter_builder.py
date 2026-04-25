from PyQt6.QtCore import pyqtSignal
from PyQt6.QtWidgets import (
    QComboBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QVBoxLayout,
    QWidget,
)

from .gui_utils import FILTER_MODES, FilterSpec


class FilterConditionRow(QWidget):
    """Single filter condition row with column/mode/value and remove button."""

    changed = pyqtSignal()
    removed = pyqtSignal(object)

    def __init__(self, columns, parent=None):
        super().__init__(parent)
        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 2, 0, 2)

        self.column_combo = QComboBox()
        self.column_combo.addItems(columns)
        self.column_combo.setMinimumWidth(120)
        layout.addWidget(self.column_combo)

        self.mode_combo = QComboBox()
        self.mode_combo.addItems(FILTER_MODES)
        self.mode_combo.setFixedWidth(90)
        self.mode_combo.currentTextChanged.connect(self._on_mode_changed)
        layout.addWidget(self.mode_combo)

        self.value_input = QLineEdit()
        self.value_input.setPlaceholderText("Value...")
        layout.addWidget(self.value_input, 1)

        self.value_input2 = QLineEdit()
        self.value_input2.setPlaceholderText("Upper bound...")
        self.value_input2.setVisible(False)
        layout.addWidget(self.value_input2, 1)

        self.remove_btn = QPushButton("X")
        self.remove_btn.setFixedWidth(28)
        self.remove_btn.clicked.connect(lambda: self.removed.emit(self))
        layout.addWidget(self.remove_btn)

    def _on_mode_changed(self, mode):
        self.value_input2.setVisible(mode == "between")

    def to_filter_spec(self):
        col = self.column_combo.currentText()
        mode = self.mode_combo.currentText()
        val = self.value_input.text()
        val2 = self.value_input2.text() if mode == "between" else ""
        if col and val:
            return FilterSpec(col, mode, val, val2)
        return None


class FilterBuilder(QWidget):
    """Multi-condition filter builder with AND/OR join mode."""

    filterRequested = pyqtSignal(list, str)  # (list[FilterSpec], join_mode)
    cleared = pyqtSignal()

    def __init__(self, parent=None):
        super().__init__(parent)
        self._columns = []
        self._rows = []

        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(0, 0, 0, 0)

        # header row: join mode + add/apply/clear buttons
        header = QHBoxLayout()

        header.addWidget(QLabel("Join:"))
        self.join_combo = QComboBox()
        self.join_combo.addItems(["AND", "OR"])
        self.join_combo.setFixedWidth(60)
        header.addWidget(self.join_combo)

        self.add_btn = QPushButton("+ Add Condition")
        self.add_btn.clicked.connect(self._add_row)
        header.addWidget(self.add_btn)

        header.addStretch()

        self.apply_btn = QPushButton("Apply")
        self.apply_btn.clicked.connect(self._on_apply)
        header.addWidget(self.apply_btn)

        self.clear_btn = QPushButton("Clear")
        self.clear_btn.clicked.connect(self._on_clear)
        header.addWidget(self.clear_btn)

        main_layout.addLayout(header)

        # container for condition rows
        self._rows_container = QVBoxLayout()
        self._rows_container.setContentsMargins(0, 0, 0, 0)
        main_layout.addLayout(self._rows_container)

    def set_columns(self, columns):
        self._columns = list(columns)
        # clear existing rows and add one default
        self._clear_rows()
        self._add_row()

    def _add_row(self):
        row = FilterConditionRow(self._columns)
        row.removed.connect(self._remove_row)
        self._rows.append(row)
        self._rows_container.addWidget(row)

    def _remove_row(self, row):
        if len(self._rows) <= 1:
            return  # keep at least one
        self._rows.remove(row)
        self._rows_container.removeWidget(row)
        row.deleteLater()

    def _clear_rows(self):
        for row in self._rows:
            self._rows_container.removeWidget(row)
            row.deleteLater()
        self._rows.clear()

    def _on_apply(self):
        specs = []
        for row in self._rows:
            spec = row.to_filter_spec()
            if spec:
                specs.append(spec)
        if specs:
            self.filterRequested.emit(specs, self.join_combo.currentText())

    def _on_clear(self):
        self._clear_rows()
        self._add_row()
        self.cleared.emit()
