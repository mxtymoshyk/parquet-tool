import json

from PyQt6.QtGui import QFont
from PyQt6.QtWidgets import (
    QApplication,
    QHBoxLayout,
    QPushButton,
    QTreeWidget,
    QTreeWidgetItem,
    QVBoxLayout,
    QWidget,
)


class NestedDataViewer(QWidget):
    """Tree viewer for nested Parquet values (structs, lists, maps)."""

    def __init__(self, parent=None):
        super().__init__(parent)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)

        # toolbar
        toolbar = QHBoxLayout()
        self.expand_btn = QPushButton("Expand All")
        self.expand_btn.setFixedWidth(80)
        self.expand_btn.clicked.connect(lambda: self.tree.expandAll())
        toolbar.addWidget(self.expand_btn)

        self.collapse_btn = QPushButton("Collapse All")
        self.collapse_btn.setFixedWidth(80)
        self.collapse_btn.clicked.connect(lambda: self.tree.collapseAll())
        toolbar.addWidget(self.collapse_btn)

        self.copy_btn = QPushButton("Copy JSON")
        self.copy_btn.setFixedWidth(80)
        self.copy_btn.clicked.connect(self._copy_json)
        toolbar.addWidget(self.copy_btn)

        toolbar.addStretch()
        layout.addLayout(toolbar)

        # tree
        self.tree = QTreeWidget()
        self.tree.setHeaderLabels(["Key", "Value", "Type"])
        self.tree.setColumnWidth(0, 180)
        self.tree.setColumnWidth(1, 300)
        self.tree.setAlternatingRowColors(True)
        mono = QFont("Menlo")
        mono.setStyleHint(QFont.StyleHint.Monospace)
        self.tree.setFont(mono)
        layout.addWidget(self.tree)

        self._raw_value = None
        self._column_name = ""

    def set_value(self, value, column_name=""):
        """Display a nested value in the tree."""
        self._raw_value = value
        self._column_name = column_name
        self.tree.clear()

        if value is None:
            QTreeWidgetItem(self.tree, [column_name, "null", "null"])
            return

        self._add_value(self.tree, column_name, value)
        self.tree.expandAll()

    def clear(self):
        self.tree.clear()
        self._raw_value = None

    def _add_value(self, parent, key, value):
        """Recursively add a value to the tree."""
        type_name = type(value).__name__

        if isinstance(value, dict):
            item = QTreeWidgetItem(
                parent,
                [str(key), f"{{{len(value)} fields}}", "struct"],
            )
            for k, v in value.items():
                self._add_value(item, str(k), v)

        elif isinstance(value, list):
            item = QTreeWidgetItem(
                parent,
                [str(key), f"[{len(value)} items]", "list"],
            )
            for i, v in enumerate(value):
                self._add_value(item, str(i), v)

        else:
            display = str(value) if value is not None else "null"
            QTreeWidgetItem(parent, [str(key), display, type_name])

    def _copy_json(self):
        if self._raw_value is not None:
            text = json.dumps(self._raw_value, indent=2, default=str)
            QApplication.clipboard().setText(text)
