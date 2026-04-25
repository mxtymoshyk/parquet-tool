import os

import pyarrow as pa
from PyQt6.QtGui import QBrush, QColor, QFont
from PyQt6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QDialog,
    QFileDialog,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QTreeWidget,
    QTreeWidgetItem,
    QVBoxLayout,
)

from .parquet_backend import ParquetDirectory, ParquetFile

# colors for diff status
COLOR_MATCH = QColor("#ffffff")
COLOR_ADDED = QColor("#c8e6c9")  # green
COLOR_REMOVED = QColor("#ffcdd2")  # red
COLOR_CHANGED = QColor("#fff9c4")  # yellow

COLOR_MATCH_DARK = QColor("#2b2b2b")
COLOR_ADDED_DARK = QColor("#1b5e20")
COLOR_REMOVED_DARK = QColor("#b71c1c")
COLOR_CHANGED_DARK = QColor("#f57f17")


def _type_label(t):
    """Short label for a type (e.g. 'struct' instead of full struct<...>)."""
    if pa.types.is_struct(t):
        return "struct"
    if pa.types.is_list(t) or pa.types.is_large_list(t):
        return f"list<{_type_label(t.value_type)}>"
    if pa.types.is_map(t):
        return f"map<{_type_label(t.key_type)}, {_type_label(t.item_type)}>"
    return str(t)


def _diff_types(type_a, type_b):
    """Recursively compare two pyarrow types. Returns children diffs."""
    children = []

    # both structs: recurse into fields
    if pa.types.is_struct(type_a) and pa.types.is_struct(type_b):
        fa_map = {type_a.field(i).name: type_a.field(i) for i in range(type_a.num_fields)}
        fb_map = {type_b.field(i).name: type_b.field(i) for i in range(type_b.num_fields)}
        all_names = list(dict.fromkeys(list(fa_map.keys()) + list(fb_map.keys())))
        for name in all_names:
            children.append(_diff_field(fa_map.get(name), fb_map.get(name), name))

    # both lists: compare element type
    elif _is_list(type_a) and _is_list(type_b):
        va, vb = type_a.value_type, type_b.value_type
        sub = _diff_types(va, vb)
        status = "match" if str(va) == str(vb) else "changed"
        children.append(
            {
                "name": "<element>",
                "type_a": _type_label(va),
                "type_b": _type_label(vb),
                "nullable_a": None,
                "nullable_b": None,
                "status": status,
                "children": sub,
            }
        )

    # both maps: compare key and item
    elif pa.types.is_map(type_a) and pa.types.is_map(type_b):
        for label, ta, tb in [
            ("<key>", type_a.key_type, type_b.key_type),
            ("<value>", type_a.item_type, type_b.item_type),
        ]:
            sub = _diff_types(ta, tb)
            status = "match" if str(ta) == str(tb) else "changed"
            children.append(
                {
                    "name": label,
                    "type_a": _type_label(ta),
                    "type_b": _type_label(tb),
                    "nullable_a": None,
                    "nullable_b": None,
                    "status": status,
                    "children": sub,
                }
            )

    return children


def _is_list(t):
    return pa.types.is_list(t) or pa.types.is_large_list(t)


def _diff_field(fa, fb, name):
    """Compare two fields (either may be None)."""
    if fa and not fb:
        return {
            "name": name,
            "type_a": _type_label(fa.type),
            "type_b": "-",
            "nullable_a": fa.nullable,
            "nullable_b": None,
            "status": "removed",
            "children": _leaf_children(fa.type, None),
        }
    if fb and not fa:
        return {
            "name": name,
            "type_a": "-",
            "type_b": _type_label(fb.type),
            "nullable_a": None,
            "nullable_b": fb.nullable,
            "status": "added",
            "children": _leaf_children(None, fb.type),
        }

    type_match = str(fa.type) == str(fb.type)
    null_match = fa.nullable == fb.nullable
    children = _diff_types(fa.type, fb.type)
    status = "match" if (type_match and null_match) else "changed"
    return {
        "name": name,
        "type_a": _type_label(fa.type),
        "type_b": _type_label(fb.type),
        "nullable_a": fa.nullable,
        "nullable_b": fb.nullable,
        "status": status,
        "children": children,
    }


def _leaf_children(type_a, type_b):
    """Build child tree for a field that only exists on one side."""
    t = type_a or type_b
    if t is None:
        return []
    side = "a" if type_a else "b"
    children = []
    if pa.types.is_struct(t):
        for i in range(t.num_fields):
            f = t.field(i)
            entry = {
                "name": f.name,
                "type_a": _type_label(f.type) if side == "a" else "-",
                "type_b": _type_label(f.type) if side == "b" else "-",
                "nullable_a": f.nullable if side == "a" else None,
                "nullable_b": f.nullable if side == "b" else None,
                "status": "removed" if side == "a" else "added",
                "children": _leaf_children(f.type, None)
                if side == "a"
                else _leaf_children(None, f.type),
            }
            children.append(entry)
    return children


def diff_schemas(schema_a, schema_b):
    """Compare two pyarrow schemas and return list of differences.

    Returns list of dicts: {name, type_a, type_b, status, children}.
    Status: "match", "added", "removed", "changed".
    Children contain recursive diffs for nested types (struct fields,
    list elements, map key/values).
    """
    fields_a = {f.name: f for f in schema_a}
    fields_b = {f.name: f for f in schema_b}

    all_names = list(dict.fromkeys(list(fields_a.keys()) + list(fields_b.keys())))

    return [_diff_field(fields_a.get(n), fields_b.get(n), n) for n in all_names]


def _subtree_has_diff(diff):
    """Check if this diff node or any descendant has a non-match status."""
    if diff["status"] != "match":
        return True
    return any(_subtree_has_diff(c) for c in diff.get("children", []))


class SchemaDiffDialog(QDialog):
    """Dialog for comparing schemas of two parquet files."""

    def __init__(self, parent=None, initial_path="", open_sessions=None, is_dark=False):
        super().__init__(parent)
        self.setWindowTitle("Compare Schemas")
        self.setMinimumSize(800, 600)

        self._open_sessions = open_sessions or {}
        self._is_dark = is_dark

        layout = QVBoxLayout(self)

        # file A selector
        row_a = QHBoxLayout()
        row_a.addWidget(QLabel("File A:"))
        self.path_a = QLineEdit(initial_path)
        row_a.addWidget(self.path_a, 1)
        browse_a = QPushButton("Browse...")
        browse_a.clicked.connect(lambda: self._browse(self.path_a))
        row_a.addWidget(browse_a)

        if open_sessions:
            self.open_combo_a = QComboBox()
            self.open_combo_a.addItem("(browse)")
            for p in open_sessions:
                self.open_combo_a.addItem(os.path.basename(p), p)
            self.open_combo_a.currentIndexChanged.connect(
                lambda i: self._select_open(self.open_combo_a, self.path_a)
            )
            row_a.addWidget(self.open_combo_a)

        layout.addLayout(row_a)

        # file B selector
        row_b = QHBoxLayout()
        row_b.addWidget(QLabel("File B:"))
        self.path_b = QLineEdit()
        row_b.addWidget(self.path_b, 1)
        browse_b = QPushButton("Browse...")
        browse_b.clicked.connect(lambda: self._browse(self.path_b))
        row_b.addWidget(browse_b)

        if open_sessions:
            self.open_combo_b = QComboBox()
            self.open_combo_b.addItem("(browse)")
            for p in open_sessions:
                self.open_combo_b.addItem(os.path.basename(p), p)
            self.open_combo_b.currentIndexChanged.connect(
                lambda i: self._select_open(self.open_combo_b, self.path_b)
            )
            row_b.addWidget(self.open_combo_b)

        layout.addLayout(row_b)

        # compare button + filter
        btn_row = QHBoxLayout()
        compare_btn = QPushButton("Compare")
        compare_btn.clicked.connect(self._compare)
        btn_row.addStretch()
        btn_row.addWidget(compare_btn)
        btn_row.addStretch()

        self.diff_only_cb = QCheckBox("Differences only")
        self.diff_only_cb.toggled.connect(self._apply_diff_filter)
        btn_row.addWidget(self.diff_only_cb)

        layout.addLayout(btn_row)

        # results tree
        self.tree = QTreeWidget()
        self.tree.setHeaderLabels(["Name", "File A Type", "File B Type", "Status"])
        self.tree.setColumnWidth(0, 250)
        self.tree.setColumnWidth(1, 250)
        self.tree.setColumnWidth(2, 250)
        self.tree.setAlternatingRowColors(True)
        layout.addWidget(self.tree)

        # summary label
        self.summary = QLabel("")
        layout.addWidget(self.summary)

        self._last_diffs = []

    def _browse(self, line_edit):
        path, _ = QFileDialog.getOpenFileName(
            self,
            "Select Parquet File",
            "",
            "Parquet Files (*.parquet);;All Files (*)",
        )
        if path:
            line_edit.setText(path)

    def _select_open(self, combo, line_edit):
        path = combo.currentData()
        if path:
            line_edit.setText(path)

    def _compare(self):
        path_a = self.path_a.text().strip()
        path_b = self.path_b.text().strip()
        if not path_a or not path_b:
            return

        try:
            schema_a = self._load_schema(path_a)
            schema_b = self._load_schema(path_b)
        except Exception as e:
            self.summary.setText(f"Error: {e}")
            return

        self._last_diffs = diff_schemas(schema_a, schema_b)
        diff_only = self.diff_only_cb.isChecked()
        self._populate_tree(self._last_diffs, diff_only)

        # summary counts all top-level fields
        added = sum(1 for d in self._last_diffs if d["status"] == "added")
        removed = sum(1 for d in self._last_diffs if d["status"] == "removed")
        changed = sum(1 for d in self._last_diffs if d["status"] == "changed")
        matched = sum(1 for d in self._last_diffs if d["status"] == "match")
        self.summary.setText(
            f"{len(self._last_diffs)} columns: {matched} match, "
            f"{added} added, {removed} removed, {changed} changed"
        )

    def _load_schema(self, path):
        # check if we have an open session for this path
        if path in self._open_sessions:
            return self._open_sessions[path].pf.schema

        if os.path.isdir(path):
            return ParquetDirectory(path).schema
        return ParquetFile(path).schema

    def _apply_diff_filter(self):
        if self._last_diffs:
            self._populate_tree(self._last_diffs, self.diff_only_cb.isChecked())

    def _populate_tree(self, diffs, diff_only=False):
        self.tree.clear()
        colors = self._get_colors()
        bold_font = QFont()
        bold_font.setBold(True)

        for diff in diffs:
            if diff_only and not _subtree_has_diff(diff):
                continue
            item = self._add_tree_item(self.tree, diff, colors, bold_font, diff_only)
            # auto-expand items that contain nested diffs
            if diff["children"] and _subtree_has_diff(diff):
                self._expand_diff_path(item)

        for col in range(4):
            self.tree.resizeColumnToContents(col)

    def _get_colors(self):
        if self._is_dark:
            return {
                "match": COLOR_MATCH_DARK,
                "added": COLOR_ADDED_DARK,
                "removed": COLOR_REMOVED_DARK,
                "changed": COLOR_CHANGED_DARK,
            }
        return {
            "match": COLOR_MATCH,
            "added": COLOR_ADDED,
            "removed": COLOR_REMOVED,
            "changed": COLOR_CHANGED,
        }

    def _add_tree_item(self, parent, diff, colors, bold_font, diff_only):
        item = QTreeWidgetItem(
            parent,
            [
                diff["name"],
                diff["type_a"],
                diff["type_b"],
                diff["status"].upper(),
            ],
        )
        color = colors.get(diff["status"], colors["match"])
        brush = QBrush(color)
        for col in range(4):
            item.setBackground(col, brush)

        # bold items that have diffs in their subtree
        if diff["status"] != "match" or _subtree_has_diff(diff):
            for col in range(4):
                item.setFont(col, bold_font)

        for child in diff.get("children", []):
            if diff_only and not _subtree_has_diff(child):
                continue
            self._add_tree_item(item, child, colors, bold_font, diff_only)

        return item

    def _expand_diff_path(self, item):
        """Expand this item and any children that contain diffs."""
        item.setExpanded(True)
        for i in range(item.childCount()):
            child = item.child(i)
            # expand if status is not MATCH or if it has children with diffs
            if child.text(3) != "MATCH" or child.childCount() > 0:
                self._expand_diff_path(child)
