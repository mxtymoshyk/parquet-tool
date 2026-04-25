from PyQt6.QtCore import Qt
from PyQt6.QtGui import QFont
from PyQt6.QtWidgets import (
    QAbstractItemView,
    QComboBox,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QMenu,
    QPlainTextEdit,
    QPushButton,
    QSplitter,
    QStackedWidget,
    QTabBar,
    QTableView,
    QTableWidget,
    QTableWidgetItem,
    QTreeWidget,
    QTreeWidgetItem,
    QVBoxLayout,
    QWidget,
)

from .filter_builder import FilterBuilder
from .gui_utils import (
    ColumnFilter,
    PaginationBar,
    SearchBar,
    format_number,
    format_size,
)
from .nested_viewer import NestedDataViewer

# -- data tab --


def create_data_tab(viewer):
    """Create the data viewing tab with search, table, pagination, and JSON row view."""
    widget = QWidget()
    outer = QVBoxLayout(widget)
    outer.setContentsMargins(0, 0, 0, 0)

    splitter = QSplitter(Qt.Orientation.Vertical)

    top = QWidget()
    layout = QVBoxLayout(top)
    layout.setContentsMargins(4, 4, 0, 4)

    search_bar = SearchBar()
    layout.addWidget(search_bar)

    column_filter = ColumnFilter()
    layout.addWidget(column_filter)

    # advanced filter toggle + builder
    advanced_toggle = QPushButton("Advanced Filter...")
    advanced_toggle.setCheckable(True)
    advanced_toggle.setFixedWidth(140)
    layout.addWidget(advanced_toggle)

    filter_builder = FilterBuilder()
    filter_builder.setVisible(False)
    layout.addWidget(filter_builder)

    def _toggle_advanced(checked):
        filter_builder.setVisible(checked)
        advanced_toggle.setText("Simple Filter..." if checked else "Advanced Filter...")

    advanced_toggle.toggled.connect(_toggle_advanced)

    table_view = QTableView()
    table_view.setAlternatingRowColors(True)
    table_view.setSortingEnabled(True)
    table_view.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
    table_view.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
    header = table_view.horizontalHeader()
    header.setStretchLastSection(True)
    header.setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
    header.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
    header.customContextMenuRequested.connect(
        lambda pos: _show_column_visibility_menu(viewer, table_view, pos)
    )
    table_view.verticalHeader().setDefaultSectionSize(24)
    layout.addWidget(table_view, 1)

    pagination = PaginationBar()
    layout.addWidget(pagination)

    splitter.addWidget(top)

    json_group = QGroupBox("JSON")
    json_inner = QVBoxLayout(json_group)

    # tab bar to switch between Row JSON and Cell Tree
    detail_tabs = QTabBar()
    detail_tabs.addTab("Row JSON")
    detail_tabs.addTab("Cell Tree")
    json_inner.addWidget(detail_tabs)

    detail_stack = QStackedWidget()

    json_pane = QPlainTextEdit()
    json_pane.setReadOnly(True)
    json_pane.setPlaceholderText("Select a row to view it as JSON")
    mono = QFont("Menlo")
    mono.setStyleHint(QFont.StyleHint.Monospace)
    json_pane.setFont(mono)
    detail_stack.addWidget(json_pane)

    nested_viewer = NestedDataViewer()
    detail_stack.addWidget(nested_viewer)

    detail_tabs.currentChanged.connect(detail_stack.setCurrentIndex)
    json_inner.addWidget(detail_stack)
    splitter.addWidget(json_group)

    splitter.setSizes([600, 200])
    outer.addWidget(splitter)

    viewer.data_table_view = table_view
    viewer.search_bar = search_bar
    viewer.column_filter = column_filter
    viewer.filter_builder = filter_builder
    viewer.pagination_bar = pagination
    viewer.json_pane = json_pane
    viewer.json_group = json_group
    viewer.nested_viewer = nested_viewer
    viewer.detail_stack = detail_stack
    viewer.detail_tabs = detail_tabs

    return widget


def _show_column_visibility_menu(viewer, table_view, pos):
    """Show context menu to toggle column visibility."""
    model = table_view.model()
    if model is None:
        return

    menu = QMenu(table_view)

    # show all action
    show_all = menu.addAction("Show All Columns")
    menu.addSeparator()

    # one checkable action per column
    actions = []
    visible_set = set(model.visible_columns)
    for col_name in model.column_names:
        action = menu.addAction(col_name)
        action.setCheckable(True)
        action.setChecked(col_name in visible_set)
        actions.append((action, col_name))

    chosen = menu.exec(table_view.horizontalHeader().mapToGlobal(pos))
    if chosen is None:
        return

    if chosen == show_all:
        model.set_visible_columns(model.column_names)
        viewer.column_filter.set_columns(model.column_names)
        return

    # toggle the selected column
    for action, col_name in actions:
        if chosen == action:
            if action.isChecked():
                visible_set.add(col_name)
            # keep at least one column visible
            elif len(visible_set) > 1:
                visible_set.discard(col_name)
            break

    new_visible = [c for c in model.column_names if c in visible_set]
    model.set_visible_columns(new_visible)
    viewer.column_filter.set_columns(new_visible)


# -- schema tab --


def create_schema_tab(viewer):
    """Create the schema viewing tab with tree widget."""
    widget = QWidget()
    layout = QVBoxLayout(widget)

    tree = QTreeWidget()
    tree.setHeaderLabels(["Name", "Type", "Nullable"])
    tree.setColumnWidth(0, 250)
    tree.setColumnWidth(1, 200)
    tree.setAlternatingRowColors(True)
    layout.addWidget(tree)

    viewer.schema_tree = tree
    return widget


def populate_schema_tree(tree, schema_info):
    """Populate the schema tree widget with column info."""
    tree.clear()
    for field in schema_info:
        _add_field_item(tree, None, field)
    tree.expandAll()


def _add_field_item(tree, parent, field):
    """Recursively add a field to the tree."""
    nullable = "NULLABLE" if field["nullable"] else "NOT NULL"
    values = [field["name"], field["type"], nullable]

    if parent is None:
        item = QTreeWidgetItem(tree, values)
    else:
        item = QTreeWidgetItem(parent, values)

    for child in field.get("children", []):
        _add_field_item(tree, item, child)

    return item


# -- metadata tab --


def create_metadata_tab(viewer):
    """Create the metadata inspection tab."""
    widget = QWidget()
    layout = QVBoxLayout(widget)

    splitter = QSplitter(Qt.Orientation.Vertical)

    # file metadata
    file_group = QGroupBox("File Metadata")
    file_form = QFormLayout(file_group)

    viewer.meta_labels = {}
    for key, display in [
        ("path", "Path"),
        ("created_by", "Created By"),
        ("format_version", "Format Version"),
        ("num_rows", "Total Rows"),
        ("num_row_groups", "Row Groups"),
        ("serialized_size", "Metadata Size"),
        ("file_size", "File Size"),
    ]:
        label = QLabel("-")
        label.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        file_form.addRow(f"{display}:", label)
        viewer.meta_labels[key] = label

    splitter.addWidget(file_group)

    # row group overview
    rg_group = QGroupBox("Row Groups")

    rg_table = QTableWidget()
    rg_table.setColumnCount(4)
    rg_table.setHorizontalHeaderLabels(["RG #", "Rows", "Total Size", "Compression"])
    rg_table.horizontalHeader().setStretchLastSection(True)
    rg_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
    rg_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
    QVBoxLayout(rg_group).addWidget(rg_table)

    viewer.rg_table = rg_table
    splitter.addWidget(rg_group)

    # column chunk detail
    chunks_group = QGroupBox("Column Chunks (select a row group above)")

    chunks_table = QTableWidget()
    chunks_table.setColumnCount(7)
    chunks_table.setHorizontalHeaderLabels(
        [
            "Column",
            "Compression",
            "Compressed Size",
            "Uncompressed Size",
            "Min",
            "Max",
            "Null Count",
        ]
    )
    chunks_table.horizontalHeader().setStretchLastSection(True)
    chunks_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
    QVBoxLayout(chunks_group).addWidget(chunks_table)

    viewer.chunks_table = chunks_table
    viewer.chunks_group = chunks_group
    splitter.addWidget(chunks_group)

    splitter.setSizes([150, 200, 300])
    layout.addWidget(splitter)

    return widget


def populate_file_metadata(viewer, metadata):
    """Fill in the file metadata labels."""
    viewer.meta_labels["path"].setText(metadata["path"])
    viewer.meta_labels["created_by"].setText(metadata["created_by"])
    viewer.meta_labels["format_version"].setText(metadata["format_version"])
    viewer.meta_labels["num_rows"].setText(format_number(metadata["num_rows"]))
    viewer.meta_labels["num_row_groups"].setText(str(metadata["num_row_groups"]))
    viewer.meta_labels["serialized_size"].setText(format_size(metadata["serialized_size"]))
    viewer.meta_labels["file_size"].setText(format_size(metadata["file_size"]))


def populate_row_groups(viewer, pf):
    """Fill in the row groups table from parquet metadata."""
    viewer.rg_table.setRowCount(pf.num_row_groups)

    for i in range(pf.num_row_groups):
        rg = pf.get_row_group_metadata(i)
        viewer.rg_table.setItem(i, 0, QTableWidgetItem(str(i)))
        viewer.rg_table.setItem(i, 1, QTableWidgetItem(format_number(rg["num_rows"])))
        viewer.rg_table.setItem(i, 2, QTableWidgetItem(format_size(rg["total_byte_size"])))
        compressions = {col["compression"] for col in rg["columns"]}
        viewer.rg_table.setItem(i, 3, QTableWidgetItem(", ".join(sorted(compressions))))


def populate_column_chunks(viewer, pf, rg_index):
    """Fill in column chunks for a specific row group."""
    rg_meta = pf.get_row_group_metadata(rg_index)
    columns = rg_meta["columns"]

    viewer.chunks_group.setTitle(f"Column Chunks (Row Group {rg_index})")
    viewer.chunks_table.setRowCount(len(columns))

    for i, col in enumerate(columns):
        viewer.chunks_table.setItem(i, 0, QTableWidgetItem(col["name"]))
        viewer.chunks_table.setItem(i, 1, QTableWidgetItem(col["compression"]))
        viewer.chunks_table.setItem(
            i, 2, QTableWidgetItem(format_size(col["total_compressed_size"]))
        )
        viewer.chunks_table.setItem(
            i,
            3,
            QTableWidgetItem(format_size(col["total_uncompressed_size"])),
        )

        stats = col.get("statistics")
        if stats:
            min_val = stats.get("min")
            max_val = stats.get("max")
            null_count = stats.get("null_count")
            viewer.chunks_table.setItem(
                i, 4, QTableWidgetItem(str(min_val) if min_val is not None else "N/A")
            )
            viewer.chunks_table.setItem(
                i, 5, QTableWidgetItem(str(max_val) if max_val is not None else "N/A")
            )
            viewer.chunks_table.setItem(
                i,
                6,
                QTableWidgetItem(format_number(null_count) if null_count is not None else "N/A"),
            )
        else:
            for col_idx in (4, 5, 6):
                viewer.chunks_table.setItem(i, col_idx, QTableWidgetItem("N/A"))


# -- statistics tab --


def create_stats_tab(viewer):
    """Create the column statistics tab."""
    widget = QWidget()
    layout = QVBoxLayout(widget)

    # column selector
    selector_layout = QHBoxLayout()
    selector_layout.addWidget(QLabel("Column:"))

    column_combo = QComboBox()
    column_combo.setMinimumWidth(200)
    selector_layout.addWidget(column_combo, 1)

    layout.addLayout(selector_layout)

    # stats display
    stats_group = QGroupBox("Statistics")
    stats_form = QFormLayout(stats_group)

    viewer.stats_labels = {}
    for key, display in [
        ("type", "Type"),
        ("count", "Count"),
        ("null_count", "Null Count"),
        ("valid_count", "Valid Count"),
        ("unique_count", "Unique Count"),
        ("min", "Min"),
        ("max", "Max"),
        ("mean", "Mean"),
    ]:
        label = QLabel("-")
        label.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        stats_form.addRow(f"{display}:", label)
        viewer.stats_labels[key] = label

    layout.addWidget(stats_group)

    # value distribution
    dist_group = QGroupBox("Value Distribution")
    dist_layout = QVBoxLayout(dist_group)

    top_n_layout = QHBoxLayout()
    top_n_layout.addWidget(QLabel("Top N:"))
    from PyQt6.QtWidgets import QSpinBox

    top_n_spin = QSpinBox()
    top_n_spin.setRange(5, 100)
    top_n_spin.setValue(20)
    top_n_spin.setFixedWidth(70)
    top_n_layout.addWidget(top_n_spin)
    top_n_layout.addStretch()
    dist_layout.addLayout(top_n_layout)

    dist_table = QTableWidget()
    dist_table.setColumnCount(3)
    dist_table.setHorizontalHeaderLabels(["Value", "Count", "%"])
    dist_table.horizontalHeader().setStretchLastSection(True)
    dist_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
    dist_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
    dist_layout.addWidget(dist_table)

    layout.addWidget(dist_group)

    viewer.stats_column_combo = column_combo
    viewer.dist_table = dist_table
    viewer.top_n_spin = top_n_spin
    return widget


def populate_stats(viewer, stats):
    """Fill in column statistics labels."""
    viewer.stats_labels["type"].setText(stats.get("type", "-"))
    viewer.stats_labels["count"].setText(format_number(stats.get("count", 0)))

    null_count = stats.get("null_count", 0)
    count = stats.get("count", 0)
    null_pct = f" ({null_count / count * 100:.1f}%)" if count > 0 else ""
    viewer.stats_labels["null_count"].setText(f"{format_number(null_count)}{null_pct}")

    viewer.stats_labels["valid_count"].setText(format_number(stats.get("valid_count", 0)))

    unique = stats.get("unique_count")
    viewer.stats_labels["unique_count"].setText(
        format_number(unique) if unique is not None else "N/A"
    )

    viewer.stats_labels["min"].setText(str(stats["min"]) if stats.get("min") is not None else "N/A")
    viewer.stats_labels["max"].setText(str(stats["max"]) if stats.get("max") is not None else "N/A")

    mean = stats.get("mean")
    if mean is not None:
        viewer.stats_labels["mean"].setText(f"{mean:.4f}")
    else:
        viewer.stats_labels["mean"].setText("N/A (non-numeric)")


def populate_distribution(viewer, dist_data):
    """Fill in the value distribution table."""
    viewer.dist_table.setRowCount(len(dist_data))
    for i, entry in enumerate(dist_data):
        viewer.dist_table.setItem(i, 0, QTableWidgetItem(str(entry["value"])))
        viewer.dist_table.setItem(i, 1, QTableWidgetItem(format_number(entry["count"])))
        viewer.dist_table.setItem(i, 2, QTableWidgetItem(f"{entry['percentage']:.1f}%"))
