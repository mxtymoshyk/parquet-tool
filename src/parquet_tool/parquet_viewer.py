import json
import os

from PyQt6.QtCore import Qt
from PyQt6.QtGui import QAction, QDragEnterEvent, QDropEvent, QKeySequence
from PyQt6.QtWidgets import (
    QApplication,
    QFileDialog,
    QLabel,
    QMainWindow,
    QMessageBox,
    QProgressBar,
    QStatusBar,
    QTabWidget,
    QVBoxLayout,
    QWidget,
)

from .data_model import PAGE_SIZE, ParquetTableModel
from .file_session import FileSession
from .gui_tabs import (
    create_data_tab,
    create_metadata_tab,
    create_schema_tab,
    create_stats_tab,
    populate_column_chunks,
    populate_distribution,
    populate_file_metadata,
    populate_row_groups,
    populate_schema_tree,
    populate_stats,
)
from .gui_utils import SettingsManager, ThemeManager, format_number, format_size
from .parquet_backend import ParquetDirectory, ParquetFile
from .schema_diff import SchemaDiffDialog
from .workers import (
    DistributionWorker,
    ExportWorker,
    MultiFilterWorker,
    SearchWorker,
    StatsWorker,
)


class ParquetViewer(QMainWindow):
    MAX_RECENT = 10

    def __init__(self, theme_manager=None, settings=None):
        super().__init__()
        self.setWindowTitle("Parquet Tool")
        self.setGeometry(100, 100, 1200, 800)
        self.setAcceptDrops(True)

        self._active_worker = None
        self._sessions = {}
        self._active_session = None
        self._settings = settings or SettingsManager()
        self._recent_files = self._settings.recent_files
        self.theme_manager = theme_manager or ThemeManager()

        self._setup_ui()
        self._setup_toolbar()
        self._setup_menu_bar()
        self._setup_shortcuts()
        self._setup_status_bar()
        self._restore_window_state()

    @property
    def _s(self):
        """Shorthand for the active session."""
        return self._active_session

    @property
    def _pf(self):
        return self._s.pf if self._s else None

    @property
    def _model(self):
        return self._s.model if self._s else None

    def _setup_ui(self):
        central = QWidget()
        self.setCentralWidget(central)
        layout = QVBoxLayout(central)
        layout.setContentsMargins(4, 4, 4, 4)

        self.file_tabs = QTabWidget()
        self.file_tabs.setTabsClosable(True)
        self.file_tabs.tabCloseRequested.connect(self._on_file_tab_closed)
        self.file_tabs.currentChanged.connect(self._on_file_tab_changed)

        self._init_empty_tab()

        layout.addWidget(self.file_tabs)

    def _setup_toolbar(self):
        self._open_file_action = QAction("Open File...", self)
        self._open_file_action.setShortcut(QKeySequence("Ctrl+O"))
        self._open_file_action.triggered.connect(self._on_open_file)

        self._open_dir_action = QAction("Open Directory...", self)
        self._open_dir_action.setShortcut(QKeySequence("Ctrl+Shift+O"))
        self._open_dir_action.triggered.connect(self._on_open_directory)

    def _setup_menu_bar(self):
        menu_bar = self.menuBar()

        file_menu = menu_bar.addMenu("File")
        file_menu.addAction(self._open_file_action)
        file_menu.addAction(self._open_dir_action)

        self.recent_menu = file_menu.addMenu("Recent Files")
        self._update_recent_menu()

        file_menu.addSeparator()

        compare_action = QAction("Compare Schemas...", self)
        compare_action.setShortcut(QKeySequence("Ctrl+Shift+C"))
        compare_action.triggered.connect(self._show_schema_diff)
        file_menu.addAction(compare_action)

        file_menu.addSeparator()

        export_action = QAction("Export to CSV...", self)
        export_action.setShortcut(QKeySequence("Ctrl+E"))
        export_action.triggered.connect(self._export_csv)
        file_menu.addAction(export_action)

        file_menu.addSeparator()

        quit_action = QAction("Quit", self)
        quit_action.setShortcut(QKeySequence("Ctrl+Q"))
        quit_action.triggered.connect(self.close)
        file_menu.addAction(quit_action)

        view_menu = menu_bar.addMenu("View")

        theme_action = QAction("Toggle Dark/Light Theme", self)
        theme_action.setShortcut(QKeySequence("Ctrl+T"))
        theme_action.triggered.connect(self._toggle_theme)
        view_menu.addAction(theme_action)

    def _setup_shortcuts(self):
        for i in range(4):
            action = QAction(self)
            action.setShortcut(QKeySequence(f"Ctrl+{i + 1}"))
            idx = i
            action.triggered.connect(lambda checked, x=idx: self._switch_inner_tab(x))
            self.addAction(action)

    def _switch_inner_tab(self, index):
        if self._s and self._s.inner_tabs:
            self._s.inner_tabs.setCurrentIndex(index)

    def _setup_status_bar(self):
        self.status_bar = QStatusBar()
        self.setStatusBar(self.status_bar)

        self.status_path = QLabel("No file loaded")
        self.status_filter = QLabel("")
        self.status_row = QLabel("")
        self.status_rows = QLabel("")
        self.status_cols = QLabel("")
        self.status_size = QLabel("")
        self.status_encoding = QLabel("")

        self.progress_bar = QProgressBar()
        self.progress_bar.setFixedWidth(120)
        self.progress_bar.setFixedHeight(16)
        self.progress_bar.setVisible(False)

        self.status_bar.addWidget(self.status_path, 1)
        self.status_bar.addWidget(self.status_filter)
        self.status_bar.addPermanentWidget(self.progress_bar)
        self.status_bar.addPermanentWidget(self.status_row)
        self.status_bar.addPermanentWidget(self.status_rows)
        self.status_bar.addPermanentWidget(self.status_cols)
        self.status_bar.addPermanentWidget(self.status_size)
        self.status_bar.addPermanentWidget(self.status_encoding)

    def _init_empty_tab(self):
        """Create an initial empty tab with all widgets so shortcuts work."""
        session = FileSession(None, None)
        inner = QTabWidget()
        inner.addTab(create_data_tab(session), "Data")
        inner.addTab(create_schema_tab(session), "Schema")
        inner.addTab(create_metadata_tab(session), "Metadata")
        inner.addTab(create_stats_tab(session), "Statistics")
        session.inner_tabs = inner
        self._active_session = session
        self.file_tabs.addTab(inner, "No file loaded")
        session.data_table_view.doubleClicked.connect(self._on_cell_double_clicked)

    def _create_session_tab(self, pf, label):
        """Create a new file session with inner tabs and add to outer tabs."""
        model = ParquetTableModel(pf)
        session = FileSession(pf, model)
        inner = QTabWidget()
        inner.addTab(create_data_tab(session), "Data")
        inner.addTab(create_schema_tab(session), "Schema")
        inner.addTab(create_metadata_tab(session), "Metadata")
        inner.addTab(create_stats_tab(session), "Statistics")
        session.inner_tabs = inner

        path = pf.path
        self._sessions[path] = session

        if self.file_tabs.count() == 1 and self._s is not None and self._s.pf is None:
            self.file_tabs.removeTab(0)

        idx = self.file_tabs.addTab(inner, label)
        self.file_tabs.setCurrentIndex(idx)
        return session

    def _on_file_tab_changed(self, index):
        if index < 0:
            return
        widget = self.file_tabs.widget(index)
        for session in self._sessions.values():
            if session.inner_tabs is widget:
                self._activate_session(session)
                return

    def _on_file_tab_closed(self, index):
        widget = self.file_tabs.widget(index)
        to_remove = None
        for path, session in self._sessions.items():
            if session.inner_tabs is widget:
                to_remove = path
                break
        if to_remove:
            del self._sessions[to_remove]
        self.file_tabs.removeTab(index)

        if self.file_tabs.count() == 0:
            self._init_empty_tab()

    def _activate_session(self, session):
        """Switch to a session: reconnect signals, refresh UI."""
        self._cancel_active_worker()
        if self._s and self._s.data_table_view:
            try:
                self._s.data_table_view.doubleClicked.disconnect(self._on_cell_double_clicked)
            except (TypeError, RuntimeError):
                pass
        self._active_session = session
        self._connect_model()
        self._populate_all_tabs()
        self._update_status_bar()
        self._s.data_table_view.doubleClicked.connect(self._on_cell_double_clicked)

    def open_file(self, path):
        """Open a parquet file by path."""
        try:
            pf = ParquetFile(path)
            label = os.path.basename(path)
            self._create_session_tab(pf, label)
            self._add_recent(path)
            self.setWindowTitle(f"Parquet Tool - {label}")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to open file:\n{e}")

    def open_directory(self, dir_path):
        """Open a directory of parquet files as a unified dataset."""
        try:
            pd = ParquetDirectory(dir_path)
            n = len(pd.files)
            label = f"{os.path.basename(dir_path)} ({n} file{'s' if n != 1 else ''})"
            self._create_session_tab(pd, label)
            self._add_recent(dir_path)
            self.setWindowTitle(f"Parquet Tool - {label}")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to open directory:\n{e}")

    def _on_open_file(self):
        path, _ = QFileDialog.getOpenFileName(
            self,
            "Open Parquet File",
            "",
            "Parquet Files (*.parquet);;All Files (*)",
        )
        if path:
            self.open_file(path)

    def _on_open_directory(self):
        dir_path = QFileDialog.getExistingDirectory(self, "Open Parquet Directory")
        if dir_path:
            self.open_directory(dir_path)

    def _connect_model(self):
        """Wire up the model to the data tab widgets."""
        s = self._s
        if s is None or s.pf is None or s.model is None:
            return

        for signal in [
            s.pagination_bar.pageChanged,
            s.pagination_bar.rowHighlightRequested,
            s.search_bar.searchRequested,
            s.search_bar.cleared,
            s.column_filter.filterRequested,
            s.column_filter.cleared,
            s.filter_builder.filterRequested,
            s.filter_builder.cleared,
            s.stats_column_combo.currentTextChanged,
            s.top_n_spin.valueChanged,
            s.rg_table.cellClicked,
        ]:
            try:
                signal.disconnect()
            except TypeError:
                pass

        try:
            s.data_table_view.selectionModel().currentRowChanged.disconnect()
        except (TypeError, AttributeError, RuntimeError):
            pass

        s.data_table_view.setModel(s.model)
        s.json_pane.setPlainText("")
        s.json_group.setTitle("JSON")
        self.status_filter.setText("")
        self.status_row.setText("")
        s.data_table_view.selectionModel().currentRowChanged.connect(self._on_row_selected)

        self._refresh_pagination(0)
        s.pagination_bar.pageChanged.connect(self._on_page_changed)
        s.pagination_bar.rowHighlightRequested.connect(self._on_highlight_row)

        s.search_bar.searchRequested.connect(self._on_search)
        s.search_bar.cleared.connect(self._on_clear_filters)

        s.column_filter.set_columns(s.pf.schema.names)
        s.column_filter.filterRequested.connect(self._on_column_filter)
        s.column_filter.cleared.connect(self._on_clear_filters)

        s.filter_builder.set_columns(s.pf.schema.names)
        s.filter_builder.filterRequested.connect(self._on_multi_filter)
        s.filter_builder.cleared.connect(self._on_clear_filters)

        s.rg_table.cellClicked.connect(self._on_rg_selected)

        s.stats_column_combo.blockSignals(True)
        s.stats_column_combo.clear()
        s.stats_column_combo.addItems(s.pf.schema.names)
        s.stats_column_combo.blockSignals(False)
        s.stats_column_combo.currentTextChanged.connect(self._on_stats_column_changed)
        s.top_n_spin.valueChanged.connect(self._on_top_n_changed)

    def _populate_all_tabs(self):
        """Populate all tabs with data from current file."""
        s = self._s
        populate_schema_tree(s.schema_tree, s.pf.get_schema_info())
        populate_file_metadata(s, s.pf.get_file_metadata())
        populate_row_groups(s, s.pf)

        if s.pf.num_row_groups > 0:
            populate_column_chunks(s, s.pf, 0)

        if s.pf.schema.names:
            self._on_stats_column_changed(s.pf.schema.names[0])

    def _update_status_bar(self):
        if self._pf is None:
            return
        self.status_path.setText(f"  {self._pf.path}")
        self.status_rows.setText(f"{format_number(self._pf.num_rows)} rows  ")
        self.status_cols.setText(f"{len(self._pf.schema)} cols  ")
        self.status_size.setText(f"{format_size(self._pf.file_size)}  ")
        if self._pf.num_row_groups > 0:
            rg = self._pf.get_row_group_metadata(0)
            codecs = sorted({col["compression"] for col in rg["columns"]})
            self.status_encoding.setText(f"  {', '.join(codecs)}")
        else:
            self.status_encoding.setText("")

    def _refresh_pagination(self, page=0):
        """Update pagination bar from current model state."""
        self._s.pagination_bar.update_state(
            page,
            self._model.get_page_count(),
            self._model.total_rows,
            PAGE_SIZE,
        )

    def _on_page_changed(self, page):
        self._model.set_page(page)
        self._refresh_pagination(page)

    def _on_highlight_row(self, row_in_page):
        if self._model is None:
            return
        if 0 <= row_in_page < self._model.rowCount():
            idx = self._model.index(row_in_page, 0)
            self._s.data_table_view.setCurrentIndex(idx)
            self._s.data_table_view.scrollTo(idx)

    def _on_search(self, query):
        if self._model is None:
            return
        if not query:
            self._model.set_search("")
            self._refresh_pagination()
            self.status_filter.setText("")
            return
        self._model._search_query = query
        self._model._column_filter = None
        self._model._current_page = 0
        self._run_search_worker(query=query)

    def _on_column_filter(self, spec):
        if self._model is None:
            return
        self._model._column_filter = spec
        self._model._search_query = ""
        self._model._current_page = 0
        self._run_search_worker(column_filter=spec)

    def _on_multi_filter(self, specs, join_mode):
        if self._model is None:
            return
        self._model._multi_filter = (specs, join_mode)
        self._model._search_query = ""
        self._model._column_filter = None
        self._model._current_page = 0
        self._run_multi_filter_worker(specs, join_mode)

    def _on_clear_filters(self):
        if self._model is None:
            return
        self._model.clear_filters()
        self._s.search_bar.search_input.clear()
        self._s.column_filter.clear_filter()
        self._refresh_pagination()
        self.status_filter.setText("")

    def _on_row_selected(self, current, _previous):
        if self._model is None or not current.isValid():
            self._s.json_pane.setPlainText("")
            self._s.json_group.setTitle("JSON")
            self.status_row.setText("")
            return
        row = current.row()
        abs_row = self._model._current_page * PAGE_SIZE + row + 1
        row_dict = {
            name: self._model._cell_value(col, row)
            for col, name in enumerate(self._model._column_names)
        }
        self._s.json_group.setTitle(f"JSON (row {abs_row})")
        self.status_row.setText(f"  Row {format_number(abs_row)}  ")
        self._s.json_pane.setPlainText(json.dumps(row_dict, indent=2, default=str))

    def _on_cell_double_clicked(self, index):
        if self._model is None or not index.isValid():
            return
        raw = self._model.raw_data(index)
        if isinstance(raw, (dict, list)):
            col_name = self._model.headerData(index.column(), Qt.Orientation.Horizontal)
            self._s.nested_viewer.set_value(raw, col_name)
            self._s.detail_tabs.setCurrentIndex(1)
            self.status_bar.showMessage(f"Viewing nested value: {col_name}", 2000)
        else:
            value = self._model.data(index, Qt.ItemDataRole.DisplayRole)
            if value:
                QApplication.clipboard().setText(value)
                self.status_bar.showMessage(f"Copied: {value}", 2000)

    def _on_rg_selected(self, row, _col):
        if self._pf is None:
            return
        populate_column_chunks(self._s, self._pf, row)

    def _on_stats_column_changed(self, column_name):
        if self._pf is None or not column_name:
            return
        self._show_progress()
        worker = StatsWorker(self._pf, column_name)
        worker.progress.connect(self._on_progress)
        worker.finished.connect(self._on_stats_finished)
        worker.error.connect(self._on_stats_error)
        worker.finished.connect(lambda _: self._hide_progress())
        worker.error.connect(lambda _: self._hide_progress())
        self._stats_worker = worker
        worker.start()
        self._run_distribution(column_name)

    def _on_top_n_changed(self, _value):
        column = self._s.stats_column_combo.currentText()
        if column and self._pf is not None:
            self._run_distribution(column)

    def _run_distribution(self, column_name):
        s = self._s
        top_n = s.top_n_spin.value()
        worker = DistributionWorker(self._pf, column_name, top_n)
        worker.finished.connect(lambda dist: populate_distribution(s, dist))
        worker.error.connect(
            lambda msg: self.status_bar.showMessage(f"Distribution error: {msg}", 5000)
        )
        self._dist_worker = worker
        worker.start()

    def _on_stats_finished(self, stats):
        populate_stats(self._s, stats)

    def _on_stats_error(self, msg):
        for label in self._s.stats_labels.values():
            label.setText("-")
        self.status_bar.showMessage(f"Error computing stats: {msg}", 5000)

    def _export_csv(self):
        if self._model is None:
            QMessageBox.warning(self, "Warning", "No file loaded.")
            return

        path, _ = QFileDialog.getSaveFileName(
            self, "Export to CSV", "", "CSV Files (*.csv);;All Files (*)"
        )
        if not path:
            return
        self._show_progress()
        worker = ExportWorker(
            self._pf,
            path,
            search_query=self._model._search_query,
            column_filter=self._model._column_filter,
            multi_filter=self._model._multi_filter,
        )
        worker.progress.connect(self._on_progress)
        worker.finished.connect(self._on_export_finished)
        worker.error.connect(self._on_export_error)
        self._export_worker = worker
        worker.start()

    def _on_export_finished(self, path):
        self._hide_progress()
        self.status_bar.showMessage(f"Exported to {path}", 5000)

    def _on_export_error(self, msg):
        self._hide_progress()
        QMessageBox.critical(self, "Error", f"Export failed:\n{msg}")

    def _show_schema_diff(self):
        initial = self._pf.path if self._pf else ""
        dialog = SchemaDiffDialog(
            self,
            initial_path=initial,
            open_sessions=self._sessions,
            is_dark=self.theme_manager.is_dark,
        )
        dialog.exec()

    def _toggle_theme(self):
        app = QApplication.instance()
        self.theme_manager.toggle(app)

    def _cancel_active_worker(self):
        if self._active_worker is not None and self._active_worker.isRunning():
            self._active_worker.cancel()
            self._active_worker.wait(2000)
        self._active_worker = None

    def _show_progress(self):
        self.progress_bar.setValue(0)
        self.progress_bar.setVisible(True)

    def _hide_progress(self):
        self.progress_bar.setVisible(False)

    def _on_progress(self, current, total):
        if total > 0:
            self.progress_bar.setMaximum(total)
            self.progress_bar.setValue(current)

    def _run_search_worker(self, query=None, column_filter=None):
        self._cancel_active_worker()
        self._show_progress()
        worker = SearchWorker(
            self._pf,
            query=query,
            column_filter=column_filter,
            offset=0,
            limit=PAGE_SIZE,
        )
        worker.progress.connect(self._on_progress)
        worker.finished.connect(self._on_search_finished)
        worker.error.connect(self._on_search_error)
        self._active_worker = worker
        self._pending_search_query = query
        self._pending_column_filter = column_filter
        worker.start()

    def _on_search_finished(self, table, total):
        self._hide_progress()
        if self._model is None:
            return
        self._model.set_page_data(table, total)
        self._refresh_pagination()
        if self._pending_search_query:
            self.status_filter.setText(
                f'  Search: "{self._pending_search_query}"  {format_number(total)} matches  '
            )
        elif self._pending_column_filter:
            spec = self._pending_column_filter
            self.status_filter.setText(
                f'  Filter: {spec.column} {spec.mode} "{spec.value}"  '
                f"{format_number(total)} matches  "
            )

    def _on_search_error(self, msg):
        self._hide_progress()
        self.status_bar.showMessage(f"Search error: {msg}", 5000)

    def _run_multi_filter_worker(self, specs, join_mode):
        self._cancel_active_worker()
        self._show_progress()
        worker = MultiFilterWorker(
            self._pf,
            specs,
            join_mode,
            offset=0,
            limit=PAGE_SIZE,
        )
        worker.progress.connect(self._on_progress)
        worker.finished.connect(self._on_multi_filter_finished)
        worker.error.connect(self._on_search_error)
        self._active_worker = worker
        self._pending_multi_filter = (specs, join_mode)
        worker.start()

    def _on_multi_filter_finished(self, table, total):
        self._hide_progress()
        if self._model is None:
            return
        self._model.set_page_data(table, total)
        self._refresh_pagination()
        specs, join = self._pending_multi_filter
        n = len(specs)
        self.status_filter.setText(
            f"  Advanced: {n} condition{'s' if n != 1 else ''} ({join})  "
            f"{format_number(total)} matches  "
        )

    def _add_recent(self, path):
        if path in self._recent_files:
            self._recent_files.remove(path)
        self._recent_files.insert(0, path)
        self._recent_files = self._recent_files[: self.MAX_RECENT]
        self._settings.recent_files = self._recent_files
        self._update_recent_menu()

    def _update_recent_menu(self):
        self.recent_menu.clear()
        for path in self._recent_files:
            action = QAction(os.path.basename(path), self)
            action.setToolTip(path)
            action.triggered.connect(
                lambda checked, p=path: (
                    self.open_directory(p) if os.path.isdir(p) else self.open_file(p)
                )
            )
            self.recent_menu.addAction(action)

        if not self._recent_files:
            action = QAction("(no recent files)", self)
            action.setEnabled(False)
            self.recent_menu.addAction(action)

    def dragEnterEvent(self, event: QDragEnterEvent):
        if event.mimeData().hasUrls():
            event.acceptProposedAction()

    def dropEvent(self, event: QDropEvent):
        for url in event.mimeData().urls():
            path = url.toLocalFile()
            if path.endswith(".parquet"):
                self.open_file(path)
                return
            if os.path.isdir(path):
                self.open_directory(path)
                return

    def _restore_window_state(self):
        geo = self._settings.window_geometry
        if geo is not None:
            self.restoreGeometry(geo)
        state = self._settings.window_state
        if state is not None:
            self.restoreState(state)

    def _save_settings(self):
        self._settings.recent_files = self._recent_files
        self._settings.is_dark = self.theme_manager.is_dark
        self._settings.window_geometry = self.saveGeometry()
        self._settings.window_state = self.saveState()

    def closeEvent(self, event):
        self._cancel_active_worker()
        self._save_settings()
        super().closeEvent(event)
