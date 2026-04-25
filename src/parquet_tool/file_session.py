class FileSession:
    """Encapsulates all state for one open parquet file/directory."""

    def __init__(self, pf, model):
        self.pf = pf
        self.model = model
        self.inner_tabs = None

        # data tab widgets (set by create_data_tab)
        self.data_table_view = None
        self.search_bar = None
        self.column_filter = None
        self.filter_builder = None
        self.pagination_bar = None
        self.json_pane = None
        self.json_group = None
        self.nested_viewer = None
        self.detail_stack = None
        self.detail_tabs = None

        # schema tab widgets (set by create_schema_tab)
        self.schema_tree = None

        # metadata tab widgets (set by create_metadata_tab)
        self.meta_labels = {}
        self.rg_table = None
        self.chunks_table = None
        self.chunks_group = None

        # stats tab widgets (set by create_stats_tab)
        self.stats_column_combo = None
        self.stats_labels = {}
        self.dist_table = None
        self.top_n_spin = None
