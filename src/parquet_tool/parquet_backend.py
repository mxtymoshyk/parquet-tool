import os

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq


def build_search_mask(table, query, columns=None):
    """OR-combine per-column substring masks for full-text search."""
    search_cols = columns if columns is not None else table.column_names
    mask = None
    for col_name in search_cols:
        if col_name not in table.column_names:
            continue
        try:
            str_col = pc.cast(table.column(col_name), pa.string())
            col_mask = pc.match_substring(str_col, query, ignore_case=True)
            col_mask = pc.fill_null(col_mask, False)
        except (pa.ArrowNotImplementedError, pa.ArrowInvalid):
            continue
        mask = col_mask if mask is None else pc.or_(mask, col_mask)
    return mask


def build_column_mask(table, column, value, mode="contains", value2=""):
    """Build a filter mask for a single column with the given mode."""
    try:
        col = table.column(column)

        if mode == "contains":
            str_col = pc.cast(col, pa.string())
            mask = pc.match_substring(str_col, value, ignore_case=True)
            return pc.fill_null(mask, False)

        if mode == "exact":
            str_col = pc.cast(col, pa.string())
            mask = pc.equal(str_col, value)
            return pc.fill_null(mask, False)

        if mode == "regex":
            str_col = pc.cast(col, pa.string())
            mask = pc.match_substring_regex(str_col, value, ignore_case=True)
            return pc.fill_null(mask, False)

        # numeric comparison modes
        if mode in (">", ">=", "<", "<=", "between"):
            num_col = pc.cast(col, pa.float64())
            num_val = float(value)
            if mode == ">":
                mask = pc.greater(num_col, num_val)
            elif mode == ">=":
                mask = pc.greater_equal(num_col, num_val)
            elif mode == "<":
                mask = pc.less(num_col, num_val)
            elif mode == "<=":
                mask = pc.less_equal(num_col, num_val)
            elif mode == "between":
                num_val2 = float(value2) if value2 else num_val
                mask = pc.and_(
                    pc.greater_equal(num_col, num_val),
                    pc.less_equal(num_col, num_val2),
                )
            return pc.fill_null(mask, False)

        # fallback to contains
        str_col = pc.cast(col, pa.string())
        mask = pc.match_substring(str_col, value, ignore_case=True)
        return pc.fill_null(mask, False)

    except (pa.ArrowNotImplementedError, pa.ArrowInvalid, ValueError, KeyError):
        return None


def build_composite_mask(table, filter_specs, join_mode="AND"):
    """Combine multiple filter specs with AND or OR logic."""
    masks = []
    for spec in filter_specs:
        mask = build_column_mask(table, spec.column, spec.value, spec.mode, spec.value2)
        if mask is not None:
            masks.append(mask)

    if not masks:
        return None

    result = masks[0]
    combine = pc.and_ if join_mode == "AND" else pc.or_
    for mask in masks[1:]:
        result = combine(result, mask)
    return result


def _compute_column_statistics(col, field):
    """Compute statistics dict from a pyarrow column and field."""
    valid_count = len(col) - col.null_count
    result = {
        "count": len(col),
        "null_count": col.null_count,
        "valid_count": valid_count,
        "type": str(field.type),
    }

    if valid_count > 0:
        try:
            mm = pc.min_max(col).as_py()
            result["min"] = mm["min"]
            result["max"] = mm["max"]
        except (pa.ArrowNotImplementedError, pa.ArrowInvalid):
            result["min"] = None
            result["max"] = None

        try:
            result["unique_count"] = pc.count_distinct(col).as_py()
        except (pa.ArrowNotImplementedError, pa.ArrowInvalid):
            result["unique_count"] = None

        if (
            pa.types.is_integer(field.type)
            or pa.types.is_floating(field.type)
            or pa.types.is_decimal(field.type)
        ):
            try:
                result["mean"] = pc.mean(col).as_py()
            except (pa.ArrowNotImplementedError, pa.ArrowInvalid):
                result["mean"] = None
        else:
            result["mean"] = None
    else:
        result["min"] = None
        result["max"] = None
        result["unique_count"] = None
        result["mean"] = None

    return result


def _compute_value_distribution(col, top_n=20):
    """Compute top-N value frequencies from a pyarrow column."""
    try:
        vc = pc.value_counts(col)
        values = vc.field("values").to_pylist()
        counts = vc.field("counts").to_pylist()
    except (pa.ArrowNotImplementedError, pa.ArrowInvalid):
        return []

    pairs = sorted(zip(counts, values), reverse=True)[:top_n]
    total = len(col)
    result = []
    for count, value in pairs:
        pct = (count / total * 100) if total > 0 else 0
        result.append({"value": value, "count": count, "percentage": pct})
    return result


def filtered_scan(
    rg_reader,
    num_row_groups,
    mask_fn,
    offset,
    limit,
    progress_cb=None,
    cancelled_fn=None,
):
    """Scan row groups with a mask function, return [offset:offset+limit] window.

    rg_reader(rg_idx) -> pyarrow.Table reads one row group.
    mask_fn(table) -> BooleanArray or None produces a filter mask.
    progress_cb(rg_idx, total) reports progress per row group.
    cancelled_fn() -> bool checks if operation was cancelled.

    Once the page is full, remaining row groups are counted via mask
    summation without materializing filtered tables.
    """
    collected = []
    collected_count = 0
    rows_seen = 0
    page_full = False

    for rg_idx in range(num_row_groups):
        if cancelled_fn and cancelled_fn():
            break

        table = rg_reader(rg_idx)
        mask = mask_fn(table)

        if mask is None:
            if progress_cb:
                progress_cb(rg_idx + 1, num_row_groups)
            continue

        if page_full:
            # count matches without materializing filtered rows
            rg_matches = pc.sum(mask).as_py()
        else:
            filtered = table.filter(mask)
            rg_matches = len(filtered)

            if rg_matches > 0 and collected_count < limit:
                window_start = max(rows_seen, offset)
                window_end = min(rows_seen + rg_matches, offset + limit)

                if window_start < window_end:
                    local_start = window_start - rows_seen
                    local_len = window_end - window_start
                    collected.append(filtered.slice(local_start, local_len))
                    collected_count += local_len

            if collected_count >= limit:
                page_full = True

        rows_seen += rg_matches

        if progress_cb:
            progress_cb(rg_idx + 1, num_row_groups)

    if not collected:
        return None, rows_seen
    return pa.concat_tables(collected), rows_seen


class ParquetFile:
    """Wraps a parquet file with lazy loading and metadata access."""

    def __init__(self, path):
        self.path = os.path.abspath(path)
        self.pq_file = pq.ParquetFile(path)
        self.schema = self.pq_file.schema_arrow
        self.metadata = self.pq_file.metadata
        self.num_rows = self.metadata.num_rows
        self.num_row_groups = self.metadata.num_row_groups
        self.file_size = os.path.getsize(path)

        self._rg_offsets = []
        offset = 0
        for i in range(self.num_row_groups):
            rg = self.metadata.row_group(i)
            self._rg_offsets.append((offset, rg.num_rows))
            offset += rg.num_rows

    def read_row_group(self, index, columns=None):
        """Read a single row group, optionally with column projection."""
        return self.pq_file.read_row_group(index, columns=columns)

    def read_range(self, offset, limit, columns=None):
        """Read rows [offset:offset+limit] with optional column projection."""
        if offset >= self.num_rows:
            return self._empty_table()

        limit = min(limit, self.num_rows - offset)
        tables = []
        remaining = limit
        current_offset = offset

        for rg_idx in range(self.num_row_groups):
            rg_start, rg_rows = self._rg_offsets[rg_idx]
            rg_end = rg_start + rg_rows

            if current_offset >= rg_end:
                continue
            if remaining <= 0:
                break

            table = self.read_row_group(rg_idx, columns=columns)

            local_start = current_offset - rg_start
            local_len = min(remaining, rg_rows - local_start)
            table = table.slice(local_start, local_len)

            tables.append(table)
            remaining -= local_len
            current_offset = rg_start + local_start + local_len

        if not tables:
            return self._empty_table()
        return pa.concat_tables(tables)

    def get_schema_info(self):
        """Return schema as list of dicts with name, type, nullable, children."""
        return [self._field_to_dict(field) for field in self.schema]

    def get_file_metadata(self):
        """Return file-level metadata dict."""
        meta = self.metadata
        return {
            "path": self.path,
            "created_by": meta.created_by or "Unknown",
            "format_version": str(meta.format_version),
            "num_rows": meta.num_rows,
            "num_row_groups": meta.num_row_groups,
            "serialized_size": meta.serialized_size,
            "file_size": self.file_size,
        }

    def get_row_group_metadata(self, rg_index):
        """Return detailed metadata for a specific row group."""
        rg = self.metadata.row_group(rg_index)
        columns = []

        for j in range(rg.num_columns):
            col = rg.column(j)
            col_info = {
                "name": col.path_in_schema,
                "compression": str(col.compression),
                "total_compressed_size": col.total_compressed_size,
                "total_uncompressed_size": col.total_uncompressed_size,
                "data_page_offset": col.data_page_offset,
            }

            if col.is_stats_set:
                stats = col.statistics
                col_info["statistics"] = {
                    "min": stats.min if stats.has_min_max else None,
                    "max": stats.max if stats.has_min_max else None,
                    "null_count": stats.null_count if stats.has_null_count else None,
                    "num_values": stats.num_values,
                    "distinct_count": (stats.distinct_count if stats.has_distinct_count else None),
                }
            else:
                col_info["statistics"] = None

            columns.append(col_info)

        return {
            "num_rows": rg.num_rows,
            "total_byte_size": rg.total_byte_size,
            "columns": columns,
        }

    def get_column_statistics(self, column_name):
        """Compute statistics for a column by reading only that column."""
        field = self.schema.field(self.schema.get_field_index(column_name))
        table = pq.read_table(self.path, columns=[column_name])
        return _compute_column_statistics(table.column(0), field)

    def get_value_distribution(self, column_name, top_n=20):
        """Return top-N most frequent values with counts and percentages."""
        table = pq.read_table(self.path, columns=[column_name])
        return _compute_value_distribution(table.column(0), top_n)

    def search(
        self, query, columns=None, offset=0, limit=1000, progress_cb=None, cancelled_fn=None
    ):
        """Full-text search across columns. Returns (table, total_matches)."""
        if not query:
            return self.read_range(offset, limit, columns), self.num_rows

        search_cols = columns or self.schema.names

        def mask_fn(table):
            return build_search_mask(table, query, search_cols)

        table, total = filtered_scan(
            self.read_row_group,
            self.num_row_groups,
            mask_fn,
            offset,
            limit,
            progress_cb,
            cancelled_fn,
        )
        if table is None:
            return self._empty_table(), total
        return table, total

    def filter_column(
        self,
        column,
        value,
        offset=0,
        limit=1000,
        mode="contains",
        value2="",
        progress_cb=None,
        cancelled_fn=None,
    ):
        """Filter rows where column matches value. Returns (table, total_matches)."""
        if not value:
            return self.read_range(offset, limit), self.num_rows

        def mask_fn(table):
            return build_column_mask(table, column, value, mode, value2)

        table, total = filtered_scan(
            self.read_row_group,
            self.num_row_groups,
            mask_fn,
            offset,
            limit,
            progress_cb,
            cancelled_fn,
        )
        if table is None:
            return self._empty_table(), total
        return table, total

    def filter_multi(
        self,
        filter_specs,
        join_mode="AND",
        offset=0,
        limit=1000,
        progress_cb=None,
        cancelled_fn=None,
    ):
        """Apply multiple filter conditions. Returns (table, total_matches)."""
        if not filter_specs:
            return self.read_range(offset, limit), self.num_rows

        def mask_fn(table):
            return build_composite_mask(table, filter_specs, join_mode)

        table, total = filtered_scan(
            self.read_row_group,
            self.num_row_groups,
            mask_fn,
            offset,
            limit,
            progress_cb,
            cancelled_fn,
        )
        if table is None:
            return self._empty_table(), total
        return table, total

    def _empty_table(self):
        """Return an empty table matching this file's schema."""
        return pa.table(
            {name: pa.array([], type=self.schema.field(name).type) for name in self.schema.names}
        )

    def _field_to_dict(self, field):
        """Convert a pyarrow field to a schema info dict."""
        info = {
            "name": field.name,
            "type": str(field.type),
            "nullable": field.nullable,
            "children": [],
        }
        if isinstance(field.type, pa.StructType):
            for j in range(field.type.num_fields):
                info["children"].append(self._field_to_dict(field.type.field(j)))
        elif isinstance(field.type, pa.ListType):
            info["children"].append(
                {
                    "name": "element",
                    "type": str(field.type.value_type),
                    "nullable": field.type.value_field.nullable,
                    "children": [],
                }
            )
        elif isinstance(field.type, pa.MapType):
            info["children"].append(
                {
                    "name": "key",
                    "type": str(field.type.key_type),
                    "nullable": False,
                    "children": [],
                }
            )
            info["children"].append(
                {
                    "name": "value",
                    "type": str(field.type.item_type),
                    "nullable": field.type.item_field.nullable,
                    "children": [],
                }
            )
        return info


class ParquetDirectory:
    """All .parquet files in a directory presented as one unified dataset."""

    def __init__(self, dir_path):
        self.dir_path = os.path.abspath(dir_path)
        self.path = self.dir_path
        self.files = []

        for name in sorted(os.listdir(dir_path)):
            if name.endswith(".parquet"):
                path = os.path.join(dir_path, name)
                try:
                    pf = ParquetFile(path)
                    self.files.append(pf)
                except Exception:
                    pass

        if not self.files:
            raise ValueError(f"No valid parquet files found in {dir_path}")

        self.schema = self.files[0].schema
        self.num_rows = sum(f.num_rows for f in self.files)
        self.file_size = sum(f.file_size for f in self.files)

        self._rg_map = []
        self._rg_offsets = []
        global_offset = 0
        for pf in self.files:
            for rg_i in range(pf.num_row_groups):
                rg = pf.metadata.row_group(rg_i)
                self._rg_map.append((pf, rg_i))
                self._rg_offsets.append((global_offset, rg.num_rows))
                global_offset += rg.num_rows
        self.num_row_groups = len(self._rg_map)

    def read_row_group(self, index, columns=None):
        pf, local_rg = self._rg_map[index]
        return pf.read_row_group(local_rg, columns=columns)

    def read_range(self, offset, limit, columns=None):
        if offset >= self.num_rows:
            return self._empty_table()

        limit = min(limit, self.num_rows - offset)
        tables = []
        remaining = limit
        current_offset = offset

        for rg_idx in range(self.num_row_groups):
            rg_start, rg_rows = self._rg_offsets[rg_idx]
            rg_end = rg_start + rg_rows

            if current_offset >= rg_end:
                continue
            if remaining <= 0:
                break

            table = self.read_row_group(rg_idx, columns=columns)
            local_start = current_offset - rg_start
            local_len = min(remaining, rg_rows - local_start)
            table = table.slice(local_start, local_len)

            tables.append(table)
            remaining -= local_len
            current_offset = rg_start + local_start + local_len

        if not tables:
            return self._empty_table()
        return pa.concat_tables(tables)

    def get_schema_info(self):
        return self.files[0].get_schema_info()

    def get_file_metadata(self):
        first = self.files[0]
        return {
            "path": self.dir_path,
            "created_by": first.metadata.created_by or "Unknown",
            "format_version": str(first.metadata.format_version),
            "num_rows": self.num_rows,
            "num_row_groups": self.num_row_groups,
            "serialized_size": sum(f.metadata.serialized_size for f in self.files),
            "file_size": self.file_size,
        }

    def get_row_group_metadata(self, rg_index):
        pf, local_rg = self._rg_map[rg_index]
        return pf.get_row_group_metadata(local_rg)

    def get_column_statistics(self, column_name):
        field = self.schema.field(self.schema.get_field_index(column_name))
        arrays = []
        for pf in self.files:
            col_table = pq.read_table(pf.path, columns=[column_name])
            arrays.append(col_table.column(0))
        col = pa.chunked_array(arrays)
        return _compute_column_statistics(col, field)

    def get_value_distribution(self, column_name, top_n=20):
        arrays = []
        for pf in self.files:
            col_table = pq.read_table(pf.path, columns=[column_name])
            arrays.append(col_table.column(0))
        col = pa.chunked_array(arrays)
        return _compute_value_distribution(col, top_n)

    def search(
        self, query, columns=None, offset=0, limit=1000, progress_cb=None, cancelled_fn=None
    ):
        if not query:
            return self.read_range(offset, limit, columns), self.num_rows
        search_cols = columns or self.schema.names

        def mask_fn(table):
            return build_search_mask(table, query, search_cols)

        table, total = filtered_scan(
            self.read_row_group,
            self.num_row_groups,
            mask_fn,
            offset,
            limit,
            progress_cb,
            cancelled_fn,
        )
        if table is None:
            return self._empty_table(), total
        return table, total

    def filter_column(
        self,
        column,
        value,
        offset=0,
        limit=1000,
        mode="contains",
        value2="",
        progress_cb=None,
        cancelled_fn=None,
    ):
        if not value:
            return self.read_range(offset, limit), self.num_rows

        def mask_fn(table):
            return build_column_mask(table, column, value, mode, value2)

        table, total = filtered_scan(
            self.read_row_group,
            self.num_row_groups,
            mask_fn,
            offset,
            limit,
            progress_cb,
            cancelled_fn,
        )
        if table is None:
            return self._empty_table(), total
        return table, total

    def filter_multi(
        self,
        filter_specs,
        join_mode="AND",
        offset=0,
        limit=1000,
        progress_cb=None,
        cancelled_fn=None,
    ):
        if not filter_specs:
            return self.read_range(offset, limit), self.num_rows

        def mask_fn(table):
            return build_composite_mask(table, filter_specs, join_mode)

        table, total = filtered_scan(
            self.read_row_group,
            self.num_row_groups,
            mask_fn,
            offset,
            limit,
            progress_cb,
            cancelled_fn,
        )
        if table is None:
            return self._empty_table(), total
        return table, total

    def _empty_table(self):
        return pa.table(
            {name: pa.array([], type=self.schema.field(name).type) for name in self.schema.names}
        )
