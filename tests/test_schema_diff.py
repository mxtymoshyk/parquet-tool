import pyarrow as pa

from parquet_tool.schema_diff import _subtree_has_diff, diff_schemas


class TestDiffSchemas:
    def test_identical_schemas(self):
        schema = pa.schema(
            [
                pa.field("a", pa.int64()),
                pa.field("b", pa.string()),
            ]
        )
        diffs = diff_schemas(schema, schema)
        assert len(diffs) == 2
        assert all(d["status"] == "match" for d in diffs)

    def test_added_column(self):
        a = pa.schema([pa.field("a", pa.int64())])
        b = pa.schema([pa.field("a", pa.int64()), pa.field("b", pa.string())])
        diffs = diff_schemas(a, b)
        assert len(diffs) == 2
        added = [d for d in diffs if d["status"] == "added"]
        assert len(added) == 1
        assert added[0]["name"] == "b"
        assert added[0]["type_a"] == "-"

    def test_removed_column(self):
        a = pa.schema([pa.field("a", pa.int64()), pa.field("b", pa.string())])
        b = pa.schema([pa.field("a", pa.int64())])
        diffs = diff_schemas(a, b)
        removed = [d for d in diffs if d["status"] == "removed"]
        assert len(removed) == 1
        assert removed[0]["name"] == "b"
        assert removed[0]["type_b"] == "-"

    def test_changed_type(self):
        a = pa.schema([pa.field("a", pa.int64())])
        b = pa.schema([pa.field("a", pa.string())])
        diffs = diff_schemas(a, b)
        assert len(diffs) == 1
        assert diffs[0]["status"] == "changed"
        assert diffs[0]["type_a"] == "int64"
        assert diffs[0]["type_b"] == "string"

    def test_changed_nullability(self):
        a = pa.schema([pa.field("a", pa.int64(), nullable=True)])
        b = pa.schema([pa.field("a", pa.int64(), nullable=False)])
        diffs = diff_schemas(a, b)
        assert diffs[0]["status"] == "changed"

    def test_empty_schemas(self):
        a = pa.schema([])
        b = pa.schema([])
        diffs = diff_schemas(a, b)
        assert diffs == []

    def test_preserves_order(self):
        a = pa.schema([pa.field("x", pa.int64()), pa.field("y", pa.string())])
        b = pa.schema([pa.field("y", pa.string()), pa.field("z", pa.float64())])
        diffs = diff_schemas(a, b)
        names = [d["name"] for d in diffs]
        assert names == ["x", "y", "z"]

    def test_multiple_changes(self):
        a = pa.schema(
            [
                pa.field("keep", pa.int64()),
                pa.field("remove", pa.string()),
                pa.field("change", pa.int64()),
            ]
        )
        b = pa.schema(
            [
                pa.field("keep", pa.int64()),
                pa.field("change", pa.string()),
                pa.field("add", pa.float64()),
            ]
        )
        diffs = diff_schemas(a, b)
        statuses = {d["name"]: d["status"] for d in diffs}
        assert statuses["keep"] == "match"
        assert statuses["remove"] == "removed"
        assert statuses["change"] == "changed"
        assert statuses["add"] == "added"


class TestNestedDiff:
    def test_struct_field_diff(self):
        """Struct with a changed subfield produces children."""
        a = pa.schema(
            [
                pa.field(
                    "addr",
                    pa.struct(
                        [
                            pa.field("street", pa.string()),
                            pa.field("zip", pa.int32()),
                        ]
                    ),
                )
            ]
        )
        b = pa.schema(
            [
                pa.field(
                    "addr",
                    pa.struct(
                        [
                            pa.field("street", pa.string()),
                            pa.field("zip", pa.int64()),  # changed
                        ]
                    ),
                )
            ]
        )
        diffs = diff_schemas(a, b)
        assert len(diffs) == 1
        assert diffs[0]["status"] == "changed"
        assert diffs[0]["type_a"] == "struct"
        children = diffs[0]["children"]
        assert len(children) == 2
        by_name = {c["name"]: c for c in children}
        assert by_name["street"]["status"] == "match"
        assert by_name["zip"]["status"] == "changed"
        assert by_name["zip"]["type_a"] == "int32"
        assert by_name["zip"]["type_b"] == "int64"

    def test_struct_added_subfield(self):
        a = pa.schema(
            [
                pa.field(
                    "info",
                    pa.struct(
                        [
                            pa.field("name", pa.string()),
                        ]
                    ),
                )
            ]
        )
        b = pa.schema(
            [
                pa.field(
                    "info",
                    pa.struct(
                        [
                            pa.field("name", pa.string()),
                            pa.field("age", pa.int32()),
                        ]
                    ),
                )
            ]
        )
        diffs = diff_schemas(a, b)
        children = diffs[0]["children"]
        by_name = {c["name"]: c for c in children}
        assert by_name["name"]["status"] == "match"
        assert by_name["age"]["status"] == "added"

    def test_deeply_nested_struct(self):
        """Three levels deep: outer.middle.leaf type changes."""
        a = pa.schema(
            [
                pa.field(
                    "outer",
                    pa.struct(
                        [
                            pa.field(
                                "middle",
                                pa.struct(
                                    [
                                        pa.field("leaf", pa.int32()),
                                    ]
                                ),
                            ),
                        ]
                    ),
                )
            ]
        )
        b = pa.schema(
            [
                pa.field(
                    "outer",
                    pa.struct(
                        [
                            pa.field(
                                "middle",
                                pa.struct(
                                    [
                                        pa.field("leaf", pa.string()),  # changed
                                    ]
                                ),
                            ),
                        ]
                    ),
                )
            ]
        )
        diffs = diff_schemas(a, b)
        assert diffs[0]["status"] == "changed"
        middle = diffs[0]["children"][0]
        assert middle["name"] == "middle"
        assert middle["status"] == "changed"
        leaf = middle["children"][0]
        assert leaf["name"] == "leaf"
        assert leaf["status"] == "changed"
        assert leaf["type_a"] == "int32"
        assert leaf["type_b"] == "string"

    def test_list_element_diff(self):
        a = pa.schema([pa.field("tags", pa.list_(pa.string()))])
        b = pa.schema([pa.field("tags", pa.list_(pa.int64()))])
        diffs = diff_schemas(a, b)
        assert diffs[0]["status"] == "changed"
        children = diffs[0]["children"]
        assert len(children) == 1
        assert children[0]["name"] == "<element>"
        assert children[0]["type_a"] == "string"
        assert children[0]["type_b"] == "int64"

    def test_list_of_struct_diff(self):
        a = pa.schema(
            [
                pa.field(
                    "items",
                    pa.list_(
                        pa.struct(
                            [
                                pa.field("x", pa.int32()),
                            ]
                        )
                    ),
                )
            ]
        )
        b = pa.schema(
            [
                pa.field(
                    "items",
                    pa.list_(
                        pa.struct(
                            [
                                pa.field("x", pa.int64()),
                            ]
                        )
                    ),
                )
            ]
        )
        diffs = diff_schemas(a, b)
        # list -> <element> (struct) -> x (changed)
        elem = diffs[0]["children"][0]
        assert elem["name"] == "<element>"
        assert elem["status"] == "changed"
        x_diff = elem["children"][0]
        assert x_diff["name"] == "x"
        assert x_diff["status"] == "changed"

    def test_map_diff(self):
        a = pa.schema([pa.field("m", pa.map_(pa.string(), pa.int32()))])
        b = pa.schema([pa.field("m", pa.map_(pa.string(), pa.int64()))])
        diffs = diff_schemas(a, b)
        assert diffs[0]["status"] == "changed"
        children = diffs[0]["children"]
        by_name = {c["name"]: c for c in children}
        assert by_name["<key>"]["status"] == "match"
        assert by_name["<value>"]["status"] == "changed"

    def test_matching_struct_no_diff(self):
        s = pa.struct([pa.field("x", pa.int32()), pa.field("y", pa.string())])
        schema = pa.schema([pa.field("data", s)])
        diffs = diff_schemas(schema, schema)
        assert diffs[0]["status"] == "match"
        # children still populated for matching structs
        assert len(diffs[0]["children"]) == 2
        assert all(c["status"] == "match" for c in diffs[0]["children"])

    def test_added_field_with_struct_shows_leaf_children(self):
        a = pa.schema([])
        b = pa.schema(
            [
                pa.field(
                    "info",
                    pa.struct(
                        [
                            pa.field("name", pa.string()),
                            pa.field("age", pa.int32()),
                        ]
                    ),
                )
            ]
        )
        diffs = diff_schemas(a, b)
        assert diffs[0]["status"] == "added"
        # should have children showing struct internals
        children = diffs[0]["children"]
        assert len(children) == 2
        assert all(c["status"] == "added" for c in children)

    def test_subtree_has_diff_true(self):
        d = {
            "status": "match",
            "children": [{"status": "match", "children": [{"status": "changed", "children": []}]}],
        }
        assert _subtree_has_diff(d) is True

    def test_subtree_has_diff_false(self):
        d = {"status": "match", "children": [{"status": "match", "children": []}]}
        assert _subtree_has_diff(d) is False
