from __future__ import annotations

from copy import copy

import pytest
from sqlalchemy import Column, Integer, String

from src.column_mapper import ColumnMapper
from src.config import ColumnMapping


@pytest.fixture()
def mapper() -> ColumnMapper:
    return ColumnMapper()


@pytest.fixture()
def sample_columns() -> list[Column]:
    return [
        Column("id", Integer, primary_key=True),
        Column("user_name", String(100)),
        Column("email", String(255)),
    ]


class TestMapColumns:
    def test_passthrough_without_mapping(self, mapper: ColumnMapper, sample_columns: list[Column]) -> None:
        result = mapper.map_columns(sample_columns, mapping=None)
        assert len(result) == 3
        assert [c.name for c in result] == ["id", "user_name", "email"]

    def test_rename_with_mapping(self, mapper: ColumnMapper, sample_columns: list[Column]) -> None:
        mapping = [
            ColumnMapping(source="user_name", target="username"),
        ]
        result = mapper.map_columns(sample_columns, mapping=mapping)
        names = [c.name for c in result]
        assert "username" in names
        assert "user_name" not in names
        assert "id" in names
        assert "email" in names

    def test_does_not_mutate_originals(self, mapper: ColumnMapper, sample_columns: list[Column]) -> None:
        mapping = [ColumnMapping(source="email", target="email_address")]
        mapper.map_columns(sample_columns, mapping=mapping)
        assert sample_columns[2].name == "email"

    def test_override_primary_key(self, mapper: ColumnMapper, sample_columns: list[Column]) -> None:
        result = mapper.map_columns(sample_columns, target_primary_keys=["email"])
        pk_cols = [c for c in result if c.primary_key]
        non_pk_cols = [c for c in result if not c.primary_key]
        assert len(pk_cols) == 1
        assert pk_cols[0].name == "email"
        assert "id" in [c.name for c in non_pk_cols]


class TestMapRows:
    def test_passthrough_without_mapping(self, mapper: ColumnMapper) -> None:
        rows = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        result = mapper.map_rows(rows, mapping=None)
        assert result == rows

    def test_rename_keys(self, mapper: ColumnMapper) -> None:
        rows = [{"id": 1, "old_col": "val"}]
        mapping = [ColumnMapping(source="old_col", target="new_col")]
        result = mapper.map_rows(rows, mapping=mapping)
        assert result == [{"id": 1, "new_col": "val"}]

    def test_empty_rows(self, mapper: ColumnMapper) -> None:
        result = mapper.map_rows([], mapping=None)
        assert result == []

    def test_unmapped_keys_preserved(self, mapper: ColumnMapper) -> None:
        rows = [{"a": 1, "b": 2, "c": 3}]
        mapping = [ColumnMapping(source="a", target="x")]
        result = mapper.map_rows(rows, mapping=mapping)
        assert result == [{"x": 1, "b": 2, "c": 3}]
