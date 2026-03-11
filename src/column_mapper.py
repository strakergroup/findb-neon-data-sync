from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from sqlalchemy import BigInteger, Boolean, Column, DateTime, Integer, Numeric, String, Text
from sqlalchemy.types import TypeEngine

if TYPE_CHECKING:
    from collections.abc import Iterable

    from src.config import ColumnMapping

log = logging.getLogger(__name__)

_MYSQL_TO_PG_TYPE_MAP = {
    "TINYINT": lambda c: Boolean() if getattr(c.type, "display_width", None) == 1 else Integer(),
    "SMALLINT": lambda _: Integer(),
    "MEDIUMINT": lambda _: Integer(),
    "INT": lambda _: Integer(),
    "INTEGER": lambda _: Integer(),
    "BIGINT": lambda _: BigInteger(),
    "DATETIME": lambda _: DateTime(),
    "TIMESTAMP": lambda _: DateTime(timezone=True),
}


def _pg_compatible_type(col: Column) -> TypeEngine:
    """Convert a MySQL-reflected column type to a PostgreSQL-compatible type."""
    type_name = type(col.type).__name__.upper()

    if type_name in _MYSQL_TO_PG_TYPE_MAP:
        return _MYSQL_TO_PG_TYPE_MAP[type_name](col)

    if type_name in ("VARCHAR", "CHAR", "ENUM", "SET"):
        length = getattr(col.type, "length", None)
        return String(length) if length else Text()

    if type_name == "TEXT" or type_name in ("MEDIUMTEXT", "LONGTEXT", "TINYTEXT"):
        return Text()

    if type_name == "DECIMAL":
        precision = getattr(col.type, "precision", None)
        scale = getattr(col.type, "scale", None)
        return Numeric(precision=precision, scale=scale)

    return col.type


def _clone_column(
    col: Column,
    new_name: str | None = None,
    *,
    force_primary_key: bool | None = None,
) -> Column:
    """Create a detached, PostgreSQL-compatible copy of a MySQL Column."""
    is_pk = force_primary_key if force_primary_key is not None else col.primary_key
    return Column(
        new_name or col.name,
        _pg_compatible_type(col),
        primary_key=is_pk,
        nullable=col.nullable if not is_pk else False,
        default=col.default,
    )


class ColumnMapper:
    """Translates column names (and optionally types) between source and target.

    Current implementation is pass-through. Extend ``map_columns`` and
    ``map_rows`` to apply renaming or type-casting defined in
    ``config.yaml → column_mapping``.
    """

    def map_columns(
        self,
        source_columns: Iterable[Column],
        mapping: list[ColumnMapping] | None = None,
        target_primary_keys: list[str] | None = None,
    ) -> list[Column]:
        """Return a list of SQLAlchemy Column objects for the target table.

        When ``target_primary_keys`` is provided, the PK assignment is
        overridden to match the config rather than the MySQL source.
        """
        lookup = {m.source: m for m in mapping} if mapping else {}
        result: list[Column] = []
        for col in source_columns:
            target_name = lookup[col.name].target if col.name in lookup else None
            effective_name = target_name or col.name

            force_pk = None
            if target_primary_keys is not None:
                force_pk = effective_name in target_primary_keys

            result.append(_clone_column(col, target_name, force_primary_key=force_pk))
        return result

    def map_rows(
        self,
        rows: list[dict],
        mapping: list[ColumnMapping] | None = None,
    ) -> list[dict]:
        """Rename dict keys according to the column mapping."""
        if not mapping:
            return rows

        rename = {m.source: m.target for m in mapping}
        return [
            {rename.get(k, k): v for k, v in row.items()}
            for row in rows
        ]
