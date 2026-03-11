from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone

from sqlalchemy import MetaData, Table, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from src.column_mapper import ColumnMapper
from src.config import TableConfig
from src.models import SyncMetadata

log = logging.getLogger(__name__)


@dataclass
class SyncResult:
    """Result summary for a single table sync."""

    table_name: str
    records_synced: int = 0
    last_value: str | None = None
    success: bool = True
    error: str | None = None


@dataclass
class SyncService:
    """Orchestrates incremental data transfer for a single table."""

    mysql_engine: Engine
    neon_engine: Engine | None
    table_config: TableConfig
    column_mapper: ColumnMapper = field(default_factory=ColumnMapper)
    dry_run: bool = False

    def run(self, full_refresh: bool = False) -> SyncResult:
        """Execute the sync pipeline for this table."""
        tname = self.table_config.name
        log.info("Starting sync for table: %s", tname)

        try:
            last_value = None if full_refresh else self._read_last_synced_value()
            if last_value:
                log.info("Resuming from %s = %s", self.table_config.incremental_column, last_value)
            else:
                log.info("Full sync (no previous checkpoint)")

            source_table = self._reflect_source_table()

            if self.dry_run and self.neon_engine is None:
                target_table = None
            else:
                target_table = self._reflect_or_create_target_table(source_table)

            total, new_last = self._transfer(source_table, target_table, last_value)

            if not self.dry_run and total > 0:
                self._update_metadata(new_last, total, success=True)

            log.info("Sync complete for %s: %d records transferred", tname, total)
            return SyncResult(table_name=tname, records_synced=total, last_value=new_last)

        except Exception as exc:
            log.exception("Sync failed for table %s", tname)
            if not self.dry_run:
                self._update_metadata(None, 0, success=False, error=str(exc))
            return SyncResult(table_name=tname, success=False, error=str(exc))

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _read_last_synced_value(self) -> str | None:
        if self.neon_engine is None:
            return None
        with Session(self.neon_engine) as session:
            meta = session.get(SyncMetadata, self.table_config.name)
            if meta and meta.status == "success":
                return meta.last_synced_value
        return None

    def _reflect_source_table(self) -> Table:
        metadata = MetaData(schema=self.table_config.source_schema)
        return Table(
            self.table_config.name,
            metadata,
            autoload_with=self.mysql_engine,
        )

    def _reflect_or_create_target_table(self, source_table: Table) -> Table:
        """Ensure the target table exists on NEON, creating it if needed."""
        target_meta = MetaData(schema=self.table_config.target_schema)
        try:
            return Table(
                self.table_config.name,
                target_meta,
                autoload_with=self.neon_engine,
            )
        except Exception:
            log.info("Target table %s not found on NEON; creating from source schema", self.table_config.name)
            mapped_columns = self.column_mapper.map_columns(
                source_table.columns,
                self.table_config.column_mapping,
                target_primary_keys=self.table_config.primary_key,
            )
            target = Table(
                self.table_config.name,
                target_meta,
                *mapped_columns,
            )
            target.create(self.neon_engine)
            log.info("Created target table %s.%s", self.table_config.target_schema, self.table_config.name)
            return target

    def _transfer(
        self, source_table: Table, target_table: Table, last_value: str | None
    ) -> tuple[int, str | None]:
        """Stream rows from MySQL to NEON in batches, return (count, last_value)."""
        inc_col = self.table_config.incremental_column
        batch_size = self.table_config.batch_size
        pk_cols = self.table_config.primary_key

        query = source_table.select().order_by(text(inc_col))
        if last_value:
            col = source_table.c[inc_col]
            query = query.where(col > text(":last_val")).params(last_val=last_value)

        total = 0
        new_last: str | None = last_value

        with self.mysql_engine.connect() as mysql_conn:
            result = mysql_conn.execution_options(stream_results=True).execute(query)

            while True:
                rows = result.fetchmany(batch_size)
                if not rows:
                    break

                mapped_rows = self.column_mapper.map_rows(
                    [dict(r._mapping) for r in rows],
                    self.table_config.column_mapping,
                )

                if self.dry_run:
                    log.info("[DRY RUN] Would upsert %d rows into %s", len(mapped_rows), self.table_config.name)
                else:
                    self._upsert_batch(target_table, mapped_rows, pk_cols)

                row_last = str(mapped_rows[-1].get(inc_col, ""))
                if row_last:
                    new_last = row_last

                total += len(mapped_rows)
                log.info("Batch complete: %d rows (running total: %d)", len(mapped_rows), total)

        return total, new_last

    def _upsert_batch(self, table: Table, rows: list[dict], pk_cols: list[str]) -> None:
        """Upsert rows using PostgreSQL ON CONFLICT DO UPDATE.

        Splits large batches into sub-batches of 100 rows to keep individual
        INSERT statements small and reduce network payload to remote NEON.
        """
        if not rows:
            return

        SUB_BATCH_SIZE = 100

        with self.neon_engine.begin() as conn:
            for i in range(0, len(rows), SUB_BATCH_SIZE):
                chunk = rows[i : i + SUB_BATCH_SIZE]
                stmt = pg_insert(table).values(chunk)
                update_cols = {c.name: stmt.excluded[c.name] for c in table.columns if c.name not in pk_cols}
                stmt = stmt.on_conflict_do_update(index_elements=pk_cols, set_=update_cols)
                conn.execute(stmt)

    def _update_metadata(
        self,
        last_value: str | None,
        count: int,
        *,
        success: bool,
        error: str | None = None,
    ) -> None:
        with Session(self.neon_engine) as session:
            meta = session.get(SyncMetadata, self.table_config.name)
            if meta is None:
                meta = SyncMetadata(table_name=self.table_config.name)
                session.add(meta)

            if success:
                meta.mark_success(last_value or "", count)
            else:
                meta.mark_failed(error or "unknown error")

            session.commit()
