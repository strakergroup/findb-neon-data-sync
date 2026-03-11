from __future__ import annotations

import logging
from datetime import datetime, timezone

from sqlalchemy import DateTime, Integer, String, Text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

log = logging.getLogger(__name__)


class Base(DeclarativeBase):
    pass


class SyncMetadata(Base):
    """Tracks the last-synced position for each table."""

    __tablename__ = "sync_metadata"

    table_name: Mapped[str] = mapped_column(String(255), primary_key=True)
    last_synced_value: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_synced_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    records_synced: Mapped[int] = mapped_column(Integer, default=0)
    status: Mapped[str] = mapped_column(String(32), default="pending")
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)

    def mark_success(self, synced_value: str, count: int) -> None:
        self.last_synced_value = synced_value
        self.last_synced_at = datetime.now(timezone.utc)
        self.records_synced = count
        self.status = "success"
        self.error_message = None

    def mark_failed(self, error: str) -> None:
        self.last_synced_at = datetime.now(timezone.utc)
        self.status = "failed"
        self.error_message = error[:2000]

    def __repr__(self) -> str:
        return (
            f"<SyncMetadata table={self.table_name!r} "
            f"status={self.status!r} last={self.last_synced_value!r}>"
        )


def ensure_metadata_table(neon_engine: Engine) -> None:
    """Create the sync_metadata table on NEON if it does not exist."""
    Base.metadata.create_all(neon_engine, tables=[SyncMetadata.__table__])
    log.info("sync_metadata table ensured on NEON")
