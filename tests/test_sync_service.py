from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from src.config import TableConfig
from src.models import SyncMetadata
from src.sync_service import SyncResult, SyncService


@pytest.fixture()
def table_config() -> TableConfig:
    return TableConfig(
        name="test_table",
        source_schema="app_db",
        target_schema="public",
        primary_key=["id"],
        incremental_column="updated_at",
        incremental_type="timestamp",
        batch_size=100,
    )


class TestSyncResult:
    def test_default_values(self) -> None:
        r = SyncResult(table_name="t")
        assert r.success is True
        assert r.records_synced == 0
        assert r.error is None

    def test_failed_result(self) -> None:
        r = SyncResult(table_name="t", success=False, error="boom")
        assert r.success is False
        assert r.error == "boom"


class TestSyncMetadata:
    def test_mark_success(self) -> None:
        m = SyncMetadata(table_name="t")
        m.mark_success("2025-01-01T00:00:00", 42)
        assert m.status == "success"
        assert m.records_synced == 42
        assert m.last_synced_value == "2025-01-01T00:00:00"
        assert m.error_message is None
        assert m.last_synced_at is not None

    def test_mark_failed(self) -> None:
        m = SyncMetadata(table_name="t")
        m.mark_failed("connection refused")
        assert m.status == "failed"
        assert m.error_message == "connection refused"

    def test_mark_failed_truncates_long_error(self) -> None:
        m = SyncMetadata(table_name="t")
        m.mark_failed("x" * 5000)
        assert len(m.error_message) == 2000

    def test_repr(self) -> None:
        m = SyncMetadata(table_name="users", status="success", last_synced_value="123")
        assert "users" in repr(m)
        assert "success" in repr(m)


class TestSyncServiceUnit:
    """Unit tests that do not require real database connections."""

    def test_run_catches_exception_and_returns_failure(self, table_config: TableConfig) -> None:
        svc = SyncService(
            mysql_engine=MagicMock(),
            neon_engine=MagicMock(),
            table_config=table_config,
            dry_run=True,
        )
        with patch.object(svc, "_read_last_synced_value", side_effect=RuntimeError("db down")):
            result = svc.run()
        assert result.success is False
        assert "db down" in result.error

    def test_upsert_batch_skips_empty(self, table_config: TableConfig) -> None:
        neon = MagicMock()
        svc = SyncService(
            mysql_engine=MagicMock(),
            neon_engine=neon,
            table_config=table_config,
        )
        svc._upsert_batch(MagicMock(), [], ["id"])
        neon.begin.assert_not_called()
