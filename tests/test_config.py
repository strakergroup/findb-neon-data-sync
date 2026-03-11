from __future__ import annotations

import os
import textwrap
from pathlib import Path

import pytest

from src.config import (
    AppSettings,
    ColumnMapping,
    SyncConfig,
    TableConfig,
    load_sync_config,
)


@pytest.fixture()
def yaml_file(tmp_path: Path) -> Path:
    content = textwrap.dedent("""\
        tables:
          - name: users
            source_schema: app_db
            target_schema: public
            primary_key: [id]
            incremental_column: updated_at
            incremental_type: timestamp
            batch_size: 500
            enabled: true
          - name: orders
            source_schema: app_db
            target_schema: public
            primary_key: [order_id]
            incremental_column: id
            incremental_type: id
            batch_size: 200
            enabled: false
    """)
    p = tmp_path / "config.yaml"
    p.write_text(content)
    return p


def test_load_sync_config_parses_tables(yaml_file: Path) -> None:
    cfg = load_sync_config(yaml_file)
    assert isinstance(cfg, SyncConfig)
    assert len(cfg.tables) == 2

    users = cfg.tables[0]
    assert users.name == "users"
    assert users.primary_key == ["id"]
    assert users.incremental_type == "timestamp"
    assert users.batch_size == 500
    assert users.enabled is True

    orders = cfg.tables[1]
    assert orders.enabled is False


def test_load_sync_config_missing_file(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        load_sync_config(tmp_path / "nonexistent.yaml")


def test_table_config_rejects_invalid_incremental_type() -> None:
    with pytest.raises(ValueError, match="incremental_type"):
        TableConfig(
            name="t",
            source_schema="s",
            primary_key=["id"],
            incremental_column="col",
            incremental_type="invalid",
        )


def test_table_config_rejects_empty_primary_key() -> None:
    with pytest.raises(ValueError, match="primary_key"):
        TableConfig(
            name="t",
            source_schema="s",
            primary_key=[],
            incremental_column="col",
        )


def test_app_settings_normalises_neon_url(monkeypatch: pytest.MonkeyPatch) -> None:
    env = {
        "MYSQL_HOST": "localhost",
        "MYSQL_USER": "root",
        "MYSQL_PASSWORD": "pw",
        "MYSQL_DATABASE": "db",
        "NEON_DATABASE_URL": "postgres://u:p@host/db?sslmode=require",
    }
    for k, v in env.items():
        monkeypatch.setenv(k, v)

    settings = AppSettings()  # type: ignore[call-arg]
    assert settings.neon_database_url.startswith("postgresql+psycopg2://")


def test_app_settings_mysql_url(monkeypatch: pytest.MonkeyPatch) -> None:
    env = {
        "MYSQL_HOST": "myhost",
        "MYSQL_PORT": "3307",
        "MYSQL_USER": "user",
        "MYSQL_PASSWORD": "pass",
        "MYSQL_DATABASE": "mydb",
        "NEON_DATABASE_URL": "postgresql+psycopg2://u:p@host/db",
    }
    for k, v in env.items():
        monkeypatch.setenv(k, v)

    settings = AppSettings()  # type: ignore[call-arg]
    assert settings.mysql_url == "mysql+mysqlconnector://user:pass@myhost:3307/mydb"


def test_column_mapping_model() -> None:
    m = ColumnMapping(source="old_name", target="new_name", type_cast="TEXT")
    assert m.source == "old_name"
    assert m.target == "new_name"
    assert m.type_cast == "TEXT"
