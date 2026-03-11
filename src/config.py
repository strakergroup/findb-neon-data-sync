from __future__ import annotations

import os
from enum import Enum
from pathlib import Path

import yaml
from dotenv import load_dotenv
from pydantic import BaseModel, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

load_dotenv()


class Environment(str, Enum):
    LOCAL = "local"
    DEV = "dev"
    TEST = "test"
    UAT = "uat"
    PRODUCTION = "production"


class ColumnMapping(BaseModel):
    """Maps a source column name to a target column name with optional type cast."""

    source: str
    target: str
    type_cast: str | None = None


class TableConfig(BaseModel):
    """Definition of a single table to sync."""

    name: str
    source_schema: str
    target_schema: str = "public"
    primary_key: list[str]
    incremental_column: str
    incremental_type: str = "timestamp"
    batch_size: int = 1000
    column_mapping: list[ColumnMapping] | None = None
    enabled: bool = True

    @field_validator("incremental_type")
    @classmethod
    def validate_incremental_type(cls, v: str) -> str:
        allowed = {"timestamp", "id"}
        if v not in allowed:
            raise ValueError(f"incremental_type must be one of {allowed}, got '{v}'")
        return v

    @field_validator("primary_key")
    @classmethod
    def validate_primary_key(cls, v: list[str]) -> list[str]:
        if not v:
            raise ValueError("primary_key must contain at least one column")
        return v


class SyncConfig(BaseModel):
    """Root model for the YAML config file."""

    tables: list[TableConfig]


class AppSettings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(frozen=True)

    mysql_host: str
    mysql_port: int = 3306
    mysql_user: str
    mysql_password: str
    mysql_database: str

    neon_database_url: str = ""

    environment: Environment = Environment.PRODUCTION
    log_level: str = "INFO"
    sync_config_path: str = Field(default="config.yaml")
    batch_size: int = 1000

    @field_validator("neon_database_url")
    @classmethod
    def normalise_neon_url(cls, v: str) -> str:
        if v.startswith("postgres://"):
            return v.replace("postgres://", "postgresql+psycopg2://", 1)
        if v.startswith("postgresql://") and "+psycopg2" not in v:
            return v.replace("postgresql://", "postgresql+psycopg2://", 1)
        return v

    @property
    def mysql_url(self) -> str:
        return (
            f"mysql+mysqlconnector://{self.mysql_user}:{self.mysql_password}"
            f"@{self.mysql_host}:{self.mysql_port}/{self.mysql_database}"
        )


def load_sync_config(path: str | Path | None = None) -> SyncConfig:
    """Load and validate the YAML table-sync configuration."""
    config_path = Path(path or os.getenv("SYNC_CONFIG_PATH", "config.yaml"))
    if not config_path.is_file():
        raise FileNotFoundError(f"Sync config not found at {config_path}")

    with open(config_path) as f:
        raw = yaml.safe_load(f)

    return SyncConfig.model_validate(raw)


def get_settings() -> AppSettings:
    """Create and return validated application settings."""
    return AppSettings()  # type: ignore[call-arg]
