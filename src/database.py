from __future__ import annotations

import logging

from sqlalchemy import create_engine, event, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from src.config import AppSettings, Environment

log = logging.getLogger(__name__)

_POOL_DEFAULTS = {
    Environment.LOCAL: {"pool_size": 2, "max_overflow": 3, "pool_recycle": 1800},
    Environment.DEV: {"pool_size": 3, "max_overflow": 5, "pool_recycle": 1800},
    Environment.PRODUCTION: {"pool_size": 5, "max_overflow": 10, "pool_recycle": 900},
}


def _pool_kwargs(env: Environment) -> dict:
    return _POOL_DEFAULTS.get(env, _POOL_DEFAULTS[Environment.PRODUCTION])


def create_mysql_engine(settings: AppSettings) -> Engine:
    """Create a read-only SQLAlchemy engine for the MySQL source."""
    engine = create_engine(
        settings.mysql_url,
        **_pool_kwargs(settings.environment),
        echo=(settings.environment == Environment.LOCAL),
    )

    @event.listens_for(engine, "connect")
    def _set_readonly(dbapi_conn, _rec):
        cursor = dbapi_conn.cursor()
        cursor.execute("SET SESSION TRANSACTION READ ONLY")
        cursor.close()

    log.info("MySQL engine created for %s/%s", settings.mysql_host, settings.mysql_database)
    return engine


def create_neon_engine(settings: AppSettings) -> Engine:
    """Create an SQLAlchemy engine for the NEON (PostgreSQL) target."""
    connect_args: dict = {}
    if "sslmode" not in settings.neon_database_url:
        connect_args["sslmode"] = "require"

    engine = create_engine(
        settings.neon_database_url,
        **_pool_kwargs(settings.environment),
        connect_args=connect_args,
        echo=(settings.environment == Environment.LOCAL),
    )
    log.info("NEON engine created")
    return engine


def make_session_factory(engine: Engine) -> sessionmaker[Session]:
    return sessionmaker(bind=engine, expire_on_commit=False)


def verify_connections(mysql_engine: Engine, neon_engine: Engine) -> None:
    """Smoke-test both database connections; raises on failure."""
    with mysql_engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    log.info("MySQL connection verified")

    with neon_engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    log.info("NEON connection verified")


def verify_mysql_only(mysql_engine: Engine) -> None:
    """Smoke-test only the MySQL connection."""
    with mysql_engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    log.info("MySQL connection verified (NEON skipped)")
