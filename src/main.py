"""findb-neon-data-sync: Incremental ETL from MySQL to NEON (PostgreSQL).

Usage:
    python -m src.main              # incremental sync
    python -m src.main --full       # full refresh
    python -m src.main --dry-run    # read only, log what would be written
    python -m src.main --table users # sync a single table
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timezone

from src.column_mapper import ColumnMapper
from src.config import AppSettings, get_settings, load_sync_config
from src.database import create_mysql_engine, create_neon_engine, verify_connections, verify_mysql_only
from src.models import ensure_metadata_table
from src.sync_service import SyncResult, SyncService

log = logging.getLogger("findb-neon-data-sync")


def _configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s  %(levelname)-8s  [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="MySQL → NEON incremental sync")
    parser.add_argument("--full", action="store_true", help="Ignore previous sync state; transfer all rows")
    parser.add_argument("--dry-run", action="store_true", help="Read from MySQL but do not write to NEON")
    parser.add_argument("--table", type=str, default=None, help="Sync only this table (by name)")
    parser.add_argument("--config", type=str, default=None, help="Path to config.yaml")
    return parser.parse_args()


def run(settings: AppSettings, *, full: bool, dry_run: bool, table_filter: str | None, config_path: str | None) -> list[SyncResult]:
    sync_config = load_sync_config(config_path or settings.sync_config_path)

    tables = [t for t in sync_config.tables if t.enabled]
    if table_filter:
        tables = [t for t in tables if t.name == table_filter]
        if not tables:
            log.error("Table '%s' not found or not enabled in config", table_filter)
            sys.exit(1)

    log.info("Tables to sync: %s", [t.name for t in tables])

    mysql_engine = create_mysql_engine(settings)

    neon_engine = None
    if dry_run and not settings.neon_database_url:
        log.warning("NEON_DATABASE_URL not set — dry-run will skip NEON operations")
    else:
        if not settings.neon_database_url:
            log.error("NEON_DATABASE_URL is required for non-dry-run execution")
            sys.exit(1)
        neon_engine = create_neon_engine(settings)

    if neon_engine is not None:
        verify_connections(mysql_engine, neon_engine)
        ensure_metadata_table(neon_engine)
    else:
        verify_mysql_only(mysql_engine)

    mapper = ColumnMapper()
    results: list[SyncResult] = []

    for tbl in tables:
        svc = SyncService(
            mysql_engine=mysql_engine,
            neon_engine=neon_engine,
            table_config=tbl,
            column_mapper=mapper,
            dry_run=dry_run,
        )
        result = svc.run(full_refresh=full)
        results.append(result)

    return results


def main() -> None:
    args = _parse_args()
    settings = get_settings()
    _configure_logging(settings.log_level)

    log.info("=== findb-neon-data-sync started at %s ===", datetime.now(timezone.utc).isoformat())
    if args.dry_run:
        log.info("DRY RUN mode — no data will be written to NEON")

    results = run(
        settings,
        full=args.full,
        dry_run=args.dry_run,
        table_filter=args.table,
        config_path=args.config,
    )

    succeeded = [r for r in results if r.success]
    failed = [r for r in results if not r.success]
    total_records = sum(r.records_synced for r in succeeded)

    log.info("=== Sync summary ===")
    log.info("Tables synced: %d/%d", len(succeeded), len(results))
    log.info("Total records transferred: %d", total_records)

    if failed:
        for r in failed:
            log.error("FAILED: %s — %s", r.table_name, r.error)
        sys.exit(1)

    log.info("All tables synced successfully")


if __name__ == "__main__":
    main()
