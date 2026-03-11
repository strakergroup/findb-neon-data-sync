# Changelog — findb-neon-data-sync

- [Changed]: Renamed project from mysql-neon-sync to findb-neon-data-sync, updated all references (Atsushi Hirano, 2026-03-10)
- [Fixed]: Column mapper now overrides primary key based on config.yaml rather than MySQL source PK (Atsushi Hirano, 2026-03-10)
- [Fixed]: MySQL-to-PostgreSQL type conversion — strips collation, maps TINYINT/VARCHAR/DECIMAL to PG-compatible types (Atsushi Hirano, 2026-03-10)
- [Fixed]: Upsert batch splitting into sub-batches of 100 rows for better performance with remote NEON (Atsushi Hirano, 2026-03-10)
- [Fixed]: Column cloning now creates detached Column objects to avoid SQLAlchemy Table assignment conflicts (Atsushi Hirano, 2026-03-10)
- [Changed]: Dry-run mode now works without NEON connection configured (Atsushi Hirano, 2026-03-10)
- [Added]: Initial project — config, database, models, sync_service, column_mapper, main entry point (Atsushi Hirano, 2026-03-10)
- [Added]: Dockerfile (multi-stage Pipenv), docker-compose, k8s-cronjob manifest (Atsushi Hirano, 2026-03-10)
- [Added]: Unit tests for config, column_mapper, and sync_service (23 tests) (Atsushi Hirano, 2026-03-10)
- [Added]: Documentation — overview, architecture with data flow diagrams (Atsushi Hirano, 2026-03-10)
