# Architecture — findb-neon-data-sync

## Overview

findb-neon-data-sync is a batch ETL application that incrementally transfers rows from a MySQL source database (bi_data) to a NEON (managed PostgreSQL) target for the finDB project. It is designed to run as a Kubernetes CronJob on a configurable schedule.

## Data Flow

```mermaid
flowchart LR
    subgraph k8s [Kubernetes CronJob]
        App["findb-neon-data-sync"]
    end

    subgraph source [Source]
        MySQL[(MySQL bi_data)]
    end

    subgraph target [Target]
        NEON[(NEON PostgreSQL)]
        Meta["sync_metadata table"]
    end

    ConfigYAML["config.yaml"]
    EnvVars[".env / K8s Secret"]

    ConfigYAML --> App
    EnvVars --> App
    App -->|"SELECT WHERE col > last_value\n(read-only, streaming)"| MySQL
    App -->|"INSERT ... ON CONFLICT DO UPDATE\n(batched upsert)"| NEON
    App -->|"read/write checkpoint"| Meta
```

## Sync Pipeline (per table)

```mermaid
flowchart TD
    Start["Start table sync"] --> ReadMeta["Read sync_metadata\n(get last_synced_value)"]
    ReadMeta --> Query["Build incremental query\nWHERE inc_col > last_value"]
    Query --> Stream["Stream rows from MySQL\n(fetchmany with batch_size)"]
    Stream --> Map["Apply column mapping\n(pass-through by default)"]
    Map --> Upsert["Upsert batch into NEON\n(ON CONFLICT DO UPDATE)"]
    Upsert --> More{"More batches?"}
    More -->|Yes| Stream
    More -->|No| Update["Update sync_metadata\n(success + last_value)"]
    Upsert -->|Error| Fail["Record failure in sync_metadata\nLog error, continue to next table"]
```

## Module Responsibilities

| Module | Responsibility |
|--------|---------------|
| `src/main.py` | CLI entry point, argument parsing, orchestration loop |
| `src/config.py` | Pydantic settings (env vars) + YAML table config parsing |
| `src/database.py` | SQLAlchemy engine creation for MySQL (read-only) and NEON |
| `src/models.py` | `sync_metadata` ORM model + auto-migration |
| `src/sync_service.py` | Per-table sync logic: diff query, batch streaming, upsert, metadata updates |
| `src/column_mapper.py` | Column name/type translation (pass-through for now) |

## Key Design Decisions

- **State in database, not files**: Sync checkpoints are stored in the `sync_metadata` table on NEON, avoiding the need for PersistentVolumeClaims in Kubernetes.
- **Read-only MySQL session**: The MySQL connection is set to `TRANSACTION READ ONLY` to prevent accidental writes to the source.
- **Streaming reads**: `stream_results=True` with `fetchmany()` keeps memory bounded regardless of table size.
- **PostgreSQL-native upsert**: Uses `INSERT ... ON CONFLICT DO UPDATE` via SQLAlchemy's PostgreSQL dialect for atomic insert-or-update.
- **MySQL-to-PostgreSQL type mapping**: Automatic conversion of MySQL-specific types (VARCHAR with collation, TINYINT, DECIMAL, etc.) to PostgreSQL-compatible equivalents.
- **Configurable primary key override**: The target table's primary key is defined in `config.yaml`, allowing it to differ from the MySQL source PK.
- **Column mapper as extension point**: The `ColumnMapper` class is a thin pass-through today but provides the interface for future name/type remapping when MySQL and NEON schemas diverge.
