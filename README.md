# findb-neon-data-sync

Incremental ETL application that transfers data from MySQL (bi_data) to NEON (managed PostgreSQL) for the finDB project. Designed to run as a Kubernetes CronJob.

## Features

- **Incremental sync** via timestamp-based tracking, stored in a `sync_metadata` table on NEON
- **Upsert** writes using `ON CONFLICT DO UPDATE` to handle both inserts and updates
- **YAML-driven config** for adding/removing tables without code changes
- **Batch streaming** to handle large tables with bounded memory
- **Column mapping** interface for future name/type translation between MySQL and PostgreSQL

## Quick Start

```bash
# Install dependencies
pipenv install

# Copy and fill in environment variables
cp .env.example .env

# Edit config.yaml to define tables to sync
# Then run:
pipenv run python -m src.main

# Full refresh (ignore previous sync state):
pipenv run python -m src.main --full

# Dry run (read from MySQL, log what would be written):
pipenv run python -m src.main --dry-run

# Sync a single table:
pipenv run python -m src.main --table clients
```

## Configuration

### Environment Variables

See [.env.example](.env.example) for all required variables.

### Table Definitions

Edit `config.yaml` to define which tables to sync. See [docs/overview.md](docs/overview.md) for full schema reference.

## Docker

```bash
docker compose up --build
```

## Kubernetes

Deploy as a CronJob using the provided manifest:

```bash
kubectl apply -f k8s-cronjob.yaml
```

## Testing

```bash
pipenv install --dev
pipenv run pytest tests/ -v
```

## Documentation

- [Overview](docs/overview.md)
- [Architecture](docs/architecture.md)
- [Changelog](docs/changelog.md)
