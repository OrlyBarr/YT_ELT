# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

ELT pipeline that pulls video stats for one YouTube channel through the YouTube Data API v3, loads them into Postgres (`staging` then `core` schemas), and validates them with Soda. It runs on Airflow 2.9.2 (Python 3.10, CeleryExecutor) in Docker Compose. The custom Airflow image (from `Dockerfile` + `requirements.txt`) is pulled from Docker Hub as `${DOCKERHUB_NAMESPACE}/${DOCKERHUB_REPOSITORY}:<tag>`.

## Commands

Everything runs inside the containers. Nothing is set up to run on the host venv.

```bash
docker compose up -d                 # start postgres, redis, airflow-init, webserver (localhost:8080), scheduler, worker
docker compose down                  # stop (add -v to wipe the postgres volume and re-run the DB init script)

# Tests (tests/ is mounted at /opt/airflow/tests)
docker exec -it airflow-worker pytest tests/
docker exec -it airflow-worker pytest tests/unit_test.py
docker exec -it airflow-worker pytest tests/unit_test.py::test_dags_integrity

# Soda data-quality scan by hand (SCHEMA = staging | core)
docker exec -it airflow-worker soda scan -d pg_datasource -c /opt/airflow/include/soda/configuration.yml -v SCHEMA=core /opt/airflow/include/soda/checks.yml
```

There is no linter or formatter configured.

## Architecture

All three DAGs are defined in `dags/main.py`. They are chained with `TriggerDagRunOperator`, so only the first one has a schedule:

1. `produce_json` (daily 14:00 Asia/Jerusalem): `dags/api/video_stats.py` gets the uploads playlist, then the video IDs, then video details in batches of 50, and writes them to `./data/YT_data_<today>.json`.
2. `update_db`: `dags/datawarehouse/dwh.py` has `staging_table` and `core_table`.
3. `data_quality`: `dags/dataquality/soda.py` runs one `BashOperator` per schema, executing `soda scan`.

Things that only become clear across several files:

- **The file handoff is by date.** `data_loading.load_data()` reads `YT_data_{date.today()}.json`, so `update_db` fails if that day's `produce_json` hasn't run. Paths are relative and rely on the Airflow working directory being `/opt/airflow`.
- **Staging and core use different row key names.** Staging rows are the raw JSON dicts (`video_id`, `title`, `publishedAt`, ...). Core rows are `RealDictCursor` rows read back from `staging.yt_api` (`Video_ID`, `Video_Title`, ...). `insert_rows`/`update_rows` in `data_modification.py` choose which keys to use with `if schema == 'staging'`. Keep both branches in sync when you change columns.
- **Each run fully syncs the table.** It upserts every row (update if `Video_ID` exists, otherwise insert) and then deletes IDs that are no longer in the source. Core adds `Video_Type` (Short if ≤60s) and converts the ISO-8601 `Duration` to `TIME` (`data_transformation.py`). Both schemas' tables are created in `data_utils.create_table`, and both are named `yt_api`. Column names are quoted mixed-case identifiers.
- **Config comes from environment variables, not the Airflow UI.** `AIRFLOW_VAR_API_KEY`, `AIRFLOW_VAR_CHANNEL_HANDLE` and `AIRFLOW_CONN_POSTGRES_DB_YT_ELT` are built in `docker-compose.yaml` from `.env` values. `video_stats.py` calls `Variable.get` **at import time**, so DAG parsing (and `test_dags_integrity`) fails if those variables are missing. Soda's `configuration.yml` reads the same `ELT_*`/`POSTGRES_CONN_*` environment variables.
- **One Postgres container holds three databases.** These are metadata, celery backend and ELT, created by `docker/postgres/init-multiple-databases.sh` on the first volume init only. The host port is **5433** (5432 is used by a native Windows Postgres).
- `tests/unit_test.py::test_dags_integrity` asserts exact DAG IDs and task counts (5/3/2). Update it when you add or remove tasks. `tests/integration_test.py` calls the real YouTube API and the real ELT database.

## Local vs. GitHub Actions

`.github/workflows/ci-cd_yt-elt.yaml` builds and pushes the image only when `Dockerfile`/`requirements.txt` changes, and then runs the tests. `docker-compose.yaml` has comments saying "SWITCH THIS OFF/ON WHEN USING GITHUB ACTIONS" next to `env_file: .env` and the extra postgres environment variables. Compose still substitutes `${...}` from `.env` locally even when `env_file` is commented out. The image tag must exist locally or on Docker Hub (`.env` has `IMAGE_TAG`; the compose file currently says `:latest`).
