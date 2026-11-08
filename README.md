# Orders pipeline: Airflow + dbt + PostgreSQL

An Airflow DAG that loads **yesterday's orders** from a source PostgreSQL table into a
staging table (idempotent), then runs **dbt** models that build a daily revenue table,
with `unique` / `not_null` tests.

Everything runs locally with Docker Compose. No accounts, API keys or manual setup needed.

## How it works

```
source.orders ──► staging.orders_staging ──► analytics.daily_revenue
   (seeded)        (Airflow SQL task)          (dbt model, completed orders only)
                                                     │
                                              dbt tests: unique, not_null
```

| Task | What it does |
|---|---|
| `load_orders_to_staging` | Deletes staging rows for the run date, then inserts that date's orders from `source.orders` (all statuses). |
| `dbt_run` | Builds `analytics.daily_revenue`: order count and total revenue per day, **`completed` orders only**. |
| `dbt_test` | Runs `unique` + `not_null` on `order_date` and `not_null` on `total_revenue`. |

Tasks run in this order: `load_orders_to_staging >> dbt_run >> dbt_test`.

## Requirements

- Docker Desktop (Windows / macOS) or Docker Engine with the Compose plugin (Linux)
- About 4 GB of free RAM for Docker
- Free ports **8080** (Airflow UI) and **5433** (warehouse database)

## Run it

```bash
docker compose up --build
```

The first start downloads images and installs dbt, which takes a few minutes. When the logs
settle:

1. Open http://localhost:8080 and log in with **admin / admin**.
2. The `orders_daily` DAG is enabled automatically, and on a fresh start the scheduler runs it
   once for yesterday. Open the DAG, then **Grid**: all three tasks should turn green.
3. To run it again, click the play button, then **Trigger DAG**.

Stop and delete everything (including the database volumes):

```bash
docker compose down -v
```

## Check the results

Run these three commands (they work in PowerShell, bash and zsh). Each prints one table.

**1. How many orders exist in the source for yesterday (UTC)?**

```bash
docker compose exec warehouse-db psql -U warehouse -d warehouse -c "select status, count(*) from source.orders where order_ts::date = current_date - 1 group by 1;"
```

**2. What did the DAG load into staging? (every order attempt, any status)**

```bash
docker compose exec warehouse-db psql -U warehouse -d warehouse -c "select status, count(*) from staging.orders_staging group by 1;"
```

**3. What did dbt build? (completed orders only)**

```bash
docker compose exec warehouse-db psql -U warehouse -d warehouse -c "select * from analytics.daily_revenue;"
```

The difference between 2 and 3 is intentional: staging keeps **all** orders, including
cancelled ones, as a faithful copy of the source. The revenue model filters to
`status = 'completed'`, so cancelled orders are never counted as revenue.

### Expected output on a fresh start

Seed data is generated when the database is first created, relative to that day. If you start
the stack and run the DAG on the same UTC day, you get:

Staging (command 2):

```
  status   | count
-----------+-------
 completed |     9
 cancelled |     1
```

Daily revenue (command 3):

```
 order_date | order_count | total_revenue
------------+-------------+---------------
 2026-11-07 |           9 |        974.00
```

The date is yesterday's date (UTC) at the time you run it. The `9` and `974.00` hold when the
seed and the DAG run happen on the same UTC day.

## What different outcomes mean

### Scenario 1: normal day (some completed, some cancelled)

This is the expected result above.

| Check | Result |
|---|---|
| Staging | 9 completed + 1 cancelled = 10 order attempts |
| `daily_revenue` | 1 row, `order_count = 9` |

Staging shows 10 and revenue shows 9 because the model discards cancelled orders.

### Scenario 2: every order of the day was cancelled

| Check | Result |
|---|---|
| Staging | only `cancelled` rows (for example `cancelled \| 10`) |
| `daily_revenue` | `(0 rows)` with the 3 columns `order_date`, `order_count`, `total_revenue` |
| DAG | all three tasks green, including `dbt_test` |

This is correct behaviour, not a bug: the day had order attempts but no revenue. The tests
pass because `unique` and `not_null` hold trivially on an empty table.

To reproduce it:

```bash
docker compose exec warehouse-db psql -U warehouse -d warehouse -c "update source.orders set status = 'cancelled' where order_ts::date = current_date - 1;"
```

Trigger the DAG in the UI and run commands 2 and 3 again.

### Scenario 3: no orders at all for the day

| Check | Result |
|---|---|
| Staging | `(0 rows)` |
| `daily_revenue` | `(0 rows)` |
| DAG | all three tasks green |

The load deletes the day's staging rows first, so stale data from an earlier run never
survives. To reproduce it:

```bash
docker compose exec warehouse-db psql -U warehouse -d warehouse -c "delete from source.orders where order_ts::date = current_date - 1;"
```

Trigger the DAG and run commands 2 and 3.

### Scenario 4: re-running the DAG (idempotency)

Trigger the DAG as many times as you like for the same day. Commands 2 and 3 return the same
numbers every time, because each run deletes that day's staging rows before inserting them
again. Rows are never duplicated.

### Resetting after experiments

Scenarios 2 and 3 modify the source table. To get the original seed data back:

```bash
docker compose down -v
docker compose up --build
```

## Design notes

- **Idempotent load:** delete-then-insert per run date, with all statements executed in one
  database transaction, so a failed run never leaves a half-loaded day.
- **"Yesterday":** taken from Airflow's `data_interval_start`, so it is the previous full
  **UTC** day. Scheduled and manually triggered runs both process that day.
- **dbt versions are pinned** (`dbt-core 1.8.8`, `dbt-postgres 1.8.2`). Unpinned installs pull
  a dbt-core 2.0 pre-release that does not support Postgres yet.
- **dbt runs in its own virtualenv** inside the Airflow image, so its dependencies cannot
  clash with Airflow's.
- **Credentials** are demo-only values (`warehouse/warehouse`, `airflow/airflow`,
  `admin/admin`). The databases exist only while the containers run, and all order data is
  generated fake data.
- `DBT_TARGET_PATH` and `DBT_LOG_PATH` point to `/tmp` inside the container, so dbt's generated
  files never end up in the repo.

## Repository layout

```
.
├── docker-compose.yml        # Airflow (webserver + scheduler), 2 Postgres databases
├── Dockerfile                # Airflow image + dbt in its own virtualenv
├── init/init.sql             # creates and seeds source.orders (runs on first DB start only)
├── dags/orders_daily.py      # the Airflow DAG
└── dbt/
    ├── dbt_project.yml
    ├── profiles.yml          # points dbt at the warehouse database
    └── models/
        ├── daily_revenue.sql
        └── schema.yml        # unique / not_null tests
```

## Troubleshooting

| Problem | Fix |
|---|---|
| `daily_revenue` is empty | Compare commands 1, 2 and 3. If staging has only `cancelled` rows or no rows, see scenarios 2 and 3. If you started the stack days ago, "yesterday" may be outside the seeded window: reset with `down -v`. |
| `port is already allocated` | Change the left number in `ports:` in `docker-compose.yml` (for example `"8081:8080"`). |
| Containers restart or get killed | Give Docker more memory (about 4 GB or more). |
| `no configuration file provided` | Run the command from the folder that contains `docker-compose.yml`. |
| A task is red | Open the DAG, click the task, then **Logs**. |