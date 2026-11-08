from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# The day this run is responsible for. For a daily schedule this is "yesterday",
# and a manual trigger also resolves to the last full day.
RUN_DATE = "{{ data_interval_start | ds }}"

DBT_DIRS = "--project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt"
DBT_ENV = {"DBT_TARGET_PATH": "/tmp/dbt_target", "DBT_LOG_PATH": "/tmp/dbt_logs"}

with DAG(
    dag_id="orders_daily",
    description="Load yesterday's orders into staging, then build daily revenue with dbt",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["orders", "dbt"],
) as dag:

    # Idempotent: delete that day's rows, then insert them again.
    # All statements run in one transaction, so a failure never leaves a half-loaded day.
    load_staging = SQLExecuteQueryOperator(
        task_id="load_orders_to_staging",
        conn_id="warehouse",
        sql=[
            "CREATE SCHEMA IF NOT EXISTS staging;",
            """
            CREATE TABLE IF NOT EXISTS staging.orders_staging (
                order_id    INTEGER PRIMARY KEY,
                customer_id INTEGER NOT NULL,
                order_ts    TIMESTAMP NOT NULL,
                amount      NUMERIC(10, 2) NOT NULL,
                status      TEXT NOT NULL,
                order_date  DATE NOT NULL,
                loaded_at   TIMESTAMP NOT NULL DEFAULT now()
            );
            """,
            f"DELETE FROM staging.orders_staging WHERE order_date = DATE '{RUN_DATE}';",
            f"""
            INSERT INTO staging.orders_staging
                (order_id, customer_id, order_ts, amount, status, order_date)
            SELECT order_id, customer_id, order_ts, amount, status, order_ts::date
            FROM source.orders
            WHERE order_ts >= DATE '{RUN_DATE}'
              AND order_ts <  DATE '{RUN_DATE}' + 1;
            """,
        ],
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"/home/airflow/dbt_venv/bin/dbt run {DBT_DIRS}",
        env=DBT_ENV,
        append_env=True,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"/home/airflow/dbt_venv/bin/dbt test {DBT_DIRS}",
        env=DBT_ENV,
        append_env=True,
    )

    load_staging >> dbt_run >> dbt_test