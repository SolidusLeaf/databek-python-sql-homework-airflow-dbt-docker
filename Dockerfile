FROM apache/airflow:2.9.3

# dbt lives in its own virtualenv so it can't clash with Airflow's packages
RUN python -m venv /home/airflow/dbt_venv \
 && /home/airflow/dbt_venv/bin/pip install --no-cache-dir --upgrade pip \
 && /home/airflow/dbt_venv/bin/pip install --no-cache-dir \
      --default-timeout=100 --retries 10 \
      dbt-core==1.8.8 dbt-postgres==1.8.2