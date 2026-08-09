"""
dbt Transform DAG
-----------------
Runs dbt seed, then dbt run, then dbt test against the Snowflake warehouse.
Scheduled to run daily after the OpenSky ingestion DAG completes.
"""

from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.operators.bash import BashOperator

DBT_PROJECT_DIR  = "/opt/airflow/dbt"
DBT_PROFILES_DIR = "/opt/airflow/dbt"
DBT_VENV_BIN     = "/opt/dbt-venv/bin"

default_args = {
    "owner": "suriyakc",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

@dag(
    dag_id="dbt_transform",
    default_args=default_args,
    description="Run dbt seed + run + test for the medallion layers",
    schedule="0 1 * * *",
    start_date=datetime(2025, 3, 15),
    catchup=False,
    tags=["dbt", "snowflake", "transform"],
)
def dbt_transform_dag():

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_PROJECT_DIR} && {DBT_VENV_BIN}/dbt deps --profiles-dir {DBT_PROFILES_DIR}",
    )

    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=f"cd {DBT_PROJECT_DIR} && {DBT_VENV_BIN}/dbt seed --profiles-dir {DBT_PROFILES_DIR}",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_PROJECT_DIR} && {DBT_VENV_BIN}/dbt run --profiles-dir {DBT_PROFILES_DIR}",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_PROJECT_DIR} && {DBT_VENV_BIN}/dbt test --profiles-dir {DBT_PROFILES_DIR}",
    )

    dbt_deps >> dbt_seed >> dbt_run >> dbt_test

dbt_transform_dag()