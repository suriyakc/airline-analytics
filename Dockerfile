FROM apache/airflow:2.10.4-python3.11

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

USER root

RUN python3 -m venv /opt/dbt-venv && \
    /opt/dbt-venv/bin/pip install --no-cache-dir dbt-snowflake==1.9.0 && \
    chown -R airflow:0 /opt/dbt-venv

USER airflow

ENV DBT_VENV_PATH=/opt/dbt-venv/bin

COPY dbt /opt/airflow/dbt

WORKDIR /opt/airflow