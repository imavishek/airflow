#!/bin/bash
set -e # Any subsequent commands which fail will cause the shell script to exit immediately

export AIRFLOW__CORE__AIRFLOW_HOME="$( pwd )"
export AIRFLOW_HOME="${AIRFLOW__CORE__AIRFLOW_HOME}"

echo "Setting Airflow Home to ${AIRFLOW__CORE__AIRFLOW_HOME}"

export AIRFLOW__CORE__DAGS_FOLDER="${AIRFLOW__CORE__AIRFLOW_HOME}/dags"
export AIRFLOW__CORE__BASE_LOG_FOLDER="${AIRFLOW__CORE__AIRFLOW_HOME}/logs"
export AIRFLOW__CORE__DAG_PROCESSOR_MANAGER_LOG_LOCATION="${AIRFLOW__CORE__AIRFLOW_HOME}/logs/dag_processor_manager/dag_processor_manager.log"
export AIRFLOW__CORE__PLUGINS_FOLDER="${AIRFLOW__CORE__AIRFLOW_HOME}/plugins"
export AIRFLOW__CORE__SQL_ALCHEMY_CONN="postgresql+psycopg2://airflow:airflow@127.0.0.1:5432/airflow"
export AIRFLOW__CORE__EXECUTOR="CeleryExecutor"
export AIRFLOW__CORE__LOAD_EXAMPLES="False"

export AIRFLOW__CELERY__BROKER_URL="redis://127.0.0.1:6379/1"
export AIRFLOW__CELERY__RESULT_BACKEND="db+postgresql://airflow:airflow@127.0.0.1:5432/airflow"

export AIRFLOW__SCHEDULER__CHILD_PROCESS_LOG_DIRECTORY="${AIRFLOW__CORE__AIRFLOW_HOME}/logs/scheduler"