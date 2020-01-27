import datetime
from airflow import models
from airflow.contrib.operators import kubernetes_pod_operator
from airflow.operators import dummy_operator
from airflow.contrib.kubernetes.volume import Volume
from airflow.contrib.kubernetes.volume_mount import VolumeMount

YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)

with models.DAG(
        dag_id='airflow-demo',
        schedule_interval=datetime.timedelta(days=1),
        start_date=YESTERDAY) as dag:

    task1 = kubernetes_pod_operator.KubernetesPodOperator(
        task_id='t1',
        name='task1',
        namespace='default',
        image='eu.gcr.io/taiyo-239217/dag:fae4887',
        arguments=["AlphaVantage()"],
        in_cluster=True,
        xcom_push=True,
        is_delete_operator_pod=True
    )