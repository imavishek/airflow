import datetime
from airflow import models
from airflow.contrib.operators import kubernetes_pod_operator
from airflow.operators import dummy_operator

YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)

with models.DAG(
        dag_id='demo',
        schedule_interval=datetime.timedelta(days=1),
        start_date=YESTERDAY) as dag:

    task1 = kubernetes_pod_operator.KubernetesPodOperator(
        task_id='t1',
        name='task1',
        namespace='airflow',
        image='eu.gcr.io/taiyo-239217/dag:fae4886',
        arguments=["AlphaVantage()"],
        in_cluster=True,
        xcom_push=True,
        is_delete_operator_pod=True
    )

    task2 = kubernetes_pod_operator.KubernetesPodOperator(
        task_id='t2',
        name='task2',
        namespace='airflow',
        image='eu.gcr.io/taiyo-239217/dag:fae4886',
        arguments=["FRED()"],
        in_cluster=True,
        xcom_push=True,
        is_delete_operator_pod=True
    )

    task3 = kubernetes_pod_operator.KubernetesPodOperator(
        task_id='t3',
        name='task3',
        namespace='airflow',
        image='eu.gcr.io/taiyo-239217/dag:fae4886',
        arguments=["TechFeatures()"],
        in_cluster=True,
        xcom_push=True,
        is_delete_operator_pod=True
    )

    task4 = kubernetes_pod_operator.KubernetesPodOperator(
        task_id='t4',
        name='task4',
        namespace='airflow',
        image='eu.gcr.io/taiyo-239217/dag:fae4886',
        arguments=["DataAggregation()"],
        in_cluster=True,
        xcom_push=True,
        is_delete_operator_pod=True
    )

[task1, task2] >> task3 >> task4
