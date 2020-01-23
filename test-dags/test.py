import datetime
from airflow import models
from airflow.contrib.operators import kubernetes_pod_operator
from airflow.operators import dummy_operator

YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)

volume_mount = VolumeMount('test-volume',
                            mount_path='/root/mount_file',
                            sub_path=None,
                            read_only=True)
volume_config= {
    'persistentVolumeClaim':
      {
        'claimName': 'test-volume'
      }
    }
volume = Volume(name='test-volume', configs=volume_config)

with models.DAG(
        dag_id='demo',
        schedule_interval=datetime.timedelta(days=1),
        start_date=YESTERDAY) as dag:

    task1 = kubernetes_pod_operator.KubernetesPodOperator(
        task_id='t1',
        name='task1',
        namespace='default',
        image='eu.gcr.io/taiyo-239217/dag:fae4887',
        arguments=["AlphaVantage()"],
		volume=[volume],
        volume_mounts=[volume_mount],
        in_cluster=True,
        xcom_push=True,
        is_delete_operator_pod=True
    )