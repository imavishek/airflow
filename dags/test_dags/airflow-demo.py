import os
import json
import yaml
import logging
import datetime
import pandas as pd
from airflow.operators import dummy_operator
from airflow import DAG, AirflowException, models
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators import kubernetes_pod_operator

logger = logging.getLogger(__name__)
run_time = '{{ ts }}'
dag_name = 'ansys_pipeline_test'
namespace = 'airflow-tasks'
image = 'eu.gcr.io/taiyo-239217/dag:fae5009'

default_args = {
    'owner': 'Taiyo',
    'depends_on_past': False,
    'start_date': datetime.datetime.today() - datetime.timedelta(days=1),
    'email': ['avishek.akd@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False
}


with DAG(dag_name, default_args=default_args, schedule_interval=datetime.timedelta(days=1)) as dag:

    start = BashOperator(
        task_id='start',
        bash_command='date',
    )

    alphavantage_path = dag_name + '/alphavantage.json'
    alphavantage = kubernetes_pod_operator.KubernetesPodOperator(
        task_id='alphavantage',
        name='alphavantage',
        namespace=namespace,
        image=image,
        arguments=["from pipeline_abstractions import *; AlphaVantageData(BUCKET_PATH_PUSH='{}')".format(alphavantage_path)],
        startup_timeout_seconds=900,
        is_delete_operator_pod=True
    )

    fred_path = dag_name + '/fred.json'
    fred = kubernetes_pod_operator.KubernetesPodOperator(
        task_id='fred',
        name='fred',
        namespace=namespace,
        image=image,
        arguments=["from pipeline_abstractions import *; FRED(BUCKET_PATH_PUSH='{}')".format(fred_path)],
        startup_timeout_seconds=900,
        is_delete_operator_pod=True
    )

    techfeatures_path = dag_name + '/techfeatures.json'
    techfeatures = kubernetes_pod_operator.KubernetesPodOperator(
        task_id='techfeatures',
        name='techfeatures',
        namespace=namespace,
        image=image,
        arguments=["from pipeline_abstractions import *; TechFeatures(BUCKET_PATH_PUSH='{}', BUCKET_PATH_PULL='{}')".format(techfeatures_path, alphavantage_path)],
        startup_timeout_seconds=900,
        is_delete_operator_pod=True
    )

    dataaggregation_path = dag_name + '/dataaggregation.json'
    dataaggregation = kubernetes_pod_operator.KubernetesPodOperator(
        task_id='dataaggregation',
        name='dataaggregation',
        namespace=namespace,
        image=image,
        arguments=["from pipeline_abstractions import *; DataAggregation(BUCKET_PATH_PUSH='{}', BUCKET_PATH_PULL_1='{}', BUCKET_PATH_PULL_2='{}')".format(dataaggregation_path, techfeatures_path, fred_path)],
        startup_timeout_seconds=900,
        is_delete_operator_pod=True
    )

    inference_path = dag_name + '/inference.json'
    inference = kubernetes_pod_operator.KubernetesPodOperator(
        task_id='inference',
        name='inference',
        namespace=namespace,
        image=image,
        arguments=["from pipeline_abstractions import *; Inference(BUCKET_PATH_PUSH='{}', BUCKET_PATH_PULL='{}')".format(inference_path, dataaggregation_path)],
        startup_timeout_seconds=900,
        is_delete_operator_pod=True
    )

    uncertainitybounds_path = dag_name + '/uncertainitybounds.json'
    uncertainitybounds = kubernetes_pod_operator.KubernetesPodOperator(
        task_id='uncertainitybounds',
        name='uncertainitybounds',
        namespace=namespace,
        image=image,
        arguments=["from pipeline_abstractions import *; UncertainityBounds(BUCKET_PATH_PUSH='{}', BUCKET_PATH_PULL='{}')".format(uncertainitybounds_path, inference_path)],
        startup_timeout_seconds=900,
        is_delete_operator_pod=True
    )

    resultsgen_path = dag_name + '/resultsgen.json'
    resultsgen = kubernetes_pod_operator.KubernetesPodOperator(
        task_id='resultsgen',
        name='resultsgen',
        namespace=namespace,
        image=image,
        arguments=["from pipeline_abstractions import *; ResultsGen(BUCKET_PATH_PUSH='{}', BUCKET_PATH_PULL='{}')".format(resultsgen_path, inference_path)],
        startup_timeout_seconds=900,
        is_delete_operator_pod=True
    )

    tradecards_path = dag_name + '/tradecards.json'
    tradecards = kubernetes_pod_operator.KubernetesPodOperator(
        task_id='tradecards',
        name='tradecards',
        namespace=namespace,
        image=image,
        arguments=["from pipeline_abstractions import *; TradeCards(BUCKET_PATH_PUSH='{}', BUCKET_PATH_PULL_1='{}', BUCKET_PATH_PULL_2='{}', RunTime='{}')".format(tradecards_path, uncertainitybounds_path, resultsgen_path, run_time)],
        startup_timeout_seconds=900,
        is_delete_operator_pod=True
    )


    mrm_path = dag_name + '/mrm.json'
    mrm = kubernetes_pod_operator.KubernetesPodOperator(
        task_id='mrm',
        name='mrm',
        namespace=namespace,
        image=image,
        arguments=["from pipeline_abstractions import *; MRM(BUCKET_PATH_PUSH='{}', BUCKET_PATH_PULL='{}', RunTime='{}')".format(mrm_path, resultsgen_path, run_time)],
        startup_timeout_seconds=900,
        is_delete_operator_pod=True
    )

    simtables_path = dag_name + '/simtables.json'
    dirtables_path = dag_name + '/dirtables.json'
    clsmatrix_path = dag_name + '/clsmatrix.json'
    simtables = kubernetes_pod_operator.KubernetesPodOperator(
        task_id='simtables',
        name='simtables',
        namespace=namespace,
        image=image,
        arguments=["from pipeline_abstractions import *; SimTables(BUCKET_PATH_PUSH_1='{}', BUCKET_PATH_PUSH_2='{}', BUCKET_PATH_PUSH_3='{}', BUCKET_PATH_PULL='{}', RunTime='{}')".format(simtables_path, dirtables_path, clsmatrix_path, resultsgen_path, run_time)],
        startup_timeout_seconds=900,
        is_delete_operator_pod=True
    )

    mts_path = dag_name + '/mts.json'
    mts = kubernetes_pod_operator.KubernetesPodOperator(
        task_id='mts',
        name='mts',
        namespace=namespace,
        image=image,
        arguments=["from pipeline_abstractions import *; MTS(BUCKET_PATH_PUSH='{}', BUCKET_PATH_PULL='{}', RunTime='{}')".format(mts_path, resultsgen_path, run_time)],
        startup_timeout_seconds=900,
        is_delete_operator_pod=True
    )

    publishpostgress = kubernetes_pod_operator.KubernetesPodOperator(
        task_id='publishpostgress',
        name='publishpostgress',
        namespace=namespace,
        image=image,
        arguments=["from pipeline_abstractions import *; PublishPostgress()"],
        startup_timeout_seconds=900,
        is_delete_operator_pod=True
    )

    end = BashOperator(
        task_id='end',
        bash_command='date',
    )

    start >> [fred, alphavantage]
    techfeatures << [fred, alphavantage]
    dataaggregation << techfeatures
    inference << dataaggregation
    [uncertainitybounds, resultsgen] << inference
    tradecards << [uncertainitybounds, resultsgen]
    mrm << resultsgen
    simtables << resultsgen
    mts << resultsgen
    publishpostgress << [tradecards, mrm, simtables, mts]
    end << publishpostgress