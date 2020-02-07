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
dag_name = 'ansys_pipeline'
namespace = 'airflow-tasks'
image = 'eu.gcr.io/taiyo-239217/dag:fae5002'

default_args = {
    'owner': 'Taiyo',
    'depends_on_past': False,
    'start_date': datetime.datetime.today() - datetime.timedelta(days=1),
    'email': ['avishek.akd@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True
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

    targetgenpreprocessor_path = dag_name + '/targetgenpreprocessor.json'
    targetgenpreprocessor = kubernetes_pod_operator.KubernetesPodOperator(
        task_id='targetgenpreprocessor',
        name='targetgenpreprocessor',
        namespace=namespace,
        image=image,
        arguments=["from pipeline_abstractions import *; TargetGenPreProcessor(BUCKET_PATH_PUSH='{}', BUCKET_PATH_PULL='{}')".format(targetgenpreprocessor_path, dataaggregation_path)],
        startup_timeout_seconds=900,
        is_delete_operator_pod=True
    )

    grupreprocessor_path = dag_name + '/grupreprocessor.json'
    grupreprocessor = kubernetes_pod_operator.KubernetesPodOperator(
        task_id='grupreprocessor',
        name='grupreprocessor',
        namespace=namespace,
        image=image,
        arguments=["from pipeline_abstractions import *; GRUPreProcessor(BUCKET_PATH_PUSH='{}', BUCKET_PATH_PULL='{}')".format(grupreprocessor_path, dataaggregation_path)],
        startup_timeout_seconds=900,
        is_delete_operator_pod=True
    )

    naiveforecasterpreprocessor_path = dag_name + '/naiveforecasterpreprocessor.json'
    naiveforecasterpreprocessor = kubernetes_pod_operator.KubernetesPodOperator(
        task_id='naiveforecasterpreprocessor',
        name='naiveforecasterpreprocessor',
        namespace=namespace,
        image=image,
        arguments=["from pipeline_abstractions import *; NaiveForecasterPreProcessor(BUCKET_PATH_PUSH='{}', BUCKET_PATH_PULL='{}')".format(naiveforecasterpreprocessor_path, dataaggregation_path)],
        startup_timeout_seconds=900,
        is_delete_operator_pod=True
    )
	
    lstmpreprocessor_path = dag_name + '/lstmpreprocessor.json'
    lstmpreprocessor = kubernetes_pod_operator.KubernetesPodOperator(
        task_id='lstmpreprocessor',
        name='lstmpreprocessor',
        namespace=namespace,
        image=image,
        arguments=["from pipeline_abstractions import *; LSTMPreProcessor(BUCKET_PATH_PUSH='{}', BUCKET_PATH_PULL='{}')".format(lstmpreprocessor_path, dataaggregation_path)],
        startup_timeout_seconds=900,
        is_delete_operator_pod=True
    )

    movavgpreprocessor_path = dag_name + '/movavgpreprocessor.json'
    movavgpreprocessor = kubernetes_pod_operator.KubernetesPodOperator(
        task_id='movavgpreprocessor',
        name='movavgpreprocessor',
        namespace=namespace,
        image=image,
        arguments=["from pipeline_abstractions import *; MovAvgPreProcessor(BUCKET_PATH_PUSH='{}', BUCKET_PATH_PULL='{}')".format(movavgpreprocessor_path, dataaggregation_path)],
        startup_timeout_seconds=900,
        is_delete_operator_pod=True
    )

    mlppreprocessor_path = dag_name + '/mlppreprocessor.json'
    mlppreprocessor = kubernetes_pod_operator.KubernetesPodOperator(
        task_id='mlppreprocessor',
        name='mlppreprocessor',
        namespace=namespace,
        image=image,
        arguments=["from pipeline_abstractions import *; MLPPreProcessor(BUCKET_PATH_PUSH='{}', BUCKET_PATH_PULL='{}')".format(mlppreprocessor_path, dataaggregation_path)],
        startup_timeout_seconds=900,
        is_delete_operator_pod=True
    )

    grumodel_path = dag_name + '/grumodel.json'
    grumodel = kubernetes_pod_operator.KubernetesPodOperator(
        task_id='grumodel',
        name='grumodel',
        namespace=namespace,
        image=image,
        arguments=["from pipeline_abstractions import *; GRUModel(BUCKET_PATH_PUSH='{}', BUCKET_PATH_PULL='{}')".format(grumodel_path, grupreprocessor_path)],
        startup_timeout_seconds=900,
        is_delete_operator_pod=True
    )

    naiveforecastermodel_path = dag_name + '/naiveforecastermodel.json'
    naiveforecastermodel = kubernetes_pod_operator.KubernetesPodOperator(
        task_id='naiveforecastermodel',
        name='naiveforecastermodel',
        namespace=namespace,
        image=image,
        arguments=["from pipeline_abstractions import *; NaiveForecasterModel(BUCKET_PATH_PUSH='{}', BUCKET_PATH_PULL='{}')".format(naiveforecastermodel_path, naiveforecasterpreprocessor_path)],
        startup_timeout_seconds=900,
        is_delete_operator_pod=True
    )

    lstmmodel_path = dag_name + '/lstmmodel.json'
    lstmmodel = kubernetes_pod_operator.KubernetesPodOperator(
        task_id='lstmmodel',
        name='lstmmodel',
        namespace=namespace,
        image=image,
        arguments=["from pipeline_abstractions import *; LSTMModel(BUCKET_PATH_PUSH='{}', BUCKET_PATH_PULL='{}')".format(lstmmodel_path, lstmpreprocessor_path)],
        startup_timeout_seconds=900,
        is_delete_operator_pod=True
    )

    movavgmodel_path = dag_name + '/movavgmodel.json'
    movavgmodel = kubernetes_pod_operator.KubernetesPodOperator(
        task_id='movavgmodel',
        name='movavgmodel',
        namespace=namespace,
        image=image,
        arguments=["from pipeline_abstractions import *; MovAvgModel(BUCKET_PATH_PUSH='{}', BUCKET_PATH_PULL='{}')".format(movavgmodel_path, movavgpreprocessor_path)],
        startup_timeout_seconds=900,
        is_delete_operator_pod=True
    )

    mlpmodel_path = dag_name + '/mlpmodel.json'
    mlpmodel = kubernetes_pod_operator.KubernetesPodOperator(
        task_id='mlpmodel',
        name='mlpmodel',
        namespace=namespace,
        image=image,
        arguments=["from pipeline_abstractions import *; MLPModel(BUCKET_PATH_PUSH='{}', BUCKET_PATH_PULL='{}')".format(mlpmodel_path, mlppreprocessor_path)],
        startup_timeout_seconds=900,
        is_delete_operator_pod=True
    )

    inference_path = dag_name + '/inference.json'
    inference = kubernetes_pod_operator.KubernetesPodOperator(
        task_id='inference',
        name='inference',
        namespace=namespace,
        image=image,
        arguments=["from pipeline_abstractions import *; Inference(BUCKET_PATH_PUSH='{}', BUCKET_PATH_PULL='{}')".format(inference_path, mlppreprocessor_path)],
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
        arguments=["from pipeline_abstractions import *; TradeCards(BUCKET_PATH_PUSH='{}', BUCKET_PATH_PULL='{}')".format(tradecards_path, inference_path)],
        startup_timeout_seconds=900,
        is_delete_operator_pod=True
    )

    mrm_path = dag_name + '/mrm.json'
    mrm = kubernetes_pod_operator.KubernetesPodOperator(
        task_id='mrm',
        name='mrm',
        namespace=namespace,
        image=image,
        arguments=["from pipeline_abstractions import *; MRM(BUCKET_PATH_PUSH='{}', BUCKET_PATH_PULL='{}')".format(mrm_path, inference_path)],
        startup_timeout_seconds=900,
        is_delete_operator_pod=True
    )

    simtables_path = dag_name + '/simtables.json'
    simtables = kubernetes_pod_operator.KubernetesPodOperator(
        task_id='simtables',
        name='simtables',
        namespace=namespace,
        image=image,
        arguments=["from pipeline_abstractions import *; SimTables(BUCKET_PATH_PUSH='{}', BUCKET_PATH_PULL='{}')".format(simtables_path, inference_path)],
        startup_timeout_seconds=900,
        is_delete_operator_pod=True
    )

    mts_path = dag_name + '/mts.json'
    mts = kubernetes_pod_operator.KubernetesPodOperator(
        task_id='mts',
        name='mts',
        namespace=namespace,
        image=image,
        arguments=["from pipeline_abstractions import *; MTS(BUCKET_PATH_PUSH='{}', BUCKET_PATH_PULL='{}')".format(mts_path, inference_path)],
        startup_timeout_seconds=900,
        is_delete_operator_pod=True
    )

    publishpostgress_path = dag_name + '/publishpostgress.json'
    publishpostgress = kubernetes_pod_operator.KubernetesPodOperator(
        task_id='publishpostgress',
        name='publishpostgress',
        namespace=namespace,
        image=image,
        arguments=["from pipeline_abstractions import *; PublishPostgress(BUCKET_PATH_PUSH='{}', BUCKET_PATH_PULL='{}')".format(publishpostgress_path, inference_path)],
        startup_timeout_seconds=900,
        is_delete_operator_pod=True
    )

    end = BashOperator(
        task_id='end',
        bash_command='date',
    )

    [fred, alphavantage] << start
    techfeatures << [fred, alphavantage]
    dataaggregation << techfeatures
    [targetgenpreprocessor, grupreprocessor, naiveforecasterpreprocessor, lstmpreprocessor, movavgpreprocessor, mlppreprocessor] << dataaggregation
    grumodel << grupreprocessor
    naiveforecastermodel << naiveforecasterpreprocessor
    lstmmodel << lstmpreprocessor
    movavgmodel << movavgpreprocessor
    mlpmodel << mlppreprocessor
    inference << [targetgenpreprocessor, grumodel, naiveforecastermodel, lstmmodel, movavgmodel, mlpmodel]
    [uncertainitybounds, resultsgen] << inference
    tradecards << [uncertainitybounds, resultsgen]
    mrm << [uncertainitybounds, resultsgen]
    simtables << [uncertainitybounds, resultsgen]
    mts << [uncertainitybounds, resultsgen]
    publishpostgress << [tradecards, mrm, simtables, mts]
    end << publishpostgress