import os
import sys
import yaml
import json
import logging
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from Taiyo_Models.toNN import IndexerMLP
from Taiyo_Models.toRNN import IndexerGRU
from Taiyo_Models.toRNN import IndexerLSTM
from Taiyo_Data.toMLPPreProcessor import MLP_PP
from Taiyo_Data.toLSTMPreProcessor import Lstm_PP
from Taiyo_Harvesting.toAlphaVantage import StockData
from Taiyo_Harvesting.toFredHarvester import FREDData
from Taiyo_TechFeatures.toTechFeatures import TechIndicators

sys.path.append(os.path.join(os.path.dirname(__file__)))

from toGrafanaViz import Cascade_Graph, ClassificationMatrix, ModelReliabilityV2, SimTable, bulkCardsUQ
from Taiyo_Pipeline import PublishPostgress

logger = logging.getLogger(__name__)

config = None

try:
    logger.info('Reading config file...')
    with open(os.path.join(os.path.dirname(__file__), 'Ansys.yml'), 'r') as f:
        config = yaml.load(f, Loader=yaml.SafeLoader)
        logger.info('Successful in reading config file.')
except Exception as e:
    logger.error('Error reading config file. Error: {}'.format(e))

def AlphaVantageData(**kwargs):
    global config
    FPconfig = config["Fetch Data Layer"]["Alpha Vantage"]
    SDHandle = StockData(ticker=FPconfig["ticker"], frequency=FPconfig["frequency"])
    SDdata = SDHandle.getStockPrices()
    SDdata.sort_values(by=['Date'], inplace=True)
    SDdata.set_index('Date', inplace=True)
    for c in SDdata.columns:
        SDdata[c] = pd.to_numeric(SDdata[c])
    with open("/airflow/xcom/return.json", "w") as file:
        xcom_return = {'AlphaVantage': SDdata}
        json.dump(xcom_return, file)
    kwargs['ti'].xcom_push(key='AlphaVantage', value=SDdata)

def FRED(**kwargs):
    global config
    FPconfig = config["Fetch Data Layer"]["Fred"]
    FREDHandle = FREDData(codes=FPconfig["codes"], start_date=FPconfig["start_date"])
    FREDHandle.getData()
    data = FREDHandle.data.fillna(method='ffill').fillna(method='bfill')
    kwargs['ti'].xcom_push(key='FRED', value=data)

def TechFeatures(**kwargs):
    global config
    KALconfig = config["Knowledge Abstraction Layer"]['Tech Features']
    response = kwargs['ti'].xcom_pull(key='AlphaVantage', task_ids='AlphaVantage')
    FXTI = TechIndicators(response)
    for func in KALconfig:
        getattr(FXTI, func["function"])(**func["params"])
    kwargs['ti'].xcom_push(key='TechFeatures', value=FXTI.data)

def DataAggregation(**kwargs):
    global config
    KALconfig = config["Knowledge Abstraction Layer"]['Data Aggregation']
    target = config["Meta-Data"]["Target"]
    data = kwargs['ti'].xcom_pull(key='TechFeatures', task_ids='TechFeatures')
    fred_data = kwargs['ti'].xcom_pull(key='FRED', task_ids='FRED')
    X = data.join(fred_data, how='left')
    X = X.rename(columns={target: target + "(t)"})
    X = X.dropna()
    for i in range(1, KALconfig['Lags']):
        X[target + "(t-" + str(i) + ")"] = X[target + "(t)"].shift(i)
    X.dropna(inplace=True)
    X[target + "(t+1)"] = X[target + "(t)"].shift(-1)
    kwargs['ti'].xcom_push(key='DataAggregation', value=X)

def TargetGenPreProcessor(**kwargs):
    global config
    response = kwargs['ti'].xcom_pull(key='DataAggregation', task_ids='DataAggregation')
    X = response[config["Meta-Data"]["Target"] + '(t)']
    kwargs['ti'].xcom_push(key='TargetGenPreProcessor', value=X)

def NaiveForecasterPreProcessor(**kwargs):
    global config
    response = kwargs['ti'].xcom_pull(key='DataAggregation', task_ids='DataAggregation')
    X = response[config["Meta-Data"]["Target"] + '(t)']
    X = X.rename(columns={config["Meta-Data"]["Target"] + '(t)': 'NF'})
    kwargs['ti'].xcom_push(key='NaiveForecasterPreProcessor', value=X)

def MovAvgPreProcessor(**kwargs):
    global config
    days = config['Pre-Processing Layer']['Mov_Avg']['days']
    response = kwargs['ti'].xcom_pull(key='DataAggregation', task_ids='DataAggregation')
    X = response[config["Meta-Data"]["Target"] + '(t)'].rolling(days).mean()
    X = X.rename(columns={config["Meta-Data"]["Target"] + '(t)': 'MA'})
    kwargs['ti'].xcom_push(key='MovAvgPreProcessor', value=X)

def LSTMPreProcessor(**kwargs):
    global config
    response = kwargs['ti'].xcom_pull(key='DataAggregation', task_ids='DataAggregation')
    LSTMHandle = Lstm_PP(response, input_features=response.columns, target=config["Meta-Data"]["Target"] + "(t+1)")
    LSTMHandle.compileData(**config['Pre-Processing Layer']['LSTM'])
    kwargs['ti'].xcom_push(key='LSTMPreProcessor', value=LSTMHandle)

def GRUPreProcessor(**kwargs):
    global config
    response = kwargs['ti'].xcom_pull(key='DataAggregation', task_ids='DataAggregation')
    GRUHandle = Lstm_PP(response, input_features=response.columns, target=config["Meta-Data"]["Target"] + "(t+1)")
    GRUHandle.compileData(**config['Pre-Processing Layer']['GRU'])
    kwargs['ti'].xcom_push(key='GRUPreProcessor', value=GRUHandle)

def MLPPreProcessor(**kwargs):
    global config
    response = kwargs['ti'].xcom_pull(key='DataAggregation', task_ids='DataAggregation')
    MLPHandle = MLP_PP(response, input_features=response.columns, target=config["Meta-Data"]["Target"] + "(t+1)")
    MLPHandle.compileData(**config['Pre-Processing Layer']['MLP'])
    kwargs['ti'].xcom_push(key='MLPPreProcessor', value=MLPHandle)

def NaiveForecasterModel(**kwargs):
    global config
    response = kwargs['ti'].xcom_pull(key='NaiveForecasterPreProcessor', task_ids='NaiveForecasterPreProcessor')
    kwargs['ti'].xcom_push(key='NaiveForecasterModel', value=response)

def MovAvgModel(**kwargs):
    global config
    response = kwargs['ti'].xcom_pull(key='MovAvgPreProcessor', task_ids='MovAvgPreProcessor')
    kwargs['ti'].xcom_push(key='MovAvgModel', value=response)

def MLPModel(**kwargs):
    global config
    try:
        MLP = IndexerMLP()
        MLP.loadModel(**config['Model Training Layer']['MLP']["saved_model"])
    except Exception:
        MLP = IndexerMLP(**config['Model Training Layer']['MLP']["model_params"])
        response = kwargs['ti'].xcom_pull(key='MLPPreProcessor', task_ids='MLPPreProcessor')
        MLP.fitModel(response.X_train, response.y_train, **config['Model Training Layer']['MLP']["fit_params"])
        MLP.saveModel(**config['Model Training Layer']['MLP']["saved_model"])
    kwargs['ti'].xcom_push(key='MLPModel', value=MLP)

def LSTMModel(**kwargs):
    global config
    try:
        LSTM = IndexerLSTM()
        LSTM.loadModel(**config['Model Training Layer']['LSTM']["saved_model"])
    except Exception:
        response = kwargs['ti'].xcom_pull(key='LSTMPreProcessor', task_ids='LSTMPreProcessor')
        LSTM = IndexerLSTM(response.X_train.shape[1], response.X_train.shape[2], **config['Model Training Layer']['LSTM']["model_params"])
        LSTM.buildModel()
        LSTM.compileModel()
        LSTM.fitModel(response.X_train,response.y_train,validation_data=(response.X_val,response.y_val,),**config['Model Training Layer']['LSTM']["fit_params"])
        LSTM.saveModel(**config['Model Training Layer']['LSTM']["saved_model"])
    kwargs['ti'].xcom_push(key='LSTMModel', value=LSTM)

def GRUModel(**kwargs):
    global config
    try:
        GRU = IndexerGRU()
        GRU.loadModel(**config['Model Training Layer']['GRU']["saved_model"])
    except Exception:
        response = kwargs['ti'].xcom_pull(key='GRUPreProcessor', task_ids='GRUPreProcessor')
        GRU = IndexerGRU(response.X_train.shape[1], response.X_train.shape[2],**config['Model Training Layer']['GRU']["model_params"])
        GRU.buildModel()
        GRU.compileModel()
        GRU.fitModel(response.X_train, response.y_train,validation_data=(response.X_val, response.y_val,),**config['Model Training Layer']['GRU']["fit_params"])
        GRU.saveModel(**config['Model Training Layer']['GRU']["saved_model"])
    kwargs['ti'].xcom_push(key='GRUModel', value=GRU)

def Inference(**kwargs):
    NF = kwargs['ti'].xcom_pull(key='NaiveForecasterModel', task_ids='NaiveForecasterModel')
    MA = kwargs['ti'].xcom_pull(key='MovAvgModel', task_ids='MovAvgModel')
    MLP = kwargs['ti'].xcom_pull(key='MLPModel', task_ids='MLPModel')
    LSTM = kwargs['ti'].xcom_pull(key='LSTMModel', task_ids='LSTMModel')
    GRU = kwargs['ti'].xcom_pull(key='GRUModel', task_ids='GRUModel')

    response = kwargs['ti'].xcom_pull(key='MLPPreProcessor', task_ids='MLPPreProcessor')
    MLP = response.transform_output(MLP.predict(response.X))
    response = kwargs['ti'].xcom_pull(key='LSTMPreProcessor', task_ids='LSTMPreProcessor')
    LSTM = response.transform_output(LSTM.predict(response.X))
    response = kwargs['ti'].xcom_pull(key='GRUPreProcessor', task_ids='GRUPreProcessor')
    GRU = response.transform_output(GRU.predict(response.X))

    def adjuster(values):
        ept = [np.nan for _ in range(len(NF.index))]
        balance = len(NF.index) - len(values)
        ept[balance:] = values.ravel()
        return ept

    Target = kwargs['ti'].xcom_pull(key='TargetGenPreProcessor', task_ids='TargetGenPreProcessor')
    inference = pd.DataFrame(index=NF.index)
    inference = inference.join(Target, how='left')

    inference["NF"] = NF.values
    inference["MA"] = MA.values
    inference["MLP"] = adjuster(MLP)
    inference["LSTM"] = adjuster(LSTM)
    inference["GRU"] = adjuster(GRU)

    kwargs['ti'].xcom_push(key='Inference', value=inference)

def timestepfunction(**kwargs):
    hour, minute = (kwargs[kwargs["Target"] + " Time"]).split(":")
    hour, minute = int(hour), int(minute)
    response = kwargs['ti'].xcom_pull(key='Inference', task_ids='Inference')
    datetimeIndex=response.index
    if(kwargs["Frequency"] == "Daily"):
        next_date = datetimeIndex[-1]
        if(next_date.weekday() == 4):
            next_date += timedelta(days=3)
        else:
            next_date += timedelta(days=1)
        next_date = next_date.replace(hour=hour, minute=minute)
        datetimeIndex = datetimeIndex.append(pd.to_datetime([next_date]))
    elif(kwargs["Frequency"] == "Weekly"):
        next_week = datetimeIndex[-1]
        start = next_week - timedelta(days=next_week.weekday())
        end = start + timedelta(days=4)
        if(datetimeIndex[-1] == next_week):
            next_week = end + timedelta(days=7)
        else:
            next_week = end
        next_week = next_week.replace(hour=hour, minute=minute)
        datetimeIndex = datetimeIndex.append(pd.to_datetime([next_week]))
    elif(kwargs["Frequency"] == "Monthly"):
        def get_first_day(dt, d_years=0, d_months=0):
            # d_years, d_months are "deltas" to apply to dt
            y, m = dt.year + d_years, dt.month + d_months
            a, m = divmod(m-1, 12)
            return datetime(y+a, m+1, 1)
        def get_last_day(dt):
            return get_first_day(dt, 0, 1) + timedelta(-1)
        next_month = datetimeIndex[-1] + timedelta(days=1)
        next_month = get_last_day(next_month)
        next_month = next_month.replace(hour=hour, minute=minute)
        datetimeIndex = datetimeIndex.append(pd.to_datetime([next_month]))
    return datetimeIndex

def ResultsGen(**kwargs):
    global config
    results = pd.DataFrame(index=timestepfunction(**config["Meta-Data"]))
    response = kwargs['ti'].xcom_pull(key='Inference', task_ids='Inference')
    results = results.join(response, how="left")
    results["NF"] = results["NF"].shift(1)
    results["MA"] = results["MA"].shift(1)
    results["MLP"] = results["MLP"].shift(1)
    results["LSTM"] = results["LSTM"].shift(1)
    results["GRU"] = results["GRU"].shift(1)
    kwargs['ti'].xcom_push(key='ResultsGen', value=results)

def UncertainityBounds(**kwargs):
    global config
    UQBounds_df = pd.DataFrame(index=timestepfunction(**config["Meta-Data"]))
    UQBounds_df.index.name = "Timestamp"

    UQBounds_df['NF_H'] = [0 for i in range(len(UQBounds_df))]
    UQBounds_df['NF_L'] = [0 for i in range(len(UQBounds_df))]

    UQBounds_df['MA_H'] = [0 for i in range(len(UQBounds_df))]
    UQBounds_df['MA_L'] = [0 for i in range(len(UQBounds_df))]

    UQBounds_df['MLP_H'] = [0 for i in range(len(UQBounds_df))]
    UQBounds_df['MLP_L'] = [0 for i in range(len(UQBounds_df))]

    UQBounds_df['LSTM_H'] = [0 for i in range(len(UQBounds_df))]
    UQBounds_df['LSTM_L'] = [0 for i in range(len(UQBounds_df))]

    UQBounds_df['GRU_H'] = [0 for i in range(len(UQBounds_df))]
    UQBounds_df['GRU_L'] = [0 for i in range(len(UQBounds_df))]

    kwargs['ti'].xcom_push(key='UncertainityBounds', value=UQBounds_df)

def TradeCards(**kwargs):
    RunTime = kwargs.get('templates_dict').get('RunTime', None)
    TCs = bulkCardsUQ(notebook_name=kwargs["Name"], frequency=kwargs["Frequency"], target=kwargs["Target"] + "(t)", run_time=RunTime)
    results = kwargs['ti'].xcom_pull(key='ResultsGen', task_ids='ResultsGen')
    UQ = kwargs['ti'].xcom_pull(key='UncertainityBounds', task_ids='UncertainityBounds')
    TC = TCs.bulkBuild(results, UQ, 95)
    kwargs['ti'].xcom_push(key='TradeCards', value=TC)

def MTS(**kwargs):
    RunTime = kwargs.get('templates_dict').get('RunTime', None)
    results = kwargs['ti'].xcom_pull(key='ResultsGen', task_ids='ResultsGen')
    plot_TS = Cascade_Graph(results, target=kwargs["Target"] + "(t)")
    MTS = plot_TS.bulkBuild(notebook_name=kwargs["Name"],frequency=kwargs["Frequency"],run_time=RunTime)
    kwargs['ti'].xcom_push(key='MTS', value=MTS)

def MRM(**kwargs):
    RunTime = kwargs.get('templates_dict').get('RunTime', None)
    results = kwargs['ti'].xcom_pull(key='ResultsGen', task_ids='ResultsGen')
    mrm = ModelReliabilityV2(results.dropna(), target=kwargs["Target"] + "(t)")
    MRM = mrm.bulkBuild(notebook_name=kwargs["Name"], frequency=kwargs["Frequency"], run_time=RunTime)
    kwargs['ti'].xcom_push(key='MRM', value=MRM)

def SimTables(**kwargs):
    RunTime = kwargs.get('templates_dict').get('RunTime', None)
    results = kwargs['ti'].xcom_pull(key='ResultsGen', task_ids='ResultsGen')
    ST1 = SimTable(results.dropna(), target=kwargs["Target"] + "(t)")
    change_table, color_list = ST1.changeTable(4, 500)
    ST1.directionTable()

    ST, DT = ST1.bulkBuild(notebook_name=kwargs["Name"], frequency=kwargs["Frequency"], run_time=RunTime)
    
    clf_rep = pd.DataFrame(color_list)
    clf_rep = clf_rep.applymap(
        lambda x: "Up" if x == "#00FF00" else "Down")

    CM = ClassificationMatrix(clf_rep, target=kwargs["Target"] + "(t) Change", dump_target=kwargs["Target"] + "(t)")
    CM.report(drop_cols=[kwargs["Target"] + "(t) Change"])
    CM = CM.bulkBuild(notebook_name=kwargs["Name"],frequency=kwargs["Frequency"],run_time=RunTime)

    kwargs['ti'].xcom_push(key='ST', value=ST)
    kwargs['ti'].xcom_push(key='DT', value=DT)
    kwargs['ti'].xcom_push(key='CM', value=CM)

def PublishPostgress(**kwargs):
    publisher = PublishPostgress(**kwargs)
    response = {}
    data = {}
    data["TradeCards"] = kwargs['ti'].xcom_pull(key='TradeCards', task_ids='TradeCards')
    response["TradeCards"] = publisher.publish_tradecards(data["TradeCards"], table="TradeCards")
    data["MTS"] = kwargs['ti'].xcom_pull(key='MTS', task_ids='MTS')
    response["MTS"] = publisher.publish_MTS(data["MTS"], table="TimeSeries")
    data["MRM"] = kwargs['ti'].xcom_pull(key='MRM', task_ids='MRM')
    response["MRM"] = publisher.publish_MRM(data["MRM"], table="MRM")
    data["Simtable"] = kwargs['ti'].xcom_pull(key='ST', task_ids='Simtables')
    response["Simtable"] = publisher.publish_Simtable(data["ST"], table="Simtable")
    data["Dirtable"] = kwargs['ti'].xcom_pull(key='DT', task_ids='Simtables')
    response["Dirtable"] = publisher.publish_DIRtable(data["Dirtable"], table="Dirtable")
    data["Class_M"] = kwargs['ti'].xcom_pull(key='CM', task_ids='Simtables')
    response["Class_M"] = publisher.publish_classification_matrices(data["Class_M"], table="Class_M")
    return response
