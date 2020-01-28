"""
Taiyo Pipelines
===============

All the necessary classes needed for the submission to postgress and grafana are present here.
There are three classes:

    - PostgresConnector
    - PublishPostgress
    - Logger Class
    - Pipeline Class
"""

import os
import requests
import json
from datetime import datetime

def border_msg(msg):
    """Function to display text in a box

    Args:
        msg (str): message to print the text

    Returns:
        str: String with a box wrapped around it

    """
    row = len(msg)
    h = ''.join(['+'] + ['-' *row] + ['+'])
    result= '\n'+ h + '\n'"|"+msg+"|"'\n' + h
    return result

# Helper Functions:
class PostgresConnector():
    """Basic Connector for postgress to perform data pushes

    Args:
        ip (str): IP Address of the Postgres Server
        port (str): port of the Postgres Server
    """
    def __init__(self, ip, port):
        self.ip = ip
        self.port = port
        self.endpoint = self.ip + ":" + self.port

    def create(self, table, data):
        """create endpoint is used to insert as single data point

        Args:
            table (str): table in postgress to insert the data
            data (json): data structured into a json file with the columns of the table to insert into

        Returns:
            str: Response from the server    
        """
        url = "http://" + self.endpoint + "/api/create/" + table + "/"
        response = requests.post(url, data=data)
        return response.text

    def bulkCreate(self, table, data):
        """bulk create endpoint is used to insert data points

        Args:
            table (str): table in postgress to insert the data
            data (json): data structured into a json file with the columns of the table to insert into

        Returns:
            str: Response from the server
        """
        url = "http://" + self.endpoint + "/api/bulkCreate/" + table + "/"
        headers = {'Content-type': 'application/json'}
        response = requests.post(url, data=json.dumps(data), headers=headers)
        return response.text

    def findAll(self, table):
        """findAll endpoint is used to view the data

        Args:
            table (str): table in postgress to view the data

        Returns:
            str: Response from the server
        """
        url = "http://" + self.endpoint + "/api/findAll/" + table + "/"
        response = requests.get(url)
        return response.text
    
    def search(self, table):
        """search endpoint is used to view a data point 

        Args:
            table (str): table in postgress to view the data

        Returns:
            str: Response from the server
        """
        url = "http://" + self.endpoint + "/api/search/" + table + "/"
        response = requests.get(url)
        return response.text

    def delete(self, table, rowId):
        """delete endpoint is used to delete the data based on the rowID passed

        Args:
            table (str): table in postgress to delete the data
            rowID (str): rowID of the data point to delete 

        Returns:
            str: Response from the server
        """
        url = "http://" + self.endpoint + "/api/delete/" + table + "/?rowId=" + rowId
        response = requests.get(url)
        return response.text

    def purge(self, table):
        """purge endpoint is used to delete the entire data from the table

        Args:
            table (str): table in postgress to purge the data

        Returns:
            str: Response from the server
        """
        url = "http://" + self.endpoint + "/api/purge/" + table + "/"
        response = requests.get(url)
        return response.text
        
    def bulkPush(self, table, data, target):
        """bulkPush endpoint is used to insert the data into a two tables at once. table_fk and target table

        Args:
            table (str): table in postgress to insert the data
            data (json): json data to be inserted
            target (str): which part of the json to push into the database 

        Returns:
            str: Response from the server
        """
        response = json.loads(self.bulkCreate("table_fk", data['table_fk']))
        for i in range(len(response)):
            data[target][i]['table_fk_id'] = response[i]['id']
        response = json.loads(self.bulkCreate(table, data[target]))
        return response

    def bulkPushSeries(self, table, data, target):
        """bulkPush endpoint is used to insert the data into a two tables at once. table_fk and target table

        Args:
            table (str): table in postgress to insert the data
            data (json): json data to be inserted
            target (str): which part of the json to push into the database 

        Returns:
            str: Response from the server
        """
        response = json.loads(self.bulkCreate("table_fk", data['table_fk']))
        for i in range(len(response)):
            for j in range(len(data[target])):
                data[target][j]['table_fk_id'] = response[i]['id']
        response = json.loads(self.bulkCreate(table, data[target]))
        return response

class PublishPostgress():
    """Publish Class for postgress to perform data pushes

    Args:
        config (yml dict): yml dictionary with Postgress details

    """
    def __init__(self, config):
        self.ip = config['Postgress']['IP']
        self.port = config['Postgress']['Port']
        self.PC = PostgresConnector(ip = self.ip, port = self.port)
    
    def publish_tradecards(self, data, table = "TradeCards"):
        """EndPoints to push the data
        """
        response = self.PC.bulkPush("trade_cards", data, table)
        return response

    def publish_MTS(self, data, table = "TimeSeries"):
        """EndPoints to push the data
        """
        response = self.PC.bulkPushSeries("time_series_prediction", data, table)
        return response

    def publish_MRM(self, data, table = "MRM"):
        """EndPoints to push the data
        """
        response = self.PC.bulkPush("model_reliability_matrix", data, table)
        return response

    def publish_DIRtable(self, data, table = "Dirtable"):
        """EndPoints to push the data
        """
        response = self.PC.bulkPush("direction_change", data, table)
        return response

    def publish_Simtable(self, data, table = "Simtable"):
        """EndPoints to push the data
        """
        response = self.PC.bulkPushSeries("simulation", data, table)
        return response
    
    def publish_DTW(self, data, table = "DTW"):
        """EndPoints to push the data
        """
        response = self.PC.bulkCreate("dtw", data[table])
        return response

    def publish_IS(self, data, table = "IS"):
        """EndPoints to push the data
        """
        response = self.PC.bulkPush("intraday_seasonality", data, table)
        return response

    def publish_sum_table(self, data):
        """EndPoints to push the data
        """
        response = self.PC.bulkCreate("sum_table", data)
        return response
    
    def publish_classification_matrices(self, data, table="Class_M"):
        """EndPoints to push the data
        """
        response = self.PC.bulkPush("classification_matrix", data, table)
        return response

    def publish_returns(self, data, table="Returns"):
        """EndPoints to push the data
        """
        response = self.PC.bulkPushSeries("asset_trades", data, table)
        return response

    def publish_portfolio_sum(self, data, table="portfolio_sum"):
        """EndPoints to push the data
        """
        response = self.PC.bulkPush("asset_trades", data, table)
        return response
    
    def publish_portfolio(self, data):
        """EndPoints to push the data
        """
        response = self.PC.bulkCreate("portfolio", data)
        return response

import logging
class Logger():
    """Logging Class for the Pipelines
    
    Args:
        log_level (str): DEBUG or INFO mode
        log_path (str): path to save log file 
    """
    def __init__(self, log_level, log_path):
        
        if(log_level == 'DEBUG'):
            logging.basicConfig(filename=log_path ,filemode='w', format='%(asctime)s - %(levelname)s - %(message)s', level = logging.DEBUG)
        elif(log_level == 'INFO'):
            logging.basicConfig(filename=log_path, filemode='w', format='%(asctime)s - %(levelname)s - %(message)s', level = logging.INFO)
    
    @staticmethod
    def logFunction(function):
        """Logs a Function
        """
        def wrapper(*args, **kargs):
            result = None
            try:
                logging.info("Function: {} Status: Running   ".format(function.__name__))
                result = function(*args, **kargs)
                logging.info("Function: {} Status: Done   ".format(function.__name__))
            except:
                    logging.error(" {} Status: Exception occurred".format(function.__name__), exc_info=True)
            return result
        return wrapper

    def logLayer(self, label):
        """Logs a Layer

        Args:
            label: name of the Layer
        """
        def decorator(function):
            def wrapper(*args, **kargs):
                result = None
                try:
                    logging.info(border_msg("   {} Layer Status: Running   ".format(label)))
                    result = function(*args, **kargs)
                    logging.info(border_msg("   {} Layer Status: Done   ".format(label)))
                except:
                        logging.error(" {} Status: Exception occurred".format(function.__name__), exc_info=True)
                return result
            return wrapper
        return decorator
    
    def info(self, message):
        logging.info(message)