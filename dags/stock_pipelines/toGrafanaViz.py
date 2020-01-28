"""
Finance Cards
=============

This is a HTML, CSS template for the Forecast card for displaying the forecast statement. This is a template based of Google Finance and we don't own this template. 
This basically renders a HTML string on the jupyter notebook display

The usage of this class is very simple:
e.g. a = FinanceCard("EUR/USD", value, change, per_change , timestamp)

and the finance card gets rendered. This is not using any image resources and arrows are drawn using svg packages from html. so they will render on any browser without issues.


There are three classes for finance cards:
    - Finance Cards
    - Finance Cards with Confidence Bounds
    - Finance Cards for Classifiers

"""
#%%
from IPython.display import HTML, display        
import random
import string

class FinanceCard():
    """This function basically triggers the coresponding card to be displayed
    
    Args:
        name (str): Name of the Asset
        value (float): price of the Asset 
        Timestamp (str): Timestamp of the Card 
        change (float): change in price from previous point
        per_change (float): percentage change in price from previous point
    """
    def __init__(self, name, value, Timestamp, change, per_change):
        seed = ''.join([random.choice(string.ascii_letters
            + string.digits) for n in range(32)])+"-"
        if(change < 0):
            self.__display_card(name, value, Timestamp,  change, per_change,  "#d23f31", "180", seed)
        elif(change > 0):
            self.__display_card(name, value, Timestamp, change, per_change,  "#0f9d58", "0", seed)
        else:
            self.__display_card(name, value, Timestamp, change, per_change, "#FFFFFF", "0", seed)
    
    def __display_card(self, name, value, Timestamp, change, per_change, arrow_col, arrow_rot, seed):
        """
        This function renders the card based on the arguments passed to the class
        Contains the HTML5/CSS3 code for the card.
        """
        display(HTML("""
        <style>
            
            ."""+seed+"""cardbox {
            font: 400 small Roboto-Regular,HelveticaNeue,Arial,sans-serif;
            overflow: hidden;
            box-shadow: 0 1px 2px 0 rgba(0,0,0,0.16), 0 0 0 1px rgba(0,0,0,0.08);
            border-radius: 2px;
            border-bottom: 1px hidden #fff;
            margin: 20px 275px 0px;
            }
            
            ."""+seed+"""cardblock {
            display: inline-block;
            box-sizing: border-box;
            width: 100%;
            color: rgba(0,0,0,10.95);
            padding: 16px;
            font-size: 14px;
            }
            
            ."""+seed+"""cardsent{
                display: flex;
                align-items: center;
            }
            
            ."""+seed+"""cardtitle{
            color: #1a0dab;
            text-decoration: inherit;
            font-size: 18px;
            line-height: 18px;
            padding-bottom: 1px;
            max-width: 95%;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
            display: inline-block;
            flex: 1;
            min-width: 10%;
            }
            ."""+seed+"""cardtime{
            flex: 1;
            padding-top: 10px;
            line-height: 16px;
            padding-right: 5px;
            color: rgb(0, 0, 0, 0.60);
            font-weight: normal;
            letter-spacing: 0px;
            white-space: nowrap
            }
            
            ."""+seed+"""cardvalue{
            color: #3C4043;
            text-align: right;
            }
            
            ."""+seed+"""cardbasispoints{
            color: """ + arrow_col + """;
            font-size: 14px;
            min-width: 40%;
            text-align: right;
            white-space: nowrap;
            padding-top: 10px;
            }
            ."""+seed+"""cardarrow{
             height:12px;
             width:12px;
             padding-left:4px
            }
        </style>
          <div class="""+seed+"panelcard"+""">      
            <div class = """+seed+"cardbox"+""" >
                <div class = """+seed+"cardblock"+""">
                    <div class=""" +seed+"cardsent"+""">
                        <span class="""+seed+"cardtitle"+""">"""+ name +"""</span>
                        <span class="""+seed+"cardvalue"+""">"""+ str(value) + """</span>
                    </div>
                    <div class="""+seed+"cardsent"+""">
                        <span class="""+seed+"cardtime"+""">"""+ Timestamp + """</span>
                        <span class="""+seed+"cardbasispoints"+""">
                            <span class="""+seed+"cardbasispoints"+""">"""+str(change)+ """</span> 
                            <span class="""+seed+"cardbasispoints"+""">(""" + str(per_change) + """%)
                            <span style="
                                display: inline-block;
                                fill: currentColor;
                                color:"""+ arrow_col + """;
                                height:12px;
                                width:12px;
                                padding-left:4px;
                                transform: rotate("""+ arrow_rot + """deg);
                                "><svg version="1.1" viewBox="0 0 12 12" xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink">
                                    <path d="M6,0.002L0 6.002 4.8 6.002 4.8 11.9996 7.2 11.9996 7.2 6.002 12 6.002z"></path>
                                </svg>
                            </span>
                        </div>
                    </div>
                </div>
           </div>
    """), metadata=dict(isolated=True))

class FinanceCardUQ():
    """This function basically triggers the coresponding card to be displayed

    Args:
        name (str): Name of the Asset
        value (str): Price of the Asset
        Timestamp (str): Timestamp of the Card 
        change (float): change in price from previous point
        per_change (float): percentage change in price from previous point
        High_bound (float): Higher bound for the price
        Low_bound (float): Lower bound for the price 
        con_per (integer): Confidence interval of the bounds
    """
    def __init__(self, name, value, Timestamp,  change, per_change, High_bound, Low_bound, con_per):
        self.name = name
        self.value = value
        self.Timestamp = Timestamp
        self.change = change
        self.per_change = per_change
        self.High_bound = High_bound
        self.Low_bound = Low_bound
        self.con_per = con_per
        self.seed = ''.join([random.choice(string.ascii_uppercase + string.ascii_lowercase
            + string.digits ) for n in range(8)])+"-"
        
    def __display_card(self, name, value, Timestamp, change, per_change,  High_bound, Low_bound, arrow_col, arrow_rot, con_per, seed):
        """
        This function renders the card based on the arguments passed to the class
        Contains the HTML5/CSS3 code for the card.
        """
        display(HTML("""
        <style>
            
            ."""+seed+"""cardbox {
            font: 400 small Roboto-Regular,HelveticaNeue,Arial,sans-serif;
            overflow: hidden;
            box-shadow: 0 1px 2px 0 rgba(0,0,0,0.16), 0 0 0 1px rgba(0,0,0,0.08);
            border-radius: 2px;
            border-bottom: 1px hidden #fff;
            margin: 0% 30% 0% 30%;
            }
            
            ."""+seed+"""cardblock {
            display: inline-block;
            box-sizing: border-box;
            width: 100%;
            color: rgba(0,0,0,10.95);
            padding: 16px;
            font-size: 14px;
            }
            
            ."""+seed+"""cardsent{
                display: flex;
                align-items: center;
            }
            
            ."""+seed+"""cardtitle{
            color: #1a0dab;
            text-decoration: inherit;
            font-size: 18px;
            line-height: 18px;
            padding-bottom: 1px;
            max-width: 95%;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
            display: inline-block;
            flex: 1;
            min-width: 10%;
            }
            ."""+seed+"""cardtime{
            flex: 1;
            padding-top: 10px;
            line-height: 16px;
            padding-right: 5px;
            color: rgb(0, 0, 0, 0.60);
            font-weight: normal;
            letter-spacing: 0px;
            white-space: nowrap
            }
            
            ."""+seed+"""cardvalue{
            color: #3C4043;
            text-align: right;
            padding-top: 10px;
            }
            
            ."""+seed+"""cardbasispoints{
            color: """ + arrow_col + """;
            font-size: 14px;
            min-width: 40%;
            text-align: right;
            white-space: nowrap;
            padding-top: 10px;
            }
            ."""+seed+"""cardarrow{
             height:12px;
             width:12px;
             padding-left:4px
            }
        </style>
          <div class="""+seed+"panelcard"+""">      
            <div class = """+seed+"cardbox"+""" >
                <div class = """+seed+"cardblock"+""">
                    <div class=""" +seed+"cardsent"+""">
                        <span class="""+seed+"cardtitle"+""">"""+ name +"""</span>
                        <span class="""+seed+"cardvalue"+""">"""+ str(value) + """</span>
                    </div>
                    <div class="""+seed+"cardsent"+""">
                        <span class="""+seed+"cardtime"+""">"""+ Timestamp + """</span>
                        <span class="""+seed+"cardbasispoints"+""">
                            <span class="""+seed+"cardbasispoints"+""">"""+str(change)+ """</span> 
                            <span class="""+seed+"cardbasispoints"+""">(""" + str(per_change) + """%)
                            <span style="
                                display: inline-block;
                                fill: currentColor;
                                color:"""+ arrow_col + """;
                                height:12px;
                                width:12px;
                                padding-left:4px;
                                transform: rotate("""+ arrow_rot + """deg);
                                "><svg version="1.1" viewBox="0 0 12 12" xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink">
                                    <path d="M6,0.002L0 6.002 4.8 6.002 4.8 11.9996 7.2 11.9996 7.2 6.002 12 6.002z"></path>
                                </svg>
                            </span>
                    </div>
                    <div class="""+seed+"cardsent"+""">
                        <span class="""+seed+"cardtime"+""">""" + str(con_per)+ """% Confidence Higher Bound</span> 
                        <span class="""+seed+"cardvalue"+""">"""+str(High_bound) + """
                    </div>
                    <div class="""+seed+"cardsent"+""">
                        <span class="""+seed+"cardtime"+""">""" + str(con_per)+ """% Confidence Lower Bound</span> 
                        <span class="""+seed+"cardvalue" + """>""" + str(Low_bound) + """
                    </div>
                </div>
            </div>
        </div>
    """), metadata=dict(isolated=True))
        
    def show(self):
        """function to display the finance card
        """
        if(self.change < 0):
            self.__display_card(self.name, self.value, self.Timestamp, self.change, self.per_change, self.High_bound, self.Low_bound,"#d23f31", "180", self.con_per, self.seed)
        elif(self.change > 0):
            self.__display_card(self.name, self.value, self.Timestamp, self.change, self.per_change, self.High_bound, self.Low_bound,"#0f9d58", "0", self.con_per, self.seed)
        else:
            self.__display_card(self.name, self.value, self.Timestamp, self.change, self.per_change,  self.High_bound, self.Low_bound,"#FFFFFF", "0", self.con_per, self.seed)
    
    def buildJson(self, notebook_name, frequency, target, run_time, viz_type="TradeCard"):
        """Generates dumps for the grafana dashboards.

        Args:
            notebook_name (str): Name of the Asset
            frequency (str): Frequency of the data
            target (str): target of the data e.g Open, Close etc
            run_time (str): When the dump was built
            viz_type (str): {default: TradeCard} Type of visualisation
        
        Returns:
            dict, dict: ids for the data and actual data of the finance card 

        """
        id = {
            'notebook_name':notebook_name, 
            'frequency':frequency, 
            'target': target, 
            'bot_type':self.name,
            'run_time':run_time,
            'time': self.Timestamp,
            'viz_type':viz_type
        }

        data = {
            'bot_name': self.name, 
            'time': self.Timestamp,
            'expected_value': str(self.value),
            'higher_bound': str(self.High_bound),
            'lower_bound': str(self.Low_bound),
            'change': str(self.change),
            'percentage_change': str(self.per_change)
        }
        return id, data
            
class FinanceCard_CLF():
    """Class used for the classifiers to forecast the results

    Args:
        name (str): Name of the Asset
        Timestamp (str): Timestamp of the Card 
        newDirection (int): {0,1} direction of the model

    """
    def __init__(self, name, Timestamp, newDirection):
        seed = ''.join([random.choice(string.ascii_letters
            + string.digits) for n in range(32)])+"-"
        self.seed = seed
      
        if(newDirection == 0):
            self.arrow_col2 = "#d23f31"
            self.arrow_rot2 = "180"
        elif(newDirection == 1):
            self.arrow_col2 = "#0f9d58"
            self.arrow_rot2 = "0"
            
        self.Timestamp = Timestamp
        self.name=name

    def __display_card(self, name, Timestamp, arrow_col2, arrow_rot2, seed):
        """
        This function renders the card based on the arguments passed to the class
        Contains the HTML5/CSS3 code for the card.
        """
        display(HTML("""
        <style>
            
            ."""+seed+"""cardbox {
            font: 400 small Roboto-Regular,HelveticaNeue,Arial,sans-serif;
            overflow: hidden;
            box-shadow: 0 1px 2px 0 rgba(0,0,0,0.16), 0 0 0 1px rgba(0,0,0,0.08);
            border-radius: 2px;
            border-bottom: 1px hidden #fff;
            margin: 0% 30% 0% 30%;
            }
            
            ."""+seed+"""cardblock {
            display: inline-block;
            box-sizing: border-box;
            width: 100%;
            color: rgba(0,0,0,10.95);
            padding: 16px;
            font-size: 14px;
            }
            
            ."""+seed+"""cardsent{
                display: flex;
                align-items: center;
            }
            
            ."""+seed+"""cardtitle{
            color: #1a0dab;
            text-decoration: inherit;
            font-size: 18px;
            line-height: 18px;
            padding-bottom: 1px;
            max-width: 95%;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
            display: inline-block;
            flex: 1;
            min-width: 10%;
            }
            ."""+seed+"""cardtime{
            flex: 1;
            padding-top: 10px;
            line-height: 16px;
            padding-right: 5px;
            color: rgb(0, 0, 0, 0.60);
            font-weight: normal;
            letter-spacing: 0px;
            white-space: nowrap
            }
            
            ."""+seed+"""cardvalue{
            color: #3C4043;
            text-align: right;
            padding-bottom: 10px;
            }
            
     
            ."""+seed+"""cardbasispoints{
            color: """ + arrow_col2 + """;
            font-size: 14px;
            min-width: 40%;
            text-align: right;
            white-space: nowrap;
            padding-top: 10px;
            }

        </style>
          <div class="""+seed+"panelcard"+""">      
            <div class = """+seed+"cardbox"+""" >
                <div class = """+seed+"cardblock"+""">
                    <div class=""" +seed+"cardsent"+""">
                        <span class="""+seed+"cardtitle"+""">"""+ name +"""</span>
                        <span class="""+seed+"cardbasispoints"+""">
                    </div>
                    <div class="""+seed+"cardsent"+""">
                        <span class="""+seed+"cardtime"+""">"""+ Timestamp + """</span>
                        <span class="""+seed+"cardbasispoints"+""">
                        <span class="""+seed+"cardvalue"+""">Predicted Direction: </span>
                            <span style="
                                display: inline-block;
                                fill: currentColor;
                                color:"""+ arrow_col2 + """;
                                height:12px;
                                width:12px;
                                padding-right:5px;
                                transform: rotate("""+ arrow_rot2 + """deg);
                                "><svg version="1.1" viewBox="0 0 12 12" xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink">
                                    <path d="M6,0.002L0 6.002 4.8 6.002 4.8 11.9996 7.2 11.9996 7.2 6.002 12 6.002z"></path>
                                </svg>
                            </span>
                        </div>
                    </div>
                </div>
           </div>
    """), metadata=dict(isolated=True))
    def show(self):
        """Helper function to display the card
        """
        self.__display_card(self.name, self.Timestamp, self.arrow_col2, self.arrow_rot2, self.seed)

class bulkCardsUQ():
    """Class used to generate convert finance cards into json dumps

    Args:
        notebook_name (str): Name of the Asset
        frequency (str): Frequency of the data
        target (str): target of the data e.g Open, Close etc
        run_time (str): When the dump was built
        viz_type (str): {default: TradeCard} Type of visualisation
    
    """
    def __init__(self, notebook_name, frequency, target, run_time, rnd = 4):
        self.notebook_name = notebook_name
        self.frequency = frequency
        self.target = target
        self.run_time = run_time
        self.rnd = rnd

    def bulkBuild(self, results, UQBounds_df, con_per, viz_type="Trade Cards"):
        """Function to build the dumps for each results dataset

        Args:
            results (Dataframe): Consolidated data frame with the timeseries data of all the models
            UQBounds_df (Dataframe): High and Low bounds for each model
            con_per (integer): Confidence bounds of the UQ

        Returns:
            json: A dump of all the finance cards for grafana

        """
        Trade_card = {}
        Trade_card['table_fk'] = []
        Trade_card['TradeCards'] = []
        bots = list(results.columns.values)
        bots.remove(self.target)
        for i in range(0, len(bots)):
            change = (results[bots[i]][-1] - results[self.target][-2])
            per_change = (change / results[bots[i]][-1]) * 100
            # Display the Finance Cards
            id = {
                'notebook_name':self.notebook_name, 
                'frequency':self.frequency, 
                'target': self.target, 
                'bot_type':"Bot " + str(i+1) + " (" + bots[i] + ")",
                'run_time':self.run_time,
                'time': results.index[-1].strftime("%Y-%m-%d %H:%M UTC"),
                'viz_type':viz_type
            }

            data = {
                'bot_name': "Bot " + str(i+1) + " (" + bots[i] + ")", 
                'time': results.index[-1].strftime("%Y-%m-%d %H:%M UTC"),
                'expected_value': str(round(abs(results[bots[i]][-1]), self.rnd)),
                'higher_bound': str(round(UQBounds_df[bots[i]+'_H'][-1], self.rnd)),
                'lower_bound': str(round(UQBounds_df[bots[i]+'_L'][-1], self.rnd)),
                'change': str(round(change, self.rnd)),
                'percentage_change': str(round(per_change, self.rnd))
            }
            
            Trade_card['table_fk'].append(id)
            Trade_card['TradeCards'].append(data)
        return Trade_card

"""
Model Reliablility
===================

This class computes all the regression metrics for a model based on the results from the testing set
and displays them in form of a heatmap.
There are functions to generate dumps which are sent to grafana.
"""

import math
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from copy import deepcopy
from sklearn.metrics import (explained_variance_score, mean_absolute_error,
                             mean_squared_error, median_absolute_error,
                             r2_score)

def mean_absolute_percentage_error(y_true, y_pred): 
    y_true, y_pred = np.array(y_true), np.array(y_pred)
    return np.mean(np.abs((y_true - y_pred) / y_true)) * 100



class ModelReliabilityV2():
    """
    Args:
        data (DataFrame): collects model preditions from test set on which the model reliability needs to be applied.
        target (str): column in data on which comparision takes place
    """
    def __init__(self, data, target = None):
        #generates the table for each data slices for a model
        data = deepcopy(data)
        self.table(data, target)
        self.data = data
        self.target = target

    def calculate_errors(self, y_test, y_test_):
        """function to calculate error metrics into a form of dictionaries
        
        Args:
            y_test: the testing set values
            y_test_: the testing set predictions
            
        Returns: 
            dict: a dictionary with metrics report
        """
        mesures = {
            "test":{
                "EVS": explained_variance_score(y_test, y_test_),
                "RMAE": math.sqrt(mean_absolute_error(y_test, y_test_)),
                "MSE": mean_squared_error(y_test, y_test_),
                "MAE": mean_absolute_error(y_test, y_test_),
                "RMSE": math.sqrt(mean_squared_error(y_test, y_test_)),
                "R2": r2_score(y_test, y_test_),
		"MAPE": mean_absolute_percentage_error(y_test, y_test_)
                
            }    
        }
        return mesures
    
    def table(self, data, target):
        """Pandas dataframe is built using this function
        
        Args:
            data: the data used for the calculate_errors functions
            names: names of the models
        
        """
        target_data = data[target].values
        data.drop(target, axis = 1, inplace=True)
        
        # generate dictionaries for storing data
        d1={}
        # apply regression metrics function to retrive mesures for each type of data
        for i in data.columns:
            me = self.calculate_errors(target_data, data[i].values)
            d1[i] = me["test"] 
        # convert dictionaries to pandas frames            
        d1 = pd.DataFrame(d1, columns=d1.keys())
        
        # make pandas frames data objects
        self.table_test = d1
        
    def test_heatMap(self, annot=False, cmap='rocket'):
        """Displays Heat Maps

        Args:
            annot (bool): displays the number in the cells of heat map
            cmap (str): color scheme of the heat map
        """
        sns.set(rc={'figure.figsize':(10,10)})
        sns.heatmap(self.table_test.T, annot=annot, cmap=cmap).xaxis.set_ticks_position('top')
        plt.title("Testing set", fontsize=15,  pad=20)
        plt.show()
    
    def bulkBuild(self, notebook_name, frequency, run_time, viz_type='MRM'):
        """Function used to generate json dumps for grafana

        Args:
            notebook_name (str): Name of the Asset
            frequency (str): Frequency of the data
            run_time (str): When the dump was built
            viz_type (str): Type of visualisation
        
        Returns:
            dict: Json dumps with all the MRM table data
        """
        mrm_dict = self.table_test.to_dict()
        MRM = {}
        MRM['table_fk'] = []
        MRM['MRM'] = []
        #bots.remove(self.target)
        s = list(self.data.columns.values)
        for bot in range(len(s)):
            target = s[bot]
            id = {
                'notebook_name':notebook_name, 
                'frequency':frequency, 
                'target': self.target,
                'run_time':run_time,
                'bot_type': "Bot " + str(bot + 1) + " (" + target + ")", 
                'time': self.data.index[-1].strftime("%Y-%m-%dT%H:%M"),
                'viz_type':'MRM'
            }
            MRM['table_fk'].append(id)
            data = {}
            data['evs'] = str(round(mrm_dict[target]['EVS'],6))
            data['mae'] = str(round(mrm_dict[target]['MAE'], 6))
            data['mse'] = str(round(mrm_dict[target]['MSE'], 6))
            data['r2'] = str(round(mrm_dict[target]['R2'], 6))
            data['rmae'] = str(round(mrm_dict[target]['RMAE'], 6))
            data['rmse'] = str(round(mrm_dict[target]['RMSE'], 6))
            data['mape'] = str(round(mrm_dict[target]['MAPE'], 6))
            data['model_name'] = "Bot " + str(bot + 1) + " (" + target + ")"
            data['timestamp'] = self.data.index[-1].strftime("%Y-%m-%dT%H:%M")
            MRM['MRM'].append(data)

        return MRM

from sklearn.metrics import classification_report

class ClassificationMatrix():
    def __init__(self, df, target="Close(t) Change", dump_target="Close(t)"):
        for col in df.columns:
            df[col] = pd.Categorical(df[col])
        self.categories = df[target].cat.categories 
        self.labels = df[target].cat.codes
        self.df = df   
        self.target = target
        self.dump_target = dump_target
    
    def report(self, drop_cols=None):
        self.clf_reports = {}
        for key in self.df.columns:
            self.clf_reports[key] = pd.DataFrame(
            classification_report(
                self.df[self.target],
                self.df[key],
                target_names=list(self.categories),
                output_dict=True)
            )
        final_report = pd.DataFrame()
        bots = list(self.df.columns.values)
        bots.remove(self.target)
        for keys, report in self.clf_reports.items():
            clf_report = pd.DataFrame()
            report.index.name = "metric"
            for metric in report.index:
                temp = report[report.index == metric]
                temp.columns = [metric.replace('-', '_') + "_" + str(col.replace(' ', '_')) for col in temp.columns]
                temp.reset_index(inplace=True, drop=True)
                temp.dropna(inplace=True)
                clf_report = pd.concat([clf_report, temp], axis=1)
            clf_report["model_name"] = keys
            final_report = pd.concat([final_report, clf_report], axis=0)
        final_report.set_index("model_name", inplace=True)
        final_report = final_report.T
        final_report = final_report.drop(drop_cols, axis=1).T
        indices = []
        for i in range(len(final_report.index.values)):
            indices.append("Bot " +  str(i + 1) + " (" + bots[i] + ")")
        final_report.reset_index(inplace=True)
        final_report["model_name"] = indices
        self.final_report = final_report
        return final_report

    def bulkBuild(self, notebook_name, frequency, run_time, viz_type='Class_M'):
        """Function used to generate json dumps for grafana

        Args:
            notebook_name (str): Name of the Asset
            frequency (str): Frequency of the data
            run_time (str): When the dump was built
            viz_type (str): Type of visualisation
        
        Returns:
            dict: Json dumps with all the MRM table data
        """
        class_m = {}
        class_m['table_fk'] = []
        class_m['Class_M'] = []
        bots = list(self.df.columns.values)
        bots.remove(self.target)
        for bot in self.final_report["model_name"].values:
            id = {
                'notebook_name':notebook_name, 
                'frequency':frequency, 
                'target': self.dump_target,
                'run_time':run_time,
                'bot_type': bot, 
                'time': run_time,
                'viz_type': viz_type
            }
            class_m['table_fk'].append(id)
        self.final_report["timestamp"] = run_time
        data = self.final_report.to_dict(orient="records")
        for d in data:
            for key, value in d.items():
                if(type(value) == float):
                    d[key] = str(round(value, 6))
        class_m['Class_M'] = data

        return class_m 

#from Taiyo_Simulation.toMoneyManagement import MoneyManagement
"""
Money Management
================

Money Management Class aims to provide insights into the how strategies perform on the investment

we pass the strategies like 
- buy and hold
- mean reversion strategy

use benchmarks to measure the performance of a asset with a stock index of choice 

"""
import pandas as pd
import numpy as np


class MoneyManagement():
    def __init__(self, capital, runtime):
        self.capital = capital
        self.runtime = runtime

    def buy_and_hold(self, portfolio_name, df, target, rf):
        from copy import deepcopy
        df = deepcopy(df)
        df = df[df.index >= self.runtime]
        data = df
        data["price"] = data[target].values[0]
        data["shares"] = int(self.capital / data[target].values[0])
        data["normalised_return"] = data[target] / data[target][0]
        data["allocation"] = data["shares"] * data["normalised_return"] * data["price"]
        data["daily_return"]= data["normalised_return"].pct_change(1)
        data["Returns %"] = 100 * (data["allocation"]/data["allocation"][0] - 1)
        data['cum_max'] = df["Returns %"].cummax()
        df['Drawdown'] = df['Returns %'] / df['cum_max'] - 1
        df.drop('cum_max', axis=1, inplace=True)

        temp = {}
        portfolio_sum = {}
        portfolio_sum[portfolio_name] = temp

        temp["horizon"] = len(data)
        temp["Current Value"] = data["allocation"][-1]
        temp["return %"] = 100 * (data["allocation"][-1]/data["allocation"][0] - 1)
        temp["return_mean"] = data["daily_return"].mean()
        temp["return_stdev"] = data["daily_return"].std()
        temp["sharpe_ratio"] = (temp["return_mean"] - rf)/temp["return_stdev"]
        temp["Annualised_sharpe_ratio"] = (252**0.5) * temp["sharpe_ratio"]
        temp["Unrealised P&L"] = data["allocation"][-1] - data["allocation"][0]
        temp["Initial Investment"] = data["allocation"][0]
        temp["balance"] = self.capital - temp["Initial Investment"] 
        return data, pd.DataFrame(portfolio_sum)

    def bot_strategy(self,  portfolio_name, df, target, pred_target, rf):
        from copy import deepcopy
        df = deepcopy(df)
        df = df[df.index >= self.runtime]
        df['Indicator'] =  ((df[pred_target] - df[target]) / df[target]).shift()
        df['SPY_Return'] = (df[target].shift() / df[target] - 1) * 100
        df['Diff'] = df['Indicator'].apply(lambda x: -1 if x < 0 else (1 if x > 0 else 0))
        df['Strategy_Return'] = df['SPY_Return'] * df['Diff']
        df['T_Cost'] = abs((df['Diff'].shift(1) -  df['Diff']) * 0.02)
        df['Net_Return'] = df['Strategy_Return'] - df['T_Cost']
        df['Strategy_NAV'] = df.Net_Return.fillna(0, downcast='infer').add(1).cumprod()
        df['Returns %'] = df["Strategy_NAV"]/df["Strategy_NAV"][0] - 1
        df['cum_max'] = df.Strategy_NAV.cummax()
        df['Drawdown'] = df['Strategy_NAV'] / df['cum_max'] - 1
        df.drop('cum_max', axis=1, inplace=True)
        df['Trades'] = abs(df['Diff'] - df['Diff'].shift())
        df['horizon'] = len(df)


        net_return_count = len(df['Net_Return'][1:])
        Annual_Return = sum(df['Net_Return'][1:])/net_return_count * 252
        IR = Annual_Return/np.std(df['Net_Return'][1:])*np.sqrt(252)
        Max_Drawdown = min(df['Drawdown'])
        Ret_Drawdown = Annual_Return/Max_Drawdown
        Holding_Period = sum(df['Trades'][1:])/net_return_count
        Edge_Per_Trade = sum(df['Strategy_Return'][1:])/sum(df['Trades'][1:])
        #return Annual_Return,IR, Ret_Drawdown, Max_Drawdown, Holding_Period, Edge_Per_Trade
        return_mean = df["Strategy_NAV"].mean()
        return_std = df["Strategy_NAV"].std()
        sharpe_ratio = (return_mean - rf)/return_std
        
        shares = int(self.capital/df[target].values[0])
        initial_investment = shares * df[target].values[0]
        current_value = df["Strategy_NAV"].values[-1] * initial_investment
        balance = self.capital - initial_investment
        UPNL = ( current_value - (initial_investment + balance))
        trading_strat_dict = {
            'Annual Return' : Annual_Return, 
            'IR': IR, 
            'Ret Drawdown ': Ret_Drawdown,
            'Max Drawdown ': Max_Drawdown, 
            'Holding Period ': Holding_Period,
            'Edge Per Trade ': Edge_Per_Trade,
            'horizon': len(df),
            'return_mean': return_mean,
            'return_stdev': return_std,
            'sharpe_ratio': sharpe_ratio,
            "Annualised_sharpe_ratio": (252**0.5) * sharpe_ratio,
            'Current Value': current_value,
            'Initial Investment': initial_investment,
            'balance': balance,
            'Unrealised P&L': UPNL,
            'return %': df["Strategy_NAV"].values[-1] - 1
                            }

        tsd = {}
        tsd[portfolio_name] = trading_strat_dict

        return df, pd.DataFrame(tsd)

    def bulkBuild_returns(self, df, notebook_name, frequency, target, run_time, viz_type="Returns"):
        """
        notebook_name = "Amazon (NASDAQ: AMZN)"
        frequency="Daily"
        target="Close(t)"
        run_time="2019-01-03T00:00:00Z"
        strategy = "buy_and_hold"
        viz_type="Returns"
        """
        data = {}
        data["table_fk"] = []
        data["Returns"] = []

        for col in df.columns:
            id = {
            'notebook_name':notebook_name, 
            'frequency':frequency, 
            'target': target,
            'run_time':run_time, 
            'bot_type': col, 
            'time': run_time,
            'viz_type':viz_type
                }
            data["table_fk"].append(id)
            for i in range(len(df)):
                ts = pd.to_datetime(str(df.index.values[i]))
                dump = { 
                    'strategy': col,
                    'time': ts.strftime("%Y-%m-%dT%H:%M"),
                    'identifier': col,
                    'value': str(df[col].values[i]),
                    }
                data["Returns"].append(dump)
        return data

    def bulkBuild_portfolio_sum(self, df, notebook_name, frequency, target, run_time, viz_type="portfolio_sum"):
        """notebook_name = "Amazon"
        frequency="Daily"
        target="Close(t)"
        run_time="2019-01-03T00:00:00Z"
        strategy = "buy_and_hold"
        viz_type="portfolio_sum"
        """

        data = {}
        data["table_fk"] = []
        data["portfolio_sum"] = []

        for col in df.columns:
            for i in range(len(df)):
                id = {
                'notebook_name':notebook_name, 
                'frequency':frequency, 
                'target': df.index.values[i],
                'run_time':run_time, 
                'bot_type': col, 
                'time': run_time,
                'viz_type':viz_type
                    }
                data["table_fk"].append(id)
                dump = { 
                    'strategy': col,
                    'time': run_time,
                    'identifier': df.index.values[i],
                    'value': str(df[col].values[i]),
                    }
                data["portfolio_sum"].append(dump)
        return data

"""
Cascade Graph
=============
"""

import pandas as pd
import numpy as np


class Cascade_Graph():
    """Builds Cascade graph dumps for grafana
    
    Args:
        df (DataFrame): time series data for the signals with the target signal
        target (str): target column for the dataframe
    """
    def __init__(self, df, target):
        self.df = df
        self.target = target

    def bulkBuild(self, notebook_name, frequency, run_time, viz_type='TimeSeries'):
        """Generates Json Dumps from the dataframe
        
        Args:
            notebook_name (str): Name of the Asset
            frequency (str): Frequency of the data
            target (str): target of the data e.g Open, Close etc
            run_time (str): When the dump was built
            viz_type (str): {default: TradeCard} Type of visualisation
        
        Returns:
            json: Consolidated dump for all the data from the dataframe

        """
        MTS = {}
        MTS['table_fk'] = []
        MTS['TimeSeries'] = []
        s = list(self.df.columns.values)
        res = self.df.replace({pd.np.nan: None})
        for bot in range(len(s)):
            target = s[bot]
            id = {
                'notebook_name':notebook_name, 
                'frequency':frequency, 
                'target': self.target,
                'run_time':run_time, 
                'bot_type': target if(bot == 0) else "Bot {} ({})".format((bot), target), 
                'time': run_time,
                'viz_type':viz_type
            }
            MTS['table_fk'].append(id)
            for i in range(len(res)):
                if(res[target].values[i] is not None):
                    data = {
                    'bot_type': "Bot " + str(bot + 1), 
                    'time': res.index[i].strftime("%Y-%m-%dT%H:%M"),
                    'identifier': target if(bot == 0) else "Bot {} ({})".format((bot), target),
                    'value': str(res[target].values[i]),
                    }
                    MTS['TimeSeries'].append(data)
        return MTS

import pandas as pd

# Function to get the direction of the cells

# Function to get the direction of the cells
def correct_direction(dir_actc, dir_cha1):
    count = 0
    for i in range(len(dir_actc)):
        if(dir_cha1[i] == dir_actc[i]):
            count +=1
    return (count/len(dir_actc))* 100

# Function to generate colors for the cells with Directions defined
def color_lists(cha):
    col1 = []
    col_bool = []
    for i in range(len(cha)):
        if(cha[i] < 0):
            col1.append('#FF0000')
            col_bool.append('DOWN')
        elif(cha[i] > 0):
            col1.append('#00FF00')
            col_bool.append('UP')
        else:
            col1.append('#FF0000')
            col_bool.append('NEUTRAL')
    return col1, col_bool

class SimTable():
    def __init__(self, df, target):
        '''
        param df: DataFrame with the target and other results
        param target: The target on which the direction and change needs to be calculated.
        return :
        '''
        self.df = df
        self.target = target

    def changeTable(self, round_num, horizon, smooth=0):
        '''
        Calculates the Change and Direction of the Prices and the Bots Price Predictions.
        param round_num: Choose the decimal to display
        param horizon: number of points to calculate the tables
        return pd.Dataframe(): A Dataframe with all the changes in prices
        return 2D array: Colors for the Plotly Table Class
        '''
        df = pd.DataFrame()
        for i in self.df.columns:
            df[i + " Change"] = round(self.df[i] - self.df[self.target].shift(1),round_num)
        self.change_table = df.tail(horizon)
        
        color_list = {}
        direc_list = {}
        avg_list = []
        names_list = []
        sum_list = []
        
        a, b = color_lists(self.change_table[self.target + " Change"])
        color_list[self.target + " Change"] = a
        direc_list[self.target + " Change"] = b
            

        for i in self.change_table.columns:
            a, b = color_lists(self.change_table[i])
            color_list[i] = a
            direc_list[i] = b
            if(i != self.target + " Change"):
                avg_list.append(round(self.change_table[i].mean(), round_num))
                sum_list.append(round(correct_direction(direc_list[self.target + " Change"], direc_list[i]), 2))
                names_list.append(i.replace(" Change", ""))
        
        self.direc_list = direc_list
        self.sum_list = sum_list
        self.avg_list = avg_list
        self.names_list = names_list
        
        return self.change_table, color_list
    
    def directionTable(self):
        '''
        Displayes the Direction accuracy and Avg_change of the models
        return pd.DataFrame():
        '''
        d = pd.DataFrame([self.names_list, self.sum_list, self.avg_list],index=["Name", "Direction %", "Avg Change"]).T
        self.direct_table = d.set_index("Name")
        return d.set_index("Name")



    def bulkBuildSim(self, notebook_name, frequency, run_time, viz_type='SimTable'):
        ST = {}
        ST['table_fk'] = []
        ST['Simtable'] = []

        s = list(self.change_table.columns.values)
        #s.remove(self.target+" Change")
        for bot in range(len(s)):
            target = s[bot]
            id = {
                'notebook_name':notebook_name, 
                'frequency':frequency, 
                'target': self.target,
                'run_time':run_time, 
                'bot_type': target.split(' ')[0] if(bot == 0) else "Bot {} ({})".format((bot), target), 
                'time': run_time,
                'viz_type':viz_type
            }
            ST['table_fk'].append(id)
            for i in range(len(self.change_table)):
                data = {
                'bot_type': "Bot " + str(bot + 1), 
                'time': self.change_table.index[i].strftime("%Y-%m-%dT%H:%M"),
                'value': str(self.change_table[target].values[i]),
                'identifier': target.split(' ')[0] if(bot == 0) else "Bot {} ({})".format((bot), target)
                }
                ST['Simtable'].append(data)
        return ST

    def bulkBuildDir(self, notebook_name, frequency, run_time, viz_type='DirTable'):
        DT = {}
        DT['table_fk'] = []
        DT['Dirtable'] = []
        s = list(self.df.columns.values)
        s.remove(self.target)
        for i in range(len(self.direct_table)):
            target = s[i]
            id = {
                'notebook_name':notebook_name, 
                'frequency':frequency, 
                'target': self.target,
                'run_time':run_time,
                'bot_type': "Bot " + str(i + 1) + " (" + target + ")", 
                'time': run_time,
                'viz_type':viz_type
            }
            DT['table_fk'].append(id)
            data = {
            'name': "Bot " + str(i + 1) + " (" + target + ")" , 
            'time': run_time,
            'direction': str(self.direct_table['Direction %'].values[i]),
            'average_change': str(self.direct_table['Avg Change'].values[i]),
            }
            DT['Dirtable'].append(data)
        return DT

    def bulkBuild(self, notebook_name, frequency, run_time):
        ST, DT = self.bulkBuildSim(notebook_name, frequency, run_time), self.bulkBuildDir(notebook_name, frequency, run_time)
        return ST, DT