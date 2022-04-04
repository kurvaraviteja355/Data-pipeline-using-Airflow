import pandas as pd
import numpy as np
import math
from datetime import datetime, timedelta
from datetime import date
import urllib.request
import json
import requests
import os
import glob
import warnings
from urllib.parse import quote_plus
from sqlalchemy import create_engine, event
import time 
import urllib.parse 


# read the dataset

def get_yesterday_date():
    return date.today() - timedelta(1)

def weather_extraction(**kwargs):

    yesterday_date = kwargs["execution_date"]
    

    df_germany_hardware = pd.read_csv('input_files/German_data.csv')

    df_germany_hardware['Reseller Postal Code'] = df_germany_hardware['Reseller Postal Code'].astype('category')

    # collect the zipcodes to exact the corresponding data


    #yesterday_date = date.today() - timedelta(1)

    # dd/mm/YY
    d1 = yesterday_date.strftime("%Y-%m-%d")
    #print("d1 =", str(d1))
    records=[]

    #### chnage the data according to the needs or requirements
    query_path = 'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/weatherdata/history?&contentType=json&aggregateHours=24&combinationMethod=aggregate&startDateTime='
    param1 = 'T00%3A00%3A00&endDateTime='
    param2 = 'T00%3A00%3A00&maxStations=-1&maxDistance=-1&contentType=csv&unitGroup=metric&locationMode=single&key=3SQ53JTHN8QRBRAPSMTUMLDVE&dataElements=default&locations='
    param_query = '%20Germany'

    all_zipcodes = df_germany_hardware['Reseller Postal Code'].unique()
    for i in range(len(all_zipcodes)):

        response = requests.get(query_path+str(d1)+param1+str(d1)+param2+str(all_zipcodes[i])+param_query, verify=False)
        resp_dict = json.loads(response.text)
        locations = resp_dict['location']['id'].split(' ')
        temp_data = resp_dict['location']['values']
        for i in range(len(temp_data)):
            weather_data = temp_data[i]
            weather_data.update({'Zipcode': locations[0], 'Country' : locations[1]})
            records.append(weather_data)
            
    weather_data = pd.DataFrame(records)

    

    weather_data.to_csv(r'output_files/'+str(d1)+'.csv', index = None, header = True)

    # #ti.xcom_push(key = "weather_data", value = weather_data)
    
    # #return weather_data
    print('Successfully Pulled the data from API')


#### function to preprocess the weather data

def weather_preprocessor(**kwargs):

    #yesterday_date = date.today() - timedelta(1)
    yesterday_date = kwargs["execution_date"]
    # dd/mm/YY
    d1 = yesterday_date.strftime("%Y-%m-%d")
    ## Read the data 
    weather_df = pd.read_csv('output_files/'+str(d1)+'.csv')

    #weather_df = ti.xcom_pull(key="weather_data")



    columns = ['temp', 'maxt', 'mint', 'cloudcover', 'datetimeStr', 'visibility', 'conditions', 'windchill', 'Zipcode', 'Country']
    weather_df = weather_df[columns]
    weather_df['datetimeStr'] = pd.to_datetime(weather_df['datetimeStr'], utc=True).dt.date
    weather_df = weather_df.replace('/','', regex=True)
    weather_df = weather_df.replace(',','', regex=True)
    weather_df['conditions'] = weather_df['conditions'].fillna(method='ffill')

    weather_df.rename(columns={'Zipcode' : 'Reseller Postal Code',
                              'datetimeStr' : 'datum'}, inplace=True)
    ## Convert the sale date into datetime format
    weather_df['datum'] = pd.to_datetime(weather_df['datum'])

    weather_df.to_csv(r'output_files/transformed_weather_df'+str(d1)+'.csv', index = None, header = True)

    #ti.xcom_push(key="weather_df", vlaue=weather_df)

    #return weather_data


### function to export the data to Azure database 

def connect_database_server(**kwargs):
    #yesterday_date = date.today() - timedelta(1)
    yesterday_date = kwargs["execution_date"]
    # dd/mm/YY
    d1 = yesterday_date.strftime("%Y-%m-%d")
    server = 'c-house-sql-dev.database.windows.net'
    database = 'Weather_dataDB'
    username = 'C-houseADM'
    password = 'CH$14aouse'
    driver= '{ODBC Driver 17 for SQL Server}'
    #conn = pyodbc.connect('DRIVER=' + driver + ';SERVER=' + server + ';PORT=1433;DATABASE=' + database +';UID=' + username + ';PWD=' + password)

    quoted = urllib.parse.quote_plus('DRIVER=' + driver + ';SERVER=' + server + ';PORT=1433;DATABASE=' + database +';UID=' + username + ';PWD=' + password)
    engine=create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted), fast_executemany=True)

    weather_dataframe = pd.read_csv('output_files/transformed_weather_df'+str(d1)+'.csv')

    #weather_dataframe = ti.xcom_pull(key ="weather_df")


    start = time.time()
    weather_dataframe.to_sql("Weather_data", con = engine, index=False, if_exists='append', chunksize=150, method=None)
    print('%0.2f min: Lags' % ((time.time() - start) / 60))



### function to remove the files from local system 

def remove_files(**kwargs):
    
    yesterday_date = kwargs["execution_date"]
    # dd/mm/YY
    d1 = yesterday_date.strftime("%Y-%m-%d")

    os.remove('output_files/transformed_weather_df'+str(d1)+'.csv')
    os.remove('output_files/'+str(d1)+'.csv')

    # directory = os.listdir('output_files/')

    # files = [file for file in directory if file.endswith('.csv')]

    # for file in files:
    #     path_file = os.path.join(directory, file)
    #     os.remove(path_file)