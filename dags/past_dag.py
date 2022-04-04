import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from datetime import date
import urllib.request
import json
import requests
import glob
import warnings
from urllib.parse import quote_plus
from sqlalchemy import create_engine, event
import time 
import os
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from weather_data_extraction import weather_extraction, weather_preprocessor, connect_database_server, remove_files

default_args = {'owner':'Ravi', 
                'start_date':datetime(2021, 12, 16),
                'end_date': datetime(2021, 12, 27),
                'retries': 1,
                'retry_delay': timedelta(minutes=5),
                }



with DAG(dag_id = 'Weather_API_data_Extraction', default_args = default_args,
         schedule_interval = '@daily', catchup=True) as dag:

         weather_extraction = PythonOperator(
             task_id ='weather_extraction',
             provide_context=True,
             python_callable = weather_extraction
         )


         weather_preprocessor = PythonOperator(
             task_id = "preprocessing",
             provide_context=True,
             python_callable = weather_preprocessor,

         )

         connect_database_server = PythonOperator(
             task_id = "Export_weather_data_into_DB",
             python_callable = connect_database_server
         )

         remove_files = PythonOperator(
             task_id = "delete_files_after_import",
             python_callable = remove_files
         )

        

         weather_extraction >> weather_preprocessor >> connect_database_server >> remove_files