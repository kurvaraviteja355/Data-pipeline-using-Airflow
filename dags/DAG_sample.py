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
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from weather_data_extraction import weather_extraction

default_args = {'owner':'Ravi', 
                'start_date':datetime(2021, 12, 5),
                'retries': 1,
                'retry_delay': timedelta(minutes=5),
                'catchup_by_default': False}



with DAG(dag_id = 'Sample', default_args = default_args,
         schedule_interval = '@daily') as dag:

         weather_extraction = PythonOperator(
             task_id ='weather_extraction',
             python_callable = weather_extraction
         )

         weather_extraction
    


