from datetime import datetime
import os
import json
import numpy as np
import pandas as pd
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks import PostgresHook
import numpy as np

RAW_DIR = os.path.abspath(os.path.join(__file__,'../'))


def check_return(df):
   
    """ 
    Checks if starting and ending airport is same and 'OneWayOrReturn' is
    'Return' then error and vice versa
    """ 
  
# If Reason is already set for Itinerary then no need to check for return validity  

    if df['Reason'] == "Itinerary":
        return df['Reason']

    if (df['Itinerary'].split('-')[0] != df['Itinerary'].split('-')[-1]):
        if df['OneWayOrReturn'] == 'Return':
            return 'OneWayOrReturn'
    else:
        if df['OneWayOrReturn'] != 'Return':
            return 'OneWayOrReturn'
         
def check_itinerary(df):
   
    """
    Checks if starting and ending airport is not same and 'OneWayOrReturn' is
    'Return' then error
    """ 

    #if(df['DepartureAirport'] in df['Itinerary']):
    if(df['Itinerary'].startswith(df['DepartureAirport'])):
        pass
    else:
        return 'Itinerary'


def clean_file():
    
    """
    Read input file and perform below actions:
    - remove duplicates
    - check Itinerary discripency
    - check return discripency
    """
    
    file_clean = RAW_DIR + '/data_cleansing_input.json'
    with open(file_clean) as f:
        data = json.load(f)
    print(data[0:5]) 
    df = pd.DataFrame(data)
    df = df[['TripId','Itinerary','OneWayOrReturn','DepartureAirport','ArrivalAirport','Leg','SegmentOrder','TransactionDateUTC']]
   
# drop duplicates
    df.drop_duplicates(subset=['TripId', 'Leg', 'SegmentOrder'], keep = 'first', inplace=True)

# combine column to match with Itinerary value
    df = df.groupby(['TripId','Leg','SegmentOrder','Itinerary','OneWayOrReturn','ArrivalAirport','TransactionDateUTC'])['DepartureAirport'].apply('-'.join).reset_index()
    df = df.groupby(['TripId','Itinerary','OneWayOrReturn','TransactionDateUTC'])['DepartureAirport'].apply('-'.join).reset_index()

    df['Reason'] = df.apply(lambda row: check_itinerary(row), axis=1) 
    df['Reason'] = df.apply(lambda row: check_return(row), axis=1)

# write csv for not blank Reason columns
    df = df[df['Reason'].notnull()]
    df.to_csv(RAW_DIR+'/trip_reason.csv', columns=['TripId', 'Reason'], index=False)
    print('file successfully written')
    
default_args = {
    'owner':'airflow'
}

dag = DAG('data_clean_task', description='clean json file',
                        default_args=default_args,
                        schedule_interval='0 10 * * *',
                        start_date=datetime(2019, 5, 20), catchup=False)


hello_operator = PythonOperator(task_id='clean_task',
python_callable=clean_file,  dag=dag)
hello_operator
