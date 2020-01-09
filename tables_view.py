from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import os
from airflow.hooks import PostgresHook
import json
import numpy as np
from psycopg2.extras import execute_values

def create_table(conn_id, dest_table):
    
    pg_hook = PostgresHook(postgres_conn_id=conn_id)
    delete_cmd = """
                DROP TABLE IF EXISTS {};
                 """.format(dest_table)
    create_cmd = """
                 CREATE table {} (
                 table_name VARCHAR(50),
                 row_count INTEGER
                 )
                 """.format(dest_table)
    dest_conn = pg_hook.get_conn()
    delete_cursor = dest_conn.cursor()
    delete_cursor.execute(delete_cmd)
    dest_cursor = dest_conn.cursor()
    dest_cursor.execute(create_cmd)
    dest_conn.commit()

def insert_table(conn_id, schema, src_table, dest_table):
   
    pg_hook = PostgresHook(postgres_conn_id=conn_id)
    select_cmd = """SELECT relname, n_live_tup from {} where schemaname = '{}'
    and relname != '{}';""".format(src_table,
                  schema, dest_table)
    src_conn = pg_hook.get_conn()
    src_cursor = src_conn.cursor()
    src_cursor.execute(select_cmd)
    records = src_cursor.fetchall()
    dest_conn = pg_hook.get_conn()
    dest_cursor = dest_conn.cursor()
    execute_values(dest_cursor,
                 "INSERT INTO {} VALUES %s".format(dest_table), records)
    dest_conn.commit()             

def insert_values_into_table():
    conn_id = 'postgres_default'

    create_table(conn_id, 'all_tables')
    insert_table(conn_id, 'public','pg_stat_all_tables', 'all_tables')

default_args = {
    'owner':'airflow'
}

dag = DAG('insert_task', description='insert into new tables DAG',
                        default_args=default_args,
                        schedule_interval='0 10 * * *',
                        start_date=datetime(2019, 5, 20), catchup=False)


hello_operator = PythonOperator(task_id='display_task',
python_callable=insert_values_into_table,  dag=dag)
hello_operator
