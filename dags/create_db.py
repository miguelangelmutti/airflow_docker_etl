import logging
import sys
import datetime
import psycopg2
import pendulum
from sqlalchemy import create_engine
from airflow.models.dag import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator


conn = BaseHook.get_connection("data_db")
conn_info = {'host':conn.host, 'port':conn.port,'login':conn.login, 'password':conn.password}


with DAG(

    dag_id="CREATE_TABLES_DATA",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["challenge_data"],

):
    def create_db(conn_info):    
        psql_connection_string = f"host={conn_info['host']} port={conn_info['port']} user={conn_info['login']} password={conn_info['password']}"
        conn = psycopg2.connect(psql_connection_string)
        cur = conn.cursor()    
        conn.autocommit = True
        #sql_query = f"CREATE DATABASE {conn_info['database']}"
        sql_query = f"CREATE DATABASE tremendo_pene_db"

        try:
            cur.execute(sql_query)
        except Exception as e:
            print(f"{type(e).__name__}: {e}")
            print(f"Query: {cur.query}")            
        finally:            
            conn.autocommit = False            
            conn.close()
            cur.close()
    
    start = DummyOperator(task_id='inicio')
    
    create_database = PythonOperator(
        task_id='crear_database',
        python_callable=create_db,
        op_args = [conn_info]
    )

    end = DummyOperator(task_id='fin')


start >> create_database >> end