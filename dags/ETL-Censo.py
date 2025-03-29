import logging
import sys
import datetime
from pprint import pprint
import pendulum
import requests
from pathlib import Path
import os
import pandas as pd
from sqlalchemy import create_engine
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.param import Param

from airflow.models.dag import DAG
from airflow.operators.python import (

    PythonOperator,    
    
)
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable

log = logging.getLogger(__name__)

ahora = datetime.datetime.today().strftime('%Y-%m-%d')




def descargar_archivo(**context):    
    ruta = Variable.get("data_path")
    url = Variable.get("data_url_censo")
    respuesta = requests.get(url, verify=False)
    ruta_al_archivo = Path("{ruta}/censo/censo.xlsx".format(ruta = ruta))
    ruta_al_archivo.parent.mkdir(parents=True, exist_ok=True)               
    with open(ruta_al_archivo, "wb") as archivo:
        archivo.write(respuesta.content)        
    log.info('archivo guardado en '+ str(ruta_al_archivo))    
    ti = context["task_instance"]
    ti.xcom_push(key='censo_path', value=str(ruta_al_archivo))

def purge_data_censo():    
    hook = PostgresHook('data_db')    
    cmd = f"DELETE FROM public.censo"    
    hook.run(cmd)

def load_data_censo_to_db(**context):
    ti = context["task_instance"]
    hook = PostgresHook('data_db')
    file_path = ti.xcom_pull(task_ids='get_archivo_censo', key='censo_path')        
    pg_uri = hook.get_uri()
    engine = create_engine(pg_uri)    
    df = pd.read_excel(file_path, engine='openpyxl', skiprows=4, skipfooter=4, usecols = "A:B", names=['jurisdiccion','cant_habitantes'])
    df['creado'] = ahora
    log.info(df.columns)
    df.to_sql('censo',con=engine, if_exists='append', index=False)


with DAG(

    dag_id="ETL_DATA_CENSO-Once",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["challenge_data"],

):
            

    start = DummyOperator(task_id='inicio')

    get_archivo_censo = PythonOperator(task_id='get_archivo_censo',
                                             python_callable=descargar_archivo
                                            )
    
    purge_data = PythonOperator(task_id='purge_data',
                                python_callable=purge_data_censo)

    load_censo_to_db =  PythonOperator(task_id="load_censo_to_db",
                                       python_callable=load_data_censo_to_db)

    fin = DummyOperator(task_id='fin')

    start >> get_archivo_censo >> purge_data >> load_censo_to_db >> fin

