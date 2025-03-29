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

ahora = datetime.datetime.today().strftime('%Y-%m-%d')

log = logging.getLogger(__name__)
PATH_TO_PYTHON_BINARY = sys.executable


def get_ruta_al_archivo(categoria):
    fecha = datetime.datetime.now()
    ruta = Variable.get("data_path") #/opt/airflow/data/
    dia = fecha.day
    mes = fecha.month
    anio = fecha.year
    template = '{ruta}/{categoria}/{anio}-{mes}/{categoria}-{dia}-{mes}-{anio}.csv'
    ruta_al_archivo_str = template.format(ruta = ruta, categoria=categoria, anio=anio,mes=mes, dia=dia)
    ruta_al_archivo = Path(ruta_al_archivo_str)
    return ruta_al_archivo


def descargar_archivo(**context):
    data = []
    categorias = eval(Variable.get("categorias"))

    for categoria in categorias:
        url = Variable.get(categoria['url'])
        respuesta = requests.get(url)
        ruta_al_archivo = get_ruta_al_archivo((categoria['categoria']))
        ruta_al_archivo.parent.mkdir(parents=True, exist_ok=True)               
        with open(ruta_al_archivo, "wb") as archivo:
            archivo.write(respuesta.content)                    
            data.append({'categoria':categoria['categoria'],'ruta':str(ruta_al_archivo)})
    log.info('archivos guardados en '+ str(data))
    ti = context["task_instance"]
    ti.xcom_push(key='categorias_raw_data_files', value=data)


def purge_last_data_of_the_day(**context):
    hook = PostgresHook('data_db')
    #ahora = datetime.datetime.today().strftime('%Y-%m-%d') 
    cmd = f"DELETE FROM public.raw_cines WHERE creado  = '{ahora}'"
    log.info(cmd)
    cmd2 = f"DELETE FROM public.raw_museos WHERE creado = '{ahora}'"
    cmd3 = f"DELETE FROM public.raw_museos WHERE creado = '{ahora}'"
    hook.run(cmd)
    hook.run(cmd2)
    hook.run(cmd3)


def load_raw_data_to_db(**context):
    ti = context["task_instance"]
    hook = PostgresHook('data_db')
    files_path = ti.xcom_pull(task_ids='get_archivos_categorias', key='categorias_raw_data_files')        
    pg_uri = hook.get_uri()
    engine = create_engine(pg_uri)

    for path in files_path:        
        df = pd.read_csv(path['ruta'], dtype=str)                
        df['creado'] = ahora

        # Obtener los nombres de columnas de la tabla destino en PostgreSQL
        query = f"SELECT column_name FROM information_schema.columns WHERE table_name = 'raw_{path['categoria']}' ORDER BY ordinal_position;"
        columnas_tabla = pd.read_sql(query, con=engine)['column_name'].tolist()
        

        # Renombrar las columnas del DataFrame para que coincidan exactamente con la tabla
        df_to_insert = df.copy()
        # Asignar nuevos nombres de columnas basados en la posición
        for i, col_name in enumerate(df.columns):
            if i < len(columnas_tabla):
                df_to_insert.rename(columns={col_name: columnas_tabla[i]}, inplace=True)

        # Asegurarse de que el DataFrame tenga las columnas en el mismo orden que la tabla
        df_to_insert = df_to_insert[columnas_tabla]

        log.info(path['categoria'])
        log.info(df_to_insert.columns)
        
        # Insertar en la base de datos
        df_to_insert.to_sql(f"raw_{path['categoria']}", con=engine, if_exists='append', index=False)        


with DAG(

    dag_id="ETL_DATA_RAW_ESPACIOS_CULTURALES",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["challenge_data"],
):
            

    start = DummyOperator(task_id='inicio')

    get_archivos_categorias = PythonOperator(task_id='get_archivos_categorias',
                                             python_callable=descargar_archivo
                                            )
    
    
    purge_data = PythonOperator(task_id='purge_last_data_of_the_day',
                                python_callable=purge_last_data_of_the_day)

    load_categorias_to_db =  PythonOperator(task_id="load_categorias_to_db",
                                            python_callable=load_raw_data_to_db)

    fin = DummyOperator(task_id='fin')

    start >> get_archivos_categorias >>  purge_data >> load_categorias_to_db >> fin