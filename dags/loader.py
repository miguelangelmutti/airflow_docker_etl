import logging
import sys
import datetime
import os
import pandas as pd
from pprint import pprint
import pendulum
import requests
from pathlib import Path

from airflow.models.dag import DAG
from airflow.operators.python import (

    PythonOperator,    
    
)
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable

log = logging.getLogger(__name__)

def get_last_files_path():
    ruta = Variable.get("data_path")
    categorias = eval(Variable.get("categorias"))
    rootdirs = ['./bibliotecas', './cines', './museos']
    data = []
    files_path = []
    
    for categoria_data in categorias:
        categoria = categoria_data['categoria']
        rutas_categoria = os.path.join(ruta,categoria)
        for subdir,dir,files in os.walk(rutas_categoria):
            for file in files:
                if file.endswith('csv'):
                    fecha = os.path.getmtime(os.path.join(subdir,file))
                    dt = datetime.datetime.fromtimestamp(fecha)                    
                    data.append({'categoria':categoria, 'archivo':file,'ruta':os.path.join(subdir, file), 'fecha_modif': dt})

    df = pd.DataFrame.from_dict(data)
    series_max_fecha_modif = df.groupby('categoria')['fecha_modif'].max()
    df_max_fecha_modif = pd.DataFrame(series_max_fecha_modif)
    df_max_fecha_modif = df_max_fecha_modif.rename(columns={'fecha_modif':'fecha_modif_max'})

    # Unir los DataFrames por la columna 'categoria'
    df_unido = df.merge(df_max_fecha_modif, on='categoria', how='inner')

    # Filtrar por la fecha máxima
    df_filtrado = df_unido[df_unido['fecha_modif'] == df_unido['fecha_modif_max']]

    # Visualizar el resultado
    for ind in df_filtrado.index:
        files_path.append({'categoria':df_filtrado['categoria'][ind] ,'ruta':df_filtrado['ruta'][ind]})

    log.info("rutas:")
    log.info(files_path)
                
with DAG(

    dag_id="LOADER_CATEGORIAS_TO_DB",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["challenge_data"],

)as dag:
    
        log_the_sql = PythonOperator(task_id="imprimir",
                                     python_callable=get_last_files_path,        
                                    )