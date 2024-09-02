import logging
import sys
import datetime
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


PATH_TO_PYTHON_BINARY = sys.executable

def get_ruta_al_archivo(categoria):
    #/categoria/año-mes/categoria-dia-mes-año-csv
    ruta = Variable.get("data_path") #/opt/airflow/data/
    dia = datetime.datetime.now().day
    mes = datetime.datetime.now().month
    anio = datetime.datetime.now().year
    template = '{ruta}/{categoria}/{anio}-{mes}/{categoria}-{dia}-{mes}-{anio}.csv'
    ruta_al_archivo_str = template.format(ruta = ruta, categoria=categoria, anio=anio,mes=mes, dia=dia)
    ruta_al_archivo = Path(ruta_al_archivo_str)
    return ruta_al_archivo


with DAG(

    dag_id="DOWNLOAD_CATEGORIAS_DATA",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["challenge_data"],

):
    
    def descargar_archivo():
        
        categorias = eval(Variable.get("categorias"))

        for categoria in categorias:
            url = Variable.get(categoria['url'])
            respuesta = requests.get(url)
            ruta_al_archivo = get_ruta_al_archivo((categoria['categoria']))
            ruta_al_archivo.parent.mkdir(parents=True, exist_ok=True)               
            with open(ruta_al_archivo, "wb") as archivo:
                archivo.write(respuesta.content)        
            log.info('archivo guardado en '+ str(ruta_al_archivo))
        

    start = DummyOperator(task_id='inicio')

    get_archivo_museo = PythonOperator(
        task_id='get_archivos_categorias',
        python_callable=descargar_archivo
    )

    fin = DummyOperator(task_id='fin')

    start >> get_archivo_museo >> fin