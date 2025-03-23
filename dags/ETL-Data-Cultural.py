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





def purge_last_data_of_the_day(**context):
    ParamsDict = context["params"]
    fecha_a_procesar_str = ParamsDict['fecha_a_procesar']
    #log.info('fecha_a_procesar: ' + ParamsDict['fecha_a_procesar'])
    if fecha_a_procesar_str == None:
        #datetime.datetime.strptime('2014-12-04', '%Y-%m-%d').date()
        fecha_a_procesar_str = ahora

    hook = PostgresHook('data_db')
    #ahora = datetime.datetime.today().strftime('%Y-%m-%d') 
    cmd = f"DELETE FROM public.espacios_culturales WHERE creado  = '{fecha_a_procesar_str}'"
    log.info(cmd)
    cmd2 = f"DELETE FROM public.cines WHERE creado = '{fecha_a_procesar_str}'"
    cmd3 = f"DELETE FROM public.indicadores WHERE creado = '{fecha_a_procesar_str}'"
    hook.run(cmd)
    hook.run(cmd2)


def get_last_files_path(**context):
    ParamsDict = context["params"]
    log.info('fecha_a_procesar: ' + ParamsDict['fecha_a_procesar'])
    fecha_a_procesar_str = ParamsDict['fecha_a_procesar']
    if fecha_a_procesar_str == None:
        #datetime.datetime.strptime('2014-12-04', '%Y-%m-%d').date()
        fecha_a_procesar_str = ahora

    ruta = Variable.get("data_path")
    categorias = eval(Variable.get("categorias"))    
    data = []
    files_path = []
    
    for categoria_data in categorias:
        categoria = categoria_data['categoria']
        rutas_categoria = os.path.join(ruta,categoria)
        for subdir,dir,files in os.walk(rutas_categoria):
            for file in files:
                if file.endswith('csv'):
                    fecha = os.path.getmtime(os.path.join(subdir,file))
                    dt = datetime.datetime.fromtimestamp(fecha).strftime('%Y-%m-%d')
                    data.append({'categoria':categoria, 'archivo':file,'ruta':os.path.join(subdir, file), 'fecha_modif': dt})

    df = pd.DataFrame.from_dict(data)
    log.info(df)
    df = df.query(f"fecha_modif <='{fecha_a_procesar_str}'")
    series_max_fecha_modif = df.groupby('categoria')['fecha_modif'].max()
    log.info('series_max_fecha_modif')
    log.info(series_max_fecha_modif)
    df_max_fecha_modif = pd.DataFrame(series_max_fecha_modif)
    df_max_fecha_modif = df_max_fecha_modif.rename(columns={'fecha_modif':'fecha_modif_max'})

    # Unir los DataFrames por la columna 'categoria'
    df_unido = df.merge(df_max_fecha_modif, on='categoria', how='inner')

    # Filtrar por la fecha máxima
    df_filtrado = df_unido[df_unido['fecha_modif'] == df_unido['fecha_modif_max']]

    # Visualizar el resultado
    for ind in df_filtrado.index:
        files_path.append({'categoria':df_filtrado['categoria'][ind] ,'ruta':df_filtrado['ruta'][ind]})
    
    ti = context["task_instance"]
    ti.xcom_push(key='fechas', value=files_path)

def load_to_db_from_last_files(**context):
    ti = context["task_instance"]
    hook = PostgresHook('data_db')
    files_path = ti.xcom_pull(task_ids='get_last_files_path', key='fechas')        
    pg_uri = hook.get_uri()
    engine = create_engine(pg_uri)


    ParamsDict = context["params"]
    log.info('fecha_a_procesar: ' + ParamsDict['fecha_a_procesar'])
    fecha_a_procesar_str = ParamsDict['fecha_a_procesar']
    if fecha_a_procesar_str == None:
        #datetime.datetime.strptime('2014-12-04', '%Y-%m-%d').date()
        fecha_a_procesar_str = ahora    

    for path in files_path:        
        if path['categoria'] in ('bibliotecas','museos'):
            if path['categoria'] == 'museos':
                dict_cast = {'cod_area': 'object'}                        
                columnas_reemplazo = {"Cod_Loc":'cod_localidad',
                                                "IdProvincia":'id_provincia',
                                                "IdDepartamento":'id_departamento',
                                                "direccion":'domicilio',
                                                "CP":'cp',
                                                "Mail":'mail',
                                                "Web":'web'
                                                }
            elif path['categoria'] == 'bibliotecas':
                dict_cast = {'cod_tel': 'object', 'telefono':'object'}                        
                columnas_reemplazo = {"cod_tel":'cod_area'}
            else:
                pass
        else:
            columnas_seleccionadas_cine = ["cod_localidad","id_provincia","id_departamento","categoria","provincia","localidad","nombre","direccion","cp","web","fuente","sector","pantallas","butacas","espacio_incaa"]                    
            columnas_reemplazo = {"direccion":'domicilio'}

        columnas_seleccionadas = ["cod_localidad","id_provincia","id_departamento","categoria","provincia","localidad","nombre","domicilio","cp","telefono","mail","web"]
        df = pd.read_csv(path['ruta'],dtype=dict_cast)                
        df = df.rename(columns= columnas_reemplazo)
        if path['categoria'] == 'cines':
            df['telefono'] = None
            df['mail'] = None
            s_pantallas = df.groupby('provincia')['pantallas'].sum()
            s_butacas = df.groupby('provincia')['butacas'].sum()
            s_espacios_incaa = df.groupby('provincia')['espacio_incaa'].value_counts().unstack(fill_value=0)['Si']
            df_cines = pd.DataFrame({'provincia': s_pantallas.index.tolist(),
                                     'cant_pantallas':s_pantallas,
                                     'cant_butacas':s_butacas,
                                     'cant_espacios_incaa':s_espacios_incaa,
                                     'creado': fecha_a_procesar_str}).reset_index(drop=True)            
            df_cines.to_sql('cines',con=engine, if_exists='append', index=False)
        else:
            df['telefono'] = df['cod_area'] + '-' + df['telefono']
            df.drop(['cod_area'], axis=1, inplace=True)
        df = df[columnas_seleccionadas]
        df['creado'] = fecha_a_procesar_str #datetime.datetime.today().strftime('%Y-%m-%d')                
        df.to_sql('espacios_culturales',con=engine, if_exists='append', index=False)

    #indicadores
    df = pd.read_sql_table(table_name='espacios_culturales', con=engine)
    s1 = df.groupby('categoria')['categoria'].count()
    s2 = df.groupby(['categoria','provincia'])['categoria'].count()    
    s3 = pd.concat([s1, s2])    
    lista_indice = s3.index.tolist()
    for i in range(len(lista_indice)):
        str_tuple = str(lista_indice[i])
        lista_indice[i] = str_tuple.replace("'","").replace("(","").replace(")", "")
    s3.index = lista_indice                
    df_indicadores = pd.DataFrame({'descripcion': s3.index.tolist(),
                    'cant_registros':s3},
                    ).reset_index(drop=True)
    df_indicadores['creado'] = fecha_a_procesar_str #datetime.datetime.today().strftime('%Y-%m-%d')
    df_indicadores.to_sql('indicadores',con=engine, if_exists='append', index=False)

with DAG(

    dag_id="ETL_DATA_ESPACIOS_CULTURALES",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["challenge_data"],
    params = {"fecha_a_procesar": Param(ahora,
                                type="string",
                                title="Fecha a procesar",
                                description= "Fecha a procesar, si no se encuentra info local para la fecha indicada, se intentara traer la del dia hoy",
                                )
             }
):
            

    start = DummyOperator(task_id='inicio')

    get_archivos_categorias = PythonOperator(task_id='get_archivos_categorias',
                                             python_callable=descargar_archivo
                                            )
    
    get_last_files = PythonOperator(task_id='get_last_files_path',
                                         python_callable=get_last_files_path)
    
    purge_data = PythonOperator(task_id='purge_last_data_of_the_day',
                                python_callable=purge_last_data_of_the_day)

    load_categorias_to_db =  PythonOperator(task_id="load_categorias_to_db",
                                            python_callable=load_to_db_from_last_files)

    fin = DummyOperator(task_id='fin')

    start >> get_archivos_categorias >> get_last_files >> purge_data >> load_categorias_to_db >> fin