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



def define_fecha(**context):
    ParamsDict = context["params"]
    log.info('fecha_a_procesar: ' + ParamsDict['fecha_a_procesar'])
    fecha_a_procesar_str = ParamsDict['fecha_a_procesar']
    if fecha_a_procesar_str == None:
        #datetime.datetime.strptime('2014-12-04', '%Y-%m-%d').date()
        fecha_a_procesar_str = ahora
    return fecha_a_procesar_str


def get_last_data_from_db(**context):
    hook = PostgresHook('data_db')   
    pg_uri = hook.get_uri()
    engine = create_engine(pg_uri)

    fecha_a_procesar_str = define_fecha(**context)
    
    categorias = eval(Variable.get("categorias"))    
    data = []
    db_fechas = []
    
    for categoria_data in categorias:
        categoria = categoria_data['categoria']
        df = pd.read_sql_table(table_name=f"{categoria}", con=engine)
        fechas = df['creado'].dt.strftime('%Y-%m-%d').unique().tolist()
        data.append({'categoria':categoria,  'fechas': fechas})
    filas = []
    for categoria_data in data:        
        for fecha in categoria_data["fechas"]:
            filas.append({"categoria": categoria_data["categoria"], "fecha": fecha})
    df = pd.DataFrame(filas)
    log.info(df)
    df = df.query(f"fecha <='{fecha_a_procesar_str}'")
    series_max_fecha_modif = df.groupby('categoria')['fecha'].max()
    log.info('series_max_fecha_modif')
    log.info(series_max_fecha_modif)
    df_max_fecha_modif = pd.DataFrame(series_max_fecha_modif)
    df_max_fecha_modif = df_max_fecha_modif.rename(columns={'fecha':'fecha_max'})

    # Unir los DataFrames por la columna 'categoria'
    df_unido = df.merge(df_max_fecha_modif, on='categoria', how='inner')

    # Filtrar por la fecha máxima
    df_filtrado = df_unido[df_unido['fecha'] == df_unido['fecha_max']]

    # Visualizar el resultado
    for ind in df_filtrado.index:
        db_fechas.append({'categoria':df_filtrado['categoria'][ind] , 'fecha':df_filtrado['fecha'][ind]})
    
    log.info(db_fechas)

    ti = context["task_instance"]
    ti.xcom_push(key='db_fechas', value=db_fechas)

def purge_last_data_of_the_day():
    hook = PostgresHook('data_db')
    #ahora = datetime.datetime.today().strftime('%Y-%m-%d') 
    cmd = f"DELETE FROM public.espacios_culturales WHERE creado  = '{ahora}'"    
    cmd2 = f"DELETE FROM public.cines_indicadores WHERE creado = '{ahora}'"
    cmd3 = f"DELETE FROM public.indicadores WHERE creado = '{ahora}'"
    
    hook.run(cmd)
    hook.run(cmd2)
    hook.run(cmd3)

def load_to_db_espacios_culturales(**context):
    fecha_a_procesar_str = define_fecha(**context)
    log.info('fecha_a_procesar_str: ' + fecha_a_procesar_str)
    ti = context["task_instance"]    
    categorias_data = ti.xcom_pull(task_ids='get_last_data', key='db_fechas') 
    
    hook = PostgresHook('data_db')       
    pg_uri = hook.get_uri()
    engine = create_engine(pg_uri)


    for categoria in categorias_data:                
        query = f"SELECT id_localidad,	id_categoria,	nombre,	domicilio,	cp,	latitud,	longitud, mail,	web, '{fecha_a_procesar_str}' as	creado FROM public.{categoria['categoria']} WHERE creado = '{categoria['fecha']}'"    
        df = pd.read_sql_query(sql=query, con=engine)        
        log.info(categoria['categoria'])        
        df.to_sql('espacios_culturales',con=engine, if_exists='append', index=False)



def insights_cines(**context):
    fecha_a_procesar_str = define_fecha(**context)   
    
    ti = context["task_instance"]
    db_fechas = ti.xcom_pull(task_ids='get_last_data', key='db_fechas')
    for d in db_fechas:
        if d['categoria'] == 'cines':              
            fecha_cine =  d['fecha']

    hook = PostgresHook('data_db')       
    pg_uri = hook.get_uri()
    engine = create_engine(pg_uri)
    
    
    query = f"""select p.descripcion as provincia, nombre, domicilio, piso, cp, web, cod_tel, telefono, mail, latitud, longitud, tipo_latitud_longitud, fuente, sector, pantallas, butacas, tipo_de_gestion, espacio_incaa, anio_actualizacion, creado
                from public.cines ec   
                join localidades l on ec.id_localidad = l.id
                join provincias p  on l.id_provincia = p.id 
                where ec.creado = '{fecha_cine}'
            """

    df = pd.read_sql_query(query, con=engine)
    df['pantallas'] = df['pantallas'].astype(int)
    df['butacas'] = df['butacas'].astype(int)            
    s_pantallas = df.groupby('provincia')['pantallas'].sum()
    s_butacas = df.groupby('provincia')['butacas'].sum()
    s_espacios_incaa = df.groupby('provincia')['espacio_incaa'].value_counts().unstack(fill_value=0)['Si']
    df_cines = pd.DataFrame({'provincia': s_pantallas.index.tolist(),
                                'cant_pantallas':s_pantallas,
                                'cant_butacas':s_butacas,
                                'cant_espacios_incaa':s_espacios_incaa,
                                'creado': fecha_a_procesar_str}).reset_index(drop=True)            
    df_cines.to_sql('cines_indicadores',con=engine, if_exists='append', index=False)

                    

def indicadores(**context):
    fecha_a_procesar_str = define_fecha(**context)
    hook = PostgresHook('data_db')       
    pg_uri = hook.get_uri()
    engine = create_engine(pg_uri) 

    query = f"""select p.descripcion as provincia, c.descripcion as categoria, ec.id 
                       from public.espacios_culturales ec join categorias c  on ec.id_categoria = c.id
	 				   join localidades l on ec.id_localidad = l.id
                       join provincias p  on l.id_provincia = p.id
                       where ec.creado = '{fecha_a_procesar_str}'"""
    log.info(query)
    #indicadores
    df = pd.read_sql_query(sql=query, con=engine)   
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
    

    df_indicadores['categoria'] = df_indicadores['descripcion'].str.split(',').str[0]
    df_indicadores['provincia'] = df_indicadores['descripcion'].str.split(',').str[1]
    df_indicadores.drop('descripcion', axis=1, inplace=True)
    df_indicadores['creado'] = fecha_a_procesar_str #datetime.datetime.today().strftime('%Y-%m-%d')
    log.info(df_indicadores.columns)
    log.info(df_indicadores.head(10))    
    df_indicadores.to_sql('indicadores',con=engine, if_exists='append', index=False)

with DAG(

    dag_id="ETL_ESPACIOS_CULTURALES",
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


    get_last_data = PythonOperator(task_id='get_last_data',
                                         python_callable=get_last_data_from_db)

    purge_data_cultural = PythonOperator(task_id='purge_data_cultural',
                                         python_callable=purge_last_data_of_the_day)        

    load_espacios_culturales_to_db =  PythonOperator(task_id="load_categorias_to_db",
                                            python_callable=load_to_db_espacios_culturales)
    
    load_indicadores_cines_to_db =  PythonOperator(task_id="load_indicadores_cines_to_db",
                                            python_callable=insights_cines)

    load_indicadores_to_db =  PythonOperator(task_id="load_indicadores_to_db",
                                            python_callable=indicadores)


    fin = DummyOperator(task_id='fin')

    start >> get_last_data >> purge_data_cultural >> load_espacios_culturales_to_db >> load_indicadores_cines_to_db >> load_indicadores_to_db >> fin

