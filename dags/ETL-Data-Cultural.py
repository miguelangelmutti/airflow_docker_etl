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



def purge_last_data_of_the_day(**context):

    fecha_a_procesar_str = define_fecha(**context)

    hook = PostgresHook('data_db')
    #ahora = datetime.datetime.today().strftime('%Y-%m-%d') 
    cmd = f"DELETE FROM public.espacios_culturales WHERE creado  = '{fecha_a_procesar_str}'"
    log.info(cmd)
    cmd2 = f"DELETE FROM public.cines WHERE creado = '{fecha_a_procesar_str}'"
    cmd3 = f"DELETE FROM public.indicadores WHERE creado = '{fecha_a_procesar_str}'"
    hook.run(cmd)
    hook.run(cmd2)
    hook.run(cmd3)


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
        df = pd.read_sql_table(table_name=f"raw_{categoria}", con=engine)
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

def load_to_db_from_last_files(**context):
    ti = context["task_instance"]    
    categorias_data = ti.xcom_pull(task_ids='get_last_data', key='db_fechas') 
    
    hook = PostgresHook('data_db')       
    pg_uri = hook.get_uri()
    engine = create_engine(pg_uri)


    fecha_a_procesar_str = define_fecha(**context)

    for categoria in categorias_data:
        
        if categoria['categoria'] in ('bibliotecas','museos'):
            if categoria['categoria'] == 'museos':
                dict_cast = {'cod_area': 'object'}                        
                columnas_reemplazo = {"cod_loc":'cod_localidad',
                                                "idprovincia":'id_provincia',
                                                "iddepartamento":'id_departamento',
                                                "direccion":'domicilio'
                                                }
            elif categoria['categoria'] == 'bibliotecas':
                dict_cast = {'cod_tel': 'object', 'telefono':'object'}                        
                columnas_reemplazo = {"cod_tel":'cod_area'}
            else:
                pass
        else:
            columnas_seleccionadas_cine = ["cod_localidad","id_provincia","id_departamento","categoria","provincia","localidad","latitud","longitud","nombre","direccion","cp","web","fuente","sector","pantallas","butacas","espacio_incaa"]                    
            columnas_reemplazo = {"direccion":'domicilio'}
                
        columnas_seleccionadas = ["cod_localidad","id_provincia","id_departamento","categoria","provincia","localidad","latitud","longitud","nombre","domicilio","cp","telefono","mail","web","creado"]
        
        
        query = f"SELECT * FROM public.raw_{categoria['categoria']} WHERE creado = '{categoria['fecha']}'"
        
        #df = pd.read_sql_query(sql=query, con=engine,dtype=dict_cast)
        df = pd.read_sql_query(sql=query, con=engine)
        df = df.rename(columns= columnas_reemplazo)

        log.info(categoria['categoria'])
        log.info(df.columns)



        if categoria['categoria'] == 'cines':
            df['pantallas'] = df['pantallas'].astype(int)
            df['butacas'] = df['butacas'].astype(int)            
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
    
    purge_data = PythonOperator(task_id='purge_last_data_of_the_day',
                                python_callable=purge_last_data_of_the_day)

    load_categorias_to_db =  PythonOperator(task_id="load_categorias_to_db",
                                            python_callable=load_to_db_from_last_files)
    

    fin = DummyOperator(task_id='fin')

    start >> get_last_data >> purge_data >> load_categorias_to_db >> fin