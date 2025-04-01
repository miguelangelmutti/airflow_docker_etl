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



"""
                                                        ╔═╗╔═╗╔╗╔╔═╗╔═╗
                                                        ║  ║╣ ║║║╚═╗║ ║
                                                        ╚═╝╚═╝╝╚╝╚═╝╚═╝
"""


def descargar_archivo_censo(**context):    
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
    cmd = f"DELETE FROM public.raw_censo"    
    cmd2 = f"DELETE FROM public.censo"
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
    df.to_sql('raw_censo',con=engine, if_exists='append', index=False)

"""
                                        ╔═╗╔═╗╔═╗╔═╗╔═╗╦╔═╗╔═╗  ╔═╗╦ ╦╦ ╔╦╗╦ ╦╦═╗╔═╗╦  ╔═╗╔═╗
                                        ║╣ ╚═╗╠═╝╠═╣║  ║║ ║╚═╗  ║  ║ ║║  ║ ║ ║╠╦╝╠═╣║  ║╣ ╚═╗
                                        ╚═╝╚═╝╩  ╩ ╩╚═╝╩╚═╝╚═╝  ╚═╝╚═╝╩═╝╩ ╚═╝╩╚═╩ ╩╩═╝╚═╝╚═╝
"""



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


def descargar_archivos_culturales(**context):
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


def purge_last_data_of_the_day():
    hook = PostgresHook('data_db')
    #ahora = datetime.datetime.today().strftime('%Y-%m-%d') 
    cmd = f"DELETE FROM public.raw_cines WHERE creado  = '{ahora}'"    
    cmd2 = f"DELETE FROM public.raw_museos WHERE creado = '{ahora}'"
    cmd3 = f"DELETE FROM public.raw_bibliotecas WHERE creado = '{ahora}'"
    cmd4 = f"DELETE FROM public.cines WHERE creado  = '{ahora}'"    
    cmd5 = f"DELETE FROM public.museos WHERE creado = '{ahora}'"
    cmd6 = f"DELETE FROM public.bibliotecas WHERE creado = '{ahora}'"
    
    hook.run(cmd)
    hook.run(cmd2)
    hook.run(cmd3)
    hook.run(cmd4)
    hook.run(cmd5)
    hook.run(cmd6)



def normalizar_localidad(localidad):
    #quito las comillas simples de la localidad, los acentos, algunas abreviaciones y espacios en blanco al principio y al final
    localidad = localidad.replace("'", "''")\
                         .replace("-", " ")\
                         .replace("á", "a")\
                         .replace("é", "e")\
                         .replace("í", "i")\
                         .replace("ó", "o")\
                         .replace("ú", "u")\
                         .replace("Gral.", "General")\
                         .replace("Libertador Gral. San Martin","Libertador General San Martin").replace("Libertador G.San Martin","Libertador General San Martin")\
                         .replace("El Dorado","Eldorado")\
                         .replace("Justo P. Daract","Justo Daract")\
                         .replace("Lanus Este","Lanus")\
                         .replace("Lanus Oeste","Lanus")\
                         .replace("Longchamps oeste","Longchamps")\
                         .replace("Padre A Stefanelli","Padre Alejandro Stefenelli")\
                         .replace("San Clemente del Tuyu","San Clemente")\
                         .replace("Santa Rosa de Conlara","Santa Rosa del Conlara")\
                         .replace("Santiago capital","Santiago del Estero")\
                         .replace("Tres Lomas - Pellegrini","Tres Lomas")\
                         .replace("Vicuña Mackena","Vicuña Mackenna").strip()
    return localidad


def load_raw_data_to_db(**context):
    """
    metodo para cargar la informacion de los archivos descargados en la base de datos
    """

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

        log.info(f"Se han insertado {len(df_to_insert)} filas en la tabla raw_{path['categoria']}")

def crear_catalogos(**context):
    """
    metodo para crear los catalogos de provincias y localidades en base a la informacion de las categorias obtenidas de los archivos cargados en la base de datos
    """

    set_pronvincias = set()

    categorias = eval(Variable.get("categorias"))
    hook = PostgresHook('data_db')    


    for categoria in categorias:
        #verificar si la categoria existe en la tabla categorias
        cmd = f"SELECT 1 FROM public.categorias WHERE descripcion = '{categoria['categoria']}'"
        result = hook.get_records(cmd)
        if not result:
            #inserto en tabla categorias        
            cmd = f"INSERT INTO public.categorias (descripcion) VALUES('{categoria['categoria']}')"
            hook.run(cmd)
            log.info(f"Se ha insertado la categoria {categoria['categoria']} en la tabla categorias")
        else:
            log.info(f"La categoria {categoria['categoria']} ya existe en la tabla categorias")
        
        #Leer provincias de la categoria y verificar si existen en la tabla provincias
        cmd = f"select distinct provincia from raw_{categoria['categoria']}"
        result_provincias = hook.get_records(cmd)
        list_pronvicias = []        
        for provincia in result_provincias:
            if provincia[0] == 'Tierra del Fuego, Antártida e Islas del Atlántico Sur':
                provincia = 'Tierra del Fuego'
            else:
                provincia = provincia[0]
            #quito espacios en blanco al principio y al final
            provincia = provincia.strip()
            #agrego a la lista de provincias
            list_pronvicias.append(provincia)
        set_pronvincias.update(list_pronvicias)
        log.info(f"Se han obtenido {len(set_pronvincias)} provincias de la categoria {categoria['categoria']}")
        for provincia in set_pronvincias:
            cmd = f"SELECT 1 FROM public.provincias WHERE lower(descripcion) = lower('{provincia}')"
            result = hook.get_records(cmd)
            if not result:
                #inserto en tabla provincias        
                cmd = f"INSERT INTO public.provincias (descripcion) VALUES('{provincia}')"
                hook.run(cmd)
                log.info(f"Se ha insertado la provincia {provincia} en la tabla provincias")
            else:
                log.info(f"La provincia {provincia} ya existe en la tabla provincias")
        
        #Leer localidades y provincias de la categoria y verificar si existen en la tabla localidades
        cmd = f"select distinct localidad,provincia from raw_{categoria['categoria']}"
        result_localidades = hook.get_records(cmd)
        log.info(result_localidades)
        for localidad, provincia in result_localidades:
            if localidad and provincia:
                if provincia == 'Tierra del Fuego, Antártida e Islas del Atlántico Sur':
                    provincia = 'Tierra del Fuego'
                #quito espacios en blanco al principio y al final
                provincia = provincia.strip()
                #busco el id de la provincia
                cmd = f"SELECT id FROM public.provincias WHERE lower(descripcion) = lower('{provincia}')"
                result = hook.get_records(cmd)
                if result:
                    id_provincia = result[0][0]
                    #quito las comillas simples de la localidad, los acentos, algunas abreviaciones y espacios en blanco al principio y al final                    
                    localidad = normalizar_localidad(localidad)
                    #verifico si la localidad existe en la tabla localidades                
                    cmd = f"SELECT 1 FROM public.localidades WHERE lower(descripcion) = lower('{localidad}') and id_provincia = {id_provincia}"
                    result = hook.get_records(cmd)
                    if not result:
                        #inserto en tabla localidades        
                        cmd = f"INSERT INTO public.localidades (descripcion,id_provincia) VALUES('{localidad}',{id_provincia})"
                        hook.run(cmd)
                        log.info(f"Se ha insertado la localidad {localidad} en la tabla localidades")
                    else:
                        log.info(f"La localidad {localidad} ya existe en la tabla localidades")
                else:
                    log.info(f"La provincia {provincia} no existe en la tabla provincias")

def normalizar_categorias_censo():
    """
    metodo para normalizar las categorias y censo
    """

    hook = PostgresHook('data_db')    
    pg_uri = hook.get_uri()
    engine = create_engine(pg_uri)   

    categorias = eval(Variable.get("categorias"))
    for categoria in categorias:
        df = pd.read_sql_table(table_name=f"raw_{categoria['categoria']}", con=engine)
        df = df.dropna(subset=['localidad'])
        df['provincia'] = df['provincia'].str.replace("Tierra del Fuego, Antártida e Islas del Atlántico Sur", "Tierra del Fuego")
        df['provincia'] = df['provincia'].str.strip()
        df['localidad'] = df['localidad'].apply(lambda x: normalizar_localidad(x))
        df['localidad'] = df['localidad'].str.strip()
        df['id_provincia'] = df['provincia'].apply(lambda x: hook.get_records(f"SELECT id FROM public.provincias WHERE lower(descripcion) = lower('{x}')")[0][0])
        df['id_localidad'] = df.apply(lambda x: hook.get_records(f"SELECT id FROM public.localidades WHERE lower(descripcion) = lower('{x['localidad']}') and id_provincia = {x['id_provincia']}")[0][0], axis=1)
        df['id_categoria'] = hook.get_records(f"SELECT id FROM public.categorias WHERE lower(descripcion) = lower('{categoria['categoria']}')")[0][0]
        df = df.drop('id_provincia', axis=1)

        if categoria['categoria'] == 'cines':
            df = df.drop(["cod_localidad","id_departamento","categoria","provincia","departamento","localidad"], axis=1)
            columnas_reemplazo = {"direccion":'domicilio'}
            df.rename(columns= columnas_reemplazo)                                 
        elif categoria['categoria'] == 'museos':
            df = df.drop(['cod_loc','idprovincia','iddepartamento','observaciones','categoria','subcategoria','provincia','localidad'], axis=1)             
        elif categoria['categoria'] == 'bibliotecas':
            df = df.drop(["cod_localidad",	"id_departamento",	"observacion",	"categoria","subcategoria",	"provincia","departamento",	"localidad"], axis=1)

        df.to_sql(categoria['categoria'], con=engine, if_exists='append', index=False)
        log.info(f"Se han insertado {len(df)} filas en la tabla {categoria['categoria']}")
    
    df = pd.read_sql_table(table_name="raw_censo", con=engine)
    df['jurisdiccion'] = df['jurisdiccion'].str.replace("Tierra del Fuego, Antártida e Islas del Atlántico Sur", "Tierra del Fuego")
    df['jurisdiccion'] = df['jurisdiccion'].str.replace("Rio Negro", "Río Negro")
    df['jurisdiccion'] = df['jurisdiccion'].str.strip()
    df['id_provincia'] = df['jurisdiccion'].apply(lambda x: hook.get_records(f"SELECT id FROM public.provincias WHERE lower(descripcion) = lower('{x}')")[0][0])
    df = df.drop('jurisdiccion', axis=1)
    df.to_sql('censo', con=engine, if_exists='append', index=False)
    log.info(f"Se han insertado {len(df)} filas en la tabla censo")


with DAG(

    dag_id="ETL_DATA_RAW_ESPACIOS_CULTURALES",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["challenge_data"]    
):
            

    start = DummyOperator(task_id='inicio')

    get_archivos_categorias = PythonOperator(task_id='get_archivos_categorias',
                                             python_callable=descargar_archivos_culturales
                                            )
    
    
    purge_data = PythonOperator(task_id='purge_last_data_of_the_day',
                                python_callable=purge_last_data_of_the_day)

    load_categorias_to_db =  PythonOperator(task_id="load_categorias_to_db",
                                            python_callable=load_raw_data_to_db)
    
    
    get_archivo_censo = PythonOperator(task_id='get_archivo_censo',
                                             python_callable=descargar_archivo_censo
                                            )
    
    purge_data_censo_2022 = PythonOperator(task_id='purge_data_censo_2022',
                                python_callable=purge_data_censo)

    load_censo_to_db =  PythonOperator(task_id="load_censo_to_db",
                                       python_callable=load_data_censo_to_db)

    ddl_catalog_data =  PythonOperator(task_id="ddl_catalog_data",
                                       python_callable=crear_catalogos)

    normalizar_data =  PythonOperator(task_id="normalizar_data",
                                       python_callable=normalizar_categorias_censo)       
    

    fin = DummyOperator(task_id='fin')

    start >> [get_archivos_categorias,get_archivo_censo]
    get_archivos_categorias >>  purge_data >> load_categorias_to_db >> ddl_catalog_data
    get_archivo_censo >> purge_data_censo_2022 >> load_censo_to_db >> ddl_catalog_data
    ddl_catalog_data >> normalizar_data >> fin