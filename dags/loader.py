import logging
import sys
import datetime 
import os
import pandas as pd
from pprint import pprint
import pendulum
import requests
from pathlib import Path
from sqlalchemy import create_engine
from airflow.providers.postgres.hooks.postgres import PostgresHook

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
    
    ahora = datetime.datetime.today().strftime('%Y-%m-%d')  
    cmd = f"DELETE FROM public.espacios_culturales WHERE creado >= '{ahora}'"
    log.info(cmd)
    cmd2 = 'DELETE FROM public.cines'
    hook = PostgresHook('data_db')
    hook.run(cmd)
    hook.run(cmd2)

    pg_uri = hook.get_uri()
    engine = create_engine(pg_uri)

    for path in files_path:
        log.info(path['categoria'] + ' - ' + path['ruta'])
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
                                    'cant_espacios_incaa':s_espacios_incaa}).reset_index(drop=True)
            df_cines.to_sql('cines',con=engine, if_exists='append', index=False)
        else:
            df['telefono'] = df['cod_area'] + '-' + df['telefono']
            df.drop(['cod_area'], axis=1, inplace=True)
        df = df[columnas_seleccionadas]
        df['creado'] = datetime.datetime.now()                
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
    df_indicadores.to_sql('indicadores',con=engine, if_exists='append', index=False)





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