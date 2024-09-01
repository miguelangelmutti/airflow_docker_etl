import requests
import os 
import datetime
import db
import pandas as pd
from cfg import categorias
from constants import BASE_FILE_DIR

def get_fechas_string(tipo):
    
    meses_ingles_espanol = {
    "January": "Enero",
    "February": "Febrero",
    "March": "Marzo",
    "April": "Abril",
    "May": "Mayo",
    "June": "Junio",
    "July": "Julio",
    "August": "Agosto",
    "September": "Septiembre",
    "October": "Octubre",
    "November": "Noviembre",
    "December": "Diciembre"
    }

    x = datetime.datetime.now()
    if tipo == 1:
        periodo_ingles = x.strftime("%B-%Y")
        periodo = periodo_ingles.replace(x.strftime("%B"),meses_ingles_espanol[x.strftime("%B")])
    elif tipo == 2:
        periodo = x.strftime("%d-%m-%Y")
    else: 
        pass 
    return periodo


def crear_carpetas(categorias):
    for categoria in categorias:
        #if not os.path.exists(path):
        periodo = get_fechas_string(1)
        new_path = f"{categoria['name']}/{periodo}"
        if not os.path.exists(new_path):
            os.makedirs(new_path)
    

def descargar_archivo(categoria):
    respuesta = requests.get(categoria["url"])
    nombre = categoria["name"]
    respuesta_diccionario = {"nombre" : nombre, "respuesta": respuesta}
    return respuesta_diccionario 

def guardar_archivo(respuesta_diccionario, categoria):
    respuesta = respuesta_diccionario["respuesta"]
    nombre = categoria + "-" + get_fechas_string(2)
    periodo = get_fechas_string(1)

    ruta_al_archivo = BASE_FILE_DIR / f"{categoria}/{periodo}/{nombre}.csv"
    ruta_al_archivo.parent.mkdir(parents=True, exist_ok=True)

    with open(ruta_al_archivo, "wb") as archivo:
        archivo.write(respuesta.content)
    #print(f"Descarga completada: {respuesta.status_code}")
    print('archivo guardado en '+ str(ruta_al_archivo))
    return {'categoria': categoria, 'ruta': ruta_al_archivo}


def get_last_files_path():
    rootdirs = ['./bibliotecas', './cines', './museos']
    data = []
    files_path = []

    for rootdir in rootdirs:
        for subdir, dirs, files in os.walk(rootdir):
            for file in files:
                #print(os.path.join(subdir, file))
                fecha = os.path.getmtime(os.path.join(subdir, file))
                dt = datetime.datetime.fromtimestamp(fecha)
                data.append({'categoria':rootdir.replace('./',''), 'archivo':file,'ruta':os.path.join(subdir, file), 'fecha_modif': dt})

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

    return files_path



if __name__ == '__main__':
            
            #crear_carpetas(categorias)
            
            #for categoria in categorias:                                
            #    respuesta = descargar_archivo(categoria)
            #    guardar_archivo(respuesta, categoria["name"])
            
            

            #TODO Logeo de todo                        
            
            
            conn_info = db.load_connection_info()
            db.drop_db(conn_info)
            db.create_db(conn_info)
            
            #drop table
            drop_table = db.read_sql('./sql/drop_tables.sql')
            db.execute_sql(conn_info,drop_table)

            #create table espacios_culturales
            create_tables = db.read_sql('./sql/create_tables.sql')
            db.execute_sql(conn_info, create_tables)
            
            engine = db.get_db_engine(conn_info)

            list_ruta_archivos = []
            for categoria in categorias:                                
                respuesta = descargar_archivo(categoria)
                datapath = guardar_archivo(respuesta, categoria["name"])
                list_ruta_archivos.append(datapath)

            #dict_ruta_archivos = get_last_files_path()

            for ruta_archivo in list_ruta_archivos:
                if ruta_archivo['categoria'] in ('bibliotecas','museos'):
                    if ruta_archivo['categoria'] == 'museos':
                        dict_cast = {'cod_area': 'object'}                        
                        columnas_reemplazo = {"Cod_Loc":'cod_localidad',
                                                        "IdProvincia":'id_provincia',
                                                        "IdDepartamento":'id_departamento',
                                                        "direccion":'domicilio',
                                                        "CP":'cp',
                                                        "Mail":'mail',
                                                        "Web":'web'
                                                        }
                    elif ruta_archivo['categoria'] == 'bibliotecas':
                        dict_cast = {'cod_tel': 'object', 'telefono':'object'}                        
                        columnas_reemplazo = {"cod_tel":'cod_area'}
                    else:
                        pass
                else:
                    columnas_seleccionadas_cine = ["cod_localidad","id_provincia","id_departamento","categoria","provincia","localidad","nombre","direccion","cp","web","fuente","sector","pantallas","butacas","espacio_incaa"]                    
                    columnas_reemplazo = {"direccion":'domicilio'}

                columnas_seleccionadas = ["cod_localidad","id_provincia","id_departamento","categoria","provincia","localidad","nombre","domicilio","cp","telefono","mail","web"]
                df = pd.read_csv(ruta_archivo['ruta'],dtype=dict_cast)                
                df = df.rename(columns= columnas_reemplazo)
                if ruta_archivo['categoria'] == 'cines':
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



