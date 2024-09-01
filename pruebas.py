import os
import datetime
import pandas as pd
import db 

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
  columnas_reemplazo = {"direccion":'domicilio'}

  df = pd.read_csv('./cines/Abril-2024/cines-09-04-2024.csv')                
  df = df.rename(columns= columnas_reemplazo)
  df['telefono'] = None
  df['mail'] = None
  s_pantallas = df.groupby('provincia')['pantallas'].sum()
  s_butacas = df.groupby('provincia')['butacas'].sum()
  s_espacios_incaa = df.groupby('provincia')['espacio_incaa'].value_counts().unstack(fill_value=0)['Si']
  #print(s)
  df_cines = pd.DataFrame({'provincia': s_pantallas.index.tolist(),
                           'Cant_pantallas':s_pantallas,
                           'Cant_butacas':s_butacas,
                           'Cant_espacios_INCAA':s_espacios_incaa}).reset_index(drop=True)
  
  