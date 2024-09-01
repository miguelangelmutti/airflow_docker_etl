import psycopg2
from decouple import config 
from sqlalchemy import create_engine

def load_connection_info():
            # Create a dictionary of the variables stored under the "postgresql" section of the .ini
            conn_info = {'host': config('HOST'), 
                            'port': config('PORT'), 
                            'database': config('DATABASE'),
                            'usuario': config('USUARIO'), 
                            'password': config('PASSWORD'),
                            'db_url': config('DATABASE_URL')}
            return conn_info

def create_db(conn_info):    
    psql_connection_string = f"host={conn_info['host']} port={conn_info['port']} user={conn_info['usuario']} password={conn_info['password']}"
    conn = psycopg2.connect(psql_connection_string)
    cur = conn.cursor()    
    conn.autocommit = True
    sql_query = f"CREATE DATABASE {conn_info['database']}"

    try:
        cur.execute(sql_query)
    except Exception as e:
        print(f"{type(e).__name__}: {e}")
        print(f"Query: {cur.query}")            
    finally:            
        conn.autocommit = False            
        conn.close()
        cur.close()            

def drop_db(conn_info):    
    psql_connection_string = f"host={conn_info['host']} port={conn_info['port']} user={conn_info['usuario']} password={conn_info['password']}"
    conn = psycopg2.connect(psql_connection_string)
    cur = conn.cursor()    
    conn.autocommit = True
    sql_query = f"DROP DATABASE IF EXISTS {conn_info['database']}"

    try:
        cur.execute(sql_query)
    except Exception as e:
        print(f"{type(e).__name__}: {e}")
        print(f"Query: {cur.query}")            
    finally:            
        conn.autocommit = False            
        conn.close()
        cur.close()


def execute_sql(conn_info, sql_query) :
    psql_connection_string = f"host={conn_info['host']} port={conn_info['port']} dbname={conn_info['database']} user={conn_info['usuario']} password={conn_info['password']}"        
    conn = psycopg2.connect(psql_connection_string)
    cur = conn.cursor()    
    try:        
        cur.execute(sql_query)
    except Exception as e:
        print(f"{type(e).__name__}: {e}")
        print(f"Query: {cur.query}")
        conn.rollback()
        cur.close()
    finally:        
        conn.commit()
        cur.close()
        conn.close()

def read_sql(archivo_sql):
    try:
        with open(archivo_sql, "r") as f:
            sql = f.read()
            return sql
    except Exception as e:
        print(f"Error al leer el archivo SQL: {e}")
        exit()

def get_db_engine(conn_info):    
    engine = create_engine(conn_info['db_url'])
    return engine

if __name__ == '__main__':
    pass