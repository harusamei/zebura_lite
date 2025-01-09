import sys,os
sys.path.insert(0, os.getcwd().lower())
import pymysql
from pymysql.cursors import Cursor, DictCursor
import psycopg2
from sqlalchemy import create_engine

# sqlalchemy 提供，用于创建一个引擎对象，提供一致的接口
def get_engine(dbServer):

    db_type = dbServer.get('db_type')
    db_name = dbServer.get('db_name')
    if db_type is None or db_name is None:
        print(f"ERR: db_type|db_name cannot be None")
        return None
    tmpl = "{ttype}://{username}:{pwd}@{host}:{port}/{db_name}"
    host = dbServer['host']
    port = int(dbServer['port'])
    username =dbServer['user']
    pwd  = dbServer['pwd']
    if db_type == 'mysql':
        ttype = 'mysql+pymysql'
    elif db_type == 'postgres':
        ttype = 'postgresql+psycopg2'   
    else:
        print(f"ERR: {db_type} not supported")
        return None
    
    db_uri = tmpl.format(ttype=ttype, username=username, pwd=pwd, host=host, port=port, db_name=db_name)
    engine = create_engine(db_uri)
    return engine
    
def connect(dbServer):

    db_type = dbServer.get('db_type')
    if db_type is None:
        print(f"ERR: db_type cannot be None")
        return None
    if db_type == 'mysql':
        dbServer['db_name'] = dbServer.get('db_name','mysql')
        connection = mysql_connect(dbServer)
    elif db_type == 'postgres':
        dbServer['db_name'] = dbServer.get('db_name','postgres')
        connection = Postgres_connect(dbServer)
    else:
        print(f"ERR: {db_type} not supported")
        raise ValueError(f"ERR_cursor: {db_type} not supported")
        
    return connection

def mysql_connect(dbServer):

    if dbServer is None or dbServer.get('db_name') is None:
        print(f"ERR: dbServer or db_name cannot be None")
        raise ValueError(f"ERR_cursor: dbServer or db_name cannot be None")
    
    db_params = {
        'host': dbServer['host'],
        'port': dbServer['port'],
        'user': dbServer['user'],
        'passwd': dbServer['pwd'],
        'charset': dbServer.get('charset','utf8mb4') # 设置字符编码
    }
    if dbServer.get('cursorclass') is not None:
        db_params['cursorclass'] = dbServer['cursorclass']
    db_name = dbServer['db_name']
    try:
        cnx = pymysql.connect(**db_params)
        cur = cnx.cursor()
        cur.execute("SHOW DATABASES")
        result = cur.fetchall()
        if result is not None:
            print("Connection to MySQL DB successful")
            cur.execute(f"USE {db_name}")
        else:
            raise ValueError("No database exists")
    except pymysql.MySQLError as e:
        print(e)
        sys.exit()

    return cnx

def Postgres_connect(dbServer):
    if dbServer is None:
        print(f"ERR: dbServer cannot be None")
        raise ValueError(f"ERR_cursor: dbServer cannot be None")
    
    db_params = {
        'dbname': dbServer['db_name']
    }
    db_params['host'] = dbServer['host']
    db_params['port'] = dbServer['port']
    db_params['user'] = dbServer['user']
    db_params['password']  = dbServer['pwd']
        
    try:
        cnx = psycopg2.connect(**db_params)
        cnx.autocommit = True
        cursor = cnx.cursor()
        # show all databases
        cursor.execute("SELECT datname FROM pg_database WHERE datistemplate = false;")
        result = cursor.fetchone()
        if result is not None:
            print("Connection to Postgres DB successful")
        else:
            raise ValueError("No database was found")
    except pymysql.MySQLError as e:
        print(e)
        sys.exit()

    return cnx

# Example usage:
if __name__ == "__main__":
    from zebura_core.placeholder import make_dbServer
    dbServer = make_dbServer('Postgres1')
    dbServer['db_name'] = 'lecomm'
    
    connection = connect(dbServer)
    cur = connection.cursor()
    if dbServer['db_type'] == 'mysql':
        cur.execute("SELECT DATABASE()")
    elif dbServer['db_type'] == 'postgres':
        cur.execute("SELECT current_database();")
    result = cur.fetchone()
    print(f"Current database: {result[0]}")
    get_engine(dbServer)
    cur.close()