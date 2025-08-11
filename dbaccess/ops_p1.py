# 对mysql的一些操作进行了封装，包括创建数据库，将DF表保存入RDB, drop 原有表等操作
import sys,os
sys.path.insert(0, os.getcwd().lower())
from sqlalchemy import text
from zebura_core.utils.conndb1 import connect, db_execute

# create db if not exists
def create_database(engine, db_name):
    # 是否存在数据库
    query1 = f"SELECT 1 FROM pg_database WHERE datname ILIKE '{db_name}'"
    # 创建数据库
    query2 = f"CREATE DATABASE {db_name} WITH ENCODING 'UTF8'"
    try:
        with engine.connect() as connection:
            connection.execution_options(isolation_level="AUTOCOMMIT")
            result = connection.execute(text(query1))
            if result.rowcount == 0:
                connection.execute(text(query2))
            else:
                print(f"Database {db_name} exists")
    except Exception as e:
        print(f"Error: {e}")
        return None
    return True

# 需要创建新的数据库连接
def use_database(engine, dbServer, db_name):
    query1 = f"SELECT 1 FROM pg_database WHERE datname ILIKE '{db_name}'"
    try:
        with engine.connect() as connection:
            connection.execution_options(isolation_level="AUTOCOMMIT")
            result = connection.execute(text(query1))
            if result.rowcount == 0:
                print(f"Database {db_name} does not exist, create it first")
                return None
        
        engine.dispose()
        dbServer['db_name'] = db_name
        new_engine = connect(dbServer)
        return new_engine
    except Exception as e:
        print(f"Error: {e}")
        return None

def create_table(engine, tb_name, colsInfo, primary_clause):
    query = "CREATE TABLE {tb_name}(\n{colsInfo}{primary_clause}\n)"
    query = query.format(tb_name=tb_name, colsInfo=colsInfo, primary_clause=primary_clause)
    try:
        with engine.connect() as connection:
            connection.execute(query)
    except Exception as e:
        print(f"Error: {e}")
        return False
    return True

# rows, tuple list [(row),(),...]
def insert_data(engine,tb_name, col_names, tuples):

    header = [f'"{h}"' for h in col_names]  # 使用双引号，防止列名与关键字冲突
    fields = ', '.join(header)        
    ph_vals ='%s, '*len(header)             # placeholder for values
    ph_vals = ph_vals[:-2]
    query = f"INSERT INTO {tb_name}({fields}) VALUES ({ph_vals})"
    try:
        with engine.connect() as connection:
            connection.execute(query, tuples)
    except Exception as e:
        print(f"Error: {e}")
        print(f"INFO: {query}")
        return False
    return True

def drop_table(engine, table_name):
    sql_query = f"DROP TABLE IF EXISTS {table_name}"
    try:
        with engine.connect() as connection:
            connection.execute(sql_query)
    except Exception as e:
        print(f"Error: {e}")
        return None
    return True

def show_tb_schema(engine, tb_name):
    query = f"SELECT column_name, data_type, character_maximum_length, is_nullable, column_default FROM information_schema.columns WHERE table_name = '{tb_name}'"
    try:
        result = db_execute(engine, query)
        tb_scma = result.mappings().all()
        return tb_scma
    except Exception as e:
        print(f"Error: {e}")
        print(f"INFO: {query}")
        return None
    
# 列出主键列
def show_primary_key(engine, tb_name):
    query = f"""
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_name = kcu.table_name
            WHERE tc.constraint_type = 'PRIMARY KEY'
                AND tc.table_name = '{tb_name}';
    """
    try:
        return db_execute(engine, query)
    except Exception as e:
        print(f"Error: {e}")
        print(f"INFO: {query}")
        return None
    

