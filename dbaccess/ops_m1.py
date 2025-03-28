# 对mysql的一些操作进行了封装，包括创建数据库，将DF表保存入RDB, drop 原有表等操作
import sys,os
sys.path.insert(0, os.getcwd().lower())
import pandas as pd
from sqlalchemy import text
from zebura_core.utils.conndb1 import connect

def create_database(engine, db_name):
    sql_query = f"CREATE DATABASE IF NOT EXISTS {db_name}"
    try:
        with engine.connect() as connection:
            connection.execution_options(isolation_level="AUTOCOMMIT")
            connection.execute(sql_query)
    except Exception as e:
        print(f"Error: {e}")
        return None
    return True

def drop_table(engine, table_name):
    sql_query = f"DROP TABLE IF EXISTS {table_name}"
    try:
        with engine.connect() as connection:
            connection.execute(sql_query)
    except Exception as e:
        print(f"Error: {e}")
        return False
    return True

def use_database(engine, dbServer, db_name):
    db_name = db_name.lower()
    query1 = f"SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '{db_name}'"

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
    query = query.replace('"', '`')
    try:
        with engine.connect() as connection:
            connection.execute(query)
    except Exception as e:
        print(f"Error: {e}")
        return False
    return True


def insert_data(engine,tb_name, col_names, rows):
    
    header = [f'`{h}`' for h in col_names]  # 使用反引号，防止列名与关键字冲突
    fields = ', '.join(header)        
    ph_vals ='%s, '*len(header)             # placeholder for values
    ph_vals = ph_vals[:-2]
    query = f"INSERT INTO {tb_name} ({fields}) VALUES ({ph_vals})"
    try:
        with engine.connect() as connection:
            connection.execute(query, rows)
    except Exception as e:
        print(f"Error: {e}")
        print(f"INFO: {query}")
        return False
    return True

# 列出主键列
# 返回例: [('rank',), ('title',)]
def show_primary_key(engine, tb_name) -> list:
    query = f"""
            SELECT 
                kcu.column_name
            FROM 
                information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            WHERE 
                tc.constraint_type = 'PRIMARY KEY' 
                AND tc.table_name = '{tb_name}'
                AND tc.table_schema = DATABASE();
    """
    try:
        with engine.connect() as connection:
            result = connection.execute(query)
            return result.fetchall()
    except Exception as e:
        print(f"Error: {e}")
        print(f"INFO: {query}")
        return None
    