# 执行SQL语句
import sys,os
sys.path.insert(0, os.getcwd().lower())
from settings import z_config
from zebura_core.utils.conndb import connect,get_engine
import logging
import pandas as pd
from zebura_core.placeholder import make_a_log
from placeholder import make_dbServer
from sqlalchemy import create_engine

class ExeActivity:
    def __init__(self):

        serverName = z_config['Training', 'server_name']
        self.db_name = z_config['Training', 'db_name']
        dbServer = make_dbServer(serverName)
        self.db_type = dbServer['db_type']
        dbServer['db_name'] = self.db_name
        self.cnx = connect(dbServer)
        if self.cnx is None:
            raise ValueError("Database connection failed")
        self.engine = get_engine(dbServer)
        logging.info(f"ExeActivity init success")

    def checkDB(self,db_name=None) -> str:  # failed, succ

        if db_name is None:
            db_name = self.db_name
        
        if self.db_type == "mysql":
            sql_query = f"SHOW DATABASES LIKE '{db_name}'"
        elif self.db_type == "postgres":
            sql_query = f"SELECT 1 FROM pg_database WHERE datname = '{db_name}'"
        else:
            print(f"ERR: {self.db_type} not supported")
            return "failed"
        
        cursor = self.cnx.cursor()
        cursor.execute(sql_query)
        result = cursor.fetchone()
        cursor.close()

        if not result:
            print(f"{db_name} not found, create it first")
            return "failed"
        return "succ"
    
    def exeSQL(self, sql):

        answer = make_a_log("exeSQL")
        answer["format"] = "dict"
        try:
            cursor = self.cnx.cursor()
            cursor.execute(sql)
            result = cursor.fetchall()
            cursor.close()
            if len(result) > 0:
                answer["msg"] = result
            else:
                answer['status'] = "failed"
                answer['msg'] = "err_cursor: no result"
        except Exception as e:
            print(f"Error: {e}")
            answer["msg"] = f"err_cursor, {e}"
            answer["status"] = "failed"

        return answer
    
    # database 与 dataframe直接关联
    def sql2df(self, sql):
        try:
            with self.engine.connect() as conn:
                df = pd.read_sql_query(
                    sql=sql,
                    con=conn.connection
                )
        except Exception as e:
            print(f"Error: {e}")
            df = pd.DataFrame()
        return df
    
if __name__ == "__main__":
    
    exr = ExeActivity()
    exr.checkDB()
    sql = """
SELECT title AS "Movie Title",
       revenuemillions AS "Revenue (Millions)",
       year AS "Release Year",
       genre AS "Genre",
       description AS "Movie Description",
       director AS "Director",
       actors AS "Main Actors"
FROM imdb_movie_dataset
WHERE revenuemillions = (SELECT MAX(revenuemillions) FROM imdb_movie_dataset);
"""
    
    results = exr.exeSQL(sql)
    df = exr.sql2df(sql)
    if not df.empty:
        print(df.head())

