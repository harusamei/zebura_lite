# 对mysql的一些操作进行了封装，包括创建数据库，将DF表保存入RDB, drop 原有表等操作
import sys,os
sys.path.insert(0, os.getcwd().lower())
from discard.conndb import connect
import pandas as pd

class DBmops:
    def __init__(self, dbServer):
        self.dbServer = dbServer
        if dbServer.get('db_type') != 'mysql':
            raise ValueError("Only support mysql database")
        self.db_type = 'mysql'
        self.cnx = connect(dbServer)
        if self.cnx is None:
            raise ValueError("Failed to connect mysql database")
        self.cursor = self.cnx.cursor()

    def __del__(self):
        print("Closing mysql connection")
        try:
            if self.cursor is not None:
                self.cursor.close()
            if self.cnx is not None:
                self.cnx.close()
        except Exception as e:
            print(f"Failed to close mysql connection: {e}")

    # create db if not exists
    def create_database(self, db_name): 
        cursor = self.cursor
        cursor.execute(f'show databases like "{db_name}"')
        result = cursor.fetchone()
        if result is None:
            print(f"Creating database {db_name}")
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
        else:
            print(f"Database {db_name} exists")
        self.cnx.commit()
        return

    def show_current_database(self): 
        cursor = self.cursor
        cursor.execute("SELECT DATABASE();")
        db_name = cursor.fetchone()[0]
        return db_name
    
    # change current database
    def connect_database(self, db_name):
        cursor = self.cursor
        cursor.execute(f"USE {db_name};")
        return self.cnx

    # 读数据库名
    def show_databases(self) ->list:
        cursor = self.cursor
        cursor.execute("SHOW DATABASES")
        databases = cursor.fetchall()
        return databases

    # 读表名
    def show_tables(self) ->list:
        cursor = self.cursor
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        return tables

    def is_table_exist(self, table_name):
        cursor = self.cursor
        cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        result = cursor.fetchone()
        if result is None:
            return False
        return True
    
    def drop_table(self, table_name):
        cursor = self.cursor
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        self.cnx.commit()
        return
    
    # 读列名
    def show_columns(self, table_name) ->list:
        cursor = self.cursor
        query = f"SHOW COLUMNS FROM {table_name}"
        cursor.execute(query)
        result = cursor.fetchall()
        columns = [col[0] for col in result]
        return columns
    
    def count_items(self, table_name):
        cursor = self.cursor
        query = f"SELECT COUNT(*) FROM {table_name}"
        cursor.execute(query)
        count = cursor.fetchone()[0]
        return count
    
    @staticmethod
    def get_insert_query(tb_name, df_headers):
        df_headers = [f'`{h}`' for h in df_headers]  # 使用反引号，防止列名与关键字冲突
        fields = ', '.join(df_headers)
        vals ='%s, '*len(df_headers)
        vals = vals[:-2]
        query = f"INSERT IGNORE INTO {tb_name} ({fields}) VALUES ({vals})"
        return query
    
    @staticmethod
    def infer_dtype(pandas_dtype):
        if pd.api.types.is_integer_dtype(pandas_dtype):
            return "INT"
        elif pd.api.types.is_float_dtype(pandas_dtype):
            return "FLOAT"
        elif pd.api.types.is_bool_dtype(pandas_dtype):
            return "TINYINT(1)"
        elif pd.api.types.is_datetime64_any_dtype(pandas_dtype):
            return "DATETIME"
        elif pandas_dtype == 'date':
            return "DATETIME"
        else:
            return "TEXT"
        
    def show_tb_schema(self,table_name):
        curr_db = self.show_current_database()
        query = f"""
            SELECT *
            FROM INFORMATION_SCHEMA.COLUMNS
             WHERE TABLE_SCHEMA = '{curr_db}' AND TABLE_NAME = '{table_name}'
            """            

        cursor = self.cursor
        cursor.execute(query)
        result = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]  # 获取列名
        df = pd.DataFrame(result,columns=columns)
        # 与postgres 列名小写保持一致
        df.rename(columns=str.lower, inplace=True)
        output = df[['column_name', 'data_type']].to_markdown(index=False)
        print(output)
        return df
    
    def refine_data(self,sxm_df, data):
        tdict = {}
        for k, v in data.items():
            tdf = sxm_df[sxm_df['column_name'] == k]['data_type']
            tdf = tdf.str.lower()
            if tdf.empty:
                return None
            ty = tdf.iloc[0].lower()
            if pd.isna(v) or v == '' or v == 'N/A':
                tdict[k] = None
            elif ty in ['smallint', 'mediumint', 'int', 'integer', 'bigint']:
                tdict[k] = int(v)
            elif ty in ['float', 'double', 'decimal', 'numeric']:
                tdict[k] = float(v)
            elif ty == 'tinyint':
                tdict[k] = bool(v)
            elif 'datetime' in ty:
                tdict[k] = pd.to_datetime(v)
            else:
                tdict[k] = v
        return tdict

    def show_randow_rows(self, table_name, n=5):
        cursor = self.cursor
        query = f"SELECT * FROM {table_name} ORDER BY RAND() LIMIT {n}"
        cursor.execute(query)
        result = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(result,columns=columns)
        return df
    
