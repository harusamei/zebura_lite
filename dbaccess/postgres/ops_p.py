# 对mysql的一些操作进行了封装，包括创建数据库，将DF表保存入RDB, drop 原有表等操作
import sys,os
sys.path.insert(0, os.getcwd().lower())
from zebura_core.utils.conndb import connect
import pandas as pd

# postgresql db operator
class DBpops:
    def __init__(self, dbServer):
        self.dbServer = dbServer
        if dbServer.get('db_type') != 'postgres':
            raise ValueError("Only support postgres database")
        
        self.db_type = 'postgres'
        self.cnx = connect(dbServer)
        self.cursor = self.cnx.cursor()

    def __del__(self):
        self.cursor.close()
        self.cnx.close()

    # create db if not exists
    def create_database(self, db_name):
        cursor = self.cursor
        cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{db_name}'")
        result = cursor.fetchone()
        if result is None:
            print(f"Creating database {db_name}")
            cursor.execute(f"CREATE DATABASE {db_name}")
        else:
            print(f"Database {db_name} exists")
        return

    def show_current_database(self): 
        cursor = self.cursor
        cursor.execute("SELECT current_database()")
        db_name = cursor.fetchone()[0]
        return db_name
    
    # change current database
    def connect_database(self, db_name):
        self.cnx.close()
        self.dbServer['db_name'] = db_name
        self.cnx = connect(self.dbServer)
        self.cursor = self.cnx.cursor()
        return self.cnx

    # 读数据库名
    def show_databases(self) ->list:
        cursor = self.cursor
        cursor.execute("SELECT datname FROM pg_database")
        databases = cursor.fetchall()
        return databases

    # 读表名
    def show_tables(self) ->list:
        cursor = self.cursor
        query = "SELECT tablename FROM pg_tables WHERE schemaname = 'public';"
        cursor.execute(query)
        tables = cursor.fetchall()
        return tables

    def is_table_exist(self, table_name):
        cursor = self.cursor
        query = f"SELECT to_regclass('{table_name}')"
        cursor.execute(query)
        result = cursor.fetchone()
        if result is not None and result[0] is not None:
            return True
        else:
            return False

    def drop_table(self, table_name):
        cursor = self.cursor
        query = f"DROP TABLE IF EXISTS {table_name}"
        cursor.execute(query)
        self.cnx.commit()
        return
      
    # 读列名
    def show_columns(self, table_name) ->list:
        cursor = self.cursor
        query = f"""
                    SELECT column_name
                    FROM 
                        information_schema.columns
                    WHERE 
                        table_schema = 'public' 
                        AND table_name = '{table_name}';
                """
        cursor.execute(query)
        result = cursor.fetchall()
        columns = [desc[0] for desc in result]  # 获取列名
        return columns
    
    def count_items(self, table_name):
        cursor = self.cursor
        query = f"""SELECT COUNT(*) FROM {table_name}"""
        cursor.execute(query)
        count = cursor.fetchone()[0]
        return count
    
    @staticmethod
    def get_insert_query(tb_name, df_headers):
        # 为了防止列名与关键字冲突，列名使用双引号
        df_headers = [f'"{h}"' for h in df_headers]
        fields = ', '.join(df_headers)
        vals ='%s, '*len(df_headers)
        vals = vals[:-2]
        return f"INSERT INTO {tb_name} ({fields}) VALUES ({vals}) ON CONFLICT DO NOTHING"
    
    @staticmethod
    def infer_dtype(pandas_dtype):
        if pd.api.types.is_integer_dtype(pandas_dtype):
            return "integer"
        elif pd.api.types.is_float_dtype(pandas_dtype):
            return "float"
        elif pd.api.types.is_bool_dtype(pandas_dtype):
            return "boolean"
        elif pd.api.types.is_datetime64_any_dtype(pandas_dtype):
            return "timestamp"
        elif pandas_dtype == 'date':
            return "timestamp"
        else:
            return "character varying"
         
    def get_tb_schema(self,tb_name):
        query = f"""
            SELECT *
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'public' AND TABLE_NAME = '{tb_name}'
            """    
        cursor = self.cnx.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]  # 获取列名
        df = pd.DataFrame(result,columns=columns)
        # mysql 类型为大写，此处为小写
        output = df[['column_name', 'data_type']].to_markdown(index=False)
        print(output)
        return df
    
    def refine_data(self,sxm_df, data):
        tdict = {}
        for k, v in data.items():
            tdf = sxm_df[sxm_df['column_name'] == k]['data_type']
            if tdf.empty:
                return None
            ty = tdf.iloc[0].lower()
            if pd.isna(v) or v == '' or v == 'N/A':
                tdict[k] = None
            elif ty == 'integer':
                tdict[k] = int(v)
            elif ty == 'float':
                tdict[k] = float(v)
            elif 'double' in ty:
                tdict[k] = float(v)
            elif ty == 'boolean':
                tdict[k] = bool(v)
            elif 'timestamp' in ty:
                tdict[k] = pd.to_datetime(v)
            else:
                tdict[k] = v
        return tdict
    
    def get_random_rows(self, table_name, n=5):
        cursor = self.cursor
        query = f"SELECT * FROM {table_name} ORDER BY RANDOM() LIMIT {n}"
        cursor.execute(query)
        result = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(result,columns=columns)
        return df
    
