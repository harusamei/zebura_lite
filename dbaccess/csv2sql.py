################################
# 将指定目录下所有CSV文件load到数据库中, 一个CSV文件对应一个表
# 不支持表更新，每次导入之前同名表的数据会在DB中被清空，但表结构不会被删除
################################
import pandas as pd
import sys,os,re
sys.path.insert(0, os.getcwd())
from dbaccess.db_ops import DBops

# load csv file to mysql or postgresql
class CSV2SQL:
    # db_name: CSV数据被load到这个数据库中，一个CSV对应一个table
    def __init__(self, dbServer):
        
        # 数据库名默认为小写
        db_name = dbServer['db_name'].lower()
        dbServer['db_name'] = dbServer['db_type']       #assume 每种数据库默认db 与类型同名， 'postgres'
        self.ops = DBops(dbServer)
        self.ops.create_database(db_name=db_name)
        self.cnx = self.ops.connect_database(db_name)
        currDB = self.ops.show_current_database()
        print(f"connect {dbServer['db_type']} successful, current database: {currDB}")

    # 指定列的数据类型
    # dtypes {"col1": "int64", "col2": "float64", "col3": "str"}
    @staticmethod
    def astype_df(df,dtypes):
        for c_name, c_type in dtypes.items():
            df[c_name] = df[c_name].astype(c_type)
        return df

    # DF 整形，列名不使用空格和特殊字符，虽然列名可以使用空格，但它会让查询变得繁琐
    def regularize_df(self, df):
        df.dropna(axis=0, how='all', inplace=True)      # 删除全为空的行
        for c_name in df.columns:       #列名不使用空格和特殊字符
            name = c_name
            name1 = name.replace('(', ' ')      # 替换括号
            name1 = name1.replace(r'\s+', '_')     # 替换空格
            name1 = re.sub(r'[^\w]', '', name1) # 删除其它非字母数字字符 
            name1 = name1.lower()
            if name1 != name:
                df.rename(columns={name: name1}, inplace=True)
            ctype = df[name1].dtype
            if pd.api.types.is_string_dtype(ctype):
                df[name1].fillna(value='', inplace=True)

    # CSV的列名可能有重复，删除重复列
    def drop_dupCols(self, df):
        dupIndx = df.columns.duplicated()
        if dupIndx.any():
            print(f"Duplicate columns: {df.columns[dupIndx]}")
            df.drop(columns=df.columns[dupIndx], inplace=True)
        return
          
    # 文件所在目录，load该目录下所有CSV文件
    def load_files(self, filePath):
        
        csv_files = [f for f in os.listdir(filePath) if f.endswith('.csv')]
        # 以文件名为表名
        for fileName in csv_files:
            tb_name = fileName[:-4]
            print(f"Loading {fileName} to {tb_name}")
            df = pd.read_csv(f"{filePath}/{fileName}",header=0,encoding='utf-8')
            df = df.convert_dtypes()
            df.dropna(axis=0, how='all', inplace=True)         # 删除全为空的行
            self.drop_dupCols(df)
            self.regularize_df(df)
            print(df.columns.tolist())

            tb_name = self.create_table(df, tb_name)
            if tb_name is None:
                continue
            self.saveInDB(df, tb_name)
    
    # 创建表, drop=True 存在则删除
    # dtypes， 指定列的数据类型； pk，主键
    def create_table(self, df, tb_name, drop=True, dtypes=None, pk=None):
        tb_name = tb_name.lower()
        result = self.ops.is_table_exist(tb_name)
        # 如果表存在，清空表
        if result:
            print(f"Table {tb_name} exists")
            if drop:
                print(f"Drop table {tb_name}")
                self.ops.drop_table(tb_name)
            else:
                print(f"skip it")
                return tb_name
        else:
            print(f"Table {tb_name} does not exist, create it now")
        sql = self.ops.get_create_table_query(tb_name, df, dtypes, pk)
        try: 
            cursor = self.cnx.cursor()
            cursor.execute(sql)
            self.cnx.commit()
        except Exception as e:
            print(f"Error: {e}")
            self.cnx.rollback()
            tb_name = None
        cursor.close()
        return tb_name
    
    def saveInDB(self, df, tb_name):
        print(f"Saving {tb_name} to DB")
        cursor = self.cnx.cursor()
        sxm_df = self.ops.show_tb_schema(tb_name)

        headers = sxm_df['column_name'].tolist()
        query = self.ops.get_insert_query(tb_name, headers)
        # print(query)
        count = 0
        for _, row in df.iterrows():
           
            row = self.ops.refine_data(sxm_df, row)
            if row is None:
                print("Data type mismatch, skip this row")
                print(row)
                continue
            values = tuple(row.values())
            try:
                cursor.execute(query, values)
                self.cnx.commit()
                count += 1
            except Exception as e:
                print(f"Error: {e}")
                self.cnx.rollback()
        print(f"total rows: {len(df)}")
        print(f"Inserted {count} rows")
        cursor.close()

    # data 为 列名:值 的字典  
    # def refine_data(self,sxm_df, data):
    #     tdict = {}
    #     for k, v in data.items():
    #         tdf = sxm_df[sxm_df['column_name'] == k]['data_type']
    #         if tdf.empty:
    #             return None
    #         ty = tdf.iloc[0].lower()
    #         if pd.isna(v) or v == '' or v == 'N/A':
    #             tdict[k] = None
    #         elif ty == 'integer':
    #             cleaned_v = re.sub(r'[^0-9]', '', str(v))  # 替换非数字字符
    #             tdict[k] = int(cleaned_v) if cleaned_v else 0  # 防止空字符串转换
    #         elif 'float' in ty or 'double' in ty:
    #             cleaned_v = re.sub(r'[^0-9.]', '', str(v))  # 保留点号，替换其他非数字字符
    #             tdict[k] = float(cleaned_v) if cleaned_v else 0  # 防止空字符串转换
    #         else:
    #             tdict[k] = str(v)[:254]
    #     return tdict

# Example usage
if __name__ == "__main__":
    # 文件所在目录 ..\zebura_db\training\le_products\raw data
    # python dbaccess/postgres/csv2psql.py --server_name Postgres1 --db_name IMDB --csv_path "training/IMDB/raw data"
    from zebura_core.placeholder import make_dbServer
    
    s_name = 'Postgres1'
    db_name = 'IMDB'
    csv_path = "training/IMDB/raw data"
    dbServer = make_dbServer(s_name)
    dbServer['db_name'] = db_name
    csv_path = os.path.join(os.getcwd(), csv_path)
    csv2sql = CSV2SQL(dbServer)
    csv2sql.load_files(csv_path)
    print("Done")
