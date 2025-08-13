################################
# 将指定目录下所有CSV文件load到db_server中, 一个CSV文件对应一个表,目录名为数据库名
# 不支持表更新，每次导入之前同名表被删除
################################
import pandas as pd
import sys,os
sys.path.insert(0, os.getcwd())
from dbaccess.db_ops1 import DBops
from dbaccess.optimize_csv import optz_data
# load csv file to db_server
class CSV2SQL:
    # db_name: CSV数据被load到这个数据库中，一个CSV对应一个table
    # server: 用户数据库的配置信息，如Postgres1, Mysql1
    def __init__(self, dbServer, db_name):
        
        self.dbServer = dbServer
        self.ops = DBops(dbServer)
        db_name = db_name.lower()
        self.ops.create_database(db_name)
        if self.ops.use_database(db_name) is False:
            raise ValueError(f"can not create/use database '{db_name}'")
        
        self.dbServer['db_name'] = db_name
        currDB = self.ops.show_current_database().fetchone()[0]
        print(f"connect {dbServer['db_type']} successful, current database: {currDB}")

        self.df_optz = optz_data()
          
    # 文件所在目录，load该目录下所有CSV文件
    def load_files(self, filePath):
        
        csv_files = [f for f in os.listdir(filePath) if f.endswith('.csv')]
        # 以文件名为表名
        for fileName in csv_files:
            print(f"Loading {fileName}")
            tb_name = fileName[:-4]     # 表名为文件名去掉.csv
            df = pd.read_csv(f"{filePath}/{fileName}",header=0,encoding='utf-8')
            df = self.df_optz.optz_csv(df)
            fields = self.df_optz.get_db_fields(df, self.dbServer['db_type'])
            if fields is None:
                continue
            tb_name = self.create_table(tb_name,fields)
            if tb_name is None:
                continue
            self.saveInDB(tb_name, df, fields)
            print(f"loaded {fileName} to {tb_name}")
    
    # 创建表, drop=True 存在则删除
    # 默认自动识别dataframe的数据类型
    def create_table(self, tb_name,fields, drop=True):
        tb_name = tb_name.lower()
        if drop:    # drop table if exists
            self.ops.drop_table(tb_name)
        if self.ops.is_table_exist(tb_name):
            print(f"Table {tb_name} exists")
            return tb_name
        
        self.ops.create_table(tb_name,fields)
        return tb_name
    
    def saveInDB(self, tb_name, df, fields):
        print(f"Loading {tb_name} to DB, the total rows: {len(df)}")
        #cols_types = {k: v.name for k, v in df.dtypes.to_dict().items()}
        col_Names = fields.keys()
        for i in range(0, len(df), 1000):
            rows = df[i:i+1000]
            tups = self.df_optz.regz_values(rows, fields)
            self.ops.insert_data(tb_name, col_Names, tups)
            print(f"Loaded {i+1000} rows...")
        print(f"Saved {tb_name} to DB")
        

# Example usage
if __name__ == "__main__":
    from zebura_core.placeholder import make_dbServer
    
    s_name = 'Mysql1'
    db_name = 'ebook'
    csv_path = "training/book/raw_data"
    dbServer = make_dbServer(s_name)
    
    csv_path = os.path.join(os.getcwd(), csv_path)
    csv2sql = CSV2SQL(dbServer, db_name)
    csv2sql.load_files(csv_path)
    print("Done")
