#   解决SQL dialect 问题
#   mysql, postgres 统一接口
###########################
import sys,os
sys.path.insert(0, os.getcwd().lower())
from dbaccess.mysql.ops_m import DBmops
from dbaccess.postgres.ops_p import DBpops

class DBops:
    def __init__(self, dbServer):
        self.dbServer = dbServer
        if dbServer.get('db_type') == 'mysql':
            self.ops = DBmops(dbServer)
        elif dbServer.get('db_type') == 'postgres':
            self.ops = DBpops(dbServer)
        else:
            raise ValueError("Only support mysql or postgres database")
        
        self.db_type = self.ops.db_type
        self.cnx = self.ops.cnx
        self.cursor = self.ops.cursor

    def __del__(self):
        try:
            if self.cursor is not None:
                self.cursor.close()
                self.cursor = None
            if self.cnx is not None:
                self.cnx.close()
                self.cnx = None
        except Exception as e:
            print(f"Failed to close db connection: {e}")

    def create_database(self, db_name):
        self.ops.create_database(db_name)
    
    def show_current_database(self):
        return self.ops.show_current_database()
    
    def connect_database(self, db_name):
        return self.ops.connect_database(db_name)
    
    def show_databases(self):
        return self.ops.show_databases()
    
    def show_tables(self):
        return self.ops.show_tables()
    
    def is_table_exist(self, table_name):
        return self.ops.is_table_exist(table_name)
    
    def drop_table(self, table_name):
        return self.ops.drop_table(table_name)
    
    def show_columns(self, table_name):
        return self.ops.show_columns(table_name)
    
    def count_items(self, table_name):
        return self.ops.count_items(table_name)
    
    # 包装格式不一样的SQL statement, 不执行
    def get_insert_query(self, tb_name, headers):
        return self.ops.get_insert_query(tb_name, headers)
    
    def get_create_table_query(self, tb_name, df, dtypes=None, pk=None):
        typeList = []
        for c_name in df.columns:
            if dtypes is not None and c_name in dtypes:
                col_type = dtypes[c_name]
            else:   
                tcol = df[c_name]
                col_type = tcol.dtype
            db_type = self.ops.infer_dtype(col_type)            
            typeList.append(f'"{c_name}" {db_type},')
        typeList[-1] = typeList[-1][:-1]  # remove the last comma

        primary_clause = f', PRIMARY KEY ("{pk}")' if pk else ''
        sql = "CREATE TABLE {tb_name}(\n{cols}{primary_clause}\n)"
        cols = '\n'.join(typeList)
        sql = sql.format(tb_name=tb_name, cols=cols, primary_clause=primary_clause) + ';'

        if self.db_type == 'mysql':
            sql = sql.replace('"', '`')
        return sql
        
    def get_tb_schema(self, tb_name):
        return self.ops.get_tb_schema(tb_name)
    
    def refine_data(self,sxm_df, data):
        return self.ops.refine_data(sxm_df, data)
    
    def get_random_rows(self, table_name, n=5):
        return self.ops.get_random_rows(table_name, n)
    
if __name__ == '__main__':
    from settings import z_config
    from zebura_core.placeholder import make_dbServer
 
    server_name = z_config['Training','server_name']
    dbserver = make_dbServer(server_name)
    db_name = z_config['Training','db_name']
    dbserver['db_name'] = db_name
    dbops = DBops(dbserver)
    print(dbops.show_databases())
    print(dbops.show_current_database())
    print(dbops.show_tables())