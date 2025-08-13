#   解决SQL dialect 问题， 以函数统一接口
#   每支持一种database，增加 ops_x， 以及sql_eqs.json , vtype_eqs.json 对应部分
###########################
import sys,os
sys.path.insert(0, os.getcwd().lower())
from zebura_core.utils.conndb1 import connect, db_execute
import dbaccess.ops_p1 as ops_p
import dbaccess.ops_m1 as ops_m
import logging
import json


class DBops:
    # 不同database类型的sql equivalents 
    sql_eqs = None
    sql_funcs = ['create_database', 'drop_table','show_primary_key','show_tb_schema']
    ModuleMap = {
        'mysql': ops_m,
        'postgres': ops_p
    }

    def __init__(self, dbServer):
        
        self.dbServer = dbServer
        self.db_eng = connect(dbServer)
        self.db_type = dbServer.get('db_type')
        if self.db_eng is None:
            raise ValueError("Database connection failed")
        cwd = os.getcwd()
        if DBops.sql_eqs is None:
            file_path = os.path.join(cwd, 'dbaccess/sql_eqs.json')
            with open(file_path, 'r') as f:
                DBops.sql_eqs = json.load(f)
        
        # 动态添加方法
        for name, dialect in DBops.sql_eqs.items():
            if name.startswith('_') or name.startswith('?'):
                continue
            setattr(self, name, self._create_method(name, dialect))
        for func_name in DBops.sql_funcs:
            setattr(self, func_name, self._create_method1(func_name))

        logging.info("DBops init success")
    
    # 动态创建方法, 方法将执行对应的SQL   
    def _create_method(self, name, dialect):
        def method(*args, **kwargs):
            db_type = self.db_type
            if db_type in dialect:
                query = dialect[db_type]
            else:
                print(f"ERR: {db_type} not supported")
                return None
            if args:
                arg_names = [key.split('}')[0] for key in query.split('{') if '}' in key]
                kwargs.update(dict(zip(arg_names, args)))
            
            f_query = query.format(*args, **kwargs)
            logging.info(f_query)
            try:
                result = db_execute(self.db_eng, f_query)
            except Exception as e:
                print(f"ERR: {e}")
                return None
            return result
        method.__name__ = name  # 设置方法的名称
        return method
    
    # 动态创建方法, 调用不同module库同名方法
    def _create_method1(self, func_name):
        def method(*args, **kwargs):
            db_type = self.db_type
            if db_type not in DBops.ModuleMap:
                print(f"ERR: {db_type} not supported")
                return None
            
            ops_x = DBops.ModuleMap[db_type]
            if func_name in ops_x.__dict__:
                func = ops_x.__dict__[func_name]
                logging.info(f"call {func_name}")
            else:
                print(f"ERR: {func_name} not supported")
                return None
            
            # 参数只基于位置
            try:
                result = func(self.db_eng, *args, **kwargs)
            except Exception as e:
                print(f"ERR: {e}")
                return None
            return result
        method.__name__ = func_name  # 设置方法的名称
        return method
    
    # 一些不好统一生成的方法
    def is_table_exist(self, tb_name):
        # postgres, mysql 结果不一样
        result = self.table_exist(tb_name)
        result = result.fetchone()  # fetchone() 返回一行
        if result is not None:
            if (isinstance(result[0],str) and len(result[0])>0):
                return True
        return False
    
    def choose_opsx(self):
        db_type = self.db_type
        if db_type not in DBops.ModuleMap:
            print(f"ERR: {db_type} not supported")
            return None
        return DBops.ModuleMap[db_type]
    
    # 切换到指定数据库
    def use_database(self, db_name):
        ops_x = self.choose_opsx()
        if ops_x is None:
            return False
        dbserver = dict(self.dbServer)
        db_eng = ops_x.use_database(self.db_eng, dbserver, db_name)           
        if db_eng is None:
            print(f"ERR: unknown database '{db_name}'")
            return False
        else:
            self.db_eng = db_eng
            self.dbServer['db_name'] = db_name 
        return True
    
    # 表头信息，fields={'col1': {'vtype': 'BIGINT', 'default': 0}, ...}
    def create_table(self, tb_name, fields):
        ops_x = self.choose_opsx()
        if ops_x is None:
            return False
        
        cols = []
        primary_clause = ''
        c_name, c_type = '', ''
        for c_name, tDict in fields.items():
            c_type = tDict['vtype']
            cols.append(f'"{c_name}" {c_type},')
            if tDict.get('primary_key'):
                primary_clause = f', PRIMARY KEY ("{c_name}")'
        cols[-1] = cols[-1][:-1]  # remove the last comma
        colsInfo = '\n'.join(cols)
        return ops_x.create_table(self.db_eng, tb_name, colsInfo, primary_clause)
    
    
    # 向表中插入数据
    # tb_name: 表名
    # tuples: [(val1, val2, ...), ...] 
    # col_names: ['col1', 'col2', ...]  
    def insert_data(self, tb_name, col_names, tuples):
        ops_x = self.choose_opsx()
        if ops_x is None:
            return False
        return ops_x.insert_data(self.db_eng, tb_name, col_names, tuples)
  

if __name__ == '__main__':
    from settings import z_config
    from zebura_core.placeholder import make_dbServer
 
    server_name = z_config['Training','server_name']
    dbserver = make_dbServer(server_name)
    db_name = z_config['Training','db_name']
    dbserver['db_name'] = db_name
    dbops = DBops(dbserver)
    
    dbops.count_items('imdb_movie_dataset')
    dbops.show_columns('imdb_movie_dataset')
    result = dbops.show_current_database()
    print(result.fetchall())
    dbops.show_databases()
    dbops.show_tables()
    result = dbops.show_tb_schema('imdb_movie_dataset')
    print(result)
    dbops.show_randow_rows('imdb_movie_dataset', 3)
    print(dbops.is_table_exist('imdb_movie_dataset'))
    
    result = dbops.show_primary_key('imdb_movie_dataset')
    print('Primary key:', result)
    dbops.create_database('imdb1')
    dbops.show_databases()
    dbops.dbServer['db_name'] = 'imdb1'
    dbops.use_database('imdb1')
    print(dbops.show_current_database().fetchall())
    print(dbops.drop_table('imdb_movie_dataset1'))
