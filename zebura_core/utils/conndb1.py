import sys,os
sys.path.insert(0, os.getcwd().lower())
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError

# sqlalchemy 提供，用于创建一个引擎对象，提供一致的接口
def get_engine(dbServer):

    db_type = dbServer.get('db_type')
    db_name = dbServer.get('db_name')
    db_name = db_name.lower()
    if db_type is None or db_name is None:
        print("ERR: db_type|db_name cannot be None")
        return None
    tmpl = "{ttype}://{username}:{pwd}@{host}:{port}/{db_name}?charset=utf8mb4"
    host = dbServer['host']
    port = int(dbServer['port'])
    username =dbServer['user']
    pwd  = dbServer['pwd']
    if db_type == 'mysql':
        ttype = 'mysql+pymysql'
        tmpl = "{ttype}://{username}:{pwd}@{host}:{port}/{db_name}?charset=utf8mb4"
    elif db_type == 'postgres':
        ttype = 'postgresql+psycopg2'
        tmpl = "{ttype}://{username}:{pwd}@{host}:{port}/{db_name}" 
    else:
        print(f"ERR: {db_type} not supported")
        return None
    
    db_uri = tmpl.format(ttype=ttype, username=username, pwd=pwd, host=host, port=port, db_name=db_name)
    pool_params = {
        'pool_size': 10,          # 最大连接数
        'max_overflow': 5,        # 额外连接
        'pool_recycle': 1800,     # 每 30 分钟回收连接，防止 `wait_timeout`
        'pool_pre_ping': True     # 启用 `ping` 机制检测连接·
    }
    engine = create_engine(db_uri, **pool_params)

    return engine

def make_dbSession(engine):
    Session = sessionmaker(bind=engine)
    session = Session()
    return session

# 创建数据库引擎，并测试连接
def connect(dbServer) -> object:

    db_type = dbServer.get('db_type')
    sql_query = None
    if db_type == 'mysql':
        dbServer['db_name'] = dbServer.get('db_name','mysql')
        sql_query = "SHOW DATABASES"
    elif db_type == 'postgres':
        dbServer['db_name'] = dbServer.get('db_name','postgres')
        sql_query = "SELECT datname FROM pg_database WHERE datistemplate = false;"
    else:
        print(f"ERR: {db_type} not supported")
        raise ValueError(f"ERR_cursor: {db_type} not supported")
    
    try:
        # 创建数据库引擎
        engine = get_engine(dbServer)
        # 创建会话
        session = make_dbSession(engine)
        # 使用 text 函数执行原生 SQL 查询
        result = session.execute(text(sql_query)).fetchall()
        if result is not None:
            print(f"Connection to {db_type} DB successful")
        session.close()
    except Exception as e:
        print(f"ERR: {e}")
        return None   
    return engine

# session为None时，创建会话，执行SQL查询，关闭会话
# session不为None时，执行SQL查询，不关闭会话
def db_execute(engine,sql_query):
    try:
        session = make_dbSession(engine)
        # 使用 text 函数执行原生 SQL 查询
        result = session.execute(text(sql_query)).fetchall()
        session.close()
        return result
    except OperationalError as e:
        # 处理连接失效
        session.rollback()
        session.close()
        print(f"ERR: {e}")
        return None


# Example usage:
if __name__ == "__main__":
    from zebura_core.placeholder import make_dbServer
    dbServer = make_dbServer('Mysql1')
    dbServer['db_name'] = 'imdb'
    
    engine = connect(dbServer)   
    sql_query = "SELECT current_database();"        # for postgres
    sql_query = "SHOW DATABASES"                    # for mysql
    result = db_execute(engine, sql_query)
    print(result)
    
