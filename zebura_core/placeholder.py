# 一些需要保持一致的数据结构
#
############################
import sys,os
sys.path.insert(0, os.getcwd().lower())
from settings import z_config

def make_dbServer(server_name=None):
    if server_name is None:
        dbServer = {
            'db_name':'',
            'db_type':'unknown',
            'host':'localhost',
            'port':1234,
            'user':'totoro',
            'pwd':'123456'
        }
    else:
        dbServer = {
            'db_name': '',               # 数据库名允许未配置
            'db_type':z_config[server_name,'db_type'],
            'host':z_config[server_name,'host'],
            'port':int(z_config[server_name,'port']),
            'user':z_config[server_name,'user'],
            'pwd':z_config[server_name,'pwd']
        }
    return dbServer

def make_esServer(server_name=None):
    if server_name is None:
        esServer = {
            'es_name':'',
            'host':'localhost',
            'port':9200,
            'user':'totoro',    # es的认证用户
            'pwd':'123456',     # es的认证密码
            'ca_path':'',       # es的ca证书路径
            'auth':False
        }
    else:
        esServer = {
            'es_name':server_name,
            'host':z_config[server_name,'host'],
            'port':int(z_config[server_name,'port']),
            'user':z_config[server_name,'user'],
            'pwd':z_config[server_name,'pwd'],
            'ca_path':z_config[server_name,'ca_path'],
            'auth':z_config[server_name,'auth']
        }
    return esServer

#         "type": "query/chat/sql/reset/transaction", # 用户， controller, 增加 action间切换, reset 到某一action
#         "format": "text/md/sql/dict...", # content格式，与显示相关
#         "status": "new/hold/failed/succ", # 新对话,多轮继续；执行失败；执行成
# action log
def make_a_log(funcName):
        return {
            'sql': '',              # 目前为止的sql
            'question': '',         # 用户提问
            'msg': '',              # 当前步骤产生的主要信息
            'note': '',             # 当前步骤产生的次要信息
            'status': 'succ',
            'from': funcName,       # 当前完成的模块
            'type': 'transaction',  # 当前状态类型
            'format': 'text',   
            'others': {},           # 当前步骤产生的其它信息
            'context': []           # 上下文信息
        }

def make_a_req(query:str):
    return {
        "msg": query,
        "context": [],
        "format": "text",
        "type": "query",
        "status": "new"
    }

# Example usage
if __name__ == '__main__':
    print(make_dbServer())
