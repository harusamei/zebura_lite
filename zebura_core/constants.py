################################################
# 非全局，子模块内部使用的default or constant values
# D, default; C, constant, S, setting
################################################
import types

C_SOFTWARE_NAME = 'zebura'       # name of our software icon file name
C_ADM_dbServer = 'ADMdatabase'   # name of system relation database server
C_ADM_Search = 'ADMsearch'       # name of system search server

D_TOP_K = 5   # default top k for search
D_TOP_GOODCASES = 3            # default top k for good cases
D_EXPN_LIMIT = 10              # default limit for expansion of values
D_MAX_ROWS = 1000000           # default max rows for read_csv
D_ES_SHORT_MAX = 32767         # default max value of short type in ES
D_ES_DUPLICATE_THRESHOLD = 0.9           # default threshold for duplicate
D_ES_BULK_MAX = 1000           # default max docs of insert
D_SIMILITY_THRESHOLD = 0.8     # default threshold for similarity
D_SIMILITY_METHOD = 'cosine'   # default method for similarity

# GPT default limit is 8,192 tokens, 约 32,768 characters(英语 1 token 约4 char; 中文1 token 约 1 hanzi，in+out);
# GPT-4-32K 32,768 tokens, 约 131,072 characters
D_MAX_PROMPT_LEN = 4000        # default max length of prompt

# 项目数据存放位置及SCHEMA文件名
S_METADATA_FILE = 'metadata.xlsx'    # metadata file name
S_TRAINING_PATH = 'training'
S_PROMPT_FILE = 'zebura_core/LLM/prompt.txt'         # prompt file name

# 系统数据库中meta表的字段
Z_META_TBNAME = 'meta_{pj_name}'    # meta表名
Z_CASES_TBNAME = 'cases_{pj_name}'  # admdb中的cases表名,存放于 [ADMdatabase,db_name]
Z_CASES_INDEX = 'zebura_cases_{pj_name}'   # cases索引名
Z_ALIGN_DAYS = 7   # 同步频率， 每周rdb 与 es 同步一次
# chat_lang, 该项目默认交流语言

Z_META_PROJECT = tuple(['database_name','domain','db_desc','chat_lang','possessor'])# 项目信息, 所有表整合的prompt，数据归属
# val_lang, 字段内容的语言
Z_META_FIELDS = tuple(['table_name','column_name','hypernym','column_desc','column_type',
                'column_key','column_length','val_lang', 'sample_data','comment']) # 字段信息
# tb_lang，字段本身的语言
Z_META_TABLES = tuple(['table_name','tb_desc','column_count','tb_prompt','tb_lang',
                'group','tags','sample_data'])
Z_META_TERMS = tuple(['term_name','term_desc','related_tables','ttype','grp_prompt'])
# 类型采用pandas的类型，解析时 datetime需要parse_dates=date_columns
Z_CASES_FIELDS = types.MappingProxyType({
    'id': 'string',
    'database_name': 'string',
    'table_name': 'string',
    'question': 'string',
    'sql': 'string',
    'lang': 'string',
    'hit': 'int64',
    'scenario': 'string',
    'target_users': 'string',
    'b_id': 'string',
    'comment': 'string',
    'updated_date': 'date',
    'deleted': 'boolean'
})
# ES中CASES字段与RDB中不完全一样，增加了qemb, en_question，删除了comment
Z_CASES_TY_MAPPING = types.MappingProxyType({
    "id": { "type": "keyword" },
    "database_name": { "type": "keyword" },
    "table_name": { "type": "keyword" },
    "question": { "type": "text" },
    "sql": { "type": "text" },
    "lang": { "type": "keyword" },
    "s_question": { "type": "text" },      #search_lang对应的quesion 翻译
    "qemb": { 
        "type": "dense_vector", 
        "dims": 768         #需替换为config中实际向量的维度
    },
    "hit": { "type": "integer" },
    "scenario": { "type": "text" },
    "target_users": { "type": "text" },
    "b_id": { "type": "keyword" },
    "updated_date": { 
        "type": "date", 
        "format": "yyyy-MM-dd" 
    }
})

if __name__ == '__main__':
    all_vars = locals()

    # 收集所有需要转换的变量
    to_convert = {var_name: var_value for var_name, var_value in all_vars.items() if isinstance(var_value, dict)}

    # 将所有字典类型的变量转换为 MappingProxyType
    for var_name, var_value in to_convert.items():
        all_vars[var_name] = types.MappingProxyType(var_value)

    # 打印所有变量名和类型
    for var_name, var_value in all_vars.items():
        print(f"{var_name}: {type(var_value)}")
