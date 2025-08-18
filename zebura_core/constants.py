################################################
# 非全局，子模块内部使用的default or constant values
# D, default; C, constant, S, setting, Z, zebura
################################################
import types

C_SOFTWARE_NAME = 'zebura'       # name of our software icon file name

D_TOP_K = 5   # default top k for search
D_TOP_GOODCASES = 3            # default top k for good cases
D_EXPN_LIMIT = 10              # default limit for expansion of values
D_MAX_ROWS = 1000000           # default max rows for read_csv

D_SIMILITY_THRESHOLD = 0.8     # default threshold for similarity
D_SIMILITY_METHOD = 'cosine'   # default method for similarity

# GPT default limit is 8,192 tokens, 约 32,768 characters(英语 1 token 约4 char; 中文1 token 约 1 hanzi，in+out);
# GPT-4-32K 32,768 tokens, 约 131,072 characters
D_MAX_PROMPT_LEN = 4000        # default max length of prompt

# 项目数据存放位置及SCHEMA文件名
S_METADATA_FILE = 'metadata.xlsx'    # metadata file name
S_TRAINING_PATH = 'training'
S_PROMPT_FILE = 'zebura_core/LLM/prompt.txt'         # prompt file name


# chat_lang, 该项目默认交流语言
# val_lang, 字段内容的语言
# tb_lang，字段名本身的语言

# 项目信息, 数据归属，tbs_info 包含的所有表名
Z_META_PROJECT = tuple(['database_name','domain','db_desc','chat_lang','possessor'])
# 字段信息, hcol：field的上位词
Z_META_FIELDS = tuple(['table_name','column_name','hcol','col_desc','column_type',
                'column_key','column_length','val_lang', 'examples','alias','comment']) 
Z_META_TABLES = tuple(['table_name','tb_desc','column_count','cols_info',
                       'tb_lang', 'group_name','hcols_info','tags','examples'])
# ttype terms的类型有三种group, tag, hcol
# group是table的上位, hcol是field的上位
# cols_info该term涉及所有表的字段
# tbs_info 该term涉及的所有表
Z_META_TERMS = tuple(['term_name','term_desc','ttype','tbs_info','cols_info'])

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
