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
# 项目信息, 所有表整合的prompt，数据归属
Z_META_PROJECT = tuple(['database_name','domain','db_desc','chat_lang','possessor','db_prompt'])
# 字段信息
Z_META_FIELDS = tuple(['table_name','column_name','hypernym','column_desc','column_type',
                'column_key','column_length','val_lang', 'examples','alias','comment']) 
Z_META_TABLES = tuple(['table_name','tb_desc','column_count','tb_prompt','tb_promptlit',
                       'tb_lang', 'group_name','grp_prompt','tags','examples'])
# ttype, table;field;tag
Z_META_TERMS = tuple(['term_name','term_desc','related_tables','ttype'])

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
