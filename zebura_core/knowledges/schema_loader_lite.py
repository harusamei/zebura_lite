# 关于当前DataBase的schema信息, 这个信息做为一种知识用于schema linkage,prompt and so on
# 从xls中读取schema信息
import sys,os,logging
sys.path.insert(0, os.getcwd().lower())
import pandas as pd
from typing import Dict, Union, List, Optional
from settings import z_config
import zebura_core.constants as const
from zebura_core.utils.lang_detector import langname2code

class ScmaLoader:
    _is_initialized = False         # 只初始化一次
    db_Info = None
    tables = {}
    project = None
    def __init__(self, db_name=None, chat_lang='English'):  # meta_file=None

        if not ScmaLoader._is_initialized:
            ScmaLoader._is_initialized = True
            self.project = None
            self.tables = {}
            if db_name is None:
                db_name = z_config['Training', 'db_name']      # 只检查config中的db, 一个项目一个db
                chat_lang = z_config['Training', 'chat_lang'] 
            
            self.db_Info = self.load_schema(db_name, chat_lang)

            ScmaLoader.db_Info = self.db_Info           # 存放table的fields
            ScmaLoader.tables = self.tables
            ScmaLoader.project = self.project
   
        if self.project is None:
            raise ValueError("No project information found in the schema file")
        logging.debug("Loader init success")
    
    def load_schema(self, pj_name, chat_lang) -> Dict:
        
        # 项目数据存放位置及SCHEMA文件名
        xls_name = const.S_METADATA_FILE  # metadata file name
        path = const.S_TRAINING_PATH

        langcode = langname2code(chat_lang)
        if langcode != 'en':
            xls_origin = xls_name
            xls_name = xls_name.replace('.xlsx', f'_{langcode}.xlsx')
        # 多语言的schema文件不存在，使用原始的
        if not os.path.exists(f"{path}/{pj_name}/{xls_name}"):
            xls_name = xls_origin
        print(f"read metadata from {path}/{pj_name}/{xls_name}")
        wk_dir = os.getcwd()
        xls_name = os.path.join(wk_dir, f"{path}/{pj_name}/{xls_name}")
        meta_dfs = pd.read_excel(xls_name, sheet_name=None)
        
        self.project = meta_dfs['database']   # list(const.Z_META_PROJECT)
        tb_df = meta_dfs['tables']            #  list(const.Z_META_TABLES)
        col_df = meta_dfs['fields']           #  [list(const.Z_META_FIELDS)
        pj_cols = self.project.columns.tolist()
        tb_cols = tb_df.columns.tolist()
        col_cols = col_df.columns.tolist()
        if set(pj_cols) != set(const.Z_META_PROJECT) or set(tb_cols) != set(const.Z_META_TABLES) or set(col_cols) != set(const.Z_META_FIELDS):
            print('different in pj_df',set(pj_cols)-set(const.Z_META_PROJECT))
            print('different in tb_df',set(tb_cols)-set(const.Z_META_TABLES))
            print('different in col_df',set(col_cols)-set(const.Z_META_FIELDS))
            raise ValueError(f"Metadata file {xls_name} is not correct")
        
        dfList = {}
        for _, row in tb_df.iterrows():
            tb_name = row['table_name']
            t_df = col_df[col_df['table_name'] == tb_name]
            t_df = t_df.fillna('')
            dfList[tb_name] = t_df
            self.tables[tb_name] = row

        return dfList
        
    def get_table_nameList(self) -> list:
        return self.tables.keys()
    
    # 表的所有column_name，None为所有表
    def get_column_nameList(self, tableNames: Optional[Union[str, List[str]]] = None) -> list:
        if tableNames is None:
            tbList = self.get_table_nameList()
        elif isinstance(tableNames, str):
            tbList = [tableNames]
        else:
            tbList = tableNames
        columnList = []
        for tableName in tbList:
            tList = self.db_Info[tableName]['column_name']
            columnList.extend(tList.tolist())
        return columnList
    
    # 一组表名的所有表
    def get_tables(self, tableNames):
        if isinstance(tableNames, str):
            tableNames = [tableNames]
        tList = []
        for name in tableNames:
            name = name.lower()
            if self.tables.get(name) is not None:
                tList.append(self.tables[name])
        
        return tList
            
    # Z_META_FIELDS = ['table_name','column_name','alias','column_desc','column_type',
    #            'column_key','column_length','val_lang', 'examples','comment']  
    # 得到某一个字段的上述meta信息
    def get_fieldInfo(self, tableName, columnName)->dict:
        tableName = tableName.lower()
        columnName = columnName.lower()
        onetb = self.db_Info.get(tableName)
        if onetb is None:
            return None
        onetb = onetb[onetb['column_name']==columnName]
        if onetb.empty:
            return None
        tdict = onetb.iloc[0].to_dict()
        return tdict

    # tb_names: relevant table list
    # max_len: the max length of all tables' prompt
    # 生成相关表信息的有长度限制的prompt  
    def gen_limited_prompt(self, max_len,tb_names=None) ->list:
        # 所有表
        if tb_names is None or len(tb_names) == 0:
            tb_names = self.get_table_nameList()

        tb_len, lit_len = 0, 0
        g_prompts ={}
        table_list = self.get_tables(tb_names)
        for table in table_list:
            tb_len += len(table['tb_prompt'])
            lit_len += len(table['tb_promptlit'])
            if table['group_name'] not in g_prompts:
                g_prompts[table['group_name']] = table['group_prompt']
        if tb_len < max_len:
            prompts = [f"Table:{table['table_name']}\n{table['tb_prompt']}" for table in table_list]
        elif lit_len < max_len:
            prompts = [f"Table:{table['table_name']}\n{table['tb_promptlit']}" for table in table_list]
        else:
            prompts = [v for v in g_prompts.values()]
            if sum(len(prompt) for prompt in prompts) > max_len:
                prompts = [self.get_db_prompt()]

        return prompts

    # 一张表的所有信息
    def get_tb_prompt(self, tb_name):
        table = self.tables.get(tb_name,None)
        if table is not None:
            return table['tb_prompt']
        else:
            return ""
    # group prompt
    def get_gp_prompt(self, tb_name):
        table = self.tables.get(tb_name,None)
        if table is not None:
            return table['group_prompt']
        else:
            return ""

     # 所有表的prompt
    def get_db_prompt(self):
        return self.project['db_prompt'][0]
   
    # 含此column_name的所有表名  
    def get_tables_with_column(self, column_name) -> list: 
        nameList = []  
        for tb_name in self.tables.keys():
            tList = self.db_Info[tb_name]['column_name']
            if column_name in tList.tolist():
                nameList.append(tb_name)

        return nameList
    
    def get_db_summary(self):
        db_info = {'database':{},'tables':[]}
        db_dict = {'name':self.project['database_name'][0],'desc':self.project['db_desc'][0]}
        db_info['database'] = db_dict

        tb_names = self.get_table_nameList()
        tables = self.get_tables(tb_names)
        db_info['tables'] = []
        for tb in tables:
            tb_info = {}
            tb_info['name'] = tb['table_name']
            tb_info['columns'] = self.get_column_nameList(tb['table_name'])
            tb_info['group'] = tb['group_name']
            tb_info['desc'] = tb['tb_desc']
            db_info['tables'].append(tb_info)
        return db_info
       
    
    # 得到每个字段的一些值
    def get_examples(self,table_name):
        examples = []
        col_names = self.get_column_nameList(table_name)
        for colName in col_names:
            col_info = self.get_columnInfo(table_name, colName)
            if col_info is None:
                continue
            if len(col_info.get('examples',[]))>1:
                examples.append(f'Column:{colName}, example values are: {col_info["examples"]}')
        return examples 

# Example usage
if __name__ == '__main__':
    # Load the SQL patterns
    
    loader = ScmaLoader('imdb', 'English')
    tbList = loader.get_table_nameList()
    tbList = list(tbList)
    print(tbList)
    print(loader.get_column_nameList())
    print(loader.get_column_nameList('imdb_movie_dataset'))
    print(loader.get_fieldInfo('imdb_movie_dataset', 'votes'))

    print(loader.get_tables(tbList[:2]))
    print(loader.get_tb_prompt('imdb_movie_dataset'))
    print(loader.get_gp_prompt('Movie Information'))
    print(loader.get_db_prompt())

    print(loader.get_tables_with_column('rank')) 
    print('_________________________')
    print(loader.get_db_summary())


