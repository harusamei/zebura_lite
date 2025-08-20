# 关于当前DataBase的schema信息, 这个信息做为一种知识用于schema linkage,prompt and so on
# 从xls中读取schema信息
###############################
import sys,os,logging
sys.path.insert(0, os.getcwd().lower())
import pandas as pd
import json
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
        self.project = meta_dfs['database']   #  list(const.Z_META_PROJECT)
        self.project = self.project.fillna('')  # 将 NaN 和 None 替换为 ''
        tb_df = meta_dfs['tables']            #  list(const.Z_META_TABLES)
        tb_df = tb_df.fillna('')  # 将 NaN 和 None 替换为 ''
        col_df = meta_dfs['fields']           #  list(const.Z_META_FIELDS)
        col_df = col_df.fillna('')  # 将 NaN 和 None 替换为 ''
        pj_cols = self.project.columns.tolist()
        tb_cols = tb_df.columns.tolist()
        col_cols = col_df.columns.tolist()
        if set(pj_cols) != set(const.Z_META_PROJECT) or set(tb_cols) != set(const.Z_META_TABLES) or set(col_cols) != set(const.Z_META_FIELDS):
            print('diff in pj_df',set(pj_cols)-set(const.Z_META_PROJECT))
            print('diff in tb_df',set(tb_cols)-set(const.Z_META_TABLES))
            print('diff in col_df',set(col_cols)-set(const.Z_META_FIELDS))
            raise ValueError(f"format of metadata file {xls_name} is not correct")
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
            
    # Z_META_FIELDS = ['table_name','column_name','alias','col_desc','column_type',
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
    # tbs_prompt的抽象，格式一样
    def get_grp_prompt(self, tb_names=None):
        if tb_names is None or len(tb_names) == 0:
            tb_names = self.get_table_nameList()
        
        table_list = self.get_tables(tb_names)
        grps = {}
        for table in table_list:
            grp_name = table['group_name']
            hcols = table['hcols_info'].split(',')
            if grp_name not in grps:
                grps[grp_name] = hcols
            else:
                grps[grp_name].extend(hcols)
        
        prompts = []
        for grp_name, hcols in grps.items():
            prompts.append(f"Group:{grp_name}")
            hcols = list(set(hcols))  # 去重
            prompts.append(f"Hyper_Columns: {', '.join(hcols)}")
        return prompts

    def gen_tbs_prompt(self, tb_names=None):
        # 所有表
        if tb_names is None or len(tb_names) == 0:
            tb_names = self.get_table_nameList()
        
        lit_flag = False
        if len(tb_names) >5:
            lit_flag = True

        prompts = []
        table_list = self.get_tables(tb_names)
        for table in table_list:
            prompts.append(f"Table:{table['table_name']}")
            if lit_flag or table['column_count'] > 10:
                prompts.append(f"Columns: {table['cols_info']}")
            else:
                prompts.append(f"Description: {table['tb_desc']}")
                prompts.append(f"Columns: {table['cols_info']}")
                prompts.append('Examples:')
                examples = json.loads(table['examples'])
                df = pd.DataFrame(examples)
                md_data = df.to_markdown(index=False, tablefmt="grid")
                prompts.append(f"[{md_data}]")
            prompts.append('\n')
        return prompts

    # 一张表的所有信息
    def get_tb_info(self, tb_name):
        table = self.tables.get(tb_name,None)
        if table is None:
            return ""
        infos = [table['table_name']]
        infos.append(table['tb_desc'])
        infos.append(table['cols_info'])
        return '\n'.join(infos)

    # group prompt
    def get_gp_info(self, tb_name):
        table = self.tables.get(tb_name,None)
        if table is None:
            return ""
        infos = [table['group_name']]
        infos.append(table['terms_info'])
        return '\n'.join(infos)

     # DB的所有表的信息
    def get_db_info(self):
        infos = [self.project['database_name'][0]]
        infos.append(self.project['db_desc'][0])
        tbList = self.get_table_nameList()
        infos.append('Tables:')
        infos.append(f"[{','.join(tbList)}]")
        return '\n'.join(infos)
   
    # 含此column_name的所有表名  
    def get_tables_with_column(self, column_name) -> list: 
        nameList = []  
        for tb_name in self.tables.keys():
            tList = self.db_Info[tb_name]['column_name']
            if column_name in tList.tolist():
                nameList.append(tb_name)

        return nameList

    def get_db_summary(self) -> dict:
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
    
    loader = ScmaLoader('olist', 'English')
    tbList = loader.get_table_nameList()
    tbList = list(tbList)
    print(tbList)
    print(loader.get_column_nameList())
    print(loader.get_column_nameList('olist_geolocation_dataset'))
    print(loader.get_fieldInfo('olist_geolocation_dataset', 'geolocation_lat'))

    print(loader.get_tables(tbList[:2]))
    print(loader.get_tb_info('olist_geolocation_dataset'))
    print(loader.get_db_info())

    print(loader.get_tables_with_column('geolocation_lat')) 
    print('_________________________')

    with open('tmp.out', 'w',encoding='utf-8') as f:
        f.write('db_summary')
        f.write(json.dumps(loader.get_db_summary()))
        f.write('tbs_prompt\n')
        f.write('\n'.join(loader.gen_tbs_prompt()))
        f.write('grp_prompt\n')
        f.write('\n'.join(loader.get_grp_prompt()))