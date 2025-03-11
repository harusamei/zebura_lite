# 一个database为一个项目，生成该DB下所有表的结构
# 导出到metadata.xlsx, 用于人工修正
# 基类，不负责具体的数据库连接
##########################################
import sys,os
sys.path.insert(0, os.getcwd().lower())
import asyncio
import pandas as pd
import re
import zebura_core.constants as const
from zebura_core.LLM.prompt_loader1 import Prompt_generator
from zebura_core.LLM.llm_agent import LLMAgent
from zebura_core.LLM.ans_extractor import AnsExtractor
from zebura_core.utils.lang_detector import langname2code

class ScmaGen:
    # 需要明确指定数据库及生成内容所用语言
    # 为获得最佳性能生成的元信息所用语言与prompt语言和应用时交互语言一致最好
    def __init__(self, dbServer, lang=None):
        if dbServer is None:
            raise ValueError("db_name must be specified")
        
        self.dbServer = dbServer
        self.db_name = dbServer['db_name']
        self.lang = lang
        self.db_type = dbServer['db_type']

        self.prompter = Prompt_generator(lang=lang)
        self.ans_extr = AnsExtractor()
        self.llm = LLMAgent()
        # MATA表内容最大长度
        self.MAX_LENGTH = 200
        self.MAX_TXT_LENGTH = 2000
        self.database = const.Z_META_PROJECT 
        self.fields = const.Z_META_FIELDS
        self.tables = const.Z_META_TABLES
        
    # 虚函数，placeholder, real function should be implemented in subclass   
    # 从SQL中读一张表的结构并生成，alias, column_desc
    async def gen_tb_scma(self, tb_name)->tuple:
        tb_dict = {'table_name':tb_name, 'tb_desc':'', 'column_count':0, 'tb_lang':self.lang}
        return (tb_dict , pd.DataFrame(columns=self.fields))
    
    def show_tables(self):
        return []

    # def astype_str(self, df, fields):
    #     for field in fields:
    #         df[field] = df[field].astype('str')
    #     return df
    
    def init_metadf(self) -> dict:
        # meta data in dataframe
        dfs = {}
        tpj = {}
        for k in self.database:
            tpj[k] = ''
        tpj['database_name'] = self.db_name
        tpj['chat_lang'] = self.lang
        tpj['possessor'] = const.C_SOFTWARE_NAME

        dfs['database'] = pd.DataFrame([tpj])       # 项目信息表
        dfs['tables'] = pd.DataFrame(columns = self.tables)
        dfs['fields'] = pd.DataFrame(columns = self.fields)
        
        return dfs
        
    # lang 生成内容所有语言
    async def gen_db_info(self) -> dict:
        # 生成三张表: database, tables, fields
        dfs = self.init_metadf()
        tables = self.show_tables()
        print(f"Database: {self.db_name}, Tables: {len(tables)}")
        
        count = 0
        for table in tables:
            table_name = table[0]
            tb_dict, m_df = await self.gen_tb_scma(table_name)
            print(count, table_name)
            # 保存表结构信息           
            # gen prompt, prompt_lit
            col_list = m_df['column_name'].tolist()
            tb_dict['tb_promptlit']  =f"Columns: [ {', '.join(col_list)} ]"
            
            t_md = m_df[['column_name','column_desc','alias','column_type','column_key','val_lang']].to_markdown(index=False)
            tb_dict['tb_prompt']= t_md

            dfs['tables'] = pd.concat([dfs['tables'], pd.DataFrame([tb_dict])], ignore_index=True)
            # remove empty columns
            m_df = m_df.dropna(axis=1, how='all')
            dfs['fields'] = dfs['fields'].dropna(axis=1, how='all')
            # 排除空或全为 NA 的列
            m_df = m_df.loc[:, m_df.notna().any()]
            dfs['fields'] = dfs['fields'].loc[:, dfs['fields'].notna().any()]
            
            dfs['fields'] = pd.concat([dfs['fields'], m_df], ignore_index=True)            
            count += 1
            #if count > 5: break
        return dfs
    
    # 增加 table grouping, prompt summary 
    async def meta_enhance(self, meta_dfs):
        # 生成table grouping
        tb_df = meta_dfs['tables']
        db_name = self.db_name
        table_count = tb_df.shape[0]
        tb_info = []
        for _, row in tb_df.iterrows():
            tb_info.append(f"table name:{row['table_name']}")
            tb_info.append(f"description:{row['tb_desc']}")
            tb_info.append(row['tb_promptlit'])
            tb_info.append('--------------')
        
        tmpl = self.prompter.tasks['tb_grouping']
        query = tmpl.format(chat_lang=self.lang, db_name=db_name, table_count=table_count, tables_info='\n'.join(tb_info))
        llm_answ = await self.llm.ask_llm(query, '')
        result = self.ans_extr.output_extr('tb_grouping',llm_answ)
        if result['status'] == 'failed':
            return False
        
        for group in result['msg']:
            group_name = group['group_name']
            group_desc = group['group_description']
            tb_list = group['included_tables']
            for tb_name in tb_list:
                tb_df.loc[tb_df['table_name'] == tb_name, 'group_name'] = group_name
                tb_df.loc[tb_df['table_name'] == tb_name, 'group_desc'] = group_desc

        return True
    
    # summary words 数限制
    async def summary_prompt(self, meta_xls, limit_length):
        # 词长转换为字符长
        # GPT default limit is 8,192 tokens, 约 32,768 characters; GPT-4-32K 32,768 tokens, 约 131,072 characters
        meta_dfs = pd.read_excel(meta_xls, sheet_name=None)
        pj_df = meta_dfs['database']
        tb_df = meta_dfs['tables']

        prompts =[]
        pj_desc = [f"data is stored in: {pj_df['database_name'][0]}"]
        tb_count = tb_df.shape[0]

        uniq_groups = tb_df['group_name'].unique()
        group_limit = limit_length/(len(uniq_groups)+1)
        tmpl = self.prompter.tasks['db_summary']
        for gname in uniq_groups:
            prompts = []
            prompts.append(f"group name:{gname}")
            g_df = tb_df[tb_df['group_name'] == gname]
            for _, row in g_df.iterrows():
                prompts.append(f"table name:{row['table_name']}")
                prompts.append(row['tb_prompt'])
                prompts.append('------------')
            query = tmpl.format(chat_lang = self.lang, limit_length=group_limit, db_info='\n'.join(prompts))
            llm_answ = await self.llm.ask_llm(query, '')
            llm_answ = re.sub(r'^Summary', '', llm_answ, flags=re.IGNORECASE)
            print(f"Group: {gname}, query:{len(query)}, summary: {len(llm_answ)}")
            # 保证group_prompt是字符串
            tb_df['group_prompt'] = tb_df['group_prompt'].astype('str')
            tb_df.loc[tb_df['group_name']==gname,'group_prompt'] = str(llm_answ)
            pj_desc.append(llm_answ)
            
        query = tmpl.format(chat_lang=self.lang, limit_length=limit_length, db_info='\n'.join(pj_desc))
        llm_answ = await self.llm.ask_llm(query, '')
        llm_answ = re.sub(r'^Summary', '', llm_answ, flags=re.IGNORECASE)
        pj_df['db_desc'] = str(llm_answ)
        # 1 word = 5 characters
        if len(''.join(pj_desc)) > limit_length*5:
            print(f"too long to input for summary: {len(''.join(pj_desc))}")
            pj_desc[0] = f'This database contains a total of {tb_count} tables, grouped according to their functionality. The schema information for the tables is as follows:' 
            pj_df['db_prompt'] = '\n'.join(pj_desc)
        else:
            sort_df = tb_df.sort_values(by='group_name')
            prompts =[f'This database contains a total of {tb_count} tables, grouped according to their functionality. The schema information for the tables is as follows:' ]
            md = sort_df[['table_name','tb_desc','tb_promptlit','group_name']].to_markdown(index=False)
            prompts.append(md)
            query = tmpl.format(chat_lang=self.lang,limit_length=limit_length, db_info='\n'.join(prompts))
            llm_answ = await self.llm.ask_llm(query, '')
            llm_answ = re.sub(r'^Summary', '', llm_answ, flags=re.IGNORECASE)
            pj_df['db_prompt'] = str(llm_answ)
            print(f"Database summary: {len(llm_answ)}")

        writer = pd.ExcelWriter(f'{meta_xls}')
        for tb_name, df in meta_dfs.items():
            df.to_excel(writer, sheet_name=f'{tb_name}', index=False)
        writer.close()

    async def output_metadata(self,xls_name) ->str:
              
        # 获取表的元信息
        tb_dfs = await self.gen_db_info()
        
        fields1 = [field for field in self.fields if field not in ['column_desc','examples','column_length']]
        str_cols =','.join([f"{col} varchar({self.MAX_LENGTH})" for col in fields1])
        str_cols += ', column_desc TEXT, examples TEXT'
        str_cols += ', column_length BIGINT'

        fields_df = tb_dfs['fields']
        fields_df = fields_df.fillna('')
        for indx, row in fields_df.iterrows():
            for field in fields1:
                fields_df.loc[indx,field] = row[field][:self.MAX_LENGTH]
            for field in ['column_desc','examples']:
                fields_df.loc[indx,field] = row[field][:self.MAX_TXT_LENGTH]    
            if row['column_length'] == '':
                fields_df.loc[indx,'column_length'] = 0
            else:
                length_val = int(row['column_length'])
                if length_val > 65535:
                    print(f"Length value too large: {length_val} in {row['table_name']}.{row['column_name']}")
                    #fields_df.loc[indx,'column_length'] = 10000
        fields_df['column_length'] = fields_df['column_length'].astype('int64')

        db_name = self.db_name
        await self.meta_enhance(tb_dfs)

        # 保存表结构内容
        langcode = langname2code(self.lang)
        if langcode != 'en':
            xls_name = xls_name.replace('.xlsx', f'_{langcode}.xlsx')
        print(f"metadata saved to {xls_name}")

        writer = pd.ExcelWriter(f'{xls_name}')
        for tb_name, df in tb_dfs.items():
            df.to_excel(writer, sheet_name=f'{tb_name}', index=False)
        writer.close()
        return xls_name

# Example usage
if __name__ == '__main__':

    from zebura_core.placeholder import make_dbServer
    s_name = 'Postgres1'
    dbServer = make_dbServer(s_name)
    dbServer['db_name'] = 'imdb1'
   
    mg = ScmaGen(dbServer,'chinese')
    # just a BVT, output empty metadata.xlsx 
    # 创建存放文件的目录
    out_path=f'{const.S_TRAINING_PATH}/{dbServer["db_name"]}'
    wk_dir = os.getcwd()
    directory = os.path.join(wk_dir,out_path)
    if not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)
    xls_name = os.path.join(directory, f'{const.S_METADATA_FILE}')  
    xls_name = asyncio.run(mg.output_metadata(xls_name))
    asyncio.run(mg.summary_prompt(xls_name, 3000))

