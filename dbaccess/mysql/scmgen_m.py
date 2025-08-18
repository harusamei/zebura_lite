# 一个database为一个项目，生成该DB下所有表的结构
# 导出到metadata.xlsx, 用于NL2SQL prompt, SQL check等
# db 存放在MySQL中
##########################################
import sys
import os
sys.path.insert(0, os.getcwd().lower())

import asyncio
import logging
import pandas as pd
from discard.conndb import connect
from zebura_core.utils.lang_detector import detect_language
from dbaccess.mysql.ops_m import DBmops
from dbaccess.scma_gen import ScmaGen
class ScmaGenerator(ScmaGen):

    def __init__(self, dbServer, lang):
        super().__init__(dbServer, lang)
        if self.db_type != 'mysql':
            raise ValueError("Only support mysql database")
        self.cnx = connect(dbServer)
        cursor = self.cnx.cursor()
        cursor.execute(f"USE {self.db_name}")
        self.ops = DBmops(dbServer)

     # 读取数据库中的表结构信息
    def show_tables(self) ->list:
        return self.ops.show_tables()
    
    # get table and fields info
    async def gen_tb_scma(self, tb_name)->tuple:

            query = f"""
            SELECT COLUMN_NAME AS 'column_name', COLUMN_TYPE AS 'column_type', 
                COLUMN_KEY AS 'column_key', COLUMN_COMMENT AS 'comment',
                CHARACTER_MAXIMUM_LENGTH AS 'char_length', NUMERIC_PRECISION AS 'num_length'
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{self.db_name}' AND TABLE_NAME = '{tb_name}'
            """            
            cursor = self.cnx.cursor()
            cursor.execute(query)
            result = cursor.fetchall()
            # self.fields = ['table_name','column_name','alias','col_desc','column_type',
            #                   'column_key','column_length','val_lang', 'examples']  # 字段信息
            ex_fields = ['column_name', 'column_type', 'column_key','comment','char_length', 'num_length']
            df = pd.DataFrame(result, columns=ex_fields)
            cols = df['column_name'].tolist()
            lang = detect_language(' '.join(cols))      # detect language of column names
            tb_dict = {'table_name':tb_name, 'tb_desc':'', 'column_count':df.shape[0], 'tb_lang':lang}
            df['table_name'] = tb_name
            df['column_length'] = df.apply(lambda row: row['char_length'] if pd.notnull(row['char_length']) else row['num_length'], axis=1)
            df.drop(columns=['char_length', 'num_length'], inplace=True)
           
            # call llm  补充 alias, descInfo
            tmpl = self.prompter.tasks['db_desc']
            column_info = df[['column_name', 'column_key']].to_markdown(index=False)
            query = tmpl.format(chat_lang=self.lang, table_name=tb_name, column_info=column_info)
            llm_answ = await self.llm.ask_llm(query, '')
            parsed = self.ans_extr.output_extr('db_desc',llm_answ)
            if parsed['status'] == 'failed':
                df['alias'] = ''
                df['col_desc'] = ''
                df['val_lang'] = ''
                return (tb_dict,df)
            tb_dict['tb_desc'] = parsed['msg'].get('table_description','')
            columns = parsed['msg']['columns']
            df1 = pd.DataFrame(columns)
            df1 = df1.rename(columns={'translation_and_aliases':'alias', 'description':'col_desc'})
            df1['alias'] = df1['alias'].apply(lambda x: ', '.join(x))
            if set(df['column_name']) != set(df1['column_name']):
                diff_cols = df['column_name'][~df['column_name'].isin(df1['column_name'])]
                print(f"LLM Missing columns: {diff_cols}")
                logging.error(f"LLM Missing columns: {diff_cols}")

            # Merge取Column 内容的交集
            m_df = pd.merge(df, df1, on='column_name')  # Merge the DataFrames based on a common column
            # Fill NaN values in df with empty strings
            m_df = m_df.fillna('')

            # detect language of values in columns
            query1 = """ SELECT `{col_name}`, COUNT(*) as frequency 
                        FROM {tb_name} 
                        GROUP BY `{col_name}` 
                        ORDER by frequency desc
                        LIMIT 10"""
            for index, row in m_df.iterrows():
                ttype = row['column_type'].lower()
                length = row['column_length']
                col_name = row['column_name']
                query = query1.format(col_name=col_name, tb_name=tb_name)
                cursor.execute(query)
                result = cursor.fetchall()
                values = [str(r[0]) for r in result]
                if 'char' not in ttype or length < 5: 
                    lang =''  # 不是文本字段
                else:
                    lang = detect_language(' '.join(values))
                m_df.loc[index, 'val_lang'] = lang
                example_values = ', '.join(values)
                m_df.loc[index,'examples'] = example_values[:self.MAX_TXT_LENGTH]

            return (tb_dict, m_df)    
    
# Example usage
if __name__ == '__main__':
    import argparse
    from zebura_core.placeholder import make_dbServer
    parser = argparse.ArgumentParser(description='Schema Generator')
    parser.add_argument('--server_name', type=str, required=True, help='Name of the dbserver')
    parser.add_argument('--db_name', type=str, required=True, help='Name of the database')
    parser.add_argument('--lang', type=str, required=True, help='Language for the schema')
    args = parser.parse_args()
    # --server_name Mysql1 --db_name IMDB --lang Chinese
    s_name = args.server_name 
    db_name = args.db_name
    lang = args.lang
    dbServer = make_dbServer(s_name)
    dbServer['db_name'] = db_name
    mg = ScmaGenerator(dbServer,lang)
    mg = ScmaGenerator(dbServer,lang)
    xls_name = asyncio.run(mg.output_metadata())
    
    asyncio.run(mg.summary_prompt(xls_name, 3000))
