# 一个database为一个项目，生成该DB下所有表的结构
# 导出到metadata.xlsx, 用于NL2SQL prompt, SQL check等
# db 存放在postgres中
##########################################
import sys
import os
sys.path.insert(0, os.getcwd().lower())
import asyncio
import logging
import pandas as pd
from discard.conndb import connect
from zebura_core.utils.lang_detector import detect_language
from dbaccess.scma_gen import ScmaGen
from dbaccess.postgres.ops_p import DBpops

class ScmaGenerator(ScmaGen):

    def __init__(self, dbServer, lang):

        super().__init__(dbServer, lang)
        self.cnx = connect(dbServer)
        self.db_type = dbServer['db_type']
        self.ops = DBpops(dbServer)
        
    # 读取数据库中的表结构信息
    def show_tables(self) ->list:
        return self.ops.show_tables()
    
    # 读一张表的schema信息, ['column_name', 'column_type', 'column_key', 'descInfo', 'char_length', 'num_length']  
    def getColInfo(self, tb_name):
        cursor = self.cnx.cursor()
        query = """
            SELECT 
                cols.column_name AS column_name, pgd.description AS comment
            FROM 
                information_schema.columns AS cols
            LEFT JOIN 
                pg_catalog.pg_description AS pgd 
            ON 
                pgd.objsubid = cols.ordinal_position
                AND pgd.objoid = (SELECT oid FROM pg_catalog.pg_class WHERE relname = %s)
            WHERE 
                cols.table_name = %s;
        """
        cursor.execute(query,(tb_name,tb_name))
        result = cursor.fetchall()
        #print(result)
        ex_fields = ['column_name','comment']
        df = pd.DataFrame(result, columns=ex_fields)

        query1 =f"""
                SELECT 
                    c.column_name as "column_name", c.data_type as "column_type", tc.constraint_type as "column_key",
                    c.character_maximum_length as "char_length", c.numeric_precision as "num_length"
                FROM 
                    information_schema.columns c
                LEFT JOIN 
                    information_schema.constraint_column_usage ccu 
                ON 
                    c.table_name = ccu.table_name 
                    AND c.column_name = ccu.column_name
                    AND c.table_schema = ccu.table_schema
                LEFT JOIN 
                    information_schema.table_constraints tc 
                ON 
                    ccu.constraint_name = tc.constraint_name
                    AND ccu.table_schema = tc.table_schema
                LEFT JOIN 
                    information_schema.check_constraints cc 
                ON 
                    tc.constraint_name = cc.constraint_name
                WHERE 
                    c.table_name = '{tb_name}' 
                    AND c.table_schema = 'public' 
                ORDER BY 
                    c.column_name;
        """  
        cursor.execute(query1)
        result = cursor.fetchall()
        #print(result)
        ex_fields = ['column_name', 'column_type', 'column_key', 'char_length', 'num_length']
        df1 = pd.DataFrame(result, columns=ex_fields)
        df = pd.merge(df, df1, on='column_name', how='left')

        return df
    
    # 从SQL中读一张表的结构并生成，alias, descInfo
    # 返回 tb_desc, df
    async def gen_tb_scma(self, tb_name)->tuple:

        df = self.getColInfo(tb_name)
        cols = df['column_name'].tolist()
        lang = detect_language(' '.join(cols))
        
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
            df['column_desc'] = ''
            df['val_lang'] = ''
            return (tb_dict,df)
        tb_dict['tb_desc'] = parsed['msg'].get('table_description','')
        columns = parsed['msg']['columns']
        df1 = pd.DataFrame(columns)
        df1 = df1.rename(columns={'translation_and_aliases':'alias', 'description':'column_desc'})
        df1['alias'] = df1['alias'].apply(lambda x: ', '.join(x))

        if set(df['column_name']) != set(df1['column_name']):
            diff_cols = df['column_name'][~df['column_name'].isin(df1['column_name'])]
            print(f"LLM Missing columns: {diff_cols}")
            logging.error(f"LLM Missing columns: {diff_cols}")

        # Merge取Column 内容的交集
        m_df = pd.merge(df, df1, on='column_name')  # Merge the DataFrames based on a common column
        # Fill NaN values in df with empty strings
        m_df = m_df.fillna('')
        
        # 字段前10个最高频值
        cursor = self.cnx.cursor()
        query1 = """ SELECT {col_name}, COUNT(*) as frequency 
                    FROM {tb_name} 
                    GROUP BY {col_name} 
                    ORDER by frequency desc
                    LIMIT 10"""
        for index, row in m_df.iterrows():
            ttype = row['column_type'].lower()
            length = row['column_length']
            
            query = query1.format(col_name=row['column_name'], tb_name=tb_name)
            cursor.execute(query)
            result = cursor.fetchall()
            values = [str(r[0]) for r in result]
            if 'char' not in ttype or length < 5: 
                lang =''  # 不是文本字段
            else:
                values = [v for v in values if v != 'None']
                lang = detect_language(' '.join(values))
            m_df.loc[index, 'val_lang'] = lang
            example_values = ', '.join(values)
            m_df.loc[index,'examples'] = example_values[:self.MAX_TXT_LENGTH]

        return (tb_dict, m_df)    

if __name__ == '__main__':
    import argparse
    from zebura_core.placeholder import make_dbServer
    parser = argparse.ArgumentParser(description='Schema Generator')
    parser.add_argument('--server_name', type=str, required=True, help='Name of the dbserver')
    parser.add_argument('--db_name', type=str, required=True, help='Name of the database')
    parser.add_argument('--lang', type=str, required=True, help='Language for the schema')
    args = parser.parse_args()
    s_name = args.server_name
    db_name = args.db_name
    lang = args.lang
    dbServer = make_dbServer(s_name)
    dbServer['db_name'] = db_name
    print(dbServer)
    mg = ScmaGenerator(dbServer,lang)
    xls_name = asyncio.run(mg.output_metadata())
    
    asyncio.run(mg.summary_prompt(xls_name, 3000))

