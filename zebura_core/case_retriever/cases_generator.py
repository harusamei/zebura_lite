################################
# 两个类，基于meta信息，生成golden cases
# 存入adm数据库
################################
import os,sys,asyncio,time,json
sys.path.insert(0, os.getcwd())
from settings import z_config

import logging
import zebura_core.constants as const
from zebura_core.LLM.llm_agent import LLMAgent
from zebura_core.LLM.prompt_loader1 import Prompt_generator
from zebura_core.LLM.ans_extractor import AnsExtractor
from zebura_core.knowledges.schema_loader_lite import ScmaLoader
from zebura_core.placeholder import make_dbServer
from zebura_core.utils.hashID_maker import string2id

from dbaccess.db_ops import DBops
import pandas as pd

# 生成golden cases, 输出到 caseCSV
class CaseGen:
    # db_name: 需要生成golden cases的数据库名
    def __init__(self, db_name, chat_lang='English'):

        self.db_name = db_name.lower()
        self.chat_lang = chat_lang
        self.gcHeader = const.Z_CASES_FIELDS.keys()

        self.prompter = Prompt_generator(chat_lang)
        self.llm = LLMAgent()
        self.ans_ext = AnsExtractor()

        self.scha_loader = ScmaLoader(db_name=db_name, chat_lang=chat_lang)
        # self.ops 与方言相关
        server_name = z_config['Training', 'server_name']
        db_server = make_dbServer(server_name)
        db_server['db_name'] = self.db_name
        self.ops = DBops(db_server)

        logging.debug("Case_generator init success")

    # 执行question 对应的 SQL
    def execute_sql(self, sql):
        try:
            cursor = self.ops.cursor
            cursor.execute(sql)
            rows = cursor.fetchall()
        except Exception as e:
            print('an error in executing sql:', e)
            return 'an error in executing sql: '+str(e)
        if rows is None or len(rows)<1:
            return 'No records found'
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(rows, columns=columns)
        return df.to_markdown()
    
    def get_random_rows(self, tb_name):
        samples = self.ops.get_random_rows(tb_name)
        return samples
    
    # 生成golden cases, 输出到 caseCSV
    async def gen_cases_of_tb(self,tb_name,lang) ->pd.DataFrame:

        tb = self.scha_loader.get_tables(tb_name)[0]
        tb_prompt= tb['tb_promptlit']
        tb_prompt = f'Table:{tb_name}\n'+tb_prompt
        samples = self.get_random_rows(tb_name)       
        
        tml = self.prompter.tasks['scn4db']
        query = tml.format(num_scenarios=5, tb_prompt=tb_prompt, sample_records=samples)
        llm_answ = await self.llm.ask_llm(query, '')
        result = self.ans_ext.output_extr('scn4db', llm_answ)
        if result['status'] == 'failed':
            print('failed to generate cases for table:', tb_name)
            return None
        scnList = result['msg']

        tml = self.prompter.tasks['sql4db']
        if lang == 'Chinese':
            chat_lang = '简体中文'
        else:
            chat_lang = lang  
        questions = []
        for scn in scnList:
            scenario = scn['scenario']
            target_users = ','.join(scn['target_users'])
            print(scenario)
            num = 5*len(scn['target_users'])
            # 每个场景生成的samples 不一样
            samples = self.get_random_rows(tb_name)
            query = tml.format(num_questions=num, target_users=target_users, scenario=scenario, 
                               chat_lang=chat_lang, tb_prompt=tb_prompt, sample_records=samples)
            llm_answ = await self.llm.ask_llm(query, '')
            result = self.ans_ext.output_extr('sql4db', llm_answ)
            if result['status'] == 'failed':
                continue
            for aws in result['msg']:
                aws['scenario'] = scenario
                aws['target_users'] = scn['target_users']
                questions.append(aws)
        print('total questions:', len(questions))
        print(questions[0])
        return questions
    
    def make_oneRow(self) ->dict:
        oneRow = {}
        [oneRow.update({k:''}) for k in self.gcHeader]
        oneRow['database_name']=self.db_name
        oneRow['lang']= self.chat_lang
        oneRow['hit']=1
        oneRow['target_users']=[]
        oneRow['updated_date']=time.strftime("%Y-%m-%d", time.localtime())
        return oneRow
    
    # 生成golden cases, 输出到 caseCSV
    async def gen_cases(self, casesCSV):

        tbList = self.scha_loader.get_table_nameList()
        tList = []
        oneRow = self.make_oneRow()
        for tb_name in tbList:
            results = await self.gen_cases_of_tb(tb_name,self.chat_lang)
            oneRow['table_name'] = tb_name
            for t in results:
                for k,v in t.items():
                    oneRow[k] = v
                tList.append(oneRow.copy())
        df = pd.DataFrame(tList)    
        print('total cases:', df.shape)  # 直接访问 df.shape 属性
        df = df.drop_duplicates(subset=['question'], keep='first')
        for indx, row in df.iterrows():
            df.at[indx,'id'] = string2id(f"{row['question']} in {row['table_name']}")

        df.to_csv(casesCSV, encoding='utf-8-sig', index=False)  
    
    # 生成后续问题
    async def gen_followups(self, casesCSV):
        df = pd.read_csv(casesCSV)
        df = df.dropna(how='all',subset=['question'])
        tList = []
        curdb = self.ops.show_current_database()
        print('current database:', curdb)
        ori_tb_name = ''
        tml = self.prompter.tasks['fups4db']
        err_count = 0
        for indx, row in df.iterrows():
            if indx%10 == 0:
                print(f'processing {indx}th case')

            lang = row['lang']
            if lang == 'Chinese':
                chat_lang = '简体中文'
            else:
                chat_lang = lang
            tb_name = row['table_name']
            if ori_tb_name != tb_name:
                tb = self.scha_loader.get_tables(tb_name)[0]
                tb_prompt= tb['tb_promptlit']
                tb_prompt = f'Table:{tb_name}\n'+tb_prompt
                samples = self.get_random_rows(tb_name)
                ori_tb_name = tb_name
            sql = row['sql']
            print(sql)
            result = self.execute_sql(sql)
            if 'error in executing sql' in result:
                row['comment'] = result
                err_count += 1
            query = tml.format(chat_lang=chat_lang, tb_prompt=tb_prompt, sample_records=samples,
                               sql_query=sql, sql_result=result)
            llm_answ = await self.llm.ask_llm(query, '')
            result = self.ans_ext.output_extr('fups4db', llm_answ)
            if result['status'] == 'failed':
                continue
            for aws in result['msg']:
                new_row = row.copy()
                new_row['b_id'] = row['id']
                new_row['question'] = aws['question']
                new_row['sql'] = aws['sql']
                new_row['id'] = string2id(f"{new_row['question']} in {new_row['table_name']}")
                tList.append(new_row)

        print('total followups:', len(tList))
        print('error radio:', err_count/df.shape[0])
        if tList:
            df1=pd.DataFrame(tList)
            df = pd.concat([df, df1], ignore_index=True)
        else:
            print("No follow-up cases generated.")
        df = df.drop_duplicates(subset=['question'], keep='first')
        df.to_csv(casesCSV, encoding='utf-8-sig', index=False)

        return

# Example usage
if __name__ == '__main__':
    from settings import z_config
    import zebura_core.constants as const
    db_name = z_config['Training','db_name']
    gen = CaseGen(db_name,'Japanese')
    asyncio.run(gen.gen_cases('training/cases.csv'))
    asyncio.run(gen.gen_followups('training/cases.csv'))
    
    
