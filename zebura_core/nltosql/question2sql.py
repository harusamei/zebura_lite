#######################################################################################
# main function of nl2sql
# function： natural language question to SQL with LLM
# requirements： slots from extractor, good cases, schema of db, ask db, gpt
# 解析信息：['columns','tables', 'conditions', 'aliases']
#######################################################################################
import os,sys,logging,json
sys.path.insert(0, os.getcwd())
from settings import z_config
import zebura_core.constants as const
from zebura_core.LLM.prompt_loader1 import Prompt_generator
from zebura_core.LLM.llm_agent import LLMAgent
from zebura_core.LLM.ans_extractor import AnsExtractor
from zebura_core.knowledges.schema_loader_lite import ScmaLoader
from zebura_core.nltosql.schlinker import Sch_linking
from zebura_core.placeholder import make_a_log

class Question2SQL:

    def __init__(self, pj_name=None, chat_lang='English'): # 一个用户的database即一个项目

        if pj_name is not None:
            self.pj_name = pj_name
        else:
            self.pj_name = z_config['Training', 'db_name']
            chat_lang = z_config['Training', 'chat_lang']
        self.chat_lang = chat_lang

        self.prompter = Prompt_generator()
        self.llm = LLMAgent()
        self.ans_ext = AnsExtractor()
        self.scha_loader = ScmaLoader(self.pj_name,self.chat_lang)  # 使用默认的 en_prompt
        # 做静态检查，不查数据库
        self.sim_linker = Sch_linking(const.D_SIMILITY_THRESHOLD)

        logging.debug("Question2SQL init success")

    # table_name 相关表名为None, 为整个DB
    # 主函数， 将question转化为SQL
    async def ques2sql(self, question, tb_names=None) -> dict:

        resp = make_a_log("ques2sql")
        gcases = None
        result = await self.get_rel_tables(question, tb_names)
        # LLM 错误 或 判断为 chat, nosql
        if isinstance(result, str) and 'err' in result:
            resp['status'] = 'failed'
            resp['msg'] = result
            resp['type'] = 'err_llm'
            return resp
        elif isinstance(result,str):
            resp['status'] = 'succ'
            resp['msg'] = result
            resp['type'] = 'chat'
            return resp
        
        reltb_names = result
        matches = self.sim_linker.link_tables(reltb_names)      # (term,like,score)
        # 只要score 不为None, 说明有匹配
        tb_names = [x[1] for x in matches if x[2] is not None]
        if gcases is None:
            examples = 'No examples'
        else:
            shots = []
            for case in gcases:
                tSet = set(case['doc'].get('table_name','').split(';'))
                if  len(tSet)>0 and tSet.issubset(reltb_names):
                    shots.append({'question':case['doc']['question'],'sql':case['doc']['sql']})
            examples = json.dumps(shots, ensure_ascii=False)
        db_info = self.get_db_prompt(tb_names)

        tmpl = self.prompter.tasks['nl_to_sql']
        query = tmpl.format(db_info=db_info, examples=examples, question=question)
        llm_answ = await self.llm.ask_llm(query, '')
        result = self.ans_ext.output_extr('nl_to_sql',llm_answ)

        # outfile = open('tmpOut.txt', 'a', encoding='utf-8-sig')
        # outfile.write(query)
        # outfile.write("\n------------\n")

        resp["status"] = result['status']
        resp['note'] = result['msg']            # all breakdown info {'columns','tables', 'values', 'sql'}
        if result['status'] == 'failed':
            resp['type'] = 'err_llm'
            resp['error'] = result['msg']
        else:
            resp['format'] = 'sql'
            if 'sql' not in result['msg']:
                logging.error(f"sql not in result['msg']:{result['msg']}")
                resp['msg'] = ''
            else:
                resp["msg"] = result['msg']['sql']      # only sql
        if gcases is not None:
            resp['others']['gcases'] = gcases
        return resp

    # determine chat or db query, if db_query return relative tables
    async def get_rel_tables(self, question, tb_names=None,gcases=None) -> list:
        if tb_names is None or len(tb_names) == 0:
            tb_names = self.scha_loader.get_table_nameList()
        
        if gcases is not None:
            tb_names1 = [name for case in gcases for name in case['doc']['table_name'].split(';')]
            tb_names = set(tb_names1).union(set(tb_names))
        db_info = self.get_db_prompt(tb_names)

        tmpl = self.prompter.tasks['tables_choose']
        query = tmpl.format(chat_lang=self.chat_lang, question=question, db_info=db_info)
        # Track the query
        outfile = open('tmpOut.txt', 'a', encoding='utf-8-sig')
        outfile.write(query)
        outfile.write("\n------------\n")

        llm_answ = await self.llm.ask_llm(query, '')
        result = self.ans_ext.output_extr('tables_choose',llm_answ)
        if result['status'] == 'failed':
            return result['msg']
        
        result = result['msg']
        if result.get('tables') is not None:
            return result['tables']
        
        return result.get('chat','can I help you')

   
    def get_db_prompt(self, tb_names) -> str:
        max_len = const.D_MAX_PROMPT_LEN
        pmtList = self.scha_loader.gen_limited_prompt(max_len, tb_names)
        return '\n'.join(pmtList)
    
    
# Example usage
if __name__ == '__main__':
    import asyncio

    querys = ['収益が最も高い映画は何ですか？','你好，请问你知道今天天气吗','你知道如何查询电脑价格吗？']
    table_names = ['imdb_movie_dataset']
    parser = Question2SQL()
    for query in querys:
        result = asyncio.run(parser.ques2sql(query,tb_names=table_names))
        print(f"query:{query}\n{result['msg']}")
