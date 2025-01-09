########################################233
# 根据query和生成的SQL确定查询DB的activity
# 一个NL2SQL转换的SQL 需要check 表名，列名，值，条件， revise SQL
############################################
import sys,os
sys.path.insert(0, os.getcwd().lower())
from settings import z_config
from zebura_core.constants import D_MAX_PROMPT_LEN as max_prompt_len
from zebura_core.placeholder import make_dbServer, make_a_log
from zebura_core.LLM.prompt_loader1 import Prompt_generator
from zebura_core.knowledges.schema_loader_lite import ScmaLoader
from zebura_core.LLM.ans_extractor import AnsExtractor
from zebura_core.LLM.llm_agent import LLMAgent
from zebura_core.activity.sql_checker import CheckSQL
from zebura_core.utils.conndb import connect
import logging,asyncio
from typing import Union, Dict, Any

class GenActivity:

    prompter = Prompt_generator()
    ans_extr = AnsExtractor()
    llm = LLMAgent()
    
    def __init__(self):
       
        serverName = z_config['Training', 'server_name']
        db_name = z_config['Training', 'db_name']
        chat_lang = z_config['Training', 'chat_lang']
        dbServer = make_dbServer(serverName)
        dbServer['db_name'] = db_name
        
        self.prompter = GenActivity.prompter
        self.ans_extr = GenActivity.ans_extr
        self.llm = GenActivity.llm
        self.checker = CheckSQL(dbServer,chat_lang)
        self.scha_loader = ScmaLoader(db_name, chat_lang)
        
        logging.info("GenActivity init done")

    # 主功能, 生成最终用于查询的SQL
    # sql 可以是sql statement, or sql statement and parsed sql with {tables, columns, values}
    async def gen_activity(self, query:str, sql:Union[str,Dict[str, Any]]):
        resp = make_a_log('gen_activity')

        all_checks = await self.checker.check_sql(sql)
        checkMsg = '\n'.join(all_checks['msg'])
        # 完美的SQL
        if all_checks['status'] == 'succ' and 'Warning' not in checkMsg:
            resp['msg'] = self.refine_sql(sql)
            return resp

        # 有EMTY 条件，需要修正
        if all_checks['status'] == 'succ' and 'Warning' in checkMsg:
            conds_check = await self.refine_conds(all_checks)
            all_checks['conds'] = conds_check

        all_checks['status'] = 'failed'
        new_sql, note = await self.revise(sql, all_checks)
        resp['note'] = note
        if new_sql is None:
            resp['status'] = 'failed'
            resp['msg'] = "err_parsesql, wrong sql structure."
        else:
            resp['msg'] = new_sql
        return resp
    
    # 优化SQL TODO
    def refine_sql(self, sql):
        if ';' not in sql:
            sql = sql+';'
        
        # hasFunc, hasLimit = False, False
        # if 'limit' in sql.lower():
        #     hasLimit = True
        # if 'count(' in sql.lower() or 'sum(' in sql.lower():
        #     hasFunc = True
        # if not hasFunc and not hasLimit:
        #     sql = sql.replace(';', f' LIMIT {k_limit} ;')
        return sql
    
    async def exploration(self, sql, result, tb_names=None) -> dict:
        # 生成SQL2DB的1个或多个SQL, 构成一个activity
        resp = make_a_log('exploration')
        resp['msg'] = sql
        db_prompts = self.scha_loader.gen_limited_prompt(max_prompt_len, tb_names)
        dbSchema ='\n'.join(db_prompts)
        tmpl = self.prompter.tasks['data_exploration']
        query = tmpl.format(db_info=dbSchema, sql_query=sql, sql_result=result)
        result = await self.llm.ask_llm(query, '')
        parsed = self.ans_extr.output_extr('data_exploration',result)
        resp['status'] = parsed['status']
        if parsed['status'] == 'succ':
            questions = parsed['msg'].get('questions', [])
            actions = parsed['msg'].get('actions', [])
            resp['msg'] = questions
            resp['note'] = actions
        else:
            resp['msg'] = parsed['msg']
        return resp
    
    # 在refine_sql基础上,对SQL输出的结果增加详细信息
    # 废弃
    async def detailed_sql(self, sql) -> dict:

        needFlag = False
        for word in ['(','where']:      # 有函数或者where条件，需要详细信息
            if word in sql.lower():
                needFlag = True
                break
        # 没有函数，也没有where条件，不需要详细信息
        if not needFlag:
            return { 'msg': '', 'status': 'failed'}
        
        all_checks = await self.checker.check_sql(sql)
        if all_checks['status'] != 'succ':
            return { 'msg': '', 'status': 'failed'}
        
        rel_tables = [tb for tb in all_checks['tables'].keys() if tb != 'status']
        db_prompts = self.scha_loader.gen_limited_prompt(max_prompt_len, rel_tables)
        dbSchema ='\n'.join(db_prompts)

        tmpl = self.prompter.tasks['sql_details']
        query = tmpl.format(dbSchema=dbSchema, sql_statement=sql)
        result = await self.llm.ask_llm(query, '')
        parsed = self.ans_extr.output_extr('sql_details',result)
        
        return parsed

    # 生成check功能发现的错误信息和tables粒度的prompt, 用于LLM修正
    def gen_checkMsgs1(self, all_checks):
        # 选择RAG需要的表
        if 'status' in all_checks['tables']:
            del all_checks['tables']['status']
        if 'status' in all_checks['columns']:
            del all_checks['columns']['status']
        tb_names = self.checker.gen_rel_tables(all_checks)
        tb_prompts = self.scha_loader.gen_limited_prompt(max_prompt_len,tb_names)

        checkMsgs = {'msg': '', 'db_prompt': ''}
        msg = '\n'.join(all_checks['msg'][:-1])
        
        checkMsgs['msg'] = msg
        checkMsgs['db_prompt'] = '\n'.join(tb_prompts)
        return checkMsgs

    # check 不合格，需要revise, 返回 new_sql, hint
    async def revise(self, sql, all_checks) -> tuple:
        if all_checks['status'] == 'succ':
            return (sql, 'correct SQL')

        # {'msg': '', 'db_struct': ''}
        checkMsgs = self.gen_checkMsgs1(all_checks)
        
        # revise by LLM
        tmpl = self.prompter.tasks['sql_revise']
        orisql = sql
        dbSchema = checkMsgs['db_prompt']
        errmsg = checkMsgs['msg']

        query = tmpl.format(dbSchema=dbSchema, ori_sql=orisql, err_msgs=errmsg)
        result = await self.llm.ask_llm(query, '')
        parsed = self.ans_extr.output_extr('sql_revise',result)

        # outFile = 'output.txt'
        # with open(outFile, 'a', encoding='utf-8') as f:
        #     f.write(query)
        #     f.write(result)
        #     f.write("\n----------------------------end\n")

        new_sql = parsed['msg']
        msg = new_sql
        if parsed['status'] == 'failed':
            new_sql = None
            msg = "err_llm: revised failed. "
        else:
            msg ='revised success'
        return new_sql, msg

    # term expansion to refine the equations in Where
    async def refine_conds(self, all_check):
        conds_check = all_check['conds']
        if conds_check['status'] == 'succ':
            return conds_check

        ni_words = {}  # need improve terms
        for cond, v in conds_check.items():
            if v == True:
                continue
            if v[2] in ['varchar', 'virtual_in', 'text']:
                col, word = cond.split(',')
                word = word.strip('\'"')
                ni_words[word] = [col, cond, v[3]]

        if len(ni_words) == 0:
            conds_check['status'] == 'succ'
            return conds_check
        
        kterms =[[key, val[0], val[2]] for key,val in ni_words.items()]
        kterms.insert(0, ['Keyword', 'Category','Output Language'])
        query = self.prompter.gen_tabulate(kterms)
        prompt = self.prompter.tasks['term_expansion']
        result = await self.llm.ask_llm(query, prompt)
        parsed = self.ans_extr.output_extr('term_expansion', result)
        
        if parsed['status'] == 'failed':
            conds_check['status'] == 'failed'
            return conds_check
        
        new_terms = parsed['msg']
        table = all_check['table'].keys()
        tname = ' '.join(table).replace('status', '')
        tname = tname.strip()
        # 匹配忽略大小写
        ni_words_lower = {key.lower(): value for key, value in ni_words.items()}
        for word, voc in new_terms.items():
            word = word.lower()
            tItem = ni_words_lower.get(word, None)
            if tItem is None:
                logging.error(f"Error: {word} not in ni_words")
                continue
            col, cond = tItem[0],tItem[1]
            check = self.checker.check_expn(tname, col, voc)
            lang = conds_check[cond][3]
            check.append(lang)
            conds_check[cond] = check
        return conds_check

# use example
if __name__ == "__main__":
    gentor = GenActivity()
    # qalist = [('当前数据库有多少张表',"SELECT table_name FROM zebura_stock;"),
    #           ('有多少种不同的产品类别？',  "SELECT COUNT(rating) AS rating_count, AVG(rating) AS average_rating FROM product WHERE category LIKE '%fan%';"),
    #           ('请告诉我苹果产品的类别', 'SELECT DISTINCT category\nFROM product\nWHERE brand = "Apple";'),
    #           ('请告诉我风扇的所有价格', 'SELECT actual_price, discounted_price FROM product WHERE category = "风扇";'),
    #           ('查一下价格大于1000的产品', 'SELECT *\nFROM product\nWHERE actual_price = 1000 AND brand = "苹果";'),
    #           ('列出品牌是电脑的产品名称', "SELECT product_name\nFROM product\nWHERE brand LIKE '%apple%';")]
    # for q, a in qalist:
    #     resp = asyncio.run(gentor.gen_activity(q, a))
    #     print(f"query:{q}\nresp:{resp['msg']}")
    
    sql = "SELECT *\nFROM product\nWHERE actual_price = 1000 AND brand = '苹果';"
    resp = asyncio.run(gentor.exploration(sql,''))
    print(f"query:{sql}\nresp:{resp}")

    