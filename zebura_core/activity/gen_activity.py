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
from zebura_core.activity.sql_checker1 import CheckSQL
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
        self.checker = CheckSQL(dbServer, chat_lang)
        self.scha_loader = ScmaLoader(db_name, chat_lang)
        
        logging.info("GenActivity init done")

    # 主功能, 生成最终用于查询的SQL
    # sql 可以是sql statement, or sql statement and parsed sql with {tables, columns, values}
    async def gen_activity(self, query: str, sql: Union[str, Dict[str, Any]]):
        resp = make_a_log('gen_activity')
        if isinstance(sql, dict):
            sql_statement = sql.get('sql','')
        else:
            sql_statement = sql

        all_checks = await self.checker.check_sql(sql)
        checkMsg = '\n'.join(all_checks['msg'])
        # 完美的SQL
        if all_checks['status'] == 'succ' and 'Warning' not in checkMsg:
            resp['msg'] = self.refine_sql(sql_statement)
            return resp

        # 有EMTY 条件，values not found in column
        if all_checks['status'] == 'succ' and 'Warning' in checkMsg:
            conds_check = await self.refine_conds(all_checks)
            all_checks['values'] = conds_check

        all_checks['status'] = 'failed'
        new_sql, note = await self.revise(sql_statement, all_checks)
        resp['note'] = note
        if new_sql is None:
            resp['status'] = 'failed'
            resp['msg'] = "err_parsesql, wrong sql structure."
        else:
            resp['msg'] = new_sql
        return resp
    
    # 优化SQL TODO
    def refine_sql(self, sql: str) -> str:
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
        parsed = self.ans_extr.output_extr('data_exploration', result)
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
        
        checkMsgs = {'msg': '', 'db_prompt': ''}
        all_checks1 = all_checks.copy()
        if 'status' in all_checks1['tables']:
            all_checks1['tables'].pop('status')
        if 'status' in all_checks1['columns']:
            all_checks1['columns'].pop('status')
        if 'status' in all_checks1['values']:
            all_checks1['values'].pop('status')

        # 选择RAG需要的表
        tb_names = self.checker.gen_rel_tables(all_checks1)
        tb_prompts = self.scha_loader.gen_limited_prompt(max_prompt_len, tb_names)
        checkMsgs['db_prompt'] = '\n'.join(tb_prompts)
        
        msgs = [msg for msg in all_checks1['msg'] if 'Warning' in msg]
        tmpl = "Suggestion: can find '{word2}' in column '{col}' that is semantically similar to '{word1}'"
        for tkey,tval in all_checks1['values'].items():
            col, word1 = tkey.split(',')
            if 'EXPN' in tval:
                word2 = tval[1]
                msg = tmpl.format(word2=word2, col=col, word1=word1)
                msgs.append(msg)
        
        checkMsgs['msg'] = '\n'.join(msgs)
        
        return checkMsgs

    # check 不合格，需要revise, 返回 new_sql, hint
    async def revise(self, sql, all_checks) -> tuple:
        if all_checks['status'] == 'succ':
            return (sql, 'correct SQL')
        # {'msg': '', 'db_prompt': ''}
        checkMsgs = self.gen_checkMsgs1(all_checks)
        # revise by LLM
        tmpl = self.prompter.tasks['sql_revise']
        orisql = sql
        dbSchema = checkMsgs['db_prompt']
        errmsg = checkMsgs['msg']

        query = tmpl.format(dbSchema=dbSchema, ori_sql=orisql, err_msgs=errmsg)
        result = await self.llm.ask_llm(query, '')
        parsed = self.ans_extr.output_extr('sql_revise', result)

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
    # 只能补救列存在，但值找不到的情况
    async def refine_conds(self, all_check):
        conds_check = all_check['values']
        ni_words = []  # need improve terms, 列名OK，值找不到
        for cond in set(conds_check.keys())-set(['status']):
            v = conds_check[cond]
            if v[2] in ['EMTY'] and len(v)>3:
                col, word = cond.split(',')
                word = word.strip('\'"')
                ni_words.append([word,col, v[3]])  

        if len(ni_words) == 0:
            conds_check['status'] == 'succ'
            return conds_check
        
        ni_words.insert(0, ['Term', 'Category', 'Output Language'])
        term_table = self.prompter.gen_tabulate(ni_words)
        tmpl = self.prompter.tasks['term_expansion']
        query = tmpl.format(term_table=term_table)
        answ = await self.llm.ask_llm(query, '')
        result = self.ans_extr.output_extr('term_expansion', answ)
        
        if result['status'] == 'failed':
            conds_check['status'] == 'failed'
            return conds_check
        
        new_terms = result['msg']
        tb_check = all_check['tables']
        tbList = []
        for tb_key in set(tb_check.keys())-set(['status']):
            tbList.append(tb_check[tb_key])
        # 匹配忽略大小写, ['Term', 'Category', 'Output Language']
        ni_words_lower = [item[0] for item in ni_words]
        for tDict in new_terms:
            word = tDict.get('term', '')
            if word not in ni_words_lower:
                logging.info(f"incorrect term expansion: {word}")
                continue
            else:
                indx = ni_words_lower.index(word)
                ni_w = ni_words[indx]
                cond = f'{ni_w[1]},{ni_w[0]}'
            
            col, exps = tDict.get('category', ''), tDict.get('expansions', [])
            check = self.checker.check_expn(tbList, col, exps)
            # lang = conds_check[cond][3]
            # check.append(lang)
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

    