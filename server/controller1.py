###########################################
# 与chatbot交互的接口, 内部是一个总控制器，负责调度各个模块最终完成DB查询，返回结果
############################################
import sys,os,json, time
sys.path.insert(0, os.getcwd().lower())

from tabulate import tabulate
import logging,asyncio,inspect
from settings import z_config
from zebura_core.nltosql.question2sql import Question2SQL
from zebura_core.answer_refiner.aggregate import Aggregate
from zebura_core.activity.exe_activity import ExeActivity
from zebura_core.activity.gen_activity import GenActivity
from zebura_core.LLM.llm_agent import LLMAgent
from zebura_core.placeholder import make_a_log, make_a_req

# 一个传递request的pipeline
# 从 Chatbot request 开始，到 type变为assistant 结束
class Controller:   
    # 一些应急话术
    utterance = {}
    with open("server/utterances.json", "r") as f:
        utterance = json.load(f)
    pj_name = z_config['Training', 'db_name']
    chat_lang = z_config['Training', 'chat_lang']
    parser = Question2SQL(pj_name=pj_name,chat_lang=chat_lang)         # nl2SQL
    act_maker = GenActivity()       # gen exectuable SQL
    executor = ExeActivity()        # query sql from DB
    answerer = Aggregate()          # give answer
    llm = LLMAgent()

    def __init__(self):

        self.prompter = Controller.parser.prompter  # prompt generator
        self.rel_tbnames = []                       # related table names
        self.matrix = {
            "(new,user)": self.nl2sql,
            "(hold,user)": self.rewrite,
            "(succ,rewrite)": self.nl2sql,          # rewrite question when multi-turn
            "(succ,nl2sql)": self.sql_refine,       # check sql before query db
            "(succ,sql4db)": self.polish,           # polish sql result
            "(succ,polish)": self.exploration,      # suggest next step
            "(succ,exploration)": self.end,         # happy path
            "(succ,sql_refine)": self.sql4db,       # query db
            "(succ,sql_correct)": self.sql4db,      # query DB again
            "(failed,transit)": self.end,           # end
            "(failed,*)": self.transit,             # reset action
            "(*,*)": self.end                       # whitelist principle: end anything not on the list
        }
        logging.info("Controller init success")
    
    def get_next(self, pipeline):
        
        lastLog = pipeline[-1]
        if lastLog['type'] == "chat":
            return self.end
        # 强制跳转
        if lastLog['type'] == "reset":
            method = getattr(self, lastLog['from'])
            lastLog['from']='transit'       # 恢复之前状态机转移时的占用
            return method

        curSt = f'({lastLog["status"]},{lastLog["from"]})'
        if curSt in self.matrix:
            return self.matrix[curSt]
        curSt = f'({lastLog["status"]},*)'
        if curSt in self.matrix:
            return self.matrix[curSt]
        curSt = '(*,*)'
        if curSt in self.matrix:
            return self.matrix[curSt]
        
        return self.end
    
    # reset state when some actions failed
    def transit(self, pipeline):
        # 默认转移到最后一个状态
        new_log = make_a_log("transit")
        new_log['type'] = "reset"
        new_log['question'] = pipeline[-1]['question']
        new_log['sql'] = pipeline[-1]['sql']

        fromList = [log['from'] for log in pipeline]
        frm = fromList[-1]
        stations = ['nl2sql', 'rewrite', 'sql_refine', 'sql_correct', 'sql4db', 'polish', 'exploration']
        # 不处理其它失败
        if frm not in stations:
            new_log['from'] = 'end'
            pipeline.append(new_log)
            return

        if frm == 'nl2sql':  # 处理 nl2sql 发起的跳转
            new_log['from'] = 'end'
            pipeline.append(new_log)
            return   
        
        if frm == 'rewrite':  # 处理 rewrite 发起的跳转
            new_log['from'] = 'end'
            pipeline.append(new_log)
            return
        
        if frm == 'sql_refine':  # 处理 sql_refine 发起的跳转
            new_log['from'] = 'sql4db'
            pipeline.append(new_log)
            return
        
        if frm == 'sql_correct':  # 处理 sql_correct 发起的跳转
            new_log['from'] = 'exploration'
            pipeline.append(new_log)
            return
        
        if frm == 'sql4db':  # 处理 sql4db 发起的跳转
            new_log['from'] = 'exploration'
            pipeline.append(new_log)
            return
        
        if frm == 'polish':  # 处理 polish 发起的跳转
            new_log['from'] = 'exploration'
            pipeline.append(new_log)
            return
        
        if frm == 'exploration':  # 处理 exploration 发起的跳转
            new_log['from'] = 'end'
            pipeline.append(new_log)
            return
        
        # 兜底转移到最后一个状态
        new_log['from'] = 'end'
        pipeline.append(new_log)
        return

    # question to sql
    async def nl2sql(self, pipeline):

        new_Log = make_a_log("nl2sql")
        query = pipeline[-1]['question']

        result = await self.parser.ques2sql(query,tb_names=self.rel_tbnames)
        new_Log = self.copy_to_log(result, new_Log)

        new_Log['question'] = query
        if new_Log['status'] =='succ' and new_Log['type'] == 'transaction':
            new_Log['sql'] = result['msg']
        else:
            new_Log['sql'] = ''
        pipeline.append(new_Log)

    # correct sql with db schema before query db
    async def sql_refine(self, pipeline):

        new_Log = make_a_log("sql_refine")
        log = pipeline[-1]
        new_Log['question'] = log['question']
        new_Log['sql'] = log['sql']
        query =log['question']
    
        if log['format'] == "sql" and isinstance(log['note'], dict):
           result = await self.act_maker.gen_activity(query, log['note'])
        elif log['format'] == "sql":
            result = await self.act_maker.gen_activity(query, log['msg'])

        new_Log = self.copy_to_log(result, new_Log)
        if new_Log['status'] =='succ':
            new_Log['sql'] = result['msg']
        pipeline.append(new_Log)

    # correct sql after query db failed
    async def sql_correct(self, pipeline):

        new_Log = make_a_log("sql_correct")
        log = pipeline[-1]
        new_Log['question'] = log['question']
        new_Log['sql'] = log['sql']
        sql = log['sql']
        err_msg = pipeline[-1]['note']
        result = await self.act_maker.correct_sql(sql, err_msg)

        new_Log = self.copy_to_log(result, new_Log)
        if new_Log['status'] =='succ':
            new_Log['sql'] = result['msg']
        pipeline.append(new_Log)

    # multi-turn, rewrite the question
    async def rewrite(self, pipeline):

        history = []
        log = pipeline[0]
        new_Log = make_a_log("rewrite")
        new_Log['question'] = log['question']

        context = log.get('context',[])
        if len(context) <1:  # 无上下文不必重写
            new_Log['status'] = "succ"
            pipeline.append(new_Log)
            return

        # 保留最近6轮的请求
        for one_req in context[-6:]:
            msg = f"{one_req['type']}: {one_req.get('msg')}"
            history.append(msg)

        history_context = "\n".join(history)
        query = log.get('msg','')
        tmpl = self.prompter.get_prompt('rewrite')
        # TODO, prompt 写得有问题
        prompt = tmpl.format(history_context=history_context, query=query)

        # outFile = 'output.txt'
        # with open(outFile, 'a', encoding='utf-8') as f:
        #     f.write(prompt)
        #     f.write("\n----------------------------end\n")

        result = await self.llm.ask_llm(prompt, "")
        if "err" in result:
            new_Log['status'] = "failed"
            new_Log['note'] = result
        else:
            new_Log['msg'] = result
            new_Log['question'] = result
        pipeline.append(new_Log)

    # query db
    def sql4db(self, pipeline):
        new_Log = make_a_log("sql4db")
        log = pipeline[-1]
        new_Log['question'] = log['question']
        new_Log['sql'] = log['sql']

        sql = log['sql']
        print(f"sql4db: {sql}")

        result = self.executor.exeSQL(sql)
        new_Log = self.copy_to_log(result, new_Log)
        pipeline.append(new_Log)

    # polish sql result: 1. markdown; 2. further query db for details
    async def polish(self, pipeline):
        new_Log = make_a_log("polish")
        log = pipeline[-1]
        new_Log['question'] = log['question']
        new_Log['sql'] = log['sql']

        fromList = [log['from'] for log in pipeline]
        # 步长-1, 反转，找到最后一个sql4db的位置
        if 'sql4db' not in fromList:
            new_Log['status'] = "failed"
            new_Log['note'] = "error: no sql4db before polish\n"
            pipeline.append(new_Log)
            return
        # 确保存在sql4db
        reverse_indx = fromList[::-1].index('sql4db')
        indx = len(fromList) - reverse_indx - 1
        # markdown
        log = pipeline[indx]
        markdown = tabulate(log['msg'], headers="keys", tablefmt="pipe")
        if len(markdown) == 0:
            new_Log['msg'] = str(log['msg'])
        else:
            new_Log['msg'] = markdown
            new_Log['format'] = 'md'
        
        pipeline.append(new_Log)
        return
     
    # 下一步探索
    async def exploration(self, pipeline):
        
        new_Log = make_a_log("exploration")
        log = pipeline[-1]
        new_Log['question'] = log['question']
        new_Log['sql'] = log['sql']

        fromList = [log['from'] for log in pipeline]
        if 'sql4db' not in fromList:
            new_Log['note'] = "error: no sql4db before exploration\n"
            new_Log['status'] = "failed"
            pipeline.append(new_Log)
            return
        # 步长-1, 反转，找到最后一个sql4db的位置
        reverse_indx = fromList[::-1].index('sql4db')
        indx = len(fromList) - reverse_indx - 1
        log = pipeline[indx]
        sql_result = log['msg']
        sql = log['sql']
        question  = log['question']
        # exploration
        result = await self.act_maker.exploration(sql, sql_result,tb_names=self.rel_tbnames)
        new_Log = self.copy_to_log(result, new_Log)
        new_Log['type'] = 'chat'
        pipeline.append(new_Log)
        return
    
    # 状态机终点
    def end(self):
        return "end"
    
    async def genAnswer(self, pipeline) -> dict:
        answ = self.answerer.gathering(pipeline)
        return answ

    def get_db_summary(self):
        scha_loader = self.parser.scha_loader
        return scha_loader.get_db_summary()
    
     # 当前问题涉及的表名
    def set_rel_tbnames(self, tbnames: list):
        self.rel_tbnames = tbnames
    
    def copy_to_log(self, result, new_log):
        keys_to_copy = (new_log.keys() & result.keys()) - {'from','sql','question'}
        for k in keys_to_copy:
            new_log[k] = result[k]
        return new_log


# 主函数, assign tasks to different workers
async def apply(request):
    controller = Controller()
    # 记录所有状态，包括transit，不删除任何状态
    # pipeline 中记录 question, sql的变化
    pipeline = list()
    new_log = make_a_log("user")
    new_log = controller.copy_to_log(request, new_log)
    new_log['question'] = request['msg']
    pipeline.append(new_log)

    nextStep = controller.get_next(pipeline)

    while nextStep != controller.end:
        if inspect.iscoroutinefunction(nextStep):
            await nextStep(pipeline)
        else:
            nextStep(pipeline)
        nextStep = controller.get_next(pipeline)
    #controller.interpret(pipeline)
    answ = await controller.genAnswer(pipeline)
    return answ

async def main():
    questions = ['演员 Christian Bale, Heath Ledger, Aaron Eckhart, Michael Caine 出演的电影中，哪部电影的 Metascore 最高？',
                '请告诉我盗梦空间的详细信息',
                'How many movies in the dataset have a revenue greater than 100 million dollars?',
                'What is the average metascore of the movies in the dataset?',
                '列出可以抽烟的餐厅']
       
    for msg in questions:
        start = time.time()
        request = make_a_req(msg)
        print(f"=============\nQuestion: {msg}")
        resp = await apply(request)
        print(f"Time: {time.time()-start}")
        for key in resp:
            if key in ['type','reasoning','question','sql','chat','error','suggestion','key_info']:
                print(f"{key}: {resp[key]}")
        print("=============")
    
   
if __name__ == "__main__":
    asyncio.run(main())
    