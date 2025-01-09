# 读取 prompt.txt中的指令模板，用于生成prompt
############################################
import os
import sys
sys.path.insert(0, os.getcwd())
from settings import z_config
import logging
import re
from tabulate import tabulate
import zebura_core.constants as const
from zebura_core.utils.lang_detector import langname2code

# prompt 模板通过文件导入，默认文件为当前目录下prompt.txt
class Prompt_generator():
    _is_initialized = False

    def __init__(self,lang=None):

        if not Prompt_generator._is_initialized:
            
            Prompt_generator._is_initialized = True
            self.tasks = {}   

            prompt_file = os.path.join(os.getcwd(), const.S_PROMPT_FILE)  # default prompt file
            if lang is None:
                lang = z_config['Training', 'chat_lang']
            langcode = langname2code(lang)
            if langcode in ['zh','ja']:    # 只支持中日语言模板，其余使用默认英语模板
                prompt_file = prompt_file.replace('.txt', f'_{langcode}.txt')  # language special prompt file

            if self.load_prompt(prompt_file):
                logging.debug("Prompt_generator init success")
            else:
                logging.debug("no prompt file, only generate default prompt")

            Prompt_generator.tasks = self.tasks

    def load_prompt(self, prompt_file):
        if not os.path.exists(prompt_file):
            print(f"Prompt file {prompt_file} not found")
            return False
        print(f"Loading prompt from {prompt_file}")

        tList = []
        content = ""
        with open(prompt_file, "r", encoding='utf-8') as f:
            lines = f.readlines()
            for line in lines:
                if line.startswith("//"): # 注释
                    continue
                if line.startswith("<TASK:"):
                    task_name = line.split(":")[1].strip()
                    task_name = re.sub(r'[^\w]', '', task_name)
                    task_name = task_name.lower()
                    self.tasks[task_name.lower()] = ""
                    content = ""
                elif line.startswith("</TASK>"):
                    self.tasks[task_name] = content
                    tList.append(task_name)
                else:
                    content += line
        return True
    
    # 得到Prompt
    def get_prompt(self,taskname):
        return self.tasks.get(taskname, f"please do {taskname}")
    
    @staticmethod
    def gen_tabulate(data):
        # 生成简单表格
        return tabulate(data, headers="firstrow", tablefmt="pipe")


# Example usage
if __name__ == '__main__':
    from zebura_core.LLM.llm_agent import LLMAgent
    from zebura_core.knowledges.schema_loader_lite import ScmaLoader
    import asyncio

    llm = LLMAgent()
    pg = Prompt_generator('japanese')
    print(pg.tasks.keys())

    sc = ScmaLoader('IMDB')
    prompt =pg.get_prompt('term_expansion')
    keywords = "product, price, 笔记本, 联想小新, lenovo, computer"
    result = asyncio.run(llm.ask_llm(keywords,prompt))
    print(result)

    prompt = pg.tasks['sql_revise']
    db_prompt = sc.get_db_prompt()
    orisql = "SELECT product_name\nFROM produt\nWHERE category = '电脑';"
    errmsg = ("table name errors:\n"
              "no 'produt' table found in the database schema.\n"
              "conditions errors:\n"
              "value '电脑' was not found in the field category. ")
    query = prompt.format(dbSchema=db_prompt, ori_sql=orisql, err_msgs=errmsg)
    result = asyncio.run(llm.ask_llm(query, ''))
    print(query)
    print(result)