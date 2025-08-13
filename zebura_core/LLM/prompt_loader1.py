# 读取 prompt.txt中的指令模板，用于生成prompt
# prompt文件由base_prompt和lang_prompt组成, base_prompt为默认英文模板，lang_prompt为语言特定的补充模板
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

# prompt 模板通过文件导入，base prompt文件为当前目录下prompt.txt
class Prompt_generator():
    _is_initialized = False

    def __init__(self,lang=None):

        if not Prompt_generator._is_initialized:
            
            Prompt_generator._is_initialized = True
            self.tasks = {}   

            prompt_file = os.path.join(os.getcwd(), const.S_PROMPT_FILE)  # default prompt file
            lang_prompt = None
            if lang is None:
                lang = z_config['Training', 'chat_lang']
            langcode = langname2code(lang)
            # language special prompt file
            lang_prompt = prompt_file.replace('.txt', f'_{langcode}.txt')  
            if self.load_prompt(prompt_file, lang_prompt):
                logging.debug("Prompt_generator init success")
            else:
                logging.debug("no prompt file, only generate default prompt")

            Prompt_generator.tasks = self.tasks

    def load_prompt(self, base_prompt, lang_prompt=None):
        loadList = [base_prompt]
        if not os.path.exists(base_prompt):
            print(f"Prompt file {base_prompt} not found")
            return False
        if lang_prompt is not None and os.path.exists(lang_prompt):
            loadList.append(lang_prompt)
        # lang_prompt 补充或更新 prompt_file中的task
        for prompt_file in loadList:
            print(f"Loading prompt from {prompt_file}")
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
                        self.tasks[task_name] = "" 
                        content = ""
                    elif line.startswith("</TASK>"):
                        self.tasks[task_name] = content
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
    import asyncio

    llm = LLMAgent()
    pg = Prompt_generator('japanese')
    print(pg.tasks.keys())

    tmpl =pg.get_prompt('translation')
    input = [{
            "text": "你好，我是一个机器人。",
            "language": "Chinese"
            },
            {
                "text": "Bonjour le monde",
                "language": "French"
            }]
    query = tmpl.format(trgt_lang="Japanese", input=input)
    result = asyncio.run(llm.ask_llm(query, ''))
    print(result)