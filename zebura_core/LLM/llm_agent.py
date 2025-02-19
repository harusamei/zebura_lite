import sys,os, asyncio  
sys.path.insert(0, os.getcwd())
from settings import z_config
from zebura_core.LLM.llm_base1 import LLMBase
import logging

class LLMAgent(LLMBase):

    def __init__(self, agentName=None, model=None):
        # 默认使用config_lite.ini 中[Training]配置
        if agentName is None:
            agentName = z_config['Training', 'llm']
            model = z_config['Training', 'llm_model']
        if model is None or len(model) == 0:
            model = "gpt-3.5-turbo"
        try:
            super().__init__(agentName, model)
        except Exception as e:
            raise ValueError(f"LLM agent init error: {e.args[0]}")

    async def ask_llm_list(self, queries: list[str], prompts: list[str]) -> list[str]:
        # create a task list
        if len(queries) == 0:
            return []

        tasks = []
        # 只处理前1000
        for i, query in enumerate(queries[:1000]):
            query = query
            task = asyncio.create_task(self.ask_llm(query, prompts[i]))
            tasks = tasks + [task]

        print(f"total {len(tasks)} queries")

        # 每次只执行100个任务
        batch_size = 100
        for i in range(0, len(tasks), batch_size):
            await asyncio.gather(*tasks[i:i + batch_size])

        # get the result of the task
        results = [None] * len(tasks)
        for i, task in enumerate(tasks):
            answer = task.result()
            results[i] = answer

        return results

    async def ask_llm(self, query: str, content: str) -> str:

        if query is None or len(query) == 0:
            return ""
        
        if content is None or len(content) == 0:
            messages = [{"role": "user", "content": query}]
        else:
            messages = [{"role": "system", "content": content}]
            messages.append({"role": "user", "content": query})

        # 输出prompt 和 query check
        # cur_loglevel = logging.getLogger().getEffectiveLevel()
        # if cur_loglevel <= 20:
        #     outFile = 'output.txt'
        #     with open(outFile, 'a', encoding='utf-8') as f:
        #         for message in messages:
        #             f.write(f"{message['role']}: {message['content']}\n")
        #         f.write("----------------------------end\n")

        try:
            answer = self.postMessage(messages)
            return answer
        except Exception as e:
            return f"err_llm: {e.args[0]}"


# Example usage
if __name__ == '__main__':
    from prompt_loader1 import Prompt_generator
    import time

    questions = ["What is the price of a Lenovo Xiaoxin computer?",
              "How much does a Lenovo Xiaoxin computer cost?",
              "Which brand is the Xiaoxin computer?",
              "The weather is pretty nice today, don't you think?",
              "请问联想小新电脑多少钱",
              "联想小新电脑多少钱",
              "请问小新电脑是什么品牌的",
              "今天天气挺好的，你觉得呢？"]
    pg = Prompt_generator()
    tmpl = pg.get_prompt('nl_to_sql')
    agents = [
        {'name': 'AIMASTER', 'model': 'llama3.2-90B'},
        {'name': 'OPENAI', 'model': 'gpt-4o'},
        {'name': 'CHATANYWHERE', 'model': 'gpt-3.5-turbo'}
    ]
    for item in agents:
        start = time.time()
        query = tmpl.format(db_info='', examples='', question=questions[0])
        agent = LLMAgent(agentName=item['name'], model=item['model'])
        answers = asyncio.run(agent.ask_llm(query, ''))
        print(answers)
        print(f"single query time: {time.time() - start}")

    for item in agents:
        agent = LLMAgent(agentName=item['name'], model=item['model'])
        prompts = [tmpl.format(db_info='', examples='', question=question) for question in questions]
        start = time.time()
        results = asyncio.run(agent.ask_llm_list(questions, prompts))
        for i, result in enumerate(results):
            print(f"query:{questions[i]}\n{result}")
        print(f"batch query time: {int(time.time() - start)}")
    