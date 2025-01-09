import http.client
import json
import os
import sys
sys.path.insert(0, os.getcwd())
from settings import z_config
import openai

# openai 转发, https://peiqishop.cn/
# agentName： OPENAI, CHATANYWHERE
class LLMBase:
    _is_initialized = False
    agentName = None
    def __init__(self,agentName:str,model="gpt-3.5-turbo-ca", temperature=0):

        if LLMBase.agentName != agentName:
            LLMBase._is_initialized = True
            
            self.agentName = agentName.upper()
            self.temperature = temperature
            sk = z_config['LLM',f'{self.agentName}_KEY']
            
            self.model = model
            messages=[{'role': 'user', 'content': 'who are you and where are you from?'}]
            if self.agentName == 'OPENAI':
                openai.api_key=sk
                self.client = openai
                self.headers = None
            elif self.agentName == 'CHATANYWHERE':
                self.client = http.client.HTTPSConnection("api.chatanywhere.tech")
                self.headers = {
                        'Authorization': sk,
                        'User-Agent': 'Apifox/1.0.0 (https://apifox.com)',
                        'Content-Type': 'application/json'
                    }
            elif self.agentName == 'AZURE':
                url = "https://openai-lr-ai-platform-cv-ncus.openai.azure.com/openai/deployments/Intent4O/chat/completions?api-version=2024-02-01"
                self.client = http.client.HTTPSConnection("openai-lr-ai-platform-cv-ncus.openai.azure.com")
                self.headers = {
                        "Content-Type": "application/json",
                        "api-key": sk    
                    }
            else:
                raise ValueError("No available LLM agents, please check the agentName")
            # try:
            #     print(f"connect GPT through {agentName}\n Message for connection test:"+self.postMessage(messages))
            # except Exception as e:
            #     raise ValueError("LLM agent is not available",e)
            LLMBase.agentName = self.agentName
            LLMBase.temperature = self.temperature
            LLMBase.model = self.model
            LLMBase.client = self.client
            LLMBase.headers = self.headers
            
            print(f"connect GPT through {agentName}")

    # 不同的agent有不同的处理方式
    # 在OpenAI的GPT-3聊天模型中，`messages` 是一个列表，用于表示一系列的对话消息。
    # `role`：这个字段表示消息的发送者。它可以是 `"system"`、`"user"` 或 `"assistant"`。
    # "system"` 通常用于设置对话的初始背景，`"user"` 和 `"assistant"` 分别表示用户和助手的消息。
    # `content`：这个字段表示消息的内容，也就是实际的文本。

    def postMessage(self,messages:list):
        
        if self.agentName == 'CHATANYWHERE':
            payload = {"model": self.model}
            payload["messages"] = messages
            payload['temperature'] = self.temperature         # 温度设为0，表示模型会给出最可能的回复
            res = self.client.request("POST", "/v1/chat/completions", json.dumps(payload), self.headers)
            res = self.client.getresponse().read()
            res = json.loads(res.decode("utf-8"))
            data = res['choices'][0]['message']['content']
        elif self.agentName == 'OPENAI':
            res = self.client.ChatCompletion.create(
                                                        messages=messages,
                                                        model=self.model,
                                                        stop=["#;\n\n"],
                                                        temperature=self.temperature
                                                        )
            data = res.choices[0].message.content
        elif self.agentName == "AZURE":
            payload = {
                    "frequency_penalty": 0,
                    "presence_penalty": 0,
                    "top_p": 0.95
            }               
            payload["messages"] = messages
            payload['temperature'] = self.temperature

            res = self.client.request("POST", "/openai/deployments/Intent4O/chat/completions?api-version=2024-02-01", json.dumps(payload), self.headers)
            res = self.client.getresponse().read()
            res = json.loads(res.decode("utf-8"))
            data = res['choices'][0]['message']['content']
        return data

# Example usage
if __name__ == '__main__':
    agent = LLMBase(agentName='OPENAI',model="gpt-3.5-turbo")
    print(agent.postMessage([{'role': 'user', 'content': 'Who won the world series in 2020?'}]))
    agent = LLMBase('CHATANYWHERE')
    print(agent.postMessage([{'role': 'user', 'content': 'Who won the world series in 2020?'}]))
    agent = LLMBase('AZURE')
