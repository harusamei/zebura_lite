import http.client
import httpx
from openai import OpenAI
import json
import os
import sys
sys.path.insert(0, os.getcwd())
from settings import z_config


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
            url = z_config['LLM',f'{self.agentName}_URL']
            self.headers = None
            self.model = model

            if self.agentName == 'OPENAI':
                self.client = OpenAI(
                    api_key=sk,  # This is the default and can be omitted
                )
            elif self.agentName == 'CHATANYWHERE':
                self.client = http.client.HTTPSConnection("api.chatanywhere.tech")
                self.headers = {
                        'Authorization': sk,
                        'User-Agent': url,
                        'Content-Type': 'application/json'
                }
            elif self.agentName == 'AZURE':
                self.client = http.client.HTTPSConnection("openai-lr-ai-platform-cv-ncus.openai.azure.com")
                self.headers = {
                        "Content-Type": "application/json",
                        "api-key": sk    
                }
            elif self.agentName == 'AIMASTER':
                client_verify = httpx.Client(verify=False)
                self.client = OpenAI(api_key=sk, base_url=url, http_client=client_verify)
            else:
                raise ValueError("No available LLM agents, please check the agentName")
            
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
            
            res = self.client.chat.completions.create(
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
        elif self.agentName == "AIMASTER":
            completion = self.client.chat.completions.create(
                    model="llama3.2-90B",
                    messages=messages,
                    stream=False,
                    temperature=self.temperature
                )
            data = completion.choices[0].message.content
        return data

# Example usage
if __name__ == '__main__':
    messages=[{'role': 'user', 'content': 'are you a AI language model?  please tell me your model details'}]
    #messages=[{'role': 'user', 'content': 'Who won the world series in 2020?'}]
    # agent = LLMBase(agentName='OPENAI',model="gpt-4o")
    # print(agent.postMessage(messages))
    # agent = LLMBase('CHATANYWHERE')
    # print(agent.postMessage(messages))
    agent = LLMBase('AIMASTER')
    print(agent.postMessage(messages))
