import http.client
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib.parse import urlparse
import httpx
from openai import OpenAI
import os
import sys
sys.path.insert(0, os.getcwd())
from settings import z_config

# agentName： OPENAI, CHATANYWHERE, AIMASTER
class LLMBase:
    _is_initialized = False
    agentName = None
    # aimaster model = llama3.2-90B
    def __init__(self,agentName:str,model="gpt-3.5-turbo", temperature=0.3):

        if LLMBase.agentName != agentName:
            LLMBase._is_initialized = True
            
            self.agentName = agentName.upper()
            self.temperature = temperature
            self.session = self.create_session_with_retries()

            sk = z_config['LLM',f'{self.agentName}_KEY']
            self.url = z_config['LLM',f'{self.agentName}_URL']
            parsed_url = urlparse(self.url)
            self.headers = {'Content-Type': 'application/json'}
            self.model = model

            if self.agentName == 'OPENAI':
                self.client = OpenAI(api_key=sk)  # This is the default and can be omitted
            elif self.agentName == 'AIMASTER':
                client_verify = httpx.Client(verify=False)
                self.client = OpenAI(api_key=sk, base_url=self.url, http_client=client_verify)
            elif self.agentName == 'CHATANYWHERE':
                self.client = http.client.HTTPSConnection(parsed_url.hostname, timeout=5) #"api.chatanywhere.tech"
                self.headers['Authorization'] = sk
                self.headers['User-Agent'] = "Apifox/1.0.0 (https://apifox.com)"
            else:
                raise ValueError("No available LLM agent, please check the agentName")
            
            LLMBase.agentName = self.agentName
            LLMBase.temperature = self.temperature
            LLMBase.model = self.model
            LLMBase.client = self.client
            LLMBase.headers = self.headers
            LLMBase.url = self.url
            LLMBase.session = self.session
            
            print(f"connecting LLM through {agentName}")
            print(f"model: {model}, temperature: {temperature}")

    def create_session_with_retries(self):
        retry_strategy = Retry(
            total=3,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS", "POST"],
            backoff_factor=1
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session = requests.Session()
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session
    
    def postMessage(self,messages:list):
        
        if self.agentName == 'CHATANYWHERE':
            
            payload = {"model": self.model}
            payload["messages"] = messages
            payload['temperature'] = self.temperature         # 温度设为0，表示模型会给出最可能的回复
            res = self.session.post(self.url, json=payload, headers=self.headers)
            res.raise_for_status
            res = res.json()
            #res = json.loads(res.decode("utf-8"))
            data = res['choices'][0]['message']['content']
        elif self.agentName in ['OPENAI','AIMASTER']:    
            res = self.client.chat.completions.create(
                                                    messages=messages,
                                                    model=self.model,
                                                    stop=["#;\n\n"],
                                                    temperature=self.temperature
                                            )
            data = res.choices[0].message.content
        else:
            raise ValueError("No available LLM agents, please check the agentName")
        return data

# Example usage
if __name__ == '__main__':
    messages=[{'role': 'user', 'content': 'are you a AI language model?  please tell me your model details'}]
    #messages=[{'role': 'user', 'content': 'Who won the world series in 2020?'}]
    
    agents = [
        {'name': 'AIMASTER', 'model': 'llama3.2-90B'},
        {'name': 'OPENAI', 'model': 'gpt-4o'},
        {'name': 'CHATANYWHERE', 'model': 'gpt-3.5-turbo'}
    ]
    for item in agents:
        try:
            agent = LLMBase(agentName=item['name'],model=item['model'])
        except Exception as e:
            print(f"Error connecting to {item['name']}")
            continue
        try:
            print(agent.postMessage(messages))
        except Exception as e:
            print(f"Error posting message to {item['name']}")
            continue
        
