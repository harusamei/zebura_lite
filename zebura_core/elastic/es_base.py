# ES 各种操作的基类
# 1. 连接ES；2. 判断index是否存在；3. 判断字段是否存在；4. 获取所有字段；5. 获取所有index
###################################
import os
import sys
sys.path.insert(0, os.getcwd())
import zebura_core.constants as const
import logging
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
from zebura_core.placeholder import make_esServer
import pandas as pd

class ES_BASE:
    def __init__(self,es_server=None):
        if es_server is None:
            es_name = const.C_ADM_Search
            es_server = make_esServer(es_name)

        host = es_server['host']
        port = es_server['port']
        auth = es_server['auth']
        
        if auth == 'True':
            user =  es_server['user']
            pwd =  es_server['pwd']
            ca_path =  es_server['ca_path']
            self.es = Elasticsearch(
                "https://"+host+":"+str(port),
                ca_certs=ca_path,
                basic_auth=(user, pwd)
            )
        else:
            self.es = Elasticsearch(hosts=[{'host': host, 'port': port,'scheme': 'http'}])
        if not self.es.ping():
            raise ValueError("Connection failed")
        
        self.es_version = f"es version: {self.es.info()['version']['number']}"
        logging.debug("ES_BASE init success")
        logging.info(self.es_version)
                    
    @property
    def all_indices(self):
        return self.es.cat.indices(format='json')
    
    def es_mapping(self, indx_name):
        if not self.is_index_exist(indx_name):
            return None
        result = self.es.indices.get_mapping(index=indx_name)
        return result[indx_name]['mappings']['properties']
    
    def get_all_fields(self,indx_name) -> list:
        try:
            mapping= self.es.indices.get_mapping(index=indx_name)
        except Exception as e:
            logging.error(f"Error: {e}")
            return []
        fields = mapping[indx_name]['mappings']['properties']
        list = []
        for fd in fields.keys():
            list.append({'fname': fd, 'ftype':fields[fd]['type']})
        return list

    def get_field_type(self, index_name, field_name):
        
        fdlist = self.get_all_fields(index_name)
        if len(fdlist) == 0:
            return None
        field = next((fd for fd in fdlist if fd['fname'] == field_name), None)
        if field:
            return field['ftype']
        else:
            return None

    def get_doc_count(self, index_name):
        if self.is_index_exist(index_name):
            return self.es.count(index=index_name)['count']
        else:
            return -1
 
    # 输出index中的docs的fields项, 类似 select * from index limit max_size
    def select_all(self, index_name, fields=None, max_size=10):
        
        if self.is_index_exist(index_name) == False:
            logging.error(f"Index '{index_name}' not exist.")
            return None
        
        scroller = scan(self.es, index=index_name, query={"query": {"match_all": {}}}, size=max_size)
        results = []
        for i, hit in enumerate(scroller):
            if i >= max_size:
                break
            results.append(hit['_source'])
        df = pd.DataFrame(results)
        cols = df.columns
        if fields != None:
            fields = set(fields) & set(cols)
            fields = list(fields)
        else:
            fields = cols
        return df[fields]
    
    #是否存在index
    def is_index_exist(self,index_name):

        if self.es.indices.exists(index=index_name):
            return True
        else: 
            return False

    def drop_index(self, indx_name):
        if self.is_index_exist(indx_name):
            result = self.es.indices.delete(index=indx_name)
            logging.info(f"Index '{indx_name}' deleted successfully.")
        else:
            logging.error(f"Index '{indx_name}' not exist.")
        return
     
    #是否缺失需要的字段
    def is_field_exist(self,indx_name, fd_name):

        if not self.is_index_exist(indx_name):
            return False
        fields = self.get_all_fields(indx_name)
        fd_names = [fd['fname'] for fd in fields]
        if fd_name in fd_names:
            return True
        else:
            return False

# Example usage
if __name__ == '__main__':

    es = ES_BASE()
    indices = es.all_indices
    indices = [item['index'] for item in indices]
    print(indices)
    print(es.es_mapping('mz_imdb1'))
    print(es.get_all_fields('mz_imdb1'))
    
    print(es.get_field_type('mz_imdb1','target_users'))
    print(es.get_doc_count('target_users'))
    print(es.is_index_exist('target_users'))
    print(es.is_field_exist('target_users','filename'))

    print(es.select_all('mz_imdb1',fields=['question','sql'],max_size=5))   
    