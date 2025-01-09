#####################
# 同步admdb中的cases to 系统ES
#####################
import os,sys, json, datetime,asyncio
import logging
sys.path.insert(0, os.getcwd())
from settings import z_config
import zebura_core.constants as const
from zebura_core.placeholder import make_dbServer
from zebura_core.elastic.es_operator1 import ESOps
from zebura_core.LLM.llm_agent import LLMAgent
from zebura_core.LLM.prompt_loader1 import Prompt_generator
from zebura_core.LLM.ans_extractor import AnsExtractor
from zebura_core.utils.embedding import Embedding
from dbaccess.db_ops import DBops
import pandas as pd

class Case_es_sync():

    # pj_name: 待同步项目名
    def __init__(self, pj_name=None):
        
        adm_serName = const.C_ADM_dbServer
        dbServer = make_dbServer(adm_serName)
        adm_dbName = z_config[adm_serName,'db_name']
        dbServer['db_name'] = adm_dbName.lower()

        self.dbServer = dbServer
        self.dbops = DBops(dbServer=dbServer)       # 连接adm数据库

        if pj_name is None:
            pj_name = z_config['Training','db_name']

        tb_name = const.Z_CASES_TBNAME.format(pj_name=pj_name)
        if self.dbops.is_table_exist(tb_name):
            print(f"Table {tb_name} exists in {dbServer['db_name']}")
        else:
            raise ValueError(f"Table {tb_name} not exists in {dbServer['db_name']}")
        # sync from tb_name to es_index
        self.srcTable = tb_name
        self.tgtIndex = const.Z_CASES_INDEX.format(pj_name =pj_name)
        # 默认ADMSearch 为ES
        self.esops = ESOps()
        adm_search = const.C_ADM_Search     # 'ADMsearch'
        self.esLang = z_config[adm_search,'search_lang']
        self.tyMapping = const.Z_CASES_TY_MAPPING.copy()
        # rdb update_date在此时间之后的数据才会被更新到ES
        self.after_time = datetime.datetime.now()-datetime.timedelta(days=const.Z_ALIGN_DAYS)
        self.embedding = None

        self.prompter = Prompt_generator()  # 使用默认的 en_prompt
        self.llm = LLMAgent()
        self.ans_ext = AnsExtractor()
        logging.debug("CaseOps init success")

    # 创建初始cases索引, drop=True时删除已有索引
    def create_index(self, drop=True):
        index_name = self.tgtIndex
        creator = self.esops.creator
        return creator.create_index(index_name, self.tyMapping, drop)
    
    # 将admdb中的cases同步到ES 目标索引
    async def insert_into_ES (self):

        doc_count = self.esops.get_doc_count(self.tgtIndex)
        if doc_count>0:
            sql = f"SELECT * FROM {self.srcTable} WHERE updated_date > '{self.after_time}' and deleted = FALSE"
        else:
            sql = f"SELECT * FROM {self.srcTable} WHERE deleted = FALSE"
        print(sql)
        cursor = self.dbops.cursor
        cursor.execute(sql)
        results = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        count = 0
        docs = []
        total = len(results)
        for row in results:
            doc = dict(zip(columns, row))
            docs.append(doc)
            count += 1
            if count == 10:
                docs = await self.complete_docs(docs)    
                self.esops.insert_docs(self.tgtIndex, docs)
                count = 0
                docs = []
        if len(docs) > 0:
            docs = await self.complete_docs(docs)    
            self.esops.insert_docs(self.tgtIndex, docs)
        doc_count1 = self.esops.get_doc_count(self.tgtIndex)
        print(f"added {doc_count1-doc_count} docs into {self.tgtIndex}")
        if doc_count1-doc_count != total:
            logging.error(f"Total {total} items in RDB, Added {doc_count1-doc_count} docs into {self.tgtIndex}")
        return

   # 删除 deleted = 'TRUE' 的doc
    def delete_from_ES(self):
        sql = f"SELECT * FROM {self.srcTable} WHERE deleted = TRUE"
        cursor = self.dbops.cursor
        cursor.execute(sql)
        results = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        count = 0
        total = len(results)
        for row in results:
            doc = dict(zip(columns, row))
            id = doc['id']
            response = self.esops.search_term(self.tgtIndex, 'id', id)
            hits = response['hits']['hits']
            if len(hits) == 0:
                print(f"Doc {id} not found in {self.tgtIndex}")
                continue
            for hit in hits:
                _id = hit['_id']
                self.esops.delete_doc(self.tgtIndex, _id)
                count += 1
        logging.info(f"Total {total} deleted items in RDB, Deleted {count} docs from {self.tgtIndex}")
        return

    # 补全 s_question, qemb
    async def complete_docs(self, batch_docs):

        # 多语言question统一翻译为esLang
        otherDocs = []
        for doc in batch_docs:
            if doc['lang'] == self.esLang:
                doc['s_question'] = doc['question']
            else:
                otherDocs.append(doc)

        tml = self.prompter.tasks['translation']
        s_txt = [{'text':doc['question'],'language':doc['lang']} for doc in otherDocs]
        input = json.dumps(s_txt, ensure_ascii=False)
        trgt_lang = self.esLang
        query = tml.format(input=input, trgt_lang=trgt_lang)
        answer = await self.llm.ask_llm(query, '')
        result = self.ans_ext.output_extr('translation', answer)
        if result['status'] == 'succ':
            for doc, aws in zip(otherDocs, result['msg']):
                doc['s_question'] = aws['target']
        else:
            logging.error('no translation', result['msg'])
        
        # 生成qemb
        if self.embedding is None:
            self.embedding = Embedding()
        texts = [doc['s_question'] for doc in batch_docs]
        embs = self.embedding.get_embedding(texts)
        for doc, emb in zip(batch_docs, embs):
            doc['qemb'] = emb.tolist()
        return batch_docs


# Example usage
if __name__ == '__main__':
    esAsync = Case_es_sync()
    esAsync.create_index(drop=True)
    asyncio.run(esAsync.insert_into_ES())
    esAsync.delete_from_ES()