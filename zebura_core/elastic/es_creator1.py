# Description: 创建ES索引，insert docs
#######################################
import sys,os, re
sys.path.insert(0, os.getcwd())
from settings import z_config
import logging
from zebura_core.elastic.es_base import ES_BASE
from elasticsearch.exceptions import RequestError  # type: ignore
from zebura_core.constants import D_ES_SHORT_MAX,D_ES_BULK_MAX
from datetime import datetime
import pandas as pd
import numpy as np

# 创建索引
class CreateIndex(ES_BASE):

    def __init__(self, es_server=None,chat_lang='English'):
        
        super().__init__(es_server)
        # 可用的分词器,需要安装相应的分词器插件， 'Chinese': "zh_analyzer", 'Japanese': "ja_analyzer"
        self.anzr_names ={'English':'en_analyzer'} # available 分词器
        self.text_analyzer = self.anzr_names.get(chat_lang,'en_analyzer')
        # desen_vector长度
        self.embDim = z_config['Embedding','embDim']
        self.similarity = z_config['Embedding','similarity']

        logging.debug("CreateIndex init success")

    # 从CSV文件加载数据到ES, dtype显示字段类型
    # dtype={"col1": "int64", "col2": "float64", "col3": "str"}
    # type使用Dataframe类型, 除dense_vector
    def load_csv(self, index_name, csv_file, dtypes=None):
       
        df = pd.read_csv(f"{csv_file}",header=0,encoding='utf-8')
        df = df.convert_dtypes()
        df.dropna(axis=0, how='all', inplace=True)         # 删除全为空的行
        df = df.replace({np.nan: None})                    # ES不允许doc中有NaN

        if dtypes is None:
            dtypes = {}
            header = df.head(1)
            for fname in header.columns:
                dtypes[fname] = header[fname].dtype
        es_mapping = self.get_mapping_from_dtypes(dtypes)
        
        print(f"ES mapping: {es_mapping}")
        if (self.create_index(index_name, es_mapping, drop = True)) is False:
            return 0
        
        count_bef = self.get_doc_count(index_name)
        docs = []
        for _, row in df.iterrows():
            doc = self.format_doc(doc=row,es_mapping=es_mapping)
            docs.append(doc)
            if len(docs) >= D_ES_BULK_MAX:
                print(f"Inserting {len(docs)} docs...")
                self.insert_docs(index_name, docs)
                docs = []
        self.insert_docs(index_name, docs)
        count = self.get_doc_count(index_name)
        if len(docs) + count_bef == count:
            print(f"Total {count} docs inserted successfully.")
        else:
            print(f"Total {count-count_bef} docs inserted within {len(docs)} docs.")
        
    def define_settings(self):
        # 定义索引主分片个数和分析器
        settings = {
                "number_of_shards": 2,
                "number_of_replicas": "1",
                "analysis": {               # 目前只有英文分词器
                    # "tokenizer": {
                    #     "smartcn_tokenizer": {
                    #         "type": "standard"  # 使用ES内置中文分词器
                    #     },
                    #     "kuromoji_tokenizer": {
                    #         "type": "kuromoji_tokenizer"  # 使用ES内置日语分词器
                    #     }
                    # },
                    "analyzer": {
                        "en_analyzer": {
                            "type": "english"  # 使用内置的英文分析器
                        },
                        # "zh_analyzer": {
                        #     "type": "custom",
                        #     "tokenizer": "smartcn_tokenizer"  # 使用自定义的中文分析器
                        # },
                        # "ja_analyzer": {
                        #     "type": "custom",
                        #     "tokenizer": "kuromoji_tokenizer"  # 使用自定义的日语分析器
                        # },
                        "keyword_analyzer": {
                            "type": "custom",
                            "tokenizer": "keyword",  # 使用关键词分词器
                            "filter": ["lowercase"]  # 可选：将文本转换为小写
                        }
                    }
                }
        }
        return settings
    
    #dtype={"col1": "int64", "col2": "float64", "col3": "str"}
    def get_mapping_from_dtypes(self, dtypes) -> list[dict]:
        ty_mapping = {}
        for fname, ftype in dtypes.items():
            es_type = self.infer_dftype(ftype)
            ty_mapping[fname]={'type': es_type}
        es_mapping = self.gen_mapping(ty_mapping)
        return es_mapping
      
    @staticmethod
    # 从dataframe的header推断es mapping
    def infer_dftype(pandas_dtype):
        if pd.api.types.is_integer_dtype(pandas_dtype):
            return "integer"
        elif pd.api.types.is_float_dtype(pandas_dtype):
            return "float"
        elif pd.api.types.is_bool_dtype(pandas_dtype):
            return "boolean"
        elif pd.api.types.is_datetime64_any_dtype(pandas_dtype):
            return "date"
        else:
            return "text"
    
    # 用于生成es mapping创建索引
    # { "id": { "type": "keyword" },  "database_name": { "type": "keyword" },...}
    def gen_mapping(self, ty_mapping) -> dict:
        es_mapping = ty_mapping.copy()  # es mapping
        for fname,val in ty_mapping.items():
            ftype = val["type"]
            if ftype == "text":
                es_mapping[fname]["analyzer"] = self.text_analyzer
            elif ftype == "dense_vector":
                es_mapping[fname]["dims"] = self.embDim
                es_mapping[fname]["similarity"]= self.similarity
            elif ftype == "keyword":
                es_mapping[fname]["analyzer"] = "keyword_analyzer"

        return es_mapping
    
    # 创建索引, drop=True 存在则删除
    def create_index(self, indx_name, es_mapping, drop=False):

        if self.is_index_exist(indx_name):
            print(f"Index '{indx_name}' already exists.")
            if drop:
                print(f"Drop index '{indx_name}' first.")
                self.drop_index(indx_name)
            else:
                print('skip it')
                return True
        
        body = es_mapping
        # 定义索引主分片个数和分析器
        settings = self.define_settings()
        indx_body = {
            "settings": settings,
            "mappings": {
                "properties": body
            }
        }
        print(f"Creating index '{indx_name}'...")
        # 索引的每个field都可以设置不同的analyzer
        try:
            self.es.indices.create(index=indx_name, body=indx_body)
            logging.info(f"Index '{indx_name}' created successfully with 5 primary shards.")
        except RequestError as e:
            logging.error(e)
            return False
        return True

    
    # 返回ES操作文档数量
    # docs是一个list，每个元素是一个dict
    def insert_docs(self, index_name, docs):
        response ={}
        count = 0
        if docs is None or len(docs) == 0:
            logging.error(f"no docs to insert")
            response['errors'] = True
            return response

        if len(docs) > D_ES_BULK_MAX:
            logging.debug("Warning: too much docs, only insert frist {D_ES_BULK_MAX}")
            docs = docs[:D_ES_BULK_MAX]
        try:
            new_docs = list(map(lambda doc: [
                {"index": {"_index": index_name}},
                doc],
                                docs))
            new_docs = sum(new_docs, [])
            # 在索引中添加文档, refresh=True 使得文档立即可见
            response = self.es.bulk(index=index_name, body=new_docs, refresh='true')
            if response['errors']:
                logging.info(f"Error inserting documents: {response}")
                for item in response['items']:
                    if 'index' in item and item['index']['status'] == 201:
                        count += 1
            else:
                count = len(docs)
        except Exception as e:
            print(f"Error inserting documents: {e}")
            response['errors'] = True
       
        if len(docs) != count:
            print(f"Warring: total docs are {len(docs)}, insert {count}")
        return count

    # 格式规范化
    def format_doc(self, doc, es_mapping) -> dict:
        # 删除空值的字段和不在mapping中的字段
        doc = {key: value for key, value in doc.items() if value != '' and key in es_mapping}
        if doc is None or len(doc) == 0:
            return None
        # data type check, 日期，整数，浮点数
        numType = ['short','float', 'integer', 'long', 'double', 'half_float', 'scaled_float', 'byte']
        dateSet = [key for key, v in es_mapping.items() if v['type'] == 'date']
        numSet = [key for key, v in es_mapping.items() if v['type'] in numType]
        boolSet = [key for key, v in es_mapping.items() if v['type'] == 'boolean']

        # date format
        now = datetime.now()
        for key in dateSet:
            t_date = doc.get(key, now.date())
            # default yyyy-mm-dd
            fmt_str = '%Y-%m-%d'
            if isinstance(t_date, datetime):
                doc[key] = t_date.strftime(fmt_str)
                continue

            date_str = str(t_date)
            if re.match(r'\d{4}/\d{2}/\d{2}', date_str):
                date_obj = datetime.strptime(date_str, '%Y/%m/%d')
            elif re.match(r'\d{4}-\d{2}-\d{2}', date_str):
                date_obj = datetime.strptime(date_str, '%Y-%m-%d')
            else:
                date_obj = now.date()
            doc[key] = date_obj.strftime(fmt_str)

        # digit format
        for key in doc.keys():
            if not isinstance(doc[key], str):
                continue
            if key in numSet and es_mapping[key]['type'] == 'short':
                doc[key] = re.sub(r'\D', '', doc[key])
                doc[key] = min(int(doc[key]), D_ES_SHORT_MAX)
            elif key in numSet:
                doc[key] = re.sub(r'[^\d.]', '', doc[key])
                doc[key] = float(doc[key])

        # boolean format
        for key in boolSet:
            value = doc.get(key)
            if value in [0, '0', 'false', 'False', False]:
                doc[key] = False
            elif value in [1, '1', 'true', 'True', True]:
                doc[key] = True
            else:
                doc[key] = False

        return doc

    
# examples usage
if __name__ == '__main__':
    from zebura_core.placeholder import make_esServer

    cwd = os.getcwd()
    csv_file = os.path.join(cwd, 'training\gcases.csv')
    es_server = make_esServer('ADMsearch')
    creator = CreateIndex(es_server)
    creator.load_csv('mz_imdb1', csv_file)
    