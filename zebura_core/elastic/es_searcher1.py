# 搜索以及结果的基本输出格式
################################
import sys
import os
sys.path.insert(0, os.getcwd())
from settings import z_config
import logging
from zebura_core.elastic.es_base import ES_BASE
from zebura_core.utils.embedding import Embedding
from zebura_core.constants import D_TOP_K
import pandas as pd

class ESearcher(ES_BASE):

    def __init__(self,es_server=None):
        super().__init__(es_server)
        self.embedding = None
        # embedding model及属性必须与createIndex中的一致
        self.embDim = z_config['Embedding','embDim']
        self.similarity = z_config['Embedding','similarity']
        logging.debug("ESearcher init success")

    def try_search(self, index, query):
        try:
            return self.es.search(index=index, body=query)
        except Exception as e:
            logging.error(e)
        return None

    def search(self, index, field, val, size=D_TOP_K):
        ty = self.get_field_type(index, field)
        if ty is None:
            return None
        # vector search
        if ty == 'dense_vector':
            if isinstance(val, str):
                if self.embedding is None:
                    self.embedding = Embedding()
                embs = self.embedding.get_embedding(val)
            else:       # val is a list of embeddings
                embs = val
            return self.search_vector(index, field, embs, size)
        else:
            return self.search_word(index, field, val, size)

    def search_word(self, index, field, word, size=D_TOP_K):
        query = {
            "size": size,
            "query": {
                "match": {field: word}
            }
        }
        return self.try_search(index, query)
       
    def search_term(self, index, field, word, size=D_TOP_K):
        query = {
            "size": size,
            "query": {
                "term": {field: word}
            }
        }
        return self.try_search(index, query)

    def search_vector(self, index, field, embs, size=D_TOP_K):

        if not self.is_field_exist(index, field):
            logging.error(f"Field {field} not found in index {index}")
            return None
        if self.similarity == "cosine":
            query = self.generate_cosine_query(field, embs, size)
        else:
            query = self.generate_knn_query(field, embs, size)
        return self.try_search(index, query)

    # 通过ES的系统_id查询
    def search_by_id(self, index, doc_id):
        query = {
            "query": {
                "term": {
                    "_id": doc_id
                }
            }
        }
        return self.try_search(index, query)

    @staticmethod
    def generate_knn_query(field_name, vec, size):

        return {
            "knn": {
                "field": field_name,
                "query_vector": vec,
                "k": 100,
                "num_candidates": 100,
                "boost": 1
            },
            "size": size
        }

    @staticmethod
    def generate_cosine_query(field_name, vec, size):

        return {
            "query": {
                "script_score": {
                    "query": {"match_all": {}},
                    "script": {
                        "source": f"cosineSimilarity(params.queryVector, '{field_name}') + 1.0",
                        "params": {
                            "queryVector": vec
                        }
                    }
                }
            },
            "size": size
        }

    #  not including some fields,即该字段为空
    def search_null_fields(self, index, null_fields, size=D_TOP_K):
        must_not = [{"exists": {"field": field}} for field in null_fields]
        body = {
            "size": size,
            "query": {
                "bool": {
                    "must_not": must_not
                }
            }
        }
        return self.try_search(index, body)

    # 多字段，多值条件下，必须满足或至少满足一个
    # 只支持 text 字段
    # kvList = [{"product_name": "小新"}, {"goods_status": "下架"}]
    # opt = "should"==OR, "must"==AND
    def search_multi_words(self, index, kvList, opt="should", size=D_TOP_K):
        if opt != "should":
            opt = "must"
        query = {
            "size": size,
            "query": {
                "bool": {
                    opt: [{"match": kv} for kv in kvList]
                }
            }
        }
        return self.try_search(index, query)
    # 返回满足所有must和至少一个should的文档
    # must表示 and, should表示 or
    def search_and_or(self, index, must_list, should_list):
        query = {
            "query": {
                "bool": {
                    "must": [{"match": fq} for fq in must_list],
                    "should": [{"match": fq} for fq in should_list],
                    "minimum_should_match": 1
                }
            }
        }
        return self.try_search(index, query)
     
    # search 多个vector, 自定义分类脚本
    # [{f_name1: vec1}, {f_name2: vec2}]
    def search_multi_vectors(self, index, multi_vectors, size=D_TOP_K):
        scores = []
        for item in multi_vectors:
            f_name = list(item.keys())[0]
            vec = item[f_name]
            score = f"cosineSimilarity(params.{f_name}, '{f_name}') + 1.0"
            scores.append(score)
            params = {f_name: vec}

        script = {}
        script['source'] = "double score = 0;" + "score += " + " + ".join(scores) + "; return score;"
        script['params'] = params

        query = {
            "size": size,
            "query": {
                "bool": {
                    "should": [
                        {
                            "script_score": {
                                "query": {
                                    "match_all": {}
                                },
                                "script": script
                            }
                        }
                    ],
                    "minimum_should_match": 1
                }
            }
        }
        return self.try_search(index, query)

    @staticmethod
    def to_dataframe(response):
   
        # 提取 _source 字段中的数据
        hits = response['hits']['hits']
        data = [hit['_source'] for hit in hits]
        # 转换为 DataFrame
        df = pd.DataFrame(data)
        return df
    
    # 保存为csv文件
    def to_csv(self, response, csvfile):
        df = self.to_dataframe(response)
        df.to_csv(csvfile, index=False)
        return
   


# Example usage
if __name__ == '__main__':
    from zebura_core.placeholder import make_esServer
    es_server = make_esServer('ADMsearch')
    esrch = ESearcher(es_server)
    index = "mz_imdb1"
    fields = esrch.get_all_fields(index)
    if len(fields) == 0:
        sys.exit(1)

    print(fields)

    fqList = [{"query": "多少钱"}, {"sql": "products"}]
    result = esrch.search_multi_words(index, fqList)
    print(result)
    df = esrch.to_dataframe(result)
    print(df)
    result = esrch.search(index, "question", "请从产品表里查一下联想小新电脑的价格")
    print(result)
    
