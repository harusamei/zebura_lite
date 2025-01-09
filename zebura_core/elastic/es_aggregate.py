# 索引聚合操作
################################
import sys
import os
sys.path.insert(0, os.getcwd())
import logging
from zebura_core.elastic.es_base import ES_BASE
import pandas as pd

class AggIndex(ES_BASE):

    def __init__(self,es_name):
        super().__init__(es_name)
        
        logging.debug("AggIndex init success")

    def try_search(self, index, query):
        try:
            return self.es.search(index=index, body=query)
        except Exception as e:
            logging.error(e)
        return None

    # 基于field的聚合
    def aggregate(self, index, field):
        query = {
            "size": 0,
            "aggs": {
                "properties": {
                    "terms": {
                        "field": field
                    }
                }
            }
        }
        response = self.try_search(index, query)
        return response['aggregations']['properties']['buckets']

    # 满足某查询条件下的基于field的聚合
    def search_agg(self, index, fqlist, field):
        query = {
            "query": {
                "bool": {
                    "should": [{"match": fq} for fq in fqlist]
                }
            },
            "size": 0,
            "aggs": {
                "properties": {  # 聚合依据
                    "terms": {
                        "field": field,
                        "size": 10
                    }
                }
            }
        }
        response = self.try_search(index, query)
        return response['aggregations']['properties']['buckets']

    # 数值统计
    def search_range(self, index, field, upper, lower):
        query = {
            "size": 3,
            "query": {
                "range": {
                    field: {
                        "gte": lower,  # 大于等于low
                        "lte": upper
                    }
                }
            }
        }
        return self.try_search(index, query)

    def search_average(self, index, field):
        query = {
            "size": 0,
            "aggs": {
                "average": {
                    "avg": {
                        "field": field
                    }
                }
            }
        }

        response = self.try_search(index, query)
        avg_value = response['aggregations']['average']['value']
        return avg_value

    # 返回最大或最小值
    def search_max_min(self, index, field, most):
        if most == "max":
            m_value = "max_value"
        else:
            m_value = "min_value"
            most = "min"
        query = {
            "size": 0,
            "aggs": {
                m_value: {
                    most: {
                        "field": field
                    }
                }
            }
        }
        response = self.try_search(index, query)
        return response['aggregations'][m_value]['value']

    @staticmethod
    def to_dataframe(response):
   
        # 提取 _source 字段中的数据
        hits = response['hits']['hits']
        data = [hit['_source'] for hit in hits]
        # 转换为 DataFrame
        df = pd.DataFrame(data)
        return df
    
    # 保存为csv文件
    def toCSV(self, response, csvfile):
        df = self.to_dataframe(response)
        df.to_csv(csvfile, index=False)
        return
   


# Example usage
if __name__ == '__main__':
    from zebura_core.placeholder import make_esServer
    es_server = make_esServer('ADMsearch')
    es = ESearcher(es_server)
    index = "goldencases"
    fields = es.get_all_fields(index)
    if len(fields) == 0:
        sys.exit(1)

    print(fields)

    fqList = [{"query": "多少钱"}, {"sql": "products"}]
    result = es.search_kvs(index, fqList)
    print(es.filter_results(result, ['qembedding']))

    result = es.search(index, "qembedding", "请从产品表里查一下联想小新电脑的价格")
    print(es.keep_results(result, ['query', 'sql']))

