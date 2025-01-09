# index中doc的增删改
#########################################
import os,sys
sys.path.insert(0, os.getcwd())
import logging
from zebura_core.elastic.es_searcher1 import ESearcher
from zebura_core.elastic.es_creator1 import CreateIndex

class ESOps(ESearcher):

    def __init__(self, es_server=None):

        super().__init__(es_server)
        self.creator = CreateIndex(es_server)
        logging.info('ESops is initial success')

    def insert_docs(self, index_name, docs) -> int:
        es_mapping = self.es_mapping(index_name)
        if es_mapping is None:
            logging.error(f"index {index_name} not exist")
            return 0
        
        new_docs = [self.creator.format_doc(doc, es_mapping) for doc in docs]
        new_docs = [doc for doc in new_docs if doc is not None]
        return self.creator.insert_docs(index_name, new_docs)

    # 忽略dense_vector字段， 可能find多个doc
    def find_doc(self, index_name, doc):

        fields = self.get_all_fields(index_name)
        # kvList = [{"product_name": "小新"}, {"goods_status": "下架"}]
        kvList = []
        for field in fields:
            fname = field['fname']
            if doc.get(fname) is not None and field['ftype'] != 'dense_vector':
                kvList.append({fname: doc[fname]})

        response = self.search_multi_words(index_name, kvList)
        hits = response['hits']['hits']
        if len(hits) == 0:
            return None
        
        return hits
    # ES中的_id字段 
    def delete_doc(self, index_name, doc_id):
        try:
            response = self.es.delete(index=index_name, id=doc_id, refresh=True)
            print(response)
            return 1
        except Exception as e:
            logging.error(f"can not delete doc {doc_id} in {index_name}")
            return 0

    # kvList = [{"product_name": "小新"}, {"goods_status": "下架"}] 
    # dense_vector字段不支持  
    def update_doc_field(self, index_name, doc_id, kvList):

        tDict = {k: v for kv in kvList for k, v in kv.items()}
        body = {
            "doc": tDict
        }
        res = self.es.update(index=index_name, id=doc_id, body=body, refresh=True)
        return res


if __name__ == '__main__':
    import pandas
    esops = ESOps()
    print(esops.select_all('mz_imdb1',max_size=5))
    doc = {'question':'What is the total number of votes received by','sql':'SELECT SUM(votes) AS total_votes FROM imdb_mov'}
    docs = esops.find_doc('mz_imdb1', doc)   
    df = pandas.DataFrame(docs)
    print(df.head())
    # print(esops.delete_doc('mz_imdb1', doc_id='GDXV0pMBdsev6QXM9vi7'))
    # kvList = [{'question':'What is the total number of votes received by'},{'sql':'SELECT SUM(votes) AS total_votes FROM imdb_mov'}]
    # esops.update_doc_field('mz_imdb1', doc_id='NTXV0pMBdsev6QXM9vi7', kvList=kvList)
    result = esops.search_by_id('mz_imdb1','NTXV0pMBdsev6QXM9vi7')
    print(result)