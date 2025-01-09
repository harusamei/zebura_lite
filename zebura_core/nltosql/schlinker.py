###########################################
# SQL与数据库schema 对齐
# 基于metadata.xls文件，对SQL进行解析，将SQL中的表名、字段名与数据库schema对齐
# 不涉及具体值的修正
############################################
import os
import sys
sys.path.insert(0, os.getcwd())
import logging
from zebura_core.knowledges.schema_loader_lite import ScmaLoader
from zebura_core.utils.compare import similarity

# schema linking, for table, column
class Sch_linking:
    _is_initialized = False
    sim = similarity()

    def __init__(self, threshold=0.7, db_name=None, chat_lang=None):
        if not Sch_linking._is_initialized:
            Sch_linking._is_initialized = True
            self.loader = ScmaLoader(db_name=db_name, chat_lang=chat_lang)
            self.db_Info = self.loader.db_Info
            
            Sch_linking.loader = self.loader
            Sch_linking.db_Info = self.db_Info

        self.threshold = threshold
        logging.info("Schema linking init done")

    # 匹配表名  (term,like,score)
    def link_tables(self, terms) -> tuple:

        if isinstance(terms, str):
            terms = [terms]

        name_list = self.loader.get_table_nameList()
        tbList = list(name_list)
        scores = self.getSimility(terms, tbList)

        matched = []
        for i, row in enumerate(scores):
            tb_like = tbList[row[0][0]]
            if row[0][1] < self.threshold:
                matched.append([terms[i], None, None, 0.0])
            else:
                matched.append([terms[i], tb_like, row[0][1]])
        # ori, like, tb, score   
        return matched
        
    # lite版不使用embedding求相似度，默认使用chrf      
    def getSimility(self, list1, list2)->list:
        
        similarity = self.sim.calc_similarity(list1, list2)
        scores = []
        for i in range(len(list1)):
            scores.append([])
            for j in range(len(list2)):
                scores[-1].append([j, similarity[i][j]])
            scores[-1] = sorted(scores[-1], key=lambda x:x[1], reverse=True)
        return scores

    # 同时匹配一组字段
    # [(ori_term, col_like, tb_like, score), ...] 
    def link_fields(self, terms, table_name=None) -> list[tuple]:
        
        cols =[]
        if isinstance(terms, str):
           terms = [terms]
        if not isinstance(terms, list):
            raise ValueError("terms should be a list")

        matched = []
        if len(terms) == 0:
            return matched
        
        cols = self.loader.get_column_nameList(table_name)
        scores = self.getSimility(terms, cols)
        for i, row in enumerate(scores):
            col_like = cols[row[0][0]]
            tb_like = self.loader.get_tables_with_column(col_like)
            if row[0][1] < self.threshold:
                matched.append([terms[i], None, None, 0.0])
            else:
                matched.append([terms[i], col_like, ','.join(tb_like), row[0][1]])
        # ori, like, tb, score   
        return matched
    

# Example usage
if __name__ == '__main__':
    
    sl = Sch_linking(db_name='imdb', chat_lang='English')
    print(sl.db_Info.keys())
    print(sl.link_tables('imdb_movie_data'))
    print(sl.link_fields(['description','rank','genre1','gneres'], 'imdb_movie_data'))





