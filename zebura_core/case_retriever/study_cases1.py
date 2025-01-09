# 从ES的样例库中找相似的案例
# 所有question统一翻译为search_lang对应译文，存于s_question
##########233####################
import os,sys,json,asyncio
sys.path.insert(0, os.getcwd())
import logging
from settings import z_config
import zebura_core.constants as const
from zebura_core.elastic.es_searcher1 import ESearcher
from zebura_core.utils.compare import similarity
from zebura_core.LLM.llm_agent import LLMAgent
from zebura_core.LLM.prompt_loader1 import Prompt_generator
from zebura_core.LLM.ans_extractor import AnsExtractor
from zebura_core.utils.embedding import Embedding

dtk = const.D_TOP_GOODCASES  # default top k for search

class CaseStudy():

    def __init__(self, pj_name=None):            
        
        if pj_name is None:
            pj_name = z_config['Training','db_name']
        
        self.gcase_index = const.Z_CASES_INDEX.format(pj_name = pj_name)
        self.esrch = ESearcher()
        self.compare = similarity()
        adm_search = const.C_ADM_Search     # 'ADMsearch'
        self.esLang = z_config[adm_search,'search_lang']

        self.prompter = Prompt_generator()  # 使用默认的 en_prompt
        self.llm = LLMAgent()
        self.ans_ext = AnsExtractor()

        self.embedding = None
        logging.debug("CaseStudy init success")

    async def translate(self, text, slang, tlang):
        
        if slang == tlang:
            return text
        
        input = json.dumps({'text': text,'language':slang},ensure_ascii=False)
        tml = self.prompter.tasks['translation']
        query = tml.format(input=input, trgt_lang=tlang)
        answer = await self.llm.ask_llm(query, '')
        result = self.ans_ext.output_extr('translation', answer)
        trans = None
        if result['status'] == 'succ':
            if isinstance(result['msg'], list):
                trans =result['msg'][0]
            else:
                trans = result['msg']
            return trans.get('target',None)
        
        return None
    
    def out_with_fields(self, fields, response):
        out = []
        for hit in response['hits']['hits']:
            doc = hit['_source']
            out.append({field: doc.get(field,'') for field in fields})
        return out
    
    def out_without_fields(self, fields, response):
        out = []
        for hit in response['hits']['hits']:
            doc = hit['_source']
            out.append({field: doc.get(field,'') for field in doc.keys() if field not in fields})
        return out
    
    # 欧氏距离或manhattan distance, _score 越小越相似，区间是[0, +∞)
    # question 相似的cases, qlang: 问题所用语言
    async def find_in_question(self, question, qlang, topk=dtk):
        
        question = await self.translate(question, qlang, self.esLang)
        index = self.gcase_index
        response = self.esrch.search_word(index, "s_question", question, topk)
        if response is None:
            return None
        results = self.out_without_fields(['qemb'], response)
        return results[:topk]
    
    # question 的下一个问题
    async def find_next(self, question, qlang, topk=dtk):

        question = await self.translate(question, qlang, self.esLang)
        index = self.gcase_index
        response = self.esrch.search_word(index, "s_question", question, topk*2)
        results = self.out_with_fields(['s_question','id'], response)
        out = []
        for doc in results:
            d_id = doc['id']
            response = self.esrch.search_term(index, 'b_id', d_id)
            nextDocs = self.out_with_fields(['s_question','question','sql','b_id'], response)
            if nextDocs:
                out.extend(nextDocs)
        return out[:topk]

    # ES为了保证所有的得分为正，实际使用（1 + 余弦相似度）/ 2，_score [0，1]。得分越接近1，表示两个向量越相似
    # question: text|embedding, qlang: 问题所用语言
    async def find_in_vector(self, question, qlang, topk=dtk):
        if self.embedding is None:
            self.embedding = Embedding()

        if isinstance(question, str):
            question = await self.translate(question, qlang, self.esLang)
            qemb = self.embedding.get_embedding(question)
        else:
            qemb = question
        index = self.gcase_index
        response = self.esrch.search_vector(index, 'qemb', qemb, topk)
        if response is None:
           return None
        else:
            results = self.out_with_fields(['s_question','question','sql'], response)
        return results

    # s_question, qemb 联合搜索
    async def assemble_find(self, question, qlang, topk=dtk):
        if self.embedding is None:
            self.embedding = Embedding()

        question1 = await self.translate(question, qlang, self.esLang)
        # 存在翻译失败的情况
        if isinstance(question, str):
            question = question1
        else:
            logging.error("translate failed", question)

        qemb = self.embedding.get_embedding(question)
        index = self.gcase_index
        response = self.esrch.search_vector(index, 'qemb', qemb, topk*2)
        rank_list = []
        resps =[]
        if response is not None:
            rank_list.append([hit['_id'] for hit in response['hits']['hits']])
            resps.append(response)
        else:
            rank_list.append([])
            resps.append(None)
        response = self.esrch.search_word(index, "s_question", question, topk*2)
        if response is not None:
            rank_list.append([hit['_id'] for hit in response['hits']['hits']])
            resps.append(response)
        else:
            rank_list.append([])
            resps.append(None)

        sorted_ids = self.rrf_weighted(rank_list)
        sorted_ids = sorted_ids[:topk]

        #print("sorted_ids:",sorted_ids)
        docs = {}
        for i in range(2):
            if resps[i] is None:
                continue
            for hit in resps[i]['hits']['hits']:
                docs[hit['_id']] = hit['_source']

        results = []
        for id, score in sorted_ids:
            # 用chrf相似度统一 score
            score = self.compare.getChrf(docs[id]['s_question'], question)
            results.append({'doc': docs[id], 'chrf_score': score})
        
        return results
    
    # 基于content的 chrf 相似度，对结果进行rerank
    def rerank(self, field_name, content, docList):
        for doc in docList:
            doc['score'] = self.compare.getChrf(doc[field_name],content)
        docList.sort(key=lambda x: x['score'], reverse=True)
        return docList
    """
        Compute the weight using Reciprocal Rank Fusion (RRF) for a list of rank lists.
        ranks_lists (list of lists): List of rank lists to be fused.
        Returns: list: Weighted rank list.
    """
    @staticmethod
    def rrf_weighted(ranks_lists):
        docs = {}
        # k是平滑因子，这里取最大的rank长度
        k = max(len(rank) for rank in ranks_lists)
        # 未出现在rank中的doc score为0
        for rank in ranks_lists:
            for i, doc in enumerate(rank):
                if not docs.get(doc):
                    docs[doc] = 1 / (i + k)
                else:
                    docs[doc] += 1 / (i + k)

        sorted_docs = sorted(docs.items(), key=lambda item: item[1], reverse=True)
        return sorted_docs

    
async def test():
    query = 'どのジャンルの映画が最も高いメタスコアを持っていますか？'
    results = await cs.assemble_find(query,'Japanese')

    for result in results:
        print(result['score'], result['doc']['question'],result['doc']['s_question'])

    
# Example usage
if __name__ == '__main__':
    
    cs = CaseStudy('imdb')
    asyncio.run(test())
    
