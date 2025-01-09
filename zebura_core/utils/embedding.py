from sentence_transformers import SentenceTransformer
import time
import os
import sys
import logging
sys.path.insert(0, os.getcwd())
from settings import z_config

class Embedding:
    # 这个是北京数元灵科技有限公司开源的语义向量模型，在中文 STS上当时榜单TOP1
    # 首次使用通过默认model_name安装在C:\Users\<Your Username>\.cache
    # 智源的BGE模型在https://www.modelscope.cn 有，不需要翻墙
    # dmeta, bge
    _is_initialized = False
    model = None
    dimension = None
    def __init__(self):
        
        if not Embedding._is_initialized:
            cwd = os.getcwd()
            embPath = z_config['Embedding','embPath']
            embPath = os.path.join(cwd, embPath)
            
            start_time = time.time()
            Embedding.model = SentenceTransformer(embPath)
            Embedding.dimension = self.model.get_sentence_embedding_dimension()
            logging.info("Embedding init done")
            print(f"Time taken to load embedding model: {time.time() - start_time} seconds")
            Embedding._is_initialized = True
    # sents: text| list of texts
    def get_embedding(self, sents):
        embs = self.model.encode(sents, normalize_embeddings=True)
        return embs
    
    @staticmethod
    def calc_similarity(emb1, emb2):
        return emb1 @ emb2.T
    
    def get_similar(self, texts1, texts2):
        embs1 = self.get_embedding(texts1)
        embs2 = self.get_embedding(texts2)
        similarity = self.calc_similarity(embs1, embs2)
        scores = []
        for i in range(len(embs1)):
            scores.append([])
            for j in range(len(embs2)):
                scores[-1].append([j, similarity[i][j]])
            scores[-1] = sorted(scores[-1], key=lambda x:x[1], reverse=True)
            print(f"查询文本：{texts1[i]}")
            similar_loc = scores[i][0][0]
            similar_sent= texts2[similar_loc]           
            print(f"相似文本：{similar_sent}，打分：{scores[i][0][1]}")

        return scores
        
    
# Example usage
if __name__ == '__main__':
    texts1 = ["鼠标多少钱？", "新上市的产品有哪些","what the difference between desktop and laptop?"]
    texts2 = ["鼠标属于哪个分类","远程数据恢复属于哪个分类","计算机属于哪个分类",
              "新上市的产品有哪些","价格在1000~15000的电脑有哪些","价格低于50的鼠标有哪些","台式机与笔记本有什么区别"]
    
    model = Embedding()
    
    embs1 = model.get_embedding(texts1)
    print('shape of current embedding',embs1.shape)

    embs2 = model.get_embedding(texts2)
    embs3 = model.get_embedding(["what the difference between desktop and laptop?"])
    print(embs1.shape, embs2.shape, embs3.shape)
    print(model.calc_similarity(embs1, embs2))

    scores = model.get_similar(texts1, texts2)
    print(scores)

