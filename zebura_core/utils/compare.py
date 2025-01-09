from rouge import Rouge
from nltk.translate.chrf_score import chrf_precision_recall_fscore_support
from nltk.translate import meteor
import re
import difflib


class diffence:

    def __init__(self):
        self.differ = difflib.Differ()

    @staticmethod
    def getLCS(s1, s2):  # longest common subsequence

        match = difflib.SequenceMatcher(None, s1, s2).find_longest_match(0, len(s1), 0, len(s2))
        return s1[match.a: match.a + match.size]

    @staticmethod
    def getClosedMatch(gent, candidates):
        return difflib.get_close_matches(gent, candidates)

    # '-'开头的行表示在s1中存在但在s2中不存在的元素，
    # '+'开头的行表示在s2中存在但在s1中不存在的元素，
    # ' '开头的行表示在两个序列中都存在的元素
    def find_difference(self, s1, s2):
        diff = self.differ.compare(s1, s2)
        return '\n'.join(diff)


class similarity:

    def __init__(self):
        self.rouge = Rouge()

    # 计算两组terms中每两个term的相似度,返回一个二维数组
    def calc_similarity(self, terms1, terms2, method = 'chrf') -> list[list]:
        scores = []*len(terms1)
        for term1 in terms1:
            scores.append([])
            for term2 in terms2:
                if method == 'chrf':
                    scores[-1].append(self.getChrf(term1, term2))
                elif method == 'meteor':
                    scores[-1].append(self.getMeteor(term1, term2))
                elif method == 'rouge':
                    scores[-1].append(self.getRouge(term1, term2))
        return scores

    # 只使用2元字符相似
    def getUpperSimil(self, gen_sent, ref_sent, n_gram=3, beta=2):
        gen_sent = gen_sent.lower()
        ref_sent = ref_sent.lower()

        score = 0
        # rouge = self.getRouge(gen_sent, ref_sent)[0]
        # score = max(score, rouge['rouge-1']['f'])
        chrf = self.getChrf(gen_sent, ref_sent, n_gram, beta)
        score = max(score, chrf)
        # score = max(score, self.getMeteor(gen_sent, ref_sent))
        return score

    def getRouge(self, gen_sent, ref_sent):
        """
        基于word级别的召回率评估，使用召回率直接作为分数
        :param reference_sentence: 传入一个str字符串
        :param generated_sentence: 传入一个str字符串
        :param rouge_n: 可选 '1','2','l',设置算法中的 n_gram=？ ,默认使用 rouge-l 的 lcs最长公共子序列的算法
        :param lang: 设置语言， 可选'en','zh'
        :return: 返回一个分数
        """
        gen_sent = self.dealData(gen_sent)
        ref_sent = self.dealData(ref_sent)
        return self.rouge.get_scores(gen_sent, ref_sent)

    def getChrf(self, gen_sent, ref_sent, n_gram=2, beta=2):
        """
        基于字符级别的召回率和精准率，beta控制的是召回率和精准率对最后分数比重的影响，现已根据论文设置n_gram=3，beta=2 为最优,
        :param n_gram: 默认为3
        :param beta: 用于调节 recall和 precise 作用于分数的权重
        """
        gen_sent = self.dealData(gen_sent)
        ref_sent = self.dealData(ref_sent)
        precision, recall, fscore, tp = chrf_precision_recall_fscore_support(
            ref_sent, gen_sent, n=n_gram, epsilon=0., beta=beta
        )
        return fscore

    def getMeteor(self, gen_sent, ref_sent, alpha=0.9, beta=3.0,
                  gamma=0.5):  # gamma=0可使两个一样的句子得分为1
        '''
        计算meteor分数
        对于两个一样的句子，默认情况下gamma!=0，Meteor得分接近但不为1
        '''
        gen_sent = self.dealData(gen_sent)
        ref_sent = self.dealData(ref_sent)
        return meteor([ref_sent.split()], gen_sent.split(), alpha=alpha, beta=beta, gamma=gamma)

    @staticmethod
    def dealData(sent):  # 简单切分
        lang = 'en'
        if re.search(r'[\u4e00-\u9fa5]', sent):
            lang = 'zh'

        if lang == 'zh':
            sent = " ".join(sent)
        if lang == 'en':
            sent = re.sub(r"([a-z])([A-Z])", r"\1 \2", sent)
            sent = sent.replace('_', ' ')
            sent = re.sub(' +', ' ', sent)
            sent = sent.lower()
        return sent

    @staticmethod
    def getLang(sent):
        lang = 'en'
        if re.search(r'[\u4e00-\u9fa5]', sent):
            lang = 'zh'
        return lang


# examples usage
if __name__ == '__main__':
    sim = similarity()
    temList = ['给我商品ID为“abcde”的产品的图片链接','苹果手机的图片链接']
    gentList = ["给我商品ID为“B0789LZTCJ”的产品的图片链接", "给我苹果手机的图片链接",'苹果手机的图片链接']
    scores = sim.calc_similarity(temList, gentList)
    print(scores)
