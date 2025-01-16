import requests
from zebura_core.placeholder import make_a_req
import time

# 假设API运行在localhost的8000端口
url = 'http://localhost:8000/zebura/'

def call_api(query):
    # 创建一个示例Item对象的字典，这应该与你的Item模型字段相匹配
    request = make_a_req(query)

    # 发送POST请求
    response = requests.post(url, json=request)

    # 打印响应数据
    print(response.json())


querys = ['家居与厨房类别中有多少种产品','列出最贵的3个种类的产品。',
              '列出所有属于家居与厨房类别的最贵商品。','帮我查一下电动切菜机套装的单价。',
              '帮我查一下I 系列 4K 超高清安卓智能 LED 电视的折扣率。','列出评分高于4.5的产品。',
              '目前有哪些电子产品的折扣价格低于500元？','评分在4.5以上的产品有哪些？找出其中最高的不超过5个',
              '哪些产品的折扣最大？能推荐几款吗？','我想知道这款产品（ID为B09RFB2SJQ）的详细信息，包括名称、价格、折扣和评分。',
              '我想看看这款产品（ID为B09RFB2SJQ）的用户评论。', '用户RAMKISAN之前写的评论都在哪儿可以找到？',
              '手动搅拌机这款产品的评分有多少个？平均评分是多少？','电子产品分类下，评分最高的几款产品有哪些？'
              ]
start = time.time()
for query in querys:
    call_api(query)
end = time.time()
print(f"avg time cost: {(end-start)/len(querys)} seconds")
