# 生成database下所有表的quesions and sql, 并保存在CSV文件中，可人工检查挑选好的cases
# database在config.ini的Training区的db_name中配置
#######################################
import sys,os,asyncio
from time import sleep
sys.path.insert(0, os.getcwd().lower())
from settings import z_config
from zebura_core.case_retriever.cases_generator import CaseGen
import argparse

if __name__ == "__main__":  
    parser = argparse.ArgumentParser(description='generate and save questions for current database')
    parser.add_argument('--csv_path', type=str, required=True, help='csv file of output results')
    parser.add_argument('--lang', type=str, required=True, help='language used in questions')
    args = parser.parse_args()
    csv_name = args.csv_path
    csv_name = os.path.join(os.getcwd(), csv_name)
    chat_lang = args.lang

    db_name = z_config['Training','db_name']
    gen = CaseGen(db_name,chat_lang)
    asyncio.run(gen.gen_cases(csv_name))
    sleep(5)       # 歇会儿，我怕你CSV文件还没写完
    asyncio.run(gen.gen_followups(csv_name))
    
