import sys
import os
sys.path.insert(0, os.getcwd().lower())
import time
import inspect
import pandas as pd
from server.controller1 import Controller
from zebura_core.placeholder import make_a_log, make_a_req
import argparse
import asyncio


controller = Controller()


async def apply(request):
    # 记录所有状态，包括transit，不删除任何状态
    # pipeline 中记录 question, sql的变化
    pipeline = list()
    new_log = make_a_log("user")
    new_log = controller.copy_to_log(request, new_log)
    new_log['question'] = request['msg']
    pipeline.append(new_log)

    nextStep = controller.get_next(pipeline)

    while nextStep != controller.end:
        if inspect.iscoroutinefunction(nextStep):
            await nextStep(pipeline)
        else:
            nextStep(pipeline)
        nextStep = controller.get_next(pipeline)
    #controller.interpret(pipeline)
    answ = await controller.genAnswer(pipeline)
    return answ


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='generate and save questions for current database')
    parser.add_argument('--csv_path', type=str, required=True, help='csv file of test data')
    args = parser.parse_args()
    csv_name = args.csv_path
    csv_name = os.path.join(os.getcwd(), csv_name)
    csv_name_out = csv_name.replace('.csv', '_out.csv')
    # 读取CSV文件
    df = pd.read_csv(csv_name)
    df['answer'] = None
    df['type'] = None
    start = time.time()
    count = 0
    for row, msg in enumerate(df['question']):
        count += 1
        if count % 50 == 0:
            print(f"Time: {time.time()-start}, count: {count}")
        print(msg)
        request = make_a_req(msg)
        resp = asyncio.run(apply(request))
        if resp['question'] != msg:
            print(f"inconsistent: {msg}, {resp['question']}")
        anws = []
        for key in resp:
            if key in ['type','question','reasoning','sql','error']:
                anws.append(f"{key}: {resp[key]}")
        df.loc[row, 'answer'] = '\n'.join(anws)
        df.loc[row, 'type'] = resp['type']
        print('\n'.join(anws))
        print("=============")
    print(f"Time: {time.time()-start}")

    # 保存结果到CSV文件
    df.to_csv(csv_name_out, index=False)