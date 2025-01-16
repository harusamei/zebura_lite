################
# 包装为 FastAPI 服务
# 用 urvicorn fastapi_run:app --reload，  reload 选项可以让服务器在代码修改后自动重启
#######################
from fastapi import FastAPI
from server.controller1 import Controller
from zebura_core.placeholder import make_a_log
import inspect

# 创建 FastAPI 实例
app = FastAPI()
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

# 创建一个简单的路由
@app.get("/")
async def read_root():
    return {"message": "welcome to try zebura"}

# 创建一个带路径参数的路由
@app.post("/zebura/")
async def nl2sql(req:dict):
    resp = await apply(req)
    return resp

# 创建一个接收 POST 请求的路由
@app.post("/items/")
async def create_item(item: dict):
    return {"item": item}


# 启动服务器
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
