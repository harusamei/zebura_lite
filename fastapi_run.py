################
# 包装为 FastAPI 服务
# 用 urvicorn fastapi_run:app --reload，  reload 选项可以让服务器在代码修改后自动重启
#######################
from fastapi import FastAPI
from server.controller1 import apply

# 创建 FastAPI 实例
app = FastAPI()

# 创建一个简单的路由
@app.get("/")
async def read_root():
    return {"message": "Hello, World!"}

# 创建一个带路径参数的路由
@app.post("/nl2sql/")
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
