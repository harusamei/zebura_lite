########################################
#      /\
# ____/_ \____   
# \  ___\ \  /  
#  \/ /  \/ /       designed by Yao, 
#  / /\__/_/\       who is a beginner in front-end dev.
# /__\ \_____\
#     \  /
#      \/
########################################
import streamlit as st
import pandas as pd
import streamlit_authenticator as stauth
from pygwalker.api.streamlit import StreamlitRenderer
from frontend.wiz_login import Login
from frontend.wiz_checkbox import rander_checkInfo
import inspect,asyncio
from server.controller1 import Controller
from zebura_core.placeholder import make_a_req,make_a_log
import time


def render_sidebar():
    print('render_sidebar')
    cont_top = st.container(height=65,border=False)
    cont_body = st.container(height=700,border=True)
    cont_buttom = st.container(height=80,border=True)
    with cont_top:
        cols =st.columns([6,1,1,2])
        cols[0].header(f'*Chat with your data*')
        btn_new =cols[1].button(label='🌸',help= 'new chat',key='new_btn')
        btn_chats = cols[2].button(label='💬',help= 'show chats',key='chats_btn')
        with cols[3]:
            doorkeeper.logout(authenticator)    
    with cont_body:
        st.write(f'Welcome, *{st.session_state.get("username","unknown")}!* :heart_eyes:')
        cont_tbInfo = st.container(height=100,border=True)
        with cont_tbInfo:
            st.write('current active tables:')
            active = st.session_state.get('dbInfo_checkBox_active',[])
            tb_names = []
            for i in active:
                tb_names.append(st.session_state['db_summary']['tables'][i]['name'])
            tStr = '; '.join(tb_names)
            st.markdown(f":blue-background[{tStr}]")
        if btn_new: 
            create_newchat()
        if btn_chats:
            st.write('output history chats, not ready yet')
        messages = st.session_state.get('messages',[])
        if len(messages) > 0:
            show_talk(cont_body)
    with cont_buttom:
        st.chat_input("Ask Zebura", key="chatBox", on_submit=lambda: asyncio.run(ask_zebura("chatBox", cont_body)))

@st.fragment()
async def ask_zebura(key,cont):
    query = st.session_state[key]
    print(f'ask zebura for {query}')
    if query is None:
        return
    with cont:
        try:
            with st.spinner("Waiting answer..."):   
                req = make_a_req(query)
                st.session_state['request']= req
                aws = await apply()
        except Exception as e:
            print(f'error: {e}')
            aws = {'status': 'failed','type':'error','error':f'ERR: {e}'}
        st.session_state.messages.append({'user': query, 'zebura': aws})

@st.fragment()
def create_newchat():
   
    st.title('Ask your question here🦓')
    st.write('*Interact with your data through an GPT powered chatbot*')
    st.session_state.messages = []
    st.session_state.request = None

def show_talk(cont=None):
    if len(st.session_state.messages) == 0:
        st.write('no chat yet')
        return
    
    aws = st.session_state.messages[-1].get('zebura')
    if isinstance(aws,dict) and 'sql' in aws.keys():
        if aws['status'] == 'succ' and aws['type'] == 'sql':
            st.session_state['show_sql'] = aws['sql']
            print(f'show sql: {aws["sql"]}')

    with cont:
        for mes in st.session_state.messages[-5:]:
            st.write('U: \n'+mes.get('user',''))
            aws = mes.get('zebura')
            if isinstance(aws,dict):
                if aws['type'] == 'chat':
                    tList=[ f'{aws.get("chat","")}']
                elif aws['type'] == 'error':
                    tList=[ f'{aws.get("error","")}']
                else:
                    tList=[ f'status: {aws.get("status","failed")}',
                            f'reasoning: {aws.get("reasoning","")}',
                            f'sql: {aws.get("sql","")}'
                    ]
                    if aws.get('sql_result') is None:
                        tList.append('no result in db')
                st.write('ZEBURA: '+'\n'.join(tList))
            else:
                st.write('ZEBURA: ')

# You should cache your pygwalker renderer, if you don't want your memory to explode
@st.cache_resource
def get_pyg_renderer(sql) -> "StreamlitRenderer":
    
    executor = st.session_state['executor']
    df = executor.sql2df(sql)
    # If you want to use feature of saving chart config, set `spec_io_mode="rw"`
    return StreamlitRenderer(df, kernel_computation=True, spec_io_mode="rw")

@st.fragment()
def render_answer(answ=None):
    st.markdown(':tulip::cherry_blossom::rose::hibiscus::sunflower::blossom:')
    if answ is not None:
        answ.pop('sql_result',None)
        st.write(answ)

@st.fragment()
def render_pyg():
    sql = st.session_state['show_sql']
    if sql is None or sql == '' or 'ERR' in sql.upper():
        st.write('waiting for a valid sql')
        return
    
    print('render_pygwalker')
    renderer = get_pyg_renderer(sql)
    renderer.explorer()

# main function
async def apply():
    
    request = st.session_state['request']    
    request['context'] = st.session_state.messages
    
    controller = st.session_state['controller']
    pipeline = list()

    new_log = make_a_log("user")
    new_log = controller.copy_to_log(request, new_log)
    new_log['question'] = request['msg']
    pipeline.append(new_log)

    nextStep = controller.get_next(pipeline)
    tb_names = []
    active = st.session_state.get('dbInfo_checkBox_active',[])
    if len(active) > 0:
        for i in active:
            tb_names.append(st.session_state['db_summary']['tables'][i]['name'])
    else:
        tb_names = [item['name'] for item in st.session_state['db_summary']['tables']]
    controller.set_rel_tbnames(tb_names)
    st.write('I’m a bit slow. Please give me some time to think...')
    start = time.time()
    while nextStep != controller.end:
        st.write(f'current step: {nextStep.__name__}')
        if inspect.iscoroutinefunction(nextStep):
            await nextStep(pipeline)
        else:
            nextStep(pipeline)
        nextStep = controller.get_next(pipeline)
        st.write(f'reached step {nextStep.__name__}, currently taking {time.time()-start} second: ')
    answ = await controller.genAnswer(pipeline)
    
    return answ

def get_db_summary():
    controller = st.session_state['controller']
    return controller.get_db_summary()

###########################functions###########################
# main function
print('begin index.py')

# 使用 Streamlit 的设置
st.set_page_config(
    page_title="Chat with your data",
    page_icon="🦓",
    layout="wide",
    initial_sidebar_state="auto",
    menu_items={
        "About": "# an app to chat with a database"
    }
)
# session initialization
if 'count' not in st.session_state.keys():
    st.session_state['count'] = 0
    st.session_state['doorkeeper'] = Login()
    doorkeeper = st.session_state['doorkeeper']
    st.session_state['login_config'] = doorkeeper.load_config('auth/users.yaml')
    # all messages in the current session
    st.session_state['messages'] = []
    st.session_state['pyg_df'] = None
    # 当前会话的消息
    st.session_state['request'] = None
    st.session_state['show_sql'] = ''
    controller = Controller()
    st.session_state['controller'] = controller
    st.session_state['llm'] = controller.llm
    st.session_state['executor'] = controller.executor
    st.session_state['db_summary'] = get_db_summary()
    tables = st.session_state['db_summary']['tables']
    # 默认所有表都是active的
    st.session_state['dbInfo_checkBox_active'] = [i for i in range(len(tables))]
    print('initialize session state')

else:
    st.session_state['count'] += 1
    print(f'count = {st.session_state.count}')

config = st.session_state['login_config']
authenticator = stauth.Authenticate(
    config['credentials'],
    config['cookie']['name'],
    config['cookie']['key'],
    config['cookie']['expiry_days']
)
doorkeeper = st.session_state['doorkeeper']
if not doorkeeper.hasLogin():
    print('not login yet')
    cols = st.columns([1,1,1])
    with cols[1]:
        doorkeeper.login(authenticator)

if doorkeeper.hasLogin():
    print(f'Demo for streamlit {st.session_state["count"]}')
    with st.sidebar:
        #asyncio.run(render_sidebar())
        render_sidebar()
    # Custom CSS to move the title up
    st.markdown(
        """
        <style>
        .custom-title {
            margin-top: -75px; /* Adjust the value as needed */
            text-align: center; /* Optional: Center the title */
            color: #660066; /* Optional: Change the font color */
            font-family: 'Roboto', sans-serif; /* Apply the Roboto font */
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    # Apply the custom CSS class to the title
    st.markdown('<h1 class="custom-title">Unlock the Power of Your Data with Zebura</h1>', unsafe_allow_html=True)
    tabs = st.tabs(["Zebura Answer","Data View", "Database Info"])
    with tabs[0]:
        if len(st.session_state.messages) == 0:
            render_answer()
        else:
            lastDiag = st.session_state.messages[-1]
            answ = lastDiag.get('zebura',{})
            render_answer(answ)    
    with tabs[1]:
        render_pyg()
    with tabs[2]:
        db_info = st.session_state['db_summary']['database']
        tables = st.session_state['db_summary']['tables']
        st.title('Summary of the database')
        st.write(f'Database name: {db_info["name"]}')
        st.write(f'Brief description: {db_info["desc"]}')
        st.write('Please choose the tables you want to use:')
        df = pd.DataFrame(tables)
        active = st.session_state.get('dbInfo_checkBox_active',[])
        rander_checkInfo(df,key='dbInfo_checkBox', active=active)
        if st.button('submit'):
            print('submit')
            st.sidebar.empty()

username = st.session_state.get('username','unknown')
print('end home.py\n-----------\n')
