##########################
#      /\
# ____/_ \____
# \  ___\ \  /
#  \/ /  \/ /
#  / /\__/_/\
# /__\ \_____\
#     \  /
#      \/
import pandas as pd
import streamlit as st
def change(data_df,key):
    changed = st.session_state[key]['edited_rows']
    fav = data_df['favorite'].tolist()  # 获取当前的favorite列
    ori = set([i for i in range(data_df.shape[0]) if fav[i]])  # 获取原来的active
    for k,v in changed.items():
        if v['favorite'] == True:
            ori.add(k)
        if v['favorite'] == False:
            ori.remove(k)
    # print(f'change: {ori}')
    key_active = f'{key}_active'
    st.session_state[key_active] = ori

# select all or unselect all
def cbox_change(key_active,value,rows):
    if value:
        st.session_state[key_active] = set()
    else:
        st.session_state[key_active] = set([i for i in range(rows)])
    print('cbox_change')

@st.fragment()
def rander_checkInfo(oneDf, key, active = []):
    #print(f'begin rander_checkInfo')
    data_df = oneDf.copy()
    key_active = f'{key}_active'
    if key_active not in st.session_state.keys():
        st.session_state[key_active] = set(active)
    else:
        active = list(st.session_state[key_active])
    favorite = [True if i in active else False for i in range(data_df.shape[0])]

    disabled = data_df.columns.tolist()   # 禁用所有列
    data_df.insert(0, "favorite", favorite)
    if len(active) == data_df.shape[0]:
        st.checkbox('selection',value=True,
                    on_change=cbox_change,args=(key_active,True,data_df.shape[0]))
    if len(active) == 0:
        st.checkbox('selection',value=False,
                    on_change=cbox_change,args=(key_active,False,data_df.shape[0]))

    st.data_editor(
        data_df,
        key=key,
        column_config={
            "favorite": st.column_config.CheckboxColumn(
                "Is Active?",
                help="Select your **favorite** rows",
                default=False,
            )
        },
        hide_index=True,
        disabled=disabled,
        on_change=change,
        args=(data_df,key)
    )

# Example usage
if __name__ == '__main__':

    df = pd.DataFrame({
        'name': ['Tom', 'Jerry', 'Mike', 'Tom'],
        'age': [20, 21, 22, 20],
        'school': ['Peking Univ', 'Tsinghua Univ', 'New York Univ', 'MIT']
    })
    if st.session_state.get('count') is None:
        st.session_state['count'] = 0
    else:
        st.session_state['count'] += 1
    count = st.session_state['count']
    print('begin program: ', count)
    cols = st.columns([1,1])
    with cols[0]:
        rander_checkInfo(df,key='cInfo', active=[0,1,3])
        btn= st.button('submit')
        if btn:
            st.write(f'cInfo: {st.session_state["cInfo"]}')
            st.write(st.session_state['cInfo_active'])
    
    print('end program')

 
