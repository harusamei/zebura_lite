# Component for login/logout 
# ！目前认证信息存放在 config.yaml 文件中, 只能管理员注册用户
###############################
import streamlit as st
import streamlit_authenticator as stauth
import yaml
from yaml.loader import SafeLoader

class Login:
    count = 0
    def __init__(self):
        #self.authenticator = None
        print(f'Login init called {str(self.count)} times')
        self.count += 1
        
    @staticmethod
    def hasLogin():
        if st.session_state.get('authentication_status'):
            print('already login')
            return True
        return False
    
    @staticmethod
    @st.cache_data
    def load_config(filename):
        print('load config')
        with open(filename) as file:
            config = yaml.load(file, Loader=SafeLoader)
        return config

    @staticmethod
    def login_callback(info):
        print('login callback ',info)
        print(st.session_state)

    @staticmethod
    def logout_callback(info):
        print('logout callback: ',info)
        print(st.session_state)
        for key in st.session_state.keys():
            del st.session_state[key]
        st.rerun()

    def login(self,authenticator):
        # if st.session_state.get('logout'):
        #     print('waiting for logout')
        #     return
        try:
            authenticator.login(location="main", key='login_button', callback=self.login_callback)
            print('run auth login')
        except Exception as e:
            st.error(e)
        
    def logout(self,authenticator):
        if st.session_state.get('username'):
            authenticator.logout(callback=self.logout_callback)  
            st.write('press button to logout')
        else:
            print('User must be logged in to use the logout button')
    

