# util/auth.py
import yaml
from yaml.loader import SafeLoader
import streamlit as st
from streamlit_authenticator import Authenticate

# Hide sidebar by default
st.set_page_config(initial_sidebar_state="collapsed")

def load_auth_config():
    with open('config.yaml') as file:
        return yaml.load(file, Loader=SafeLoader)

def initialize_authenticator():
    config = load_auth_config()
    return Authenticate(
        config['credentials'],
        config['cookie']['expiry_days']
    )

def check_authentication():
    """Central authentication check for all pages"""
    # Hide all pages in sidebar until authenticated
    if not st.session_state.get('authentication_status'):
        st.markdown(
            """<style>
            [data-testid="stSidebarNav"] {display: none !important;}
            </style>
            """,
            unsafe_allow_html=True
        )
    
    # Initialize authenticator in session state if not exists
    if 'authenticator' not in st.session_state:
        st.session_state.authenticator = initialize_authenticator()
    
    authenticator = st.session_state.authenticator
    
    # For version 0.4.2, login() doesn't return anything
    # It updates session state directly
    authenticator.login()
    
    # Check authentication status from session state
    if st.session_state.get('authentication_status') == True:
        # User is authenticated - show welcome message
        st.success(f'Welcome *{st.session_state["name"]}*')
        
        # Add user roles to session state
        config = load_auth_config()
        username = st.session_state["username"]
        if username in config["credentials"]["usernames"]:
            st.session_state["roles"] = config["credentials"]["usernames"][username]["roles"]
        
        # Show sidebar after successful login
        st.markdown(
            """<style>
            [data-testid="stSidebarNav"] {display: block !important;}
            </style>
            """,
            unsafe_allow_html=True
        )
        
        return authenticator
        
    elif st.session_state.get('authentication_status') == False:
        # Wrong credentials
        st.error('Username/password is incorrect')
        st.stop()
        
    elif st.session_state.get('authentication_status') is None:
        # Not authenticated yet - login form is displayed
        st.warning('Please enter your username and password')
        st.stop()
    
    return None