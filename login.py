# main.py
#pip install streamlit pandas numpy openpyxl msoffcrypto-tool supabase python-dotenv pywin32
import streamlit as st
import pandas as pd
import os
import numpy as np
import warnings
from datetime import datetime, time, timedelta
from openpyxl import load_workbook
import tempfile
import platform
import importlib.util
import io
import re 
import msoffcrypto
import hashlib
import jwt
import secrets
from typing import Optional, Tuple, Dict, Any

win32_available = False
if platform.system() == "Windows" and importlib.util.find_spec("win32com.client") is not None:
    try:
        import win32com.client as win32
        win32_available = True
    except ImportError:
        win32_available = False
else:
    win32_available = False

from app import App
#Processors
from processor.base import BaseProcessor as base_process
from processor.bdo_auto import BDOAutoProcessor as bdo_auto
from processor.bpi_auto_curing import BPIAutoCuringProcessor as bpi_auto_curing
from processor.rob_bike import ROBBikeProcessor as rob_bike
from processor.sumisho import SumishoProcessor as sumisho

from utils.init import DBConnection as db_connect

from supabase import create_client, Client
from dotenv import load_dotenv
load_dotenv()

# Supabase configuration
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_ANON_KEY = os.getenv("SUPABASE_ANON_KEY")

JWT_SECRET = os.getenv("JWT_SECRET")
JWT_EXPIRY_HOURS = 24
REMEMBER_ME_DAYS = 30
MAX_LOGIN_ATTEMPTS = 5
LOCKOUT_DURATION_MINUTES = 15

@st.cache_resource
def create_session_token(user_data):
    try:
        payload = {
            'user_id': user_data['user_id'],
            'username': user_data['username'],
            'exp': datetime.utcnow() + timedelta(hours=JWT_EXPIRY_HOURS)
        }
        token = jwt.encode(payload, JWT_SECRET, algorithm='HS256')
        return token
    except Exception as e:
        st.error(f"Error creating session token: {str(e)}")
        return None

def verify_session_token(token):
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=['HS256'])
        return payload
    except jwt.ExpiredSignatureError:
        return None 
    except jwt.InvalidTokenError:
        return None

def save_session_to_url(token):
    st.query_params["session"] = token

def get_session_from_url():
    return st.query_params.get("session", None)

def save_session_locally(user_data, remember_me=False):
    st.session_state.authenticated = True
    st.session_state.user_data = user_data
    st.session_state.username = user_data['username']
    st.session_state.session_created = datetime.now().isoformat()
    st.session_state.remember_me = remember_me
    
    session_token = create_session_token(user_data)
    if session_token:
        st.session_state.session_token = session_token
        st.query_params["session"] = session_token
    
    if remember_me:
        remember_token = create_remember_me_token(user_data)
        if remember_token:
            st.session_state.remember_token = remember_token
            st.query_params["remember"] = remember_token

def is_session_valid():
    if not st.session_state.get('authenticated', False):
        return False
    
    session_token = st.session_state.get('session_token')
    if session_token:
        payload = verify_session_token(session_token)
        if payload:
            return True
    
    remember_token = st.session_state.get('remember_token')
    if remember_token:
        user_data = verify_remember_me_token(remember_token)
        if user_data:
            save_session_locally(user_data, remember_me=True)
            return True
    
    return False

def create_remember_me_token(user_data):
    supabase = db_connect.init_supabase()
    if not supabase:
        return None
    
    try:
        remember_token = secrets.token_urlsafe(32)
        expiry_date = datetime.now() + timedelta(days=30) 
        
        token_data = {
            'user_id': user_data['id'],
            'token': remember_token,
            'expires_at': expiry_date.isoformat(),
            'created_at': datetime.now().isoformat()
        }
        
        existing = supabase.table('remember_tokens').select('*').eq('user_id', user_data['user_id']).execute()
        
        if existing.data:
            supabase.table('remember_tokens').update(token_data).eq('user_id', user_data['user_id']).execute()
        else:
            supabase.table('remember_tokens').insert(token_data).execute()
        
        return remember_token
    except Exception as e:
        st.error(f"Error creating remember token: {str(e)}")
        return None

def verify_remember_me_token(token):
    supabase = db_connect.init_supabase()
    if not supabase:
        return None
    
    try:
        result = supabase.table('remember_tokens').select('*, users(*)').eq('token', token).execute()
        
        if not result.data:
            return None
        
        token_data = result.data[0]
        
        expiry_date = datetime.fromisoformat(token_data['expires_at'].replace('Z', '+00:00'))
        if datetime.now(expiry_date.tzinfo) > expiry_date:
            supabase.table('remember_tokens').delete().eq('token', token).execute()
            return None
        
        return token_data['users']
    except Exception as e:
        st.error(f"Error verifying remember token: {str(e)}")
        return None

def initialize_session():
    if st.session_state.get('authenticated') and is_session_valid():
        return  
    
    st.session_state.authenticated = False
    st.session_state.user_data = None
    st.session_state.username = None
    
    session_restored = False
    
    session_token = get_session_from_url()
    if session_token and not session_restored:
        payload = verify_session_token(session_token)
        if payload:
            supabase = db_connect.init_supabase()
            if supabase:
                try:
                    result = supabase.table('users').select('*').eq('user_id', payload['user_id']).eq('is_active', True).execute()
                    if result.data:
                        user_data = result.data[0]
                        save_session_locally(user_data, remember_me=False)
                        session_restored = True
                except Exception as e:
                    st.error(f"Error restoring session: {str(e)}")
    
    remember_token = st.query_params.get("remember", None)
    if remember_token and not session_restored:
        user_data = verify_remember_me_token(remember_token)
        if user_data:
            save_session_locally(user_data, remember_me=True)
            session_restored = True
    
    if not session_restored and st.session_state.get('remember_token'):
        remember_token = st.session_state.get('remember_token')
        user_data = verify_remember_me_token(remember_token)
        if user_data:
            save_session_locally(user_data, remember_me=True)
            session_restored = True
    
    if not session_restored:
        st.session_state.authenticated = False
        st.query_params.clear()

def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()

def create_user_table():
    supabase = db_connect.init_supabase()
    if not supabase:
        return False
    
    try:
        return True
    except Exception as e:
        st.error(f"Error creating table: {str(e)}")
        return False

def get_user_profile(user_id):
    supabase = db_connect.init_supabase()
    if not supabase:
        return None
    
    try:
        result = supabase.table('users').select('*').eq('user_id', user_id).execute()
        return result.data[0] if result.data else None
    except Exception as e:
        st.error(f"Error fetching profile: {str(e)}")
        return None

def is_account_locked(user):
    if not user.get('account_locked_until'):
        return False
    
    locked_until = datetime.fromisoformat(user['account_locked_until'].replace('Z', '+00:00'))
    return datetime.now() < locked_until

def update_failed_attempts(supabase, user_id, failed_attempts):
    new_attempts = failed_attempts + 1
    
    if new_attempts >= MAX_LOGIN_ATTEMPTS:
        lockout_until = (datetime.now() + timedelta(minutes=LOCKOUT_DURATION_MINUTES)).isoformat()
        supabase.table('users').update({
            'failed_login_attempts': new_attempts,
            'account_locked_until': lockout_until
        }).eq('user_id', user_id).execute()
        return True, new_attempts  
    else:
        supabase.table('users').update({
            'failed_login_attempts': new_attempts
        }).eq('user_id', user_id).execute()
        return False, new_attempts  

def reset_failed_attempts(supabase, user_id):
    supabase.table('users').update({
        'failed_login_attempts': 0,
        'account_locked_until': None
    }).eq('user_id', user_id).execute()

def authenticate_user(username, password):
    supabase = db_connect.init_supabase()
    if not supabase:
        return False, None, "Database connection failed"
    
    try:
        result = supabase.table('users').select('*').eq('username', username).eq('is_active', True).execute()
        
        if not result.data:
            return False, None, "Invalid credentials"
        
        user = result.data[0]
        
        if is_account_locked(user):
            locked_until = datetime.fromisoformat(user['account_locked_until'].replace('Z', '+00:00'))
            remaining_time = locked_until - datetime.now()
            minutes_remaining = int(remaining_time.total_seconds() / 60)
            return False, None, f"Account is locked. Try again in {minutes_remaining} minutes."
        
        hashed_password = hash_password(password)
        if user['password'] == hashed_password:
            try:
                reset_failed_attempts(supabase, user['user_id'])
                supabase.table('users').update({
                    'last_login': datetime.now().isoformat()
                }).eq('user_id', user['user_id']).execute()

                return True, user, "Login successful"
            except Exception as e:
                st.error(f"Login update failed: {e}")
                return False, None, "Login processing error"
        else:
            failed_attempts = user.get('failed_login_attempts', 0)
            is_locked, new_attempts = update_failed_attempts(supabase, user['user_id'], failed_attempts)
            
            if is_locked:
                return False, None, "Too many failed attempts. Account locked for 15 minutes."
            else:
                remaining_attempts = 5 - new_attempts
                return False, None, f"Invalid credentials."
                
    except Exception as e:
        st.error(f"Authentication error: {str(e)}")
        return False, None, "Authentication system error"

def login_page():
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        st.divider()
        st.markdown("### Login")
        with st.form("login_form"):
            username = st.text_input("Username")
            password = st.text_input("Password", type="password")
            remember_me = st.checkbox("Remember me for 30 days")

            col_a, col_b, col_c = st.columns([1, 1, 1])
            with col_b:
                login_button = st.form_submit_button("Sign In", use_container_width=True)
            
            if login_button:
                if username and password:
                    is_authenticated, user_data, message = authenticate_user(username, password)
                    
                    if is_authenticated:
                        db_connect.init_supabase()
                        
                        st.session_state.authenticated = True
                        st.session_state.user_data = user_data
                        st.session_state.username = username
                        
                        session_token = create_session_token(user_data)
                        if session_token:
                            save_session_to_url(session_token)
                        
                        if remember_me:
                            remember_token = create_remember_me_token(user_data)
                            if remember_token:
                                st.query_params["remember"] = remember_token

                        st.success(message)
                        st.rerun()
                    else:
                        st.error(message)
                else:
                    st.error("Please provide both username and password")

def unlock_account_admin(username):
    supabase = db_connect.init_supabase()
    if not supabase:
        return False, "Database connection failed"
    
    try:
        result = supabase.table('users').select('user_id').eq('username', username).execute()
        if not result.data:
            return False, "User not found"
        
        user_id = result.data[0]['user_id']
        reset_failed_attempts(supabase, user_id)    
        return True, "Account unlocked successfully"
    except Exception as e:
        return False, f"Error unlocking account: {str(e)}"

def get_account_status(username):
    """Get current account status including lockout info"""
    supabase = db_connect.init_supabase()
    if not supabase:
        return None
    
    try:
        result = supabase.table('users').select(
            'username', 'failed_login_attempts', 'account_locked_until', 'last_login'
        ).eq('username', username).execute()
        
        if result.data:
            user = result.data[0]
            status = {
                'username': user['username'],
                'failed_attempts': user.get('failed_login_attempts', 0),
                'is_locked': is_account_locked(user),
                'locked_until': user.get('account_locked_until'),
                'last_login': user.get('last_login')
            }
            return status
    except Exception as e:
        st.error(f"Error getting account status: {str(e)}")
    
    return None
        
def check_database_connection():
    """Check if Supabase connection is working"""
    if not SUPABASE_URL or not SUPABASE_ANON_KEY:
        print("Supabase credentials not found in environment variables.")
        return False
    
    supabase = db_connect.init_supabase()
    if not supabase:
        return False
    
    try:
        supabase.table('users').select('user_id').limit(1).execute()
        return True
    except Exception as e:
        st.error(f"Database connection failed: {str(e)}")
        st.info("Ensure the 'users' table exists in your Supabase database")
        return False

def main():
    """Main application controller"""
    initialize_session()
    
    if not check_database_connection():
        st.stop()
    
    if st.session_state.get('authenticated'):
        if st.session_state.get('remember_me'):
            st.sidebar.info("Remember me is active")
    
    if st.session_state.get('authenticated', False):
        app = App()
        app.main_app()
    else:
        login_page()

if __name__ == "__main__":
    st.set_page_config(
        page_title="Automation Tool",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    st.markdown("""
    <style>
    /* Remove default padding */
    .main > div {
        padding-top: 1rem;
    }
    
    /* Clean tab styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        justify-content: center;
    }
    
    .stTabs [data-baseweb="tab"] {
        height: 45px;
        padding: 0 24px;
        background-color: transparent;
        border-radius: 4px;
        color: #666;
        font-weight: 500;
    }
    
    .stTabs [aria-selected="true"] {
        color: #fff;
    }
    
    /* Form styling */
    .stForm {
        padding: 2rem;
        border-radius: 8px;
    }
    
    /* Button styling */
    .stButton > button {
        border-radius: 4px;
        font-weight: 500;
        letter-spacing: 0.02em;
    }
    
    /* Metrics styling */
    [data-testid="metric-container"] {
        background-color: #fafafa;
        border: 1px solid #e0e0e0;
        padding: 1rem;
        border-radius: 4px;
    }
    [data-testid="stMarkdownContainer"] {
        font-size: .875rem
    }
    
    /* Hide streamlit branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
    
    /* Clean divider */
    hr {
        margin: 2rem 0;
        border: none;
        border-top: 1px solid #e0e0e0;
    }
    </style>
    """, unsafe_allow_html=True)
    
    main()