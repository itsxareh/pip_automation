import os
import streamlit as st

from supabase import create_client, Client
from dotenv import load_dotenv
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_ANON_KEY = os.getenv("SUPABASE_ANON_KEY")

class DBConnection:
    def init_supabase():
        try:
            return create_client(SUPABASE_URL, SUPABASE_ANON_KEY)

        except Exception as e:
            st.error(f"Failed to connect to Supabase: {str(e)}")
            return None