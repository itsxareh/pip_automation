import streamlit as st
import pandas as pd 
import os
from io import BytesIO

st.set_page_config(page_title="BPI Auto Curing Imports", layout="wide")
st.title("BPI Auto Curing SL")
st.write("Transform files for VOLARE formats with Visualization")

uploaded_files = st.file_uploader("Upload file:", type=["csv", "xlsx"],
                                  accept_multiple_files=False)

if uploaded_files:
    for file in uploaded_files:
        file_ext = os.path.splitext(file.name)[-1].lower()
        
        if file_ext == ".csv":
            df = pd.read_csv(file)
        elif file_ext == ".xlsx":
            df = pd.read_excel(file)
        else: 
            st.error(f"Unsupported file type: {file_ext}")
            continue
        
        st.write(f"File name: {file.name}")
        st.write(f"Size: {file.size/1024}")
        
        st.write("Preview File")
        st.dataframe(df.head())
        
        st.subheader("Data Cleaning Options")
        if st.checkbox(f"Clean Data"):
            col1, col2 = st.columns(2)
            
            with col1:
                if st.button(f"Remove Duplicates"):
                    df.drop_duplicates(inplace=True)
                    st.write("Duplicated Removed!")
                    
                    
        
        