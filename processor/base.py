import streamlit as st
import pandas as pd
import os
from openpyxl.utils import get_column_letter
from datetime import datetime, date
import io
import tempfile
import shutil
import re 
 

class BaseProcessor:
    def __init__(self):
        self.temp_dir = tempfile.mkdtemp()
        
    def __del__(self):
        try:
            if os.path.exists(self.temp_dir):
                shutil.rmtree(self.temp_dir)
        except:
            pass
          
    def process_mobile_number(self, mobile_num):
        if pd.isna(mobile_num) or mobile_num is None or str(mobile_num).strip() == "":
            return ""

        mobile_num = str(mobile_num).strip()
        mobile_num = re.sub(r'\D', '', mobile_num)

        if mobile_num.startswith('639') and len(mobile_num) == 12:
            result = '09' + mobile_num[3:]
            return result

        if mobile_num.startswith('9') and len(mobile_num) == 10:
            result = '0' + mobile_num
            return result

        if mobile_num.startswith('09') and len(mobile_num) == 11:
            return mobile_num

        if mobile_num.startswith('+639') and len(mobile_num) == 13:
            result = '09' + mobile_num[4:]
            return result

        return mobile_num

    def format_date(self, date_value):
        if pd.isna(date_value) or date_value is None:
            return ""
            
        if isinstance(date_value, (datetime, date)):
            return date_value.strftime("%m/%d/%Y")
        
        try:
            date_obj = pd.to_datetime(date_value)
            return date_obj.strftime("%m/%d/%Y")
        except:
            return str(date_value)
    def clean_data(self, df, remove_duplicates=False, remove_blanks=False, trim_spaces=False):
        if not isinstance(df, pd.DataFrame):
            raise ValueError(f"Expected a pandas DataFrame, but got {type(df)}: {df}")
        
        cleaned_df = df.copy()
        
        if remove_blanks: 
            cleaned_df = cleaned_df.dropna(how='all')
        if remove_duplicates:
            cleaned_df = cleaned_df.drop_duplicates()
        if trim_spaces:
            for col in cleaned_df.select_dtypes(include=['object']).columns:
                cleaned_df[col] = cleaned_df[col].str.strip()
                
        cleaned_df = cleaned_df.replace(r'^\s*$', pd.NA, regex=True)
        return cleaned_df
        
    def clean_only(self, file_content, sheet_name, preview_only=False, 
                   remove_duplicates=False, remove_blanks=False, trim_spaces=False, file_name=None):
        try:
            byte_stream = io.BytesIO(file_content)
            xls = pd.ExcelFile(byte_stream)
            sheet_names = xls.sheet_names
            df = pd.read_excel(xls, sheet_name=sheet_names[0])

            sanitized_headers = [re.sub(r'[^A-Za-z0-9_]', '_', str(col)) for col in df.columns]
            df.columns = sanitized_headers

            cleaned_df = self.clean_data(df, remove_duplicates, remove_blanks, trim_spaces)

            if preview_only:
                return cleaned_df

            if file_name:
                base_name = os.path.splitext(os.path.basename(file_name))[0]
                output_filename = f"{base_name}.xlsx"
            else:
                output_filename = "CLEANED_DATA.xlsx"

            output_path = os.path.join(self.temp_dir, output_filename)

            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                cleaned_df.to_excel(writer, index=False, sheet_name='Sheet1')
                worksheet = writer.sheets['Sheet1']
                for i, col in enumerate(cleaned_df.columns):
                    try:
                        max_len_in_column = cleaned_df[col].astype(str).map(len).max()
                        max_length = max(max_len_in_column, len(str(col))) + 2
                    except:
                        max_length = 15
                    col_letter = get_column_letter(i + 1)
                    worksheet.column_dimensions[col_letter].width = max_length

            with open(output_path, 'rb') as f:
                output_binary = f.read()

            return cleaned_df, output_binary, output_filename

        except Exception as e:
            st.error(f"Error cleaning file: {str(e)}")
            raise
