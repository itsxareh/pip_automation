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

win32_available = False
if platform.system() == "Windows" and importlib.util.find_spec("win32com.client") is not None:
    try:
        import win32com.client as win32
        win32_available = True
    except ImportError:
        win32_available = False
else:
    win32_available = False

#Processors
from processor.base import BaseProcessor as base_process
from processor.bdo_auto import BDOAutoProcessor as bdo_auto
from processor.bpi_auto_curing import BPIAutoCuringProcessor as bpi_auto_curing
from processor.rob_bike import ROBBikeProcessor as rob_bike
from processor.sumisho import SumishoProcessor as sumisho

#Supabase
from supabase import create_client
from dotenv import load_dotenv
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

warnings.filterwarnings('ignore', category=UserWarning, 
                        message="Cell .* is marked as a date but the serial value .* is outside the limits for dates.*")

CAMPAIGN_CONFIG = {
    "No Campaign": {
        "automation_options": ["Data Clean"],
        "automation_map": {
            "Data Clean": "clean_only",
        },
        "processor": base_process
    },
    "BPI Auto Curing": {
        "automation_options": ["Updates", "Uploads", "Cured List"],
        "automation_map": {
            "Uploads": "process_uploads",
            "Updates": "process_updates",
            "Cured List": "process_cured_list"
        },
        "processor": bpi_auto_curing
    },
    "ROB Bike": {
        "automation_options": ["Daily Remark Report", "Endorsement"],
        "automation_map": {
            "Daily Remark Report": "process_daily_remark",
            "Endorsement": "process_new_endorsement", 
        },
        "processor": rob_bike
    },
    "BDO Auto B5 & B6": {
        "automation_options": ["Agency Daily Report", "Endorsement"],
        "automation_map": {
            "Agency Daily Report": "process_agency_daily_report",
            "Endorsement": "process_new_endorsement", 
        },
        "processor": bdo_auto
    },
    "Sumisho": {
        "automation_options": ["Daily Remark Report"],
        "automation_map": {
            "Daily Remark Report": "process_daily_remark",
        },
        "processor": sumisho
    },
}

def main():
    st.set_page_config(
        page_title="Automation Tool",
        layout="wide")
    
    st.markdown("""
        <style>
            .title {
                font-size: 24px;
                font-weight: bold;
            }
            .sub-title {
                font-size: 12px;
                margin-bottom: 15px;
            }
            div[data-baseweb] {
                font-size: 12px;
                line-height: 1.6 !important;
            }
            div[data-testid="stToolbar"] {
                display: none;
            }
            div[data-testid="stFileUploaderDropzoneInstructions"] {
                display: none;
            }
            section[data-testid="stFileUploaderDropzone"] {
                padding: 0px;
                margin: 0px;
                font-size: 12px;
            }
            button[data-testid="stBaseButton-secondary"] {
                width: 100%;
            }
        </style>
    """, unsafe_allow_html=True)
    
    st.markdown("<div class='title'>Automation Tool</div>", unsafe_allow_html=True)
    st.markdown("<div class='sub-title'>Transform Files into CMS Format</div>", unsafe_allow_html=True)

    campaign = st.sidebar.selectbox("Select Campaign", CAMPAIGN_CONFIG, index=0)
    config = CAMPAIGN_CONFIG[campaign]
    processor = config["processor"]()
    automation_map = config["automation_map"]
    automation_options = config["automation_options"]

    st.sidebar.header("Settings")
    automation_type = st.sidebar.selectbox("Select Automation Type", automation_options, key=f"{campaign}_automation_type")

    preview = st.sidebar.checkbox("Preview file before processing", value=True, key='file_preview')

    uploaded_file = st.sidebar.file_uploader(
        "Upload File", 
        type=["xlsx", "xls"], 
        key=f"{campaign}_file_uploader"
    )
    
    if campaign == "ROB Bike" and automation_type == "Daily Remark Report":
        yesterday = datetime.now() - timedelta(days=1)
        report_date = st.sidebar.date_input('Date Report', value=yesterday, format="MM/DD/YYYY") 
        
        with st.sidebar.expander("Upload Other File", expanded=False):
            upload_field_result = st.file_uploader(
                "Field Result",
                type=["xlsx", "xls"],
                key=f"{campaign}_field_result"
            )
            upload_dataset = st.file_uploader(
                "Dataset",
                type=["xlsx", "xls"],
                key=f"{campaign}_dataset"
            )
            upload_disposition = st.file_uploader(
                "Disposition",
                type=["xlsx", "xls"],
                key=f"{campaign}_disposition"
            )
            
        if upload_field_result:
            TABLE_NAME = 'rob_bike_field_result'
            
            try:
                xls = pd.ExcelFile(upload_field_result)

                sheet_options = xls.sheet_names
                if len(sheet_options) > 1: 
                    selected_sheet = st.selectbox(
                        "Select a sheet from the Excel file:",
                        options=sheet_options,
                        index=0,
                        key="field_result_sheet_select"
                    )
                else:
                    selected_sheet = sheet_options[0]
                    
                if selected_sheet:
                    df = pd.read_excel(xls, sheet_name=selected_sheet)
                    df_clean = df.replace({np.nan: 0})
                
                if 'chcode' in df_clean.columns and 'status' in df_clean.columns and 'SUB STATUS' in df_clean.columns and 'DATE' in df_clean.columns and 'TIME' in df_clean.columns:
                    df_filtered = df_clean[(df_clean['status'] != 'CANCEL') & (df_clean['bank'] == 'ROB MOTOR LOAN')]
                    df_extracted = df_filtered[['chcode', 'status', 'SUB STATUS', 'DATE', 'TIME']].copy()
                    
                    df_extracted = df_extracted.rename(columns={
                        'SUB STATUS': 'substatus',
                        'DATE': 'date',
                        'TIME': 'time'
                    })
                    
                    df_extracted.loc[:, 'time'] = df_extracted['time'].astype(str).replace('NaT', '')
            
                    try:
                        temp_dates = pd.to_datetime(df_extracted['date'], errors='coerce')
                        df_extracted.loc[:, 'date'] = temp_dates.astype(str).str.split(' ').str[0]
                        df_extracted.loc[:, 'date'] = df_extracted['date'].replace('NaT', '')
                    except:
                        df_extracted.loc[:, 'date'] = df_extracted['date'].astype(str).replace('NaT', '')

                    df_extracted['inserted_date'] = pd.to_datetime(
                        df_extracted['date'].astype(str) + ' ' + df_extracted['time'].astype(str), 
                        errors='coerce'
                    )

                    df_extracted['inserted_date'] = df_extracted['inserted_date'].astype(str).replace('NaT', None)
                    
                    st.subheader("Extracted Records:")
                    st.dataframe(df_extracted)
                    
                    button_placeholder = st.empty()
                    upload_button = button_placeholder.button("Upload Records to Database", key="upload_button")
                    
                    if upload_button:
                        with st.spinner("Checking for existing records in database..."):
                            df_to_check = df_extracted.copy()
                            
                            unique_combinations = df_to_check[['chcode', 'status', 'date', 'time', 'inserted_date']].drop_duplicates()
                            
                            existing_records = []
                            total_combinations = len(unique_combinations)
                            
                            if total_combinations > 0:
                                check_progress = st.progress(0)
                                check_status = st.empty()
                                check_status.text(f"Checking 0 of {total_combinations} records...")
                                
                                batch_size = 100
                                for i in range(0, total_combinations, batch_size):
                                    batch = unique_combinations.iloc[i:i+batch_size]
                                    
                                    for _, row in batch.iterrows():
                                        chcode = row['chcode']
                                        status = row['status']
                                        inserted_date = row['inserted_date']
                                        
                                        query = supabase.table(TABLE_NAME).select("*").eq('chcode', chcode).eq('status', status)
                                        
                                        if inserted_date is not None and inserted_date != 'NaT':
                                            query = query.eq('inserted_date', inserted_date)
                                            
                                        try:
                                            response = query.execute()
                                            if hasattr(response, 'data') and response.data:
                                                existing_records.extend(response.data)
                                        except Exception as e:
                                            st.warning(f"Error checking record: {str(e)}. Continuing...")
                                    
                                    progress_value = min(1.0, (i + batch_size) / total_combinations)
                                    check_progress.progress(progress_value)
                                    check_status.text(f"Checking {min(i + batch_size, total_combinations)} of {total_combinations} records...")
                                
                                check_progress.empty()
                                check_status.empty()
                            
                            existing_df = pd.DataFrame(existing_records) if existing_records else pd.DataFrame()
                            
                            if not existing_df.empty:
                                df_extracted['chcode'] = df_extracted['chcode'].astype(str)
                                df_extracted['status'] = df_extracted['status'].astype(str)
                                
                                existing_df['chcode'] = existing_df['chcode'].astype(str)
                                existing_df['status'] = existing_df['status'].astype(str)
                                
                                df_extracted['unique_key'] = df_extracted['chcode'] + '_' + df_extracted['status'] + '_' + df_extracted['inserted_date'].astype(str)
                                
                                existing_keys = []
                                for _, row in existing_df.iterrows():
                                    key = str(row['chcode']) + '_' + str(row['status']) + '_' + str(row['inserted_date'])
                                    existing_keys.append(key)
                                
                                df_new_records = df_extracted[~df_extracted['unique_key'].isin(existing_keys)].copy()
                                df_new_records.drop('unique_key', axis=1, inplace=True)
                            else:
                                df_new_records = df_extracted.copy()
                        
                        total_records = len(df_extracted)
                        new_records = len(df_new_records)
                        duplicate_records = total_records - new_records
                        
                        st.info(f"Found {total_records} total records. {new_records} are new and {duplicate_records} already exist.")
                        
                        if new_records > 0:
                            try:
                                df_to_upload = df_new_records.copy()
                                
                                for col in df_to_upload.columns:
                                    if pd.api.types.is_datetime64_any_dtype(df_to_upload[col]):
                                        df_to_upload[col] = df_to_upload[col].dt.strftime('%Y-%m-%d %H:%M:%S')
                                
                                df_to_upload = df_to_upload.astype(object).where(pd.notnull(df_to_upload), None)
                                
                                records_to_insert = df_to_upload.to_dict(orient="records")
                                
                                if records_to_insert:
                                    batch_size = 100
                                    success_count = 0
                                    
                                    progress_bar = st.progress(0)
                                    status_text = st.empty()
                                    
                                    for i in range(0, len(records_to_insert), batch_size):
                                        batch = records_to_insert[i:i+batch_size]
                                        
                                        if batch:
                                            try:
                                                response = supabase.table(TABLE_NAME).insert(batch).execute()
                                                
                                                if hasattr(response, 'data') and response.data:
                                                    success_count += len(response.data)
                                            except Exception as e:
                                                st.error(f"Error inserting batch: {str(e)}")
                                        
                                        progress = min(i + batch_size, len(records_to_insert)) / max(1, len(records_to_insert))
                                        progress_bar.progress(progress)
                                        status_text.text(f"Uploaded {success_count} of {len(records_to_insert)} records...")
                                    
                                    st.toast(f"Field Result Updated! {success_count} unique records uploaded successfully.")
                                    st.success("Upload completed successfully!")
                                else:
                                    st.warning("No new records to upload.")
                            
                            except Exception as e:
                                st.error(f"Error uploading field result: {str(e)}")
                                import traceback
                                st.code(traceback.format_exc())
                        else:
                            st.warning("No new records to upload. All records already exist in the database.")

                else:
                    st.error("Required columns not found in the uploaded file.")
            except Exception as e:
                st.error(f"Error processing Excel file: {str(e)}")
                
        if upload_dataset:
            TABLE_NAME = 'rob_bike_dataset'
            try:
                xls = pd.ExcelFile(upload_dataset)
                
                sheet_options = xls.sheet_names
                if len(sheet_options) > 1:
                    selected_sheet = st.selectbox(
                        "Select a sheet from the Excel file:",
                        options=sheet_options,
                        index=0,
                        key="dataset_sheet_select"
                    )
                else:
                    selected_sheet = sheet_options[0]
                    
                if selected_sheet:     
                    df = pd.read_excel(xls, sheet_name=selected_sheet)
                    df_clean = df.replace({np.nan: 0})
                    df_filtered = df_clean.copy()
                
                st.subheader("Uploaded Dataset:")
                st.dataframe(df_filtered)
                
                possible_column_variants = {
                    'ChCode': ['ChCode'],
                    'Account Number': ['Account Number', 'Account_Number'],
                    'Client Name': ['Client Name', 'Client_Name'],
                    'Endorsement Date': ['Endorsement Date', 'Endorsement_Date'],
                    'Endrosement DPD': ['Endrosement DPD', 'Endrosement_DPD'],
                    'Store': ['Store'],
                    'Cluster': ['Cluster']
                }
                
                target_columns = [
                    'chcode',
                    'account_number',
                    'client_name',
                    'endo_date',
                    'endo_dpd',
                    'stores',
                    'cluster'
                ]
                
                column_mapping = {}
                for (key, variants), target in zip(possible_column_variants.items(), target_columns):
                    for variant in variants:
                        if variant in df_filtered.columns:
                            column_mapping[variant] = target
                            break 
                        
                if len(column_mapping) == len(target_columns):
                    df_selected = df_filtered[list(column_mapping.keys())].rename(columns=column_mapping)
                    
                    df_selected = df_selected.rename(columns=column_mapping)
                    
                    button_placeholder = st.empty()
                    status_placeholder = st.empty()
                    
                    upload_button = button_placeholder.button("Upload to Database", key="upload_dataset_button")
                    
                    if upload_button:
                        button_placeholder.button("Processing...", disabled=True, key="processing_dataset_button")
                        
                        try:
                            unique_id_col = 'account_number'
                            unique_ids = df_selected[unique_id_col].astype(str).str.strip().unique().tolist()
                            
                            for col in df_selected.columns:
                                if pd.api.types.is_datetime64_any_dtype(df_selected[col]):
                                    df_selected[col] = df_selected[col].dt.strftime('%Y-%m-%d')
                            
                            df_selected = df_selected.astype(object).where(pd.notnull(df_selected), None)
                            df_selected[unique_id_col] = df_selected[unique_id_col].astype(str).str.strip() 
                            
                            new_records = df_selected.to_dict(orient="records")
                            
                            existing_records = []
                            batch_size_for_query = 20
                            
                            progress_bar = st.progress(0)
                            status_text = status_placeholder.empty()
                            status_text.text("Fetching existing records...")
                            
                            for i in range(0, len(unique_ids), batch_size_for_query):
                                batch_ids = unique_ids[i:i+batch_size_for_query]
                                batch_ids = [id for id in batch_ids if id is not None and id != '']
                                
                                if batch_ids:
                                    try:
                                        batch_response = supabase.table(TABLE_NAME).select("*").in_(unique_id_col, batch_ids).execute()
                                        if hasattr(batch_response, 'data') and batch_response.data:
                                            existing_records.extend(batch_response.data)
                                    except Exception as e:
                                        st.warning(f"Error fetching batch {i}: {str(e)}. Continuing...")
                                
                                progress_value = min(1.0, (i + batch_size_for_query) / max(1, len(unique_ids)))
                                progress_bar.progress(progress_value)
                            
                            existing_df = pd.DataFrame(existing_records) if existing_records else pd.DataFrame()
                            if not existing_df.empty:
                                existing_df[unique_id_col] = existing_df[unique_id_col].astype(str).str.strip()
                            
                            records_to_insert = []
                            records_to_update = []
                            total_records = len(new_records)
                            processed_count = 0
                            
                            status_text.text("Identifying records to insert or update...")
                            progress_bar.progress(0)
                            
                            def records_differ(new_record, existing_record):
                                for key, value in new_record.items():
                                    if key in existing_record and str(value).strip() != str(existing_record[key]).strip():
                                        return True
                                return False
                            
                            for new_record in new_records:
                                processed_count += 1
                                account_number = str(new_record[unique_id_col]).strip()
                                
                                if not existing_df.empty:
                                    matching_records = existing_df[existing_df[unique_id_col] == account_number]
                                    
                                    if not matching_records.empty:
                                        existing_record = matching_records.iloc[0].to_dict()
                                        if records_differ(new_record, existing_record):
                                            new_record['id'] = existing_record['id']
                                            records_to_update.append(new_record)
                                    else:
                                        records_to_insert.append(new_record)
                                else:
                                    records_to_insert.append(new_record)
                                
                                progress_value = min(1.0, processed_count / total_records)
                                progress_bar.progress(progress_value)
                            
                            status_placeholder.info(f"Found {len(records_to_insert)} records to insert and {len(records_to_update)} records to update.")
                            
                            batch_size_for_db = 100
                            success_count = 0
                            
                            if records_to_insert:
                                status_text.text("Inserting new records...")
                                progress_bar.progress(0)
                                
                                for i in range(0, len(records_to_insert), batch_size_for_db):
                                    batch = records_to_insert[i:i+batch_size_for_db]
                                    
                                    if batch:
                                        try:
                                            response = supabase.table(TABLE_NAME).insert(batch).execute()
                                            if hasattr(response, 'data') and response.data:
                                                success_count += len(batch)
                                        except Exception as e:
                                            st.error(f"Error inserting records batch: {str(e)}")
                                    
                                    progress_value = min(1.0, min(i + batch_size_for_db, len(records_to_insert)) / max(1, len(records_to_insert)))
                                    progress_bar.progress(progress_value)
                                    status_text.text(f"Inserted {success_count} of {len(records_to_insert)} new records...")
                            
                            update_count = 0
                            if records_to_update:
                                status_text.text("Updating existing records...")
                                progress_bar.progress(0)
                                
                                for i, record in enumerate(records_to_update):
                                    record_id = record.pop('id')
                                    
                                    try:
                                        response = supabase.table(TABLE_NAME).update(record).eq('id', record_id).execute()
                                        if hasattr(response, 'data') and response.data:
                                            update_count += 1
                                    except Exception as e:
                                        st.error(f"Error updating record {record_id}: {str(e)}")
                                    
                                    progress_value = min(1.0, (i + 1) / len(records_to_update))
                                    progress_bar.progress(progress_value)
                                    status_text.text(f"Updated {update_count} of {len(records_to_update)} existing records...")
                            
                            total_processed = success_count + update_count
                            if total_processed > 0:
                                st.toast(f"Dataset Updated! {success_count} records inserted and {update_count} records updated successfully.")
                                button_placeholder.button("Upload Complete!", disabled=True, key="complete_dataset_button")
                            else:
                                st.warning("No records were processed. Either no changes were needed or the operation failed.")
                                button_placeholder.button("Try Again", key="retry_dataset_button")
                        
                        except Exception as e:
                            st.error(f"Error uploading dataset: {str(e)}")
                            import traceback
                            st.code(traceback.format_exc())
                            button_placeholder.button("Upload Failed - Try Again", key="error_dataset_button")
                else:
                    missing_cols = [col for col in possible_column_variants if col not in df_filtered.columns]
                    st.error(f"Required columns not found in the uploaded file.")
                    
            except Exception as e:
                st.error(f"Error processing Excel file: {str(e)}")
                        
        if upload_disposition:
            TABLE_NAME = 'rob_bike_disposition'
            try:
                xls = pd.ExcelFile(upload_disposition)
                
                sheet_options = xls.sheet_names
                if len(sheet_options) > 1:
                    selected_sheet = st.selectbox(
                        "Select a sheet from the Excel file:",
                        options=sheet_options,
                        index=0,
                        key="disposition_sheet_select"
                    )
                else:
                    selected_sheet = sheet_options[0]    
                    
                if selected_sheet:
                    df = pd.read_excel(xls, sheet_name=selected_sheet)
                    df_clean = df.replace({np.nan: ''})
                    df_filtered = df_clean.copy()

                st.subheader("Uploaded Disposition:")
                st.dataframe(df_filtered)

                button_placeholder = st.empty()
                upload_button = button_placeholder.button("Upload to Database", key="upload_disposition_button")

                if upload_button:
                    button_placeholder.button("Processing...", disabled=True, key="processing_disposition_button")
                    try:
                        if 'CMS Disposition' in df_filtered.columns:
                            unique_dispositions = df_filtered['CMS Disposition'].drop_duplicates().tolist()

                            existing_response = supabase.table(TABLE_NAME).select("disposition").execute()
                            if existing_response.data is None:
                                existing_dispositions = []
                            else:
                                existing_dispositions = [record['disposition'] for record in existing_response.data]

                            records_to_insert = [
                                {"disposition": d} for d in unique_dispositions if d not in existing_dispositions
                            ]

                            if records_to_insert:
                                insert_response = supabase.table(TABLE_NAME).insert(records_to_insert).execute()
                                toast_placeholder = st.empty()
                                toast_placeholder.success("Upload successful!")
                                time.sleep(3)
                                toast_placeholder.empty()
                            else:
                                st.info("No new dispositions to add; all values already exists.")

                            button_placeholder.empty()
                        else:
                            st.error("Required columns was not found in the uploaded file.")
                    except Exception as e:
                        st.error(f"Error uploading disposition: {str(e)}")
                        button_placeholder.button("Upload Failed - Try Again", key="error_disposition_button")        
            except Exception as e:
                st.error(f"An error occurred: {str(e)}")

    if campaign == "ROB Bike" and automation_type == "Endorsement":
        endo_date = st.sidebar.date_input('Endo Date', format="MM/DD/YYYY") 
    
    if campaign == "BDO Auto B5 & B6" and automation_type == "Agency Daily Report":
        def clean_number_input(label):
            raw_input = st.sidebar.text_input(label)
            clean_input = raw_input.replace(",", "")
            try:
                return float(clean_input)
            except ValueError:
                return None
                
        st.sidebar.subheader("B5")
        col1, col2 = st.columns(2)
        with col1:
            kept_count_b5 = clean_number_input("Kept Count (B5)")
        with col2:
            kept_bal_b5 = clean_number_input("Kept Balance (B5)")
        alloc_bal_b5 = clean_number_input("Allocation Balance (B5)")

        st.sidebar.subheader("B6")
        col1, col2 = st.columns(2)
        with col1:
            kept_count_b6 = clean_number_input("Kept Count (B6)")
        with col2:
            kept_bal_b6 = clean_number_input("Kept Balance (B6)")
        alloc_bal_b6 = clean_number_input("Allocation Balance (B6)")

    if campaign == "BDO Auto B5 & B6" and automation_type == "Endorsement":
        endo_date = st.sidebar.date_input('Endo Date', format="MM/DD/YYYY")
        buckets = ['BUCKET 5', 'BUCKET 6']
        bucket = st.sidebar.selectbox("Select Bucket", buckets)

        with st.sidebar.expander("Upload Other File", expanded=False):
            upload_datasets = st.file_uploader(
                "Dataset",
                type=["xlsx", "xls"],
                key=f"{campaign}_dataset",
                accept_multiple_files=True
            )

        if upload_datasets:
            TABLE_NAME = 'bdo_auto_loan_dataset'
            all_records_to_insert = []
            all_records_to_update = []
            file_dataframes = []

            for idx, upload_file in enumerate(upload_datasets):
                try:
                    xls = pd.ExcelFile(upload_file)
                    sheet_options = xls.sheet_names
                    if len(sheet_options) > 1:
                        selected_sheet = st.selectbox(
                            f"Select a sheet for file {upload_file.name}:",
                            options=sheet_options,
                            index=0,
                            key=f"sheet_select_{idx}_{campaign}"
                        )
                    else:
                        selected_sheet = sheet_options[0]

                    if selected_sheet:
                        df = pd.read_excel(xls, sheet_name=selected_sheet)
                        df_clean = df.replace({np.nan: ""})

                        st.subheader(f"Uploaded File: {upload_file.name}")
                        st.dataframe(df_clean)
                        df_clean.columns = df_clean.columns.str.strip().str.lower().str.replace(' ', '')
                        df_selected = df_clean[['accountnumber', 'chcode']].copy()
                        df_selected.columns = ['account_number', 'chcode']
                        
                        file_dataframes.append((upload_file.name, df_selected))

                except Exception as e:
                    st.error(f"Error processing file {upload_file.name}: {str(e)}")

            button_placeholder = st.empty()
            status_placeholder = st.empty()
            upload_button = button_placeholder.button("Upload All Files to Database", key="upload_all_datasets_button")

            if upload_button:
                button_placeholder.button("Processing...", disabled=True, key="processing_all_datasets_button")
                
                try:
                    unique_id_col = 'account_number'
                    all_unique_ids = set()
                    
                    for file_name, df_selected in file_dataframes:
                        unique_ids = df_selected[unique_id_col].astype(str).str.strip().unique().tolist()
                        all_unique_ids.update(unique_ids)
                    
                    all_unique_ids = [id for id in all_unique_ids if id is not None and id != '']
                    
                    existing_records = []
                    batch_size_for_query = 20
                    progress_bar = st.progress(0)
                    status_text = status_placeholder.empty()
                    status_text.text("Fetching existing records...")
                    
                    for i in range(0, len(all_unique_ids), batch_size_for_query):
                        batch_ids = all_unique_ids[i:i+batch_size_for_query]
                        if batch_ids:
                            try:
                                batch_response = supabase.table(TABLE_NAME).select("*").in_(unique_id_col, batch_ids).execute()
                                if hasattr(batch_response, 'data') and batch_response.data:
                                    existing_records.extend(batch_response.data)
                            except Exception as e:
                                st.warning(f"Error fetching batch {i}: {str(e)}. Continuing...")
                        
                        progress_value = min(1.0, (i + batch_size_for_query) / max(1, len(all_unique_ids)))
                        progress_bar.progress(progress_value)
                    
                    existing_df = pd.DataFrame(existing_records) if existing_records else pd.DataFrame()
                    if not existing_df.empty:
                        existing_df[unique_id_col] = existing_df[unique_id_col].astype(str).str.strip()
                    
                    total_records = 0
                    for file_name, df_selected in file_dataframes:
                        df_selected = df_selected.astype(object).where(pd.notnull(df_selected), None)
                        df_selected[unique_id_col] = df_selected[unique_id_col].astype(str).str.strip()
                        new_records = df_selected.to_dict(orient="records")
                        total_records += len(new_records)
                        
                        def records_differ(new_record, existing_record):
                            for key, value in new_record.items():
                                if key in existing_record and str(value).strip() != str(existing_record[key]).strip():
                                    return True
                            return False
                        
                        for new_record in new_records:
                            account_number = str(new_record[unique_id_col]).strip()
                            
                            if not existing_df.empty:
                                matching_records = existing_df[existing_df[unique_id_col] == account_number]
                                
                                if not matching_records.empty:
                                    existing_record = matching_records.iloc[0].to_dict()
                                    if records_differ(new_record, existing_record):
                                        new_record['id'] = existing_record['id']
                                        all_records_to_update.append(new_record)
                                else:
                                    all_records_to_insert.append(new_record)
                            else:
                                all_records_to_insert.append(new_record)
                        
                        progress_value = min(1.0, len(all_records_to_insert) + len(all_records_to_update) / max(1, total_records))
                        progress_bar.progress(progress_value)
                    
                    status_placeholder.info(f"Found {len(all_records_to_insert)} records to insert and {len(all_records_to_update)} records to update across all files.")
                    
                    batch_size_for_db = 100
                    success_count = 0
                    
                    if all_records_to_insert:
                        status_text.text("Inserting new records...")
                        progress_bar.progress(0)
                        
                        for i in range(0, len(all_records_to_insert), batch_size_for_db):
                            batch = all_records_to_insert[i:i+batch_size_for_db]
                            
                            if batch:
                                try:
                                    response = supabase.table(TABLE_NAME).insert(batch).execute()
                                    if hasattr(response, 'data') and response.data:
                                        success_count += len(batch)
                                except Exception as e:
                                    st.error(f"Error inserting records batch: {str(e)}")
                            
                            progress_value = min(1.0, min(i + batch_size_for_db, len(all_records_to_insert)) / max(1, len(all_records_to_insert)))
                            progress_bar.progress(progress_value)
                            status_text.text(f"Inserted {success_count} of {len(all_records_to_insert)} new records...")
                    
                    update_count = 0
                    if all_records_to_update:
                        status_text.text("Updating existing records...")
                        progress_bar.progress(0)
                        
                        for i, record in enumerate(all_records_to_update):
                            record_id = record.pop('id')
                            
                            try:
                                response = supabase.table(TABLE_NAME).update(record).eq('id', record_id).execute()
                                if hasattr(response, 'data') and response.data:
                                    update_count += 1
                            except Exception as e:
                                st.error(f"Error updating record {record_id}: {str(e)}")
                            
                            progress_value = min(1.0, (i + 1) / len(all_records_to_update))
                            progress_bar.progress(progress_value)
                            status_text.text(f"Updated {update_count} of {len(all_records_to_update)} existing records...")
                    
                    total_processed = success_count + update_count
                    if total_processed > 0:
                        st.toast(f"All Datasets Updated! {success_count} records inserted successfully.")
                        button_placeholder.button("Upload Complete!", disabled=True, key="complete_all_datasets_button")
                    else:
                        st.warning("No records were processed. Either no changes were needed or the operation failed.")
                        button_placeholder.button("Try Again", key="retry_all_datasets_button")
                
                except Exception as e:
                    st.error(f"Error uploading datasets: {str(e)}")
                    import traceback
                    st.code(traceback.format_exc())
                    button_placeholder.button("Upload Failed - Try Again", key="error_all_datasets_button")

    if campaign == "Sumisho" and automation_type == "Daily Remark Report":
        upload_madrid_daily = st.sidebar.file_uploader(
            "SP Madrid Daily",
            type=["xlsx", "xls"],
            key=f"{campaign}_sp_madrid_daily"
        )   

        if upload_madrid_daily is not None:
            sp_madrid_daily = upload_madrid_daily.getvalue()

            template_stream = io.BytesIO(sp_madrid_daily)
            template_xls = pd.ExcelFile(template_stream)
            template_sheets = template_xls.sheet_names

            selected_template_sheet = st.sidebar.selectbox("Select a sheet from the SP Madrid Daily Template", template_sheets)

            template_stream.seek(0)
            template_df_preview = pd.read_excel(template_stream, sheet_name=selected_template_sheet, header=1)
            available_columns = list(template_df_preview.columns)

            selected_date_column = st.sidebar.selectbox("Select the column to insert 'Date + Remark'", available_columns)

        else:
            st.warning("Please upload the SP Madrid Daily template file.")
            st.stop()
            

    df = None
    sheet_names = []

    if uploaded_file is not None:
        if 'previous_filename' not in st.session_state or st.session_state['previous_filename'] != uploaded_file.name:
            if 'output_binary' in st.session_state:
                del st.session_state['output_binary']
            if 'output_filename' in st.session_state:
                del st.session_state['output_filename']
            if 'result_sheet_names' in st.session_state:
                del st.session_state['result_sheet_names']
                
            st.session_state['previous_filename'] = uploaded_file.name
        
        with st.sidebar.expander("Data Cleaning Options"):
            remove_duplicates = st.checkbox("Remove Duplicates", value=False, key=f"{campaign}_remove_duplicates")
            remove_blanks = st.checkbox("Remove Blanks", value=False, key=f"{campaign}_remove_blanks")
            trim_spaces = st.checkbox("Trim Text", value=False, key=f"{campaign}_trim_spaces")
        
        with st.sidebar.expander("Data Manipulation"):
            st.markdown("#### Column Operations")
            enable_add_column = st.checkbox("Add Column", value=False)
            enable_column_removal = st.checkbox("Remove Column", value=False)
            enable_column_renaming = st.checkbox("Rename Column", value=False)
            
            st.markdown("#### Row Operations")
            enable_row_filtering = st.checkbox("Filter Row", value=False)
            enable_add_row = st.checkbox("Add Row", value=False)
            enable_row_removal = st.checkbox("Remove Row", value=False)
            
            st.markdown("#### Value Operations")
            enable_edit_values = st.checkbox("Edit Values", value=False)
          
        file_content = uploaded_file.getvalue()
        file_buffer = io.BytesIO(file_content)
                
        try:
            file_buffer.seek(0) 
            xlsx = pd.ExcelFile(file_buffer)
            sheet_names = xlsx.sheet_names
            is_encrypted = False
            decrypted_file = file_buffer

        except Exception as e:
            if "corrupted" in str(e).lower() or "encrypted" in str(e).lower() or "ole2" in str(e).lower() or "bad magic" in str(e).lower():
                is_encrypted = True
                st.sidebar.warning("This file appears to be password protected or in an unsupported format.")
                excel_password = st.sidebar.text_input("Enter Excel password", type="password")

                if not excel_password:
                    st.warning("Please enter the Excel file password.")
                    st.stop()

                try:
                    decrypted_file = io.BytesIO()
                    
                    if isinstance(file_content, io.BytesIO):
                        file_content.seek(0)
                        office_file = msoffcrypto.OfficeFile(file_content)
                    else:
                        office_file = msoffcrypto.OfficeFile(io.BytesIO(file_content))
                        
                    office_file.load_key(password=excel_password)
                    office_file.decrypt(decrypted_file)
                    decrypted_file.seek(0)

                    xlsx = pd.ExcelFile(decrypted_file)
                    sheet_names = xlsx.sheet_names
                    
                except Exception as decrypt_error:
                    st.sidebar.error(f"Decryption failed: {str(decrypt_error)}")
                    st.stop()
            else:
                st.sidebar.error(f"Error reading file: {str(e)}")
                st.stop()
        
        if len(sheet_names) > 1 :
            selected_sheet = st.sidebar.selectbox(
                "Select Sheet", 
                options=sheet_names,
                index=0,
                key=f"{campaign}_sheet_selector"
            )
        else:
            selected_sheet = sheet_names[0]
        
        try:
            if is_encrypted:
                decrypted_file.seek(0)
                df = pd.read_excel(decrypted_file, sheet_name=selected_sheet)
            else:
                df = pd.read_excel(xlsx, sheet_name=selected_sheet)
                
            if selected_sheet and preview:
                st.subheader(f"Preview of {selected_sheet}")
                df_preview = df.copy().dropna(how='all').dropna(how='all', axis=1)
                st.dataframe(df_preview, use_container_width=True)
                
        except Exception as e:
            st.sidebar.error(f"Error reading sheet: {str(e)}")
    
    process_button = st.sidebar.button("Process File", type="primary", disabled=uploaded_file is None, key=f"{campaign}_process_button")

    if uploaded_file is not None:
        file_content = uploaded_file.getvalue() if hasattr(uploaded_file, 'getvalue') else uploaded_file.read()
        
        try:
            if "renamed_df" in st.session_state:
                df = st.session_state["renamed_df"]
            else:
                pass
            
            df = df.dropna(how='all', axis=0) 
            df = df.dropna(how='all', axis=1)

            if enable_add_column:
                st.subheader("Add New Columns")

                if "column_definitions" not in st.session_state:
                    st.session_state.column_definitions = []

                with st.form("add_column_form", clear_on_submit=True):
                    new_column_name = st.text_input("New Column Name")
                    column_source_type = st.radio("Column Source", ["Input Value", "Copy From Column", "Excel-like Formula"], key="source_type")

                    source_column = modification_type = prefix_text = suffix_text = selected_function = custom_function = formula = None
                    
                    if column_source_type == "Input Value":
                        input_value = st.text_input("Value to fill in each row")
                    elif column_source_type == "Copy From Column":
                        source_column = st.selectbox("Source Column (copy from)",    df.columns.tolist(), key="source_column")
                        modification_type = st.radio("Modification Type", ["Direct Copy", "Text Prefix", "Text Suffix", "Apply Function"], key="mod_type")

                        if modification_type == "Text Prefix":
                            prefix_text = st.text_input("Prefix to add")
                        elif modification_type == "Text Suffix":
                            suffix_text = st.text_input("Suffix to add")
                        elif modification_type == "Apply Function":
                            function_options = ["To Uppercase", "To Lowercase", "Strip Spaces", "Custom Function"]
                            selected_function = st.selectbox("Select Function", function_options)
                            if selected_function == "Custom Function":
                                custom_function = st.text_area("Custom function (use 'x')", value="lambda x: x")
                    else:
                        st.info("Use column names in curly braces {} and expressions (e.g. `{Amount} * 2`, etc.)")
                        formula = st.text_area("Excel-like formula", height=80)

                    submitted = st.form_submit_button("Add to List")
                    if submitted and new_column_name:
                        st.session_state.column_definitions.append({
                            "name": new_column_name,
                            "source": column_source_type,
                            "source_column": source_column,
                            "modification_type": modification_type,
                            "prefix_text": prefix_text,
                            "suffix_text": suffix_text,
                            "function": selected_function,
                            "custom_function": custom_function,
                            "formula": formula,
                            "input_value": input_value if column_source_type == "Input Value" else None,
                        })
                        st.success(f"Queued column: {new_column_name}")

                if st.session_state.column_definitions:
                    st.write("🧾 Queued Columns to Add:")
                    for idx, col_def in enumerate(st.session_state.column_definitions):
                        st.markdown(f"- **{col_def['name']}** from **{col_def['source']}**")

                    if st.button("Apply All Column Additions"):
                            
                        try:
                            for col_def in st.session_state.column_definitions:
                                name = col_def["name"]
                                source = col_def["source"]
                                
                                if source == "Input Value":
                                    input_value = col_def["input_value"]
                                    df[name] = input_value
                                elif source == "Copy From Column":
                                    source_col = col_def["source_column"]
                                    mod_type = col_def["modification_type"]

                                    if mod_type == "Direct Copy":
                                        df[name] = df[source_col]
                                    elif mod_type == "Text Prefix":
                                        df[name] = col_def["prefix_text"] + df[source_col].astype(str)
                                    elif mod_type == "Text Suffix":
                                        df[name] = df[source_col].astype(str) + col_def["suffix_text"]
                                    elif mod_type == "Apply Function":
                                        if col_def["function"] == "To Uppercase":
                                            df[name] = df[source_col].astype(str).str.upper()
                                        elif col_def["function"] == "To Lowercase":
                                            df[name] = df[source_col].astype(str).str.lower()
                                        elif col_def["function"] == "Strip Spaces":
                                            df[name] = df[source_col].astype(str).str.strip()
                                        elif col_def["function"] == "Custom Function":
                                            func = eval(col_def["custom_function"])
                                            df[name] = df[source_col].apply(func)

                                elif source == "Excel-like Formula":
                                    formula = col_def["formula"]
                                    processed = formula
                                    for col in df.columns:
                                        pattern = r'\{' + re.escape(col) + r'\}'
                                        processed = re.sub(pattern, f"df['{col}']", processed)
                                    processed = processed.replace("IF(", "np.where(").replace("SUM(", "np.sum(")
                                    processed = processed.replace("AVG(", "np.mean(").replace("MAX(", "np.max(").replace("MIN(", "np.min(")
                                    df[name] = eval(processed)

                            st.success("All queued columns added successfully!")
                            st.session_state.renamed_df = df
                            st.session_state.column_definitions.clear()
                        except Exception as e:
                            st.error(f"Error applying column additions: {str(e)}")

            if enable_column_removal:
                st.subheader("Column Removal")
                cols = df.columns.tolist()
                cols_to_remove = st.multiselect("Select columns to remove", cols)
                if cols_to_remove:
                    df = df.drop(columns=cols_to_remove)
                    st.success(f"Removed columns: {', '.join(cols_to_remove)}")

            if enable_column_renaming:
                st.subheader("Column Renaming")
                
                rename_df = pd.DataFrame({
                    "original_name": df.columns,
                    "new_name": df.columns
                })
                
                edited_df = st.data_editor(
                    rename_df,
                    column_config={
                        "original_name": st.column_config.TextColumn("Original Column Name", disabled=True),
                        "new_name": st.column_config.TextColumn("New Column Name")
                    },
                    hide_index=True,
                    key="column_rename_editor"
                )
                
                if st.button("Apply Column Renames", key="apply_multiple_renames"):
                    rename_dict = {
                        orig: new 
                        for orig, new in zip(edited_df["original_name"], edited_df["new_name"]) 
                        if orig != new
                    }

                    if rename_dict:
                        df = df.rename(columns=rename_dict)
                        st.session_state["renamed_df"] = df
                        st.success(f"Renamed {len(rename_dict)} column(s): {', '.join([f'{k} → {v}' for k, v in rename_dict.items()])}")

            if enable_row_filtering:
                st.subheader("Row Filtering")
                filter_col = st.selectbox("Select column to filter by", df.columns.tolist())
                filter_value = st.text_input("Enter search/filter value")
                
                if filter_value and filter_col:
                    if pd.api.types.is_numeric_dtype(df[filter_col]):
                        try:
                            filter_value_num = float(filter_value)
                            filtered_df = df[df[filter_col] == filter_value_num]
                        except ValueError:
                            st.warning("Entered value is not numeric. Using string comparison instead.")
                            filtered_df = df[df[filter_col].astype(str).str.contains(filter_value, case=False, na=False)]
                    else:
                        filtered_df = df[df[filter_col].astype(str).str.contains(filter_value, case=False, na=False)]

                    st.write(f"Found {len(filtered_df)} rows matching filter: '{filter_value}' in column '{filter_col}'")
                    df = filtered_df
                    
            if enable_add_row:
                st.subheader("Add New Rows")
                with st.form("add_row_form"):
                    row_data = {}
                    for col in df.columns:
                        row_data[col] = st.text_input(f"Value for {col}", "")
                    
                    add_row_submitted = st.form_submit_button("Add Row")
                    
                    if add_row_submitted:
                        new_row = pd.DataFrame([row_data])
                        df = pd.concat([df, new_row], ignore_index=True)
                        st.success("Row added successfully!")
                        st.session_state["renamed_df"] = df

            if enable_row_removal:
                st.subheader("Remove Rows")
                st.info("Select rows to remove by index")
                
                with st.form("remove_row_form"):
                    row_indices = st.multiselect("Select row indices to remove", 
                                                options=list(range(len(df))),
                                                format_func=lambda x: f"Row {x}")
                    
                    remove_rows_submitted = st.form_submit_button("Remove Selected Rows")
                    
                    if remove_rows_submitted and row_indices:
                        df = df.drop(index=row_indices).reset_index(drop=True)
                        st.success(f"Removed {len(row_indices)} row(s)")
                        st.session_state["renamed_df"] = df

            if enable_edit_values:
                st.subheader("Edit Values")
                
                edited_df = st.data_editor(
                    df,
                    num_rows="dynamic",
                    use_container_width=True,
                    key="value_editor"
                )
                
                if st.button("Apply Value Changes"):
                    st.session_state["renamed_df"] = edited_df
                    st.success("Value changes applied!")
                    
            if enable_add_column or enable_column_removal or enable_column_renaming or enable_row_filtering or enable_add_row or enable_row_removal or enable_edit_values:
                buffer = io.BytesIO()
                df.to_excel(buffer, index=False, engine='openpyxl')
                file_content = buffer.getvalue()
                st.subheader("Modified Data Preview")
                st.dataframe(df, use_container_width=True)

        except Exception as e:
            st.error(f"Error loading or manipulating file: {str(e)}")

        if "renamed_df" in st.session_state:
            df = st.session_state["renamed_df"]
            buffer = io.BytesIO()
            df.to_excel(buffer, index=False, engine='openpyxl')
            buffer.seek(0)
            file_content = buffer.getvalue()

        if process_button and selected_sheet:
            try:
                with st.spinner("Processing file..."):
                    file_to_process = decrypted_file if is_encrypted else file_content
                    if campaign == "BPI Auto Curing" and automation_type == "Cured List":
                        result = processor.process_cured_list(
                            file_to_process, 
                            sheet_name=selected_sheet,
                            preview_only=False,
                            remove_duplicates=remove_duplicates, 
                            remove_blanks=remove_blanks, 
                            trim_spaces=trim_spaces
                        )
                        st.session_state['cured_list_result'] = result
                        
                    elif campaign == "BDO Auto B5 & B6" and automation_type == "Agency Daily Report":
                        if None in [kept_count_b5, kept_bal_b5, alloc_bal_b5, kept_count_b6, kept_bal_b6, alloc_bal_b6]:
                            st.error("Please enter valid numbers for all B5 and B6 fields (numbers only, commas allowed).")
                        else:
                            result = processor.process_agency_daily_report(
                                file_to_process, 
                                sheet_name=selected_sheet,
                                preview_only=False,
                                remove_duplicates=remove_duplicates, 
                                remove_blanks=remove_blanks, 
                                trim_spaces=trim_spaces,
                                kept_count_b5=kept_count_b5,
                                kept_bal_b5=kept_bal_b5,
                                alloc_bal_b5=alloc_bal_b5,
                                kept_count_b6=kept_count_b6,
                                kept_bal_b6=kept_bal_b6,
                                alloc_bal_b6=alloc_bal_b6
                            )
                            st.session_state['agency_daily_result'] = result
                    
                    elif campaign == "BDO Auto B5 & B6" and automation_type == "Endorsement":
                        result = processor.process_new_endorsement(
                            file_to_process, 
                            sheet_name=selected_sheet,
                            preview_only=False,
                            remove_duplicates=remove_duplicates, 
                            remove_blanks=remove_blanks, 
                            trim_spaces=trim_spaces,
                            endo_date=endo_date,
                            bucket=bucket,
                        )
                        st.session_state['new_endorsement'] = result

                    elif campaign == "ROB Bike" and automation_type == "Endorsement":
                        result = processor.process_new_endorsement(
                            file_to_process, 
                            sheet_name=selected_sheet,
                            preview_only=False,
                            remove_duplicates=remove_duplicates, 
                            remove_blanks=remove_blanks, 
                            trim_spaces=trim_spaces,
                            endo_date=endo_date,
                        )
                        st.session_state['new_endorsement'] = result
                    else:
                        if automation_type == "Data Clean":
                            result_df, output_binary, output_filename = getattr(processor, automation_map[automation_type])(
                                file_to_process, 
                                sheet_name=selected_sheet,
                                preview_only=False,
                                remove_duplicates=remove_duplicates,
                                remove_blanks=remove_blanks,
                                trim_spaces=trim_spaces,
                                file_name=uploaded_file.name
                            )
                        elif campaign == "ROB Bike" and automation_type == "Daily Remark Report":
                            result_df, output_binary, output_filename = getattr(processor, automation_map[automation_type])(
                                file_to_process,  
                                sheet_name=selected_sheet,
                                preview_only=False,
                                remove_duplicates=remove_duplicates, 
                                remove_blanks=remove_blanks, 
                                trim_spaces=trim_spaces,
                                report_date=report_date
                            )
                        elif campaign == "Sumisho" and automation_type == "Daily Remark Report":
                            result_df, output_binary, output_filename = getattr(processor, automation_map[automation_type])(
                                file_to_process,  
                                sheet_name=selected_sheet,
                                preview_only=False,
                                remove_duplicates=remove_duplicates, 
                                remove_blanks=remove_blanks, 
                                trim_spaces=trim_spaces,
                                template_content=sp_madrid_daily,
                                template_sheet=selected_template_sheet,
                                target_column=selected_date_column
                            )
                        else:
                            result_df, output_binary, output_filename = getattr(processor, automation_map[automation_type])(
                                file_to_process, 
                                sheet_name=selected_sheet,
                                preview_only=False,
                                remove_duplicates=remove_duplicates,
                                remove_blanks=remove_blanks,
                                trim_spaces=trim_spaces
                            )
                            
                        if output_binary:
                            st.session_state['output_binary'] = output_binary
                            st.session_state['output_filename'] = output_filename
                            
                            excel_file = pd.ExcelFile(io.BytesIO(output_binary))
                            result_sheet_names = excel_file.sheet_names
                            st.session_state['result_sheet_names'] = result_sheet_names
                        
                        else:
                            st.error("No output file was generated")

                if "renamed_df" in st.session_state:
                    st.session_state.pop("renamed_df", None)

            except Exception as e:
                st.error(f"Error processing file: {str(e)}")

        def add_password_protection(file_data, password):
            """Add password protection to XLSX files"""
            try:
                with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as temp_input:
                    temp_input.write(file_data)
                    temp_input_path = temp_input.name
                
                with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as temp_output:
                    temp_output_path = temp_output.name
                
                with open(temp_input_path, 'rb') as input_file:
                    office_file = msoffcrypto.OfficeFile(input_file)
                    
                    with open(temp_output_path, 'wb') as output_file:
                        office_file.encrypt(password, output_file)
                
                with open(temp_output_path, 'rb') as encrypted_file:
                    encrypted_data = encrypted_file.read()
                
                os.unlink(temp_input_path)
                os.unlink(temp_output_path)
                
                return encrypted_data
                
            except Exception as e:
                st.error(f"Error adding password protection: {str(e)}")
                st.error("Make sure 'msoffcrypto-tool' is installed: pip install msoffcrypto-tool")
                return file_data
            
        def add_password_protection_xls(file_data, password):
            """Add password protection to XLS files using Excel COM"""
            try:
                import pythoncom
                
                pythoncom.CoInitialize()
                
                with tempfile.NamedTemporaryFile(delete=False, suffix='.xls') as temp_input:
                    temp_input.write(file_data)
                    temp_input_path = temp_input.name
                
                temp_output_path = temp_input_path.replace('.xls', '_protected.xls')
                
                excel = win32.Dispatch('Excel.Application')
                excel.Visible = False
                excel.DisplayAlerts = False
                
                try:
                    wb = excel.Workbooks.Open(temp_input_path)
                    wb.SaveAs(temp_output_path, FileFormat=56, Password=password)
                    wb.Close()
                    
                    with open(temp_output_path, 'rb') as f:
                        protected_data = f.read()
                    
                    return protected_data
                    
                finally:
                    excel.Quit()
                    pythoncom.CoUninitialize()
                    
                    if os.path.exists(temp_input_path):
                        os.unlink(temp_input_path)
                    if os.path.exists(temp_output_path):
                        os.unlink(temp_output_path)
                        
            except Exception as e:
                st.error(f"Error adding XLS password protection: {str(e)}")
                st.info("XLS password protection requires Windows with Excel installed")
                return file_data
            
        def convert_to_excel_97_2003(data, filename):
            """Convert xlsx data to Excel 97-2003 (.xls) format"""
            try:
                import pythoncom  
                
                pythoncom.CoInitialize()
                
                with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as temp_xlsx:
                    temp_xlsx.write(data)
                    temp_xlsx_path = temp_xlsx.name
                
                temp_xls_path = temp_xlsx_path.replace('.xlsx', '.xls')
                
                excel = win32.Dispatch('Excel.Application')
                excel.Visible = False
                excel.DisplayAlerts = False
                
                try:
                    wb = excel.Workbooks.Open(temp_xlsx_path)
                    wb.SaveAs(temp_xls_path, FileFormat=56) 
                    wb.Close()
                    
                    with open(temp_xls_path, 'rb') as f:
                        converted_data = f.read()
                    
                    return converted_data
                    
                finally:
                    excel.Quit()
                    pythoncom.CoUninitialize()
                    
                    if os.path.exists(temp_xlsx_path):
                        os.unlink(temp_xlsx_path)
                    if os.path.exists(temp_xls_path):
                        os.unlink(temp_xls_path)
                        
            except Exception as e:
                st.error(f"Method 4 (COM) conversion error: {str(e)}")
                st.info("COM method requires Windows with Excel installed")
                return data

        def create_global_password_section(automation_type):
            """Create global password protection section for all files"""
            st.subheader("Global File Settings")
            
            col1, col2 = st.columns(2)
            
            with col1:
                apply_to_all = st.checkbox(
                    "Apply same settings to all files", 
                    value=False, 
                    key=f"{automation_type}_apply_to_all",
                    help="Use the same password and format settings for all generated files"
                )
            
            global_settings = {}
            
            if apply_to_all:
                st.info("These settings will be applied to all files.")
                
                col1, col2 = st.columns(2)
                
                if not win32_available:
                    with col1:
                        st.checkbox(
                            "Convert all to Excel 97-2003 (.xls)", 
                            value=False, 
                            key=f"{automation_type}_global_convert_xls_disabled",
                            help="XLS conversion is disabled because the current environment doesn't support it.",
                            disabled=True
                        )
                        global_convert_to_xls = False
                else:
                    with col1:
                        global_convert_to_xls = st.checkbox(
                            "Convert all to Excel 97-2003 (.xls)", 
                            value=False, 
                            key=f"{automation_type}_global_convert_xls",
                            help="Convert all files to older Excel format for compatibility"
                        )
                
                with col2:
                    global_add_password = st.checkbox(
                        "Password protect all files", 
                        value=False, 
                        key=f"{automation_type}_global_password_check"
                    )
                
                if global_add_password:
                    global_password = st.text_input(
                        "Password for all files", 
                        type="password", 
                        placeholder="Enter password (min 5 characters)",
                        key=f"{automation_type}_global_password_input",
                        help="This password will be applied to all files"
                    )
                    
                    if global_password and len(global_password) < 5:
                        st.warning("Password should be at least 5 characters long for security")
                else:
                    global_password = ""
                
                global_settings = {
                    "apply_to_all": apply_to_all,
                    "convert_to_xls": global_convert_to_xls,
                    "add_password": global_add_password,
                    "password": global_password
                }
            
            return global_settings

        def create_download_section(label, data, filename, key, mime_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", default_password=""):
            """Create download section with optional default password"""
            st.subheader("File Options")
            col1, col2 = st.columns(2)

            with col1:
                add_password = st.checkbox(
                    "Password Protection", 
                    value=bool(default_password),
                    key=f"{key}_password_check"
                )
            if win32_available:
                with col2:
                    convert_to_xls = st.checkbox(
                        "Convert to Excel 97-2003 (.xls)", 
                        value=False, 
                        key=f"{key}_convert_xls",
                        help="Convert to older Excel format for compatibility with legacy systems"
                    )
            
            if add_password:
                password = st.text_input(
                    "Password", 
                    type="password", 
                    value=default_password,  
                    placeholder="Enter password (min 5 characters)",
                    key=f"{key}_password_input"
                )
                
                if default_password and password == default_password:
                    st.caption("🔄 Using global password")
            else:
                password = ""
            
            processed_data = data
            final_extension = "xlsx" 
            final_mime_type = mime_type
            is_actually_protected = False
            
            if convert_to_xls:
                with st.spinner("Converting to Excel 97-2003 format..."):
                    processed_data = convert_to_excel_97_2003(processed_data, filename)
                    final_extension = "xls"  
                    final_mime_type = "application/vnd.ms-excel"
                st.success("Converted to Excel 97-2003 format")
            
            if add_password and password:
                if len(password) < 5:
                    st.warning("Password should be at least 5 characters long for security")
                    is_actually_protected = False
                else:
                    with st.spinner("Encrypting file... This may take a moment"):
                        if convert_to_xls:
                            processed_data = add_password_protection_xls(processed_data, password)
                            is_actually_protected = True
                        else:
                            processed_data = add_password_protection(processed_data, password)
                            is_actually_protected = True
                    st.success("File encrypted successfully")
            
            base_name = filename.rsplit('.', 1)[0]
            final_filename = f"{base_name}.{final_extension}"

            st.download_button(
                label=label,
                data=processed_data,
                file_name=final_filename,
                mime=final_mime_type,
                key=f"{key}_download"
            )
            
            return is_actually_protected

        if automation_type == "Cured List" and 'cured_list_result' in st.session_state:
            result = st.session_state['cured_list_result']
            if result != (None, None, None):
                global_password = st.text_input(
                    "Set password for all files (optional)", 
                    type="password",
                    help="This password will be pre-filled for all files. You can still modify individual passwords.",
                    key="global_password_cured_list"
                )
                
                if global_password:
                    st.info(f"Password will be applied to all files. You can still modify individual settings below.")
                
                tabs = st.tabs(["Remarks", "Reshuffle", "Payments"])
                
                with tabs[0]:
                    st.subheader("Remarks Data")
                    st.dataframe(result['remarks_df'], use_container_width=True)
                    is_protected = create_download_section(
                        "Download Remarks File", 
                        result['remarks_binary'], 
                        result['remarks_filename'], 
                        "remarks",
                        default_password=global_password
                    )
                with tabs[1]:
                    st.subheader("Reshuffle Data")
                    st.dataframe(result['others_df'], use_container_width=True)
                    is_protected = create_download_section(
                        "Download Reshuffle File", 
                        result['others_binary'], 
                        result['others_filename'], 
                        "reshuffle",
                        default_password=global_password
                    )
                with tabs[2]:
                    st.subheader("Payments Data")
                    st.dataframe(result['payments_df'], use_container_width=True)
                    is_protected = create_download_section(
                        "Download Payments File", 
                        result['payments_binary'], 
                        result['payments_filename'], 
                        "payments",
                        default_password=global_password
                    )

        elif automation_type == "Agency Daily Report" and 'agency_daily_result' in st.session_state:
            result = st.session_state['agency_daily_result']
            if result != (None, None, None):
                global_password = st.text_input(
                    "Set password for all files (optional)", 
                    type="password",
                    help="This password will be pre-filled for all files. You can still modify individual passwords.",
                    key="global_password_cured_list"
                )
                
                if global_password:
                    st.info(f"Password will be applied to all files. You can still modify individual settings below.")
                
                tabs = st.tabs(["Daily Report B5", "Daily Report B6", "B5 Prod", "B6 Prod", "VS"])
                
                with tabs[0]:
                    st.subheader("Daily Report B5")
                    st.dataframe(result['b5_df'], use_container_width=True)
                    is_protected = create_download_section(
                        "Download Agency Daily Report B5 File", 
                        result['b5_binary'], 
                        result['b5_filename'], 
                        "b5",
                        default_password=global_password
                    )
                with tabs[1]:
                    st.subheader("Daily Report B6")
                    st.dataframe(result['b6_df'], use_container_width=True)
                    is_protected = create_download_section(
                        "Download Agency Daily Report B6 File", 
                        result['b6_binary'], 
                        result['b6_filename'], 
                        "b6",
                        default_password=global_password
                    )
                with tabs[2]:
                    st.subheader("B5 Prod")
                    st.dataframe(result['b5_prod_df'], use_container_width=True)
                    is_protected = create_download_section(
                        "Download Daily Productivity B5 Report File", 
                        result['b5_prod_binary'], 
                        result['b5_prod_filename'], 
                        "b5_prod",
                        default_password=global_password
                    )
                with tabs[3]:
                    st.subheader("B6 Prod")
                    st.dataframe(result['b6_prod_df'], use_container_width=True)
                    is_protected = create_download_section(
                        "Download Daily Productivity B6 Report File", 
                        result['b6_prod_binary'], 
                        result['b6_prod_filename'], 
                        "b6_prod",
                        default_password=global_password
                    )
                with tabs[4]:
                    st.subheader("VS")
                    st.dataframe(result['vs_df'], use_container_width=True)
                    is_protected = create_download_section(
                        "Download VS File", 
                        result['vs_binary'], 
                        result['vs_filename'], 
                        "vs_report",
                        default_password=global_password
                    )
                    
        elif automation_type == "Endorsement" and 'new_endorsement' in st.session_state:
            result = st.session_state['new_endorsement']
            if result != (None, None, None):
                global_password = st.text_input(
                    "Set password for all files (optional)", 
                    type="password",
                    help="This password will be pre-filled for all files. You can still modify individual passwords.",
                    key="global_password_cured_list"
                )
                
                if global_password:
                    st.info(f"Password will be applied to all files. You can still modify individual settings below.")
                
                tabs = st.tabs(["ENDO Bot", "CMS"])
                
                with tabs[0]:
                    st.subheader("ENDO Bot")
                    st.dataframe(result['bcrm_endo_df'], use_container_width=True)
                    is_protected = create_download_section(
                        "Download ENDO Bot File", 
                        result['bcrm_endo_binary'], 
                        result['bcrm_endo_filename'], 
                        "endo_bot",
                        default_password=global_password
                    )
                with tabs[1]:
                    st.subheader("CMS")
                    st.dataframe(result['cms_endo_df'], use_container_width=True)
                    is_protected = create_download_section(
                        "Download CMS File", 
                        result['cms_endo_binary'], 
                        result['cms_endo_filename'], 
                        "cms",
                        default_password=global_password
                    )

        elif 'output_binary' in st.session_state and 'result_sheet_names' in st.session_state:
            excel_file = pd.ExcelFile(io.BytesIO(st.session_state['output_binary']))
            result_sheet_names = st.session_state['result_sheet_names']
            
            if len(result_sheet_names) > 1:
                result_sheet = st.selectbox(
                    "Select Sheet",
                    options=result_sheet_names,
                    index=0,
                    key=f"{campaign}_result_sheet"
                )
            else: 
                result_sheet = result_sheet_names[0]
            
            selected_df = pd.read_excel(io.BytesIO(st.session_state['output_binary']), sheet_name=result_sheet)

            st.subheader("Processed Preview")
            st.dataframe(selected_df, use_container_width=True)

            is_protected = create_download_section(
                "Download File", 
                st.session_state['output_binary'], 
                st.session_state['output_filename'], 
                "main_output"
            )
            

if __name__ == "__main__":
    main()
