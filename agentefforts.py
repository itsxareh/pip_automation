import streamlit as st
import pandas as pd
import os
import numpy as np
import re
import io
BASE_DIR = os.getcwd()
USER_DIR = os.path.join(BASE_DIR, "AUTOMATION", "Database");
st.title("Account Data Splitter & Counter")
uploaded_file = st.file_uploader("Upload Main Excel File", type=["xlsx", "xls"])
bucket_paths = {
    "Bucket 1": os.path.join(USER_DIR, "BUCKET1_AGENT.xlsx"),
    "Bucket 2": os.path.join(USER_DIR, "BUCKET2_AGENT.xlsx"),
    "Bucket 5&6": os.path.join(USER_DIR, "BUCKET5&6_AGENT.xlsx")
}
bank_status_path = os.path.join(USER_DIR, "BANK_STATUS.xlsx")
rfd_list = os.path.join(USER_DIR, "RFD_LISTS.xlsx")
expected_columns = [
    "S.No", "Date", "Time", "Debtor", "Account No.", "Card No.", "Service No.", "DPD", 
    "Reason For Default", "Call Status", "Status", "Remark", "Remark By", "Remark Type", 
    "Field Visit Date", "Collector", "Client", "Product Description", "Product Type", "Batch No", 
    "Account Type", "Relation", "PTP Amount", "Next Call", "PTP Date", "Claim Paid Amount", 
    "Claim Paid Date", "Dialed Number", "Days Past Write Off", "Balance", "Contact Type", 
    "Black Case No.", "Red Case No.", "Court Name", "Lawyer", "Legal Stage", "Legal Status", 
    "Next Legal Follow up", "Call Duration", "Talk Time Duration"
]
# Load BANK_STATUS.xlsx
bank_status_lookup = {}
if os.path.exists(bank_status_path):
    df_bank_status = pd.read_excel(bank_status_path)
    if "CMS STATUS" not in df_bank_status.columns or "BANK STATUS" not in df_bank_status.columns:
        st.error("Missing 'CMS STATUS' or 'BANK STATUS' column in BANK_STATUS.xlsx.")
        st.stop()
    bank_status_lookup = dict(zip(df_bank_status["CMS STATUS"].astype(str).str.strip(), 
                                  df_bank_status["BANK STATUS"].astype(str).str.strip()))
else:
    st.error(f"Missing file: {bank_status_path}")
    st.stop()
# Load RFD_LISTS.xlsx
rfd_valid_codes = set()
if os.path.exists(rfd_list):
    df_rfd_list = pd.read_excel(rfd_list)
    if "RFD CODE" not in df_rfd_list.columns:
        st.error("Missing 'RFD CODE' column in RFD_LISTS.xlsx.")
        st.stop()
    rfd_valid_codes = set(df_rfd_list["RFD CODE"].astype(str).str.upper())
else:
    st.error(f"Missing file: {rfd_list}")
    st.stop()
if uploaded_file is not None:
    df_main = pd.read_excel(uploaded_file)
    # Validate columns
    missing_columns = [col for col in expected_columns if col not in df_main.columns]
    if missing_columns:
        st.error(f"Missing required columns: {', '.join(missing_columns)}")
        st.stop()
    df_main["Remark By"] = df_main["Remark By"].astype(str).str.strip()
    df_main = df_main[~df_main["Remark"].isin([
        "Updates when case reassign to another collector", 
        "System Auto Update Remarks For PD"
    ])]
    df_main = df_main[~df_main["Card No."].isin([f"ch{i}" for i in range(1, 20)])]
    bucket_dfs = {}
    for bucket_name, bucket_path in bucket_paths.items():
        if os.path.exists(bucket_path):
            df_bucket = pd.read_excel(bucket_path)
            if "VOLARE USER" not in df_bucket.columns or "FULL NAME" not in df_bucket.columns:
                st.warning(f"{bucket_name} missing required columns. Skipping.")
                continue
            df_bucket["VOLARE USER"] = df_bucket["VOLARE USER"].astype(str).str.strip()
            df_bucket["FULL NAME"] = df_bucket["FULL NAME"].astype(str).str.strip()
            lookup_dict = dict(zip(df_bucket["VOLARE USER"], df_bucket["FULL NAME"]))
            matched_df = df_main[df_main["Remark By"].isin(df_bucket["VOLARE USER"])].copy()
            matched_df["HANDLING OFFICER2"] = matched_df["Remark By"].map(lookup_dict)
            # Bucket-specific Card No. filters
            if bucket_name == "Bucket 1":
                matched_df = matched_df[
                    (matched_df["Remark By"].isin(["SYSTEM", "LCMANZANO", "ACALVAREZ", "DSDEGUZMAN", "SRELIOT", "TANAZAIRE", "SPMADRID"]) &
                     matched_df["Card No."].astype(str).str.startswith("01")) |
                    (~matched_df["Remark By"].isin(["SYSTEM", "LCMANZANO", "ACALVAREZ", "DSDEGUZMAN", "SRELIOT", "TANAZAIRE", "SPMADRID"]))
                ]
            elif bucket_name == "Bucket 2":
                matched_df = matched_df[
                    (matched_df["Remark By"].isin(["SYSTEM", "LCMANZANO", "ACALVAREZ", "DSDEGUZMAN", "SRELIOT", "TANAZAIRE", "SPMADRID"]) &
                     matched_df["Card No."].astype(str).str.startswith("02")) |
                    (~matched_df["Remark By"].isin(["SYSTEM", "LCMANZANO", "ACALVAREZ", "DSDEGUZMAN", "SRELIOT", "TANAZAIRE", "SPMADRID"]))
                ]
            elif bucket_name == "Bucket 5&6":
                matched_df = matched_df[
                    (matched_df["Remark By"].isin(["SYSTEM", "LCMANZANO", "ACALVAREZ", "DSDEGUZMAN", "SRELIOT", "TANAZAIRE", "SPMADRID"]) &
                     matched_df["Card No."].astype(str).str.startswith(("05", "06"))) |
                    (~matched_df["Remark By"].isin(["SYSTEM", "LCMANZANO", "ACALVAREZ", "DSDEGUZMAN", "SRELIOT", "TANAZAIRE", "SPMADRID"]))
                ]
            # Convert date columns to datetime
            for col in ["PTP Date", "Claim Paid Date", "Date"]:
                matched_df[col] = pd.to_datetime(matched_df[col], errors='coerce')
            # Lookup BANK STATUS
            matched_df["BANK STATUS"] = matched_df["Status"].astype(str).str.strip().map(bank_status_lookup)
            if not matched_df.empty:
                bucket_dfs[bucket_name] = matched_df
        else:
            st.error(f"Missing file: {bucket_path}")
    def extract_and_validate_rfd(remark):
        remark = str(remark).strip().rstrip("\\")
        rfd_match = re.search(r"RFD:\s*(\S+)$", remark)
        if rfd_match:
            rfd = rfd_match.group(1).upper()
        else:
            last_word = re.findall(r"\\\s*(\S+)", remark)
            if last_word:
                rfd = last_word[-1].upper()
            else:
                last_word = remark.split()[-1] if remark else np.nan
                rfd = last_word.upper() if last_word else np.nan
        return rfd if rfd in rfd_valid_codes else np.nan
    def convert_df_to_excel(df):
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False, sheet_name="Sheet1")
        return output.getvalue()
    for bucket_name, bucket_df in bucket_dfs.items():
        st.subheader(f"{bucket_name} Matched Data (Total: {len(bucket_df)})")
        filtered_df = pd.DataFrame({
            "Card Number": bucket_df["Card No."],
            "PN": bucket_df["Account No."],
            "NAME": bucket_df["Debtor"],
            "BALANCE": bucket_df["Balance"],
            "HANDLING OFFICER2": bucket_df["HANDLING OFFICER2"].str.upper(),
            "AGENCY3": "SP MADRID",
            "STATUS4": bucket_df["BANK STATUS"],
            "DATE OF CALL": bucket_df["Date"].dt.strftime("%m/%d/%Y"),
            "PTP DATE": np.where(
                bucket_df["PTP Date"].isna(),
                np.where(bucket_df["Claim Paid Date"].isna(), np.nan, bucket_df["Claim Paid Date"].dt.strftime("%m/%d/%Y")),
                bucket_df["PTP Date"].dt.strftime("%m/%d/%Y")
            ),
            "PTP AMOUNT": np.where(
                bucket_df["PTP Amount"].isna() | (bucket_df["PTP Amount"] == 0),
                np.where(bucket_df["Claim Paid Amount"].isna() | (bucket_df["Claim Paid Amount"] == 0), np.nan, bucket_df["Claim Paid Amount"]),
                bucket_df["PTP Amount"]
            ),
            "RFD5": bucket_df["Remark"].apply(extract_and_validate_rfd)
        })
        st.write(filtered_df)
        excel_data = convert_df_to_excel(filtered_df)
        st.download_button(
            label=f"Download {bucket_name} Data as Excel",
            data=excel_data,
            file_name=f"{bucket_name}_Matched_Data.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )