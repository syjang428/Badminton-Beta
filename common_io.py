import streamlit as st
import gspread
from google.oauth2.service_account import Credentials

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

@st.cache_resource
def get_gspread_client():
    svc_info = dict(st.secrets["gcp_service_account"])
    creds = Credentials.from_service_account_info(svc_info, scopes=SCOPES)
    return gspread.authorize(creds)

@st.cache_resource
def get_workbook(name: str):
    return get_gspread_client().open(name)

# 🔑 해시 가능한 키(스프레드시트 이름, 워크시트 제목)만 인자로 사용
@st.cache_resource
def get_sheet(spreadsheet_name: str, title: str):
    wb = get_workbook(spreadsheet_name)
    return wb.worksheet(title)