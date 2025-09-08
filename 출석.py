
import streamlit as st
import pandas as pd
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import random

# ------------------ Google Sheets 연결 ------------------
@st.cache_resource
def get_gspread_client():
    scope = ["https://spreadsheets.google.com/feeds",
             "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("service_account.json", scope)
    return gspread.authorize(creds)

client = get_gspread_client()

SPREADSHEET_NAME = "출석"
workbook = client.open(SPREADSHEET_NAME)

sheet = workbook.worksheet("출석기록")    # 출석 기록용
code_sheet = workbook.worksheet("출석코드")  # 출석 코드 저장용

# ------------------ CSV 불러오기 ------------------
@st.cache_data  # ✅ TTL 제거 → 완전 캐싱 (앱 새로 실행하기 전까지는 다시 안 불러옴)
def load_members():
    return pd.read_csv("부원명단.csv", encoding="utf-8-sig")

df = load_members()

# ------------------ 출석 코드 불러오기 ------------------
@st.cache_data(ttl=300)  # ✅ 5분 캐싱 → 훨씬 빠름
def get_latest_code():
    saved_code = code_sheet.get_all_values()
    if saved_code and len(saved_code[0]) > 0:
        return saved_code[0][0]
    return ""

# ------------------ 관리자 비밀번호 설정 ------------------
ADMIN_PASSWORD = "04281202"

# ------------------ 세션 상태 초기화 ------------------
if "admin_mode" not in st.session_state:
    st.session_state.admin_mode = False
if "admin_code" not in st.session_state:
    st.session_state.admin_code = ""

# 관리자 모드
st.sidebar.subheader("관리자 전용")

if "pwd_input" not in st.session_state:
    st.session_state.pwd_input = ""
if "admin_code" not in st.session_state:
    st.session_state.admin_code = ""

# 관리자 모드 비활성화 상태 → 비밀번호 입력 폼만 보여줌
if not st.session_state.admin_mode:
    with st.sidebar.form(key="admin_form"):
        pwd = st.text_input("관리자 비밀번호 입력", type="password")
        submit_btn = st.form_submit_button("관리자 모드 활성화")
        if submit_btn:
            if pwd == ADMIN_PASSWORD:
                st.session_state.admin_mode = True
                st.success("관리자 모드가 활성화되었습니다 ✅")
            else:
                st.error("비밀번호가 올바르지 않습니다 ❌")

# 관리자 모드 활성화 상태
if st.session_state.admin_mode:
    st.sidebar.success("관리자 모드 활성화 중")

    with st.sidebar.expander("관리자 기능"):
        code_input = st.text_input(
            "오늘의 출석 코드 입력",
            value=st.session_state.admin_code,
            type="password"
        )
        if st.button("출석 코드 저장"):
            if code_input.strip() != "":
                st.session_state.admin_code = code_input
                code_sheet.clear()
                code_sheet.append_row([code_input, datetime.now().strftime("%Y-%m-%d %H:%M:%S")])
                st.success("출석 코드가 저장되었습니다.")
                st.cache_data.clear()  # ✅ 캐시 초기화 (출석 코드 갱신됨)

        if st.button("관리자 모드 해제"):
            st.session_state.admin_mode = False
            st.session_state.admin_code = ""
            st.session_state.pwd_input = ""
            st.sidebar.warning("관리자 모드가 해제되었습니다 ⚠️")
            st.rerun()

# ------------------ 사용자 출석 체크 ------------------
st.header("🏸 서천고 배드민턴부 출석 체크")

name = st.text_input("이름")
personal_code = st.text_input("개인 고유번호", type="password")
status = st.radio("출석 상태 선택", ["출석", "결석"])

if "attendance_input" not in st.session_state:
    st.session_state.attendance_input = ""

if "absence_reason" not in st.session_state:
    st.session_state.absence_reason = ""

if status == "출석":
    latest_code = get_latest_code()
    st.session_state.attendance_input = st.text_input("오늘의 출석 코드", value=st.session_state.attendance_input)

    if latest_code == "":
        st.warning("아직 관리자가 출석 코드를 설정하지 않았습니다 ⚠️")
    elif st.session_state.attendance_input != latest_code:
        st.error("출석 코드가 올바르지 않습니다.")
elif status == "결석":
    # 결석일 때 사유 입력란 표시
    st.session_state.absence_reason = st.text_input("결석 사유를 입력하세요", value=st.session_state.absence_reason)

# ------------------ 제출 ------------------
if st.button("제출"):
    if not ((df["이름"] == name) & (df["고유번호"].astype(str) == personal_code)).any():
        st.error("이름 또는 개인 고유번호가 올바르지 않습니다.")
    else:
        if status == "출석":
            latest_code = get_latest_code()
            if st.session_state.attendance_input != latest_code:
                st.error("출석 코드가 올바르지 않습니다.")
            else:
                st.success(f"{name}님 출석 완료 ✅")
                sheet.append_row([name, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "출석", ""])
                st.session_state.attendance_input = ""
        else:
            if st.session_state.absence_reason.strip() == "":
                st.error("결석 사유를 반드시 입력해야 합니다.")
            else:
                st.warning(f"{name}님 결석 처리 ❌")
                sheet.append_row([name, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "결석", st.session_state.absence_reason])
                st.session_state.absence_reason = ""
