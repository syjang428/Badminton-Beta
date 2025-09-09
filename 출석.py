import streamlit as st
import pandas as pd
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

@st.cache_resource
def get_gspread_client():
    # TOML에 triple-quoted로 넣었으면 \n 복원 필요 없음
    svc_info = dict(st.secrets["gcp_service_account"])
    # 만약 TOML에 한 줄 문자열로 넣어 \n이 이스케이프라면 아래 주석 해제
    # svc_info["private_key"] = svc_info["private_key"].replace("\\n", "\n")

    creds = Credentials.from_service_account_info(svc_info, scopes=SCOPES)
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
@st.cache_data(ttl=60)  # 1분 캐싱
def get_latest_code():
    try:
        return code_sheet.acell("A1").value or ""
    except:
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

# 상태에 따라 입력란 표시
if status == "출석":
    # ✅ 출석일 때만 시간대 선택
    time_slot = st.selectbox(
        "시간대 선택",
        ["1:00", "1:10", "1:20", "1:30", "1:40", "1:50"],
        key="time_slot_select"   # 🔑 고유 key 지정
    )

    partner = st.text_input(
        "오늘 같이 활동한 사람 이름 (여러 명일 경우 , 로 구분)",
        key="partner_input"
    )

    latest_code = get_latest_code()
    st.session_state.attendance_input = st.text_input(
        "오늘의 출석 코드",
        value=st.session_state.attendance_input,
        key="attendance_code_input"
    )

elif status == "결석":
    st.session_state.absence_reason = st.text_area(
        "결석 사유를 입력하세요",
        value=st.session_state.absence_reason,
        key="absence_reason_input"
    )
    partner = ""  # 결석일 때는 partner 값 비워주기




# ------------------ 제출 ------------------
if st.button("제출"):
    if not ((df["이름"] == name) & (df["고유번호"].astype(str) == personal_code)).any():
        st.error("이름 또는 개인 고유번호가 올바르지 않습니다.")
        st.warning("⚠️ 거짓이나 꾸며서 입력했을 시 바로 퇴출됩니다.")
    else:
        if status == "출석":
            if partner.strip() == "":
                st.error("오늘 같이 활동한 사람을 입력하세요.")
            else:
                latest_code = get_latest_code()
                if st.session_state.attendance_input.strip() == "":
                    st.error("출석 코드를 입력하세요.")
                elif st.session_state.attendance_input != latest_code:
                    st.error("출석 코드가 올바르지 않습니다.")
                    st.warning("⚠️ 거짓이나 꾸며서 입력했을 시 바로 퇴출됩니다.")
                else:
                    st.success(f"{name}님 출석 완료 ✅")
                    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    sheet.append_row([name, now_str, "출석", partner])
                    if "local_attendance" not in st.session_state:
                        st.session_state.local_attendance = []
                    st.session_state.local_attendance.append([name, now_str, "출석", partner])
                    st.cache_data.clear()
                    st.session_state.attendance_input = ""
                    st.warning("⚠️ 거짓이나 꾸며서 입력했을 시 바로 퇴출됩니다.")

        elif status == "결석":
            if st.session_state.absence_reason.strip() == "":
                st.error("결석 사유를 입력하세요.")
            else:
                st.success(f"{name}님 결석 처리 완료 ✅")
                now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                sheet.append_row([name, now_str, "결석", st.session_state.absence_reason])
                if "local_attendance" not in st.session_state:
                    st.session_state.local_attendance = []
                st.session_state.local_attendance.append([name, now_str, "결석", st.session_state.absence_reason])
                st.cache_data.clear()


# ================== 출석 현황 대시보드 (관리자 전용) ==================
@st.cache_data(ttl=300)  # 5분 캐싱
def get_attendance_df():
    """출석기록 시트를 DataFrame으로 불러오기 (헤더 자동 인식)"""
    try:
        df_att = pd.DataFrame(sheet.get_all_records())  # 헤더 = 첫 행
        if df_att.empty:
            return pd.DataFrame(columns=["이름", "시간", "상태", "사유"])
        return df_att
    except Exception as e:
        st.error(f"출석기록 불러오기 실패: {e}")
        return pd.DataFrame(columns=["이름", "시간", "상태", "사유"])


def split_today_status(df_att: pd.DataFrame):
    """오늘 기준 출석/결석/미체크 목록 반환"""
    today_str = datetime.now().strftime("%Y-%m-%d")
    # 오늘 행만 필터 (시간 포맷이 'YYYY-MM-DD HH:MM:SS' 이므로 startswith 사용)
    today_att = df_att[df_att["시간"].str.startswith(today_str, na=False)].copy()


    # 오늘 출석/결석자
    attended = set(today_att.loc[today_att["상태"] == "출석", "이름"])
    absented = set(today_att.loc[today_att["상태"] == "결석", "이름"])


    # 전체 부원 이름 세트 (CSV 기준)
    all_members = set(df["이름"].dropna().astype(str))


    # 오늘 미체크자 = 전체 - (출석 ∪ 결석)
    unchecked = all_members - (attended | absented)


    # 보기 좋게 DataFrame 구성
    df_attended = pd.DataFrame(sorted(attended), columns=["이름"])
    df_absented = pd.DataFrame(
        sorted(absented), columns=["이름"]
    ).merge(
        today_att.loc[today_att["상태"] == "결석", ["이름", "사유"]],
        on="이름",
        how="left"
    ).drop_duplicates(subset=["이름"])
    df_unchecked = pd.DataFrame(sorted(unchecked), columns=["이름"])


    return df_attended, df_absented, df_unchecked, len(all_members)


if st.session_state.admin_mode:
    st.markdown("---")
    st.subheader("📊 오늘의 출석 현황 (관리자)")


    # 데이터 조회
    att_df = get_attendance_df()
    df_attended, df_absented, df_unchecked, total_members = split_today_status(att_df)


    # 지표
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("총 인원", total_members)
    col2.metric("출석", len(df_attended))
    col3.metric("결석", len(df_absented))
    col4.metric("미체크", len(df_unchecked))


    # 표 표시
    st.markdown("#### ✅ 출석자")
    st.table(df_attended)


    st.markdown("#### ❌ 결석자 (사유 포함)")
    st.table(df_absented)


    st.markdown("#### ⏳ 미체크자")
    st.table(df_unchecked)


    # 다운로드
    colD1, colD2, colD3 = st.columns(3)
    colD1.download_button(
        "출석자 CSV 다운로드",
        data=df_attended.to_csv(index=False, encoding="utf-8-sig"),
        file_name=f"출석자_{datetime.now().strftime('%Y%m%d')}.csv",
        mime="text/csv",
    )
    colD2.download_button(
        "결석자 CSV 다운로드",
        data=df_absented.to_csv(index=False, encoding="utf-8-sig"),
        file_name=f"결석자_{datetime.now().strftime('%Y%m%d')}.csv",
        mime="text/csv",
    )
    colD3.download_button(
        "미체크자 CSV 다운로드",
        data=df_unchecked.to_csv(index=False, encoding="utf-8-sig"),
        file_name=f"미체크자_{datetime.now().strftime('%Y%m%d')}.csv",
        mime="text/csv",
    )
