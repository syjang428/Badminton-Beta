import streamlit as st
import pandas as pd
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials
import time
import threading
import uuid
from gspread.exceptions import APIError

@st.cache_resource
def get_sheet_lock():
    # 서버 프로세스 전역에서 하나만 생성되어 모든 세션이 공유
    return threading.Lock()

sheet_lock = get_sheet_lock()

_RETRY_HINTS = ("rate limit", "quota", "backendError", "internal error", "timeout", "429", "503", "500")

def safe_append_row(ws, row_values, max_retries=7):
    """
    Google Sheets append_row 안전 호출:
    - 전역 락으로 동시 호출 직렬화
    - 429/5xx/일시 오류 지수 백오프 재시도
    """
    delay = 0.6
    for attempt in range(1, max_retries + 1):
        try:
            with sheet_lock:
                ws.append_row(row_values, value_input_option="USER_ENTERED")
            return True
        except APIError as e:
            msg = str(e).lower()
            if any(h in msg for h in _RETRY_HINTS):
                time.sleep(delay)
                delay = min(delay * 2, 8.0)
                continue
            raise
        except Exception:
            time.sleep(delay)
            delay = min(delay * 2, 8.0)
    return False

@st.cache_data(ttl=30)
def existing_tokens(_ws, sheet_key: str):
    """
    _ws: gspread Worksheet (언더스코어 → 캐시 해시 대상에서 제외)
    sheet_key: 캐시 키로 쓸 해시 가능한 문자열(스프레드시트ID:워크시트ID/제목)
    """
    try:
        records = _ws.get_all_records()
        return {str(r.get("토큰", "")).strip() for r in records if r.get("토큰")}
    except Exception:
        return set()
    
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

# 캐시 키(스프레드시트ID:워크시트ID) - gspread 버전에 따라 .id가 없으면 제목으로 폴백
SHEET_KEY = f"{sheet.spreadsheet.id}:{getattr(sheet, 'id', sheet.title)}"



# 오늘 날짜 데이터만 분리하고 상태별로 나누는 함수
def split_today_status(df):
    today = datetime.today().strftime("%Y-%m-%d")
    today_att = df[df["날짜"] == today].copy()

    if "출석여부" not in today_att.columns:
        raise KeyError("⚠️ DataFrame에 '출석여부' 컬럼이 없습니다.")

    if "이름" not in today_att.columns:
        raise KeyError("⚠️ DataFrame에 '이름' 컬럼이 없습니다.")

    df_attended = today_att[today_att["출석여부"] == "출석"].copy()
    df_absented = today_att[today_att["출석여부"] == "결석"].copy()
    df_unchecked = today_att[today_att["출석여부"].isna()].copy()

    total_members = len(today_att["이름"].unique())

    return df_attended, df_absented, df_unchecked, total_members

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
                # clear/append도 경합 방지
                with sheet_lock:
                    code_sheet.clear()
                ok = safe_append_row(code_sheet, [code_input, datetime.now().strftime("%Y-%m-%d %H:%M:%S")])
                if ok:
                    st.success("출석 코드가 저장되었습니다.")
                    st.cache_data.clear()
                else:
                    st.error("코드 저장에 실패했습니다. 잠시 후 다시 시도해 주세요.")


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
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

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
                    token = str(uuid.uuid4())[:8]  # idempotency 토큰
                    values = [name, now_str, "출석", time_slot, partner, "", token]  # 마지막에 '토큰' 컬럼 추가

                    tokens = existing_tokens(sheet, SHEET_KEY)
                    if token in tokens:
                        st.info("이미 처리된 요청입니다 (중복 제출 방지).")
                    else:
                        ok = safe_append_row(sheet, values)
                        if ok:
                            st.success(f"{name}님 출석 완료 ✅")
                            if "local_attendance" not in st.session_state:
                                st.session_state.local_attendance = []
                            st.session_state.local_attendance.append(values)
                            st.cache_data.clear()
                            st.session_state.attendance_input = ""
                            st.warning("⚠️ 거짓이나 꾸며서 입력했을 시 바로 퇴출됩니다.")
                        else:
                            st.error("일시적 오류로 저장에 실패했습니다. 잠시 후 다시 시도해 주세요.")

        elif status == "결석":
            if st.session_state.absence_reason.strip() == "":
                st.error("결석 사유를 입력하세요.")
            else:
                token = str(uuid.uuid4())[:8]
                values = [name, now_str, "결석", "", "", st.session_state.absence_reason, token]

                tokens = existing_tokens(sheet, SHEET_KEY)
                if token in tokens:
                    st.info("이미 처리된 요청입니다 (중복 제출 방지).")
                else:
                    ok = safe_append_row(sheet, values)
                    if ok:
                        st.success(f"{name}님 결석 처리 완료 ✅")
                        if "local_attendance" not in st.session_state:
                            st.session_state.local_attendance = []
                        st.session_state.local_attendance.append(values)
                        st.cache_data.clear()
                    else:
                        st.error("일시적 오류로 저장에 실패했습니다. 잠시 후 다시 시도해 주세요.")


# ================== 출석 현황 대시보드 (관리자 전용) ==================
# ================== 출석 현황 대시보드 (관리자 전용) ==================
@st.cache_data(ttl=300)  # 5분 캐싱
def get_attendance_df():
    """출석기록 시트를 DataFrame으로 불러오기 (헤더 자동 인식)"""
    try:
        df_att = pd.DataFrame(sheet.get_all_records())  # 시트 헤더 첫 행
        if df_att.empty:
            return pd.DataFrame(columns=["이름", "시간", "상태", "사유"])
        return df_att
    except Exception as e:
        st.error(f"출석기록 불러오기 실패: {e}")
        return pd.DataFrame(columns=["이름", "시간", "상태", "사유"])


def split_today_status(df_att, all_members):
    import datetime
    today_str = datetime.datetime.now().strftime("%Y-%m-%d")

    # 컬럼 탐색
    col_time = next((c for c in df_att.columns if "시간" in c or "날짜" in c or "등록" in c), None)
    col_status = next((c for c in df_att.columns if "출석" in c or "상태" in c), None)
    col_name = next((c for c in df_att.columns if "이름" in c or "성명" in c), None)

    if not col_time or not col_status or not col_name:
        st.error("출석 기록에서 필수 컬럼을 찾을 수 없습니다.")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), len(all_members)

    # 오늘 날짜 필터링
    today_att = df_att[df_att[col_time].astype(str).str.startswith(today_str, na=False)].copy()

    # 출석/결석 분류
    df_attended = today_att[today_att[col_status] == "출석"].copy()
    df_absented = today_att[today_att[col_status] == "결석"].copy()

    # 중복 이름 제거
    df_attended = df_attended.drop_duplicates(subset=[col_name])
    df_absented = df_absented.drop_duplicates(subset=[col_name])

    # 출석 우선
    df_absented = df_absented[~df_absented[col_name].isin(df_attended[col_name])]

    # 미체크자 계산
    submitted_names = [str(name).strip() for name in today_att[col_name].tolist()]
    all_member_names = [str(name).strip() for name in all_members["이름"].tolist()]
    unchecked_names = [name for name in all_member_names if name not in submitted_names]

    df_unchecked = pd.DataFrame(unchecked_names, columns=["이름"])

    return df_attended, df_absented, df_unchecked, len(all_members)


# ====== 출석 현황: 모두에게 표시, 다운로드는 관리자만 ======
st.markdown("---")
st.subheader("📊 오늘의 출석 현황")

# 데이터 불러오기
att_df = get_attendance_df()

# 오늘 기준 분류 (기존 함수 그대로 사용)
df_attended, df_absented, df_unchecked, total_members = split_today_status(att_df, df)

# 지표
col1, col2, col3, col4 = st.columns(4)
col1.metric("총 인원", total_members)
col2.metric("출석", len(df_attended))
col3.metric("결석", len(df_absented))
col4.metric("미체크", len(df_unchecked))

# === 컬럼 안전 매핑 & 선택 유틸 ===
def map_columns_safe(df_):
    if df_.empty:
        return None, None, None
    col_name = next((c for c in df_.columns if "이름" in c or "성명" in c), None)
    col_time = next((c for c in df_.columns if "시간" in c or "날짜" in c or "등록" in c), None)
    col_status = next((c for c in df_.columns if "출석" in c or "상태" in c), None)
    return col_name, col_time, col_status

def safe_select(df_, cols):
    existing = [c for c in cols if c and c in df_.columns]
    if not existing:
        return pd.DataFrame(columns=[c for c in cols if c])
    return df_[existing]

name_col, time_col, status_col = map_columns_safe(df_attended)

# 표 표시
st.markdown("#### ✅ 출석자")
attended_display = safe_select(df_attended, [name_col, time_col, status_col])
st.table(attended_display)

st.markdown("#### ❌ 결석자")
absented_display = safe_select(df_absented, [name_col, time_col, status_col])
st.table(absented_display)

st.markdown("#### ⏳ 미체크자")
unchecked_display = df_unchecked[["이름"]] if not df_unchecked.empty else df_unchecked
st.table(unchecked_display)

# 🔒 CSV 다운로드는 관리자만
if st.session_state.admin_mode:
    colD1, colD2, colD3 = st.columns(3)
    colD1.download_button(
        "출석자 CSV 다운로드",
        data=attended_display.to_csv(index=False, encoding="utf-8-sig") if not attended_display.empty else "",
        file_name=f"출석자_{datetime.now().strftime('%Y%m%d')}.csv",
        mime="text/csv",
    )
    colD2.download_button(
        "결석자 CSV 다운로드",
        data=absented_display.to_csv(index=False, encoding="utf-8-sig") if not absented_display.empty else "",
        file_name=f"결석자_{datetime.now().strftime('%Y%m%d')}.csv",
        mime="text/csv",
    )
    colD3.download_button(
        "미체크자 CSV 다운로드",
        data=unchecked_display.to_csv(index=False, encoding="utf-8-sig") if not unchecked_display.empty else "",
        file_name=f"미체크자_{datetime.now().strftime('%Y%m%d')}.csv",
        mime="text/csv",
    )

# 비관리자 화면에서는 아무것도 표시하지 않음
# ✅ 비관리자 모드에서는 위 코드가 실행되지 않아 출석 현황이 표시되지 않음

