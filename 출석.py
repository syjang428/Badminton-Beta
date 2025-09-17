import streamlit as st
import pandas as pd
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials
import time
import threading
import uuid
import zoneinfo   # ✅ 추가
from gspread.exceptions import APIError
import random
from requests.exceptions import RequestException, Timeout, ConnectionError
import hashlib  # ✅ 추가
from common_io import get_sheet

# ✅ 한국 시간대 설정 (전역에서 재사용)
KST = zoneinfo.ZoneInfo("Asia/Seoul")

st.set_page_config(page_title="서천고 출석", initial_sidebar_state="collapsed")

# ------------------ 페이지 이동 아이콘 ------------------
st.page_link("pages/페널티.py", label=" 페널티 페이지", icon="🔖")


# 🔇 재실행 흐림/반투명 제거 + 우상단/사이드바 스피너 숨김
st.markdown("""
<style>
/* 본문/사이드바 재실행 시 붙는 흐림/반투명 제거 */
[data-stale="true"] { filter: none !important; opacity: 1 !important; }

/* 레이아웃 컨테이너들이 stale이어도 흐리지 않기 */
[data-testid="stAppViewContainer"] [data-stale="true"],
[data-testid="stSidebar"] [data-stale="true"],
[data-testid="stAppViewBlockContainer"] [data-stale="true"] {
  filter: none !important; opacity: 1 !important;
}

/* 우상단 'Running…' 스피너 숨김 */
[data-testid="stStatusWidget"] { visibility: hidden !important; }
/* 사이드바 안의 상태 위젯도 숨김 (환경에 따라 표시될 수 있음) */
[data-testid="stSidebar"] [data-testid="stStatusWidget"] { visibility: hidden !important; }
</style>
""", unsafe_allow_html=True)

@st.cache_resource
def get_sheet_lock():
    # 서버 프로세스 전역에서 하나만 생성되어 모든 세션이 공유
    return threading.Lock()

sheet_lock = get_sheet_lock()

_RETRY_HINTS = ("rate limit", "quota", "backendError", "internal error", "timeout", "429", "503", "500")

def safe_append_row(ws, row_values, max_retries=12):
    """
    Google Sheets append_row 안전 호출(고동시성 대응):
    - 전역 락으로 동시 호출 직렬화
    - 429/5xx/네트워크 예외에 지수 백오프 + 지터
    """
    delay = 0.6
    for attempt in range(1, max_retries + 1):
        try:
            with sheet_lock:
                ws.append_row(row_values, value_input_option="USER_ENTERED")
            return True

        except APIError as e:
            msg = str(e).lower()
            # 흔한 일시 오류 신호들
            transient = any(h in msg for h in _RETRY_HINTS) or \
                        "deadline" in msg or "socket" in msg or \
                        "ratelimitexceeded" in msg or "quotaexceeded" in msg
            if transient and attempt < max_retries:
                time.sleep(delay + random.random() * 0.5)  # 지터
                delay = min(delay * 1.8, 20.0)
                continue
            raise  # 비일시 오류 → 즉시 상향

        except (Timeout, ConnectionError, RequestException):
            if attempt < max_retries:
                time.sleep(delay + random.random() * 0.5)
                delay = min(delay * 1.8, 12.0)
                continue
            return False

        except Exception:
            if attempt < max_retries:
                time.sleep(delay + random.random() * 0.5)
                delay = min(delay * 1.8, 12.0)
                continue
            return False

    return False

def daily_token(name: str, date_str: str) -> str:
    """
    같은 사람이 같은 날에 여러 번 저장되지 않도록 고정 토큰 생성.
    (하루 1명 1건 정책 / 여러 번 허용하려면 date_str 뒤에 |status|time_slot 등 포함)
    """
    base = f"{name.strip()}|{date_str}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()[:8]

def _read_tokens_fresh(ws) -> set[str]:
    """락 구간에서 캐시된 '토큰' 열 인덱스 활용(빠름), 실패 시 폴백."""
    try:
        col_idx = _get_token_col_index(ws, SHEET_KEY)
        if col_idx:
            vals = ws.col_values(col_idx)[1:]  # 헤더 제외
            return {str(v).strip() for v in vals if v}
        # 폴백: 전체 레코드에서 '토큰' 키만 추출
        records = ws.get_all_records()
        return {str(r.get("토큰", "")).strip() for r in records if r.get("토큰")}
    except Exception:
        return set()

def append_once(ws, values, max_retries=12):
    """
    확인+쓰기까지 '한 번의 락'으로 묶어 중복을 원천 차단.
    이미 같은 토큰이 있으면 쓰지 않고 True 반환(성공 취급).
    """
    delay = 0.6
    for attempt in range(1, max_retries + 1):
        try:
            with sheet_lock:
                token = str(values[-1]).strip()
                tokens = _read_tokens_fresh(ws)
                if token in tokens:
                    return True  # 누가 먼저 썼음 → 중복 방지 OK

                ws.append_row(values, value_input_option="USER_ENTERED")
                st.cache_data.clear()  # ✅ 성공 직후 캐시 무효화
                return True

        except APIError as e:
            msg = str(e).lower()
            transient = any(h in msg for h in _RETRY_HINTS) or \
                        "deadline" in msg or "socket" in msg or \
                        "ratelimitexceeded" in msg or "quotaexceeded" in msg
            if transient and attempt < max_retries:
                time.sleep(delay + random.random() * 0.5)
                delay = min(delay * 1.8, 20.0)
                continue
            raise
        except (Timeout, ConnectionError, RequestException):
            if attempt < max_retries:
                time.sleep(delay + random.random() * 0.5)
                delay = min(delay * 1.8, 12.0)
                continue
            return False
        except Exception:
            if attempt < max_retries:
                time.sleep(delay + random.random() * 0.5)
                delay = min(delay * 1.8, 12.0)
                continue
            return False
    return False

@st.cache_data(ttl=600)
def _get_token_col_index(_ws, sheet_key: str) -> int | None:
    """'토큰' 헤더가 있는 열 인덱스를 캐싱(1-base). 못 찾으면 None."""
    try:
        cell = _ws.find("토큰")  # 헤더 탐색(비용 큼) → 10분 캐싱
        return cell.col if cell else None
    except Exception:
        return None

@st.cache_data(ttl=30)
def existing_tokens(_ws, sheet_key: str) -> set[str]:
    """
    토큰 열만 읽어서 Set으로 반환(부하 최소화).
    - 토큰 열을 못 찾으면 기존 전체 레코드 fallback.
    """
    try:
        col_idx = _get_token_col_index(_ws, sheet_key)
        if col_idx:
            # 헤더(1행) 제외
            vals = _ws.col_values(col_idx)[1:]
            return {str(v).strip() for v in vals if v}
        # fallback: 전체 레코드
        records = _ws.get_all_records()
        return {str(r.get("토큰", "")).strip() for r in records if r.get("토큰")}
    except Exception:
        return set()

    
SPREADSHEET_NAME = "출석"
sheet = get_sheet(SPREADSHEET_NAME, "출석기록")
code_sheet = get_sheet(SPREADSHEET_NAME, "출석코드")

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

# ------------------ CSV 불러오기 (고유번호 0 유지) ------------------
@st.cache_data  # ✅ TTL 제거 → 완전 캐싱 (앱 새로 실행하기 전까지는 다시 안 불러옴)
def load_members():
    # "고유번호" 컬럼을 문자열(str)로 읽어 맨 앞 0 유지
    return pd.read_csv("부원명단.csv", encoding="utf-8-sig", dtype={"고유번호": str})

df = load_members()

import re  # ← 상단 import 구역에 함께 추가

@st.cache_data
def build_gcn_map(members_df: pd.DataFrame) -> dict[str, tuple[int,int,int]]:
    """
    CSV에서 이름 → (학년, 반, 번호) 매핑 생성
    - 컬럼이 '학년','반','번호'면 그대로 사용
    - '학년반번호'(예: '1-3-12', '1학년 3반 12번') 형태도 파싱
    - 없거나 파싱 실패 시 해당 이름은 매핑 생략(정렬 후순위로 처리)
    """
    m: dict[str, tuple[int,int,int]] = {}

    # case 1) 분리 컬럼 존재
    if {"학년", "반", "번호"}.issubset(members_df.columns):
        for _, r in members_df.dropna(subset=["이름", "학년", "반", "번호"]).iterrows():
            try:
                name = str(r["이름"]).strip()
                g = int(r["학년"])
                c = int(r["반"])
                n = int(r["번호"])
                if name:
                    # 동명이인 있을 경우 더 작은 (학,반,번)을 우선 보존
                    m[name] = min(m.get(name, (999,999,999)), (g, c, n))
            except Exception:
                continue
        return m

    # case 2) 합쳐진 컬럼 찾기
    merged_col = next(
        (c for c in members_df.columns if c in ("학년반번호", "학년반", "학반번호")),
        None
    )
    if merged_col:
        for _, r in members_df.dropna(subset=["이름", merged_col]).iterrows():
            name = str(r["이름"]).strip()
            raw = str(r[merged_col])
            nums = re.findall(r"\d+", raw)
            if len(nums) >= 3:
                try:
                    g, c, n = int(nums[0]), int(nums[1]), int(nums[2])
                    if name:
                        m[name] = min(m.get(name, (999,999,999)), (g, c, n))
                except Exception:
                    continue

    return m

GCN_MAP = build_gcn_map(df)

def _gcn_tuple_for(name: str) -> tuple[int,int,int]:
    """이름으로 (학년,반,번호) 조회. 없으면 정렬 후순위 키 반환"""
    return GCN_MAP.get(str(name).strip(), (999, 999, 999))


# ------------------ 출석 코드 불러오기 ------------------
@st.cache_data(ttl=60)  # 1분 캐싱
def get_latest_code():
    try:
        value = code_sheet.acell("A1").value or ""
        return str(value)  # 앞자리 0 유지
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
        # 코드 저장을 폼으로 감싸서 재실행 최소화
        with st.form("admin_code_form", clear_on_submit=False):
            code_input = st.text_input("오늘의 출석 코드 입력",
                                    value=st.session_state.admin_code,
                                    type="password")
            save_code = st.form_submit_button("출석 코드 저장")

        if save_code and code_input.strip() != "":
            st.session_state.admin_code = code_input
            with sheet_lock:
                code_sheet.clear()
            ok = safe_append_row(code_sheet, [str(code_input), datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")])
            if ok:
                st.cache_data.clear()
                st.success("출석 코드가 저장되었습니다.")
            else:
                st.error("코드 저장에 실패했습니다. 잠시 후 다시 시도해 주세요.")

        # 모드 해제는 폼 밖 일반 버튼으로 유지
        if st.button("관리자 모드 해제"):
            st.session_state.admin_mode = False
            st.session_state.admin_code = ""
            st.session_state.pwd_input = ""
            st.sidebar.warning("관리자 모드가 해제되었습니다 ⚠️")
            st.rerun()

# ------------------ 사용자 출석 체크 ------------------
st.header("🏸 배드민턴부 출석 체크")

with st.form("attendance_form", clear_on_submit=False):
    # ✅ 맨 위: 출석 여부 → 시간대 → 활동 부원
    status = st.radio("출석 여부", ["출석", "결석"], key="status_radio")

    if status == "출석":
        time_slot = st.selectbox(
            "시간대 선택",
            ["1:00", "1:10", "1:20", "1:30", "1:40", "1:50"],
            key="time_slot_select"
        )
        partner = st.text_input(
            "오늘 같이 활동한 부원들 이름",
            key="partner_input"
        )
    else:
        time_slot = ""
        partner = ""

    # ✅ 그 다음: 이름 / 고유번호
    name = st.text_input("이름")
    personal_code = st.text_input("고유번호 (전화번호 뒷자리)", type="password")

    # ✅ 출석 코드 / 결석 사유
    if "attendance_input" not in st.session_state:
        st.session_state.attendance_input = ""
    if "absence_reason" not in st.session_state:
        st.session_state.absence_reason = ""

    if status == "출석":
        latest_code = get_latest_code()
        st.session_state.attendance_input = st.text_input(
            "오늘의 출석 코드",
            value=st.session_state.attendance_input,
            key="attendance_code_input"
        )
    else:
        st.session_state.absence_reason = st.text_area(
            "결석 사유를 입력하세요",
            value=st.session_state.absence_reason,
            key="absence_reason_input"
        )

    # ✅ 제출 버튼 맨 아래
    submitted = st.form_submit_button("제출")

# ✅ 기존 제출 로직을 submitted가 True일 때만 실행
if submitted:
    now_str = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")

    # 이름/개인번호 확인
    if not ((df["이름"] == name) & (df["고유번호"].astype(str) == personal_code)).any():
        st.error("이름 또는 개인 고유번호가 올바르지 않습니다.")
    else:
        if status == "출석":
            if partner.strip() == "":
                st.error("오늘 같이 활동한 사람을 입력하세요.")
            else:
                input_code = str(st.session_state.attendance_input).strip()
                saved_code = str(get_latest_code()).strip()

                if input_code != saved_code:
                    st.error("출석 코드가 올바르지 않습니다.")
                    st.warning("⚠️ 거짓이나 꾸며서 입력했을 시 바로 퇴출됩니다.")
                else:
                    # ✅ 코드가 맞으면 출석 기록 처리 (append_once 사용)
                    date_key = datetime.now(KST).strftime("%Y-%m-%d")   # 하루 1건 정책
                    token = daily_token(name, date_key)
                    values = [name, now_str, "출석", time_slot, partner, "", token]

                    ok = append_once(sheet, values)
                    if ok:
                        st.success(f"{name}님 출석 완료 ✅")
                        st.session_state.local_attendance = st.session_state.get("local_attendance", [])
                        st.session_state.local_attendance.append(values)
                        st.session_state.attendance_input = ""
                        st.warning("⚠️ 거짓이나 꾸며서 입력했을 시 바로 퇴출됩니다.")
                    else:
                        st.error("일시적 오류로 저장에 실패했습니다. 잠시 후 다시 시도해 주세요.")

        elif status == "결석":
            if st.session_state.absence_reason.strip() == "":
                st.error("결석 사유를 입력하세요.")
            else:
                date_key = datetime.now(KST).strftime("%Y-%m-%d")
                token = daily_token(name, date_key)
                values = [name, now_str, "결석", "", "", st.session_state.absence_reason, token]

                ok = append_once(sheet, values)
                if ok:
                    st.success(f"{name}님 결석 처리 완료 ✅")
                    st.session_state.local_attendance = st.session_state.get("local_attendance", [])
                    st.session_state.local_attendance.append(values)
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
# ================== 출석 현황 대시보드 (관리자 전용) ==================
# ================== 출석 현황 대시보드 ==================
st.markdown("---")
st.subheader("📊 오늘의 출석 현황")

# 데이터 불러오기
att_df = get_attendance_df()

# 오늘 기준 분류
df_attended, df_absented, df_unchecked, total_members = split_today_status(att_df, df)

# 지표 (관리자/비관리자 모두 표시)
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

# 출석자
# === 출석자 ===
# === 출석자 ===
with st.expander("✅ 출석자 명단 보기", expanded=False):
    attended_display = safe_select(df_attended, [name_col, time_col, status_col]).copy()
    if not attended_display.empty:
        attended_display["_g"] = attended_display[name_col].map(lambda n: _gcn_tuple_for(n)[0])
        attended_display["_c"] = attended_display[name_col].map(lambda n: _gcn_tuple_for(n)[1])
        attended_display["_n"] = attended_display[name_col].map(lambda n: _gcn_tuple_for(n)[2])
        attended_display = (
            attended_display
            .sort_values(by=["_g", "_c", "_n", name_col], kind="stable")
            .drop(columns=["_g", "_c", "_n"])
        )

        if st.session_state.admin_mode:
            selected_attendees = []
            for idx, row in attended_display.iterrows():
                col1, col2, col3 = st.columns([2, 3, 2])
                with col1:
                    checked = st.checkbox(row[name_col], key=f"attendee_{idx}")
                with col2:
                    st.write(row[time_col])
                with col3:
                    st.write(row[status_col])
                if checked:
                    selected_attendees.append(row[name_col])
            st.info(f"선택된 출석자: {', '.join(selected_attendees) if selected_attendees else '없음'}")
        else:
            st.table(attended_display)
    else:
        st.write("출석자가 없습니다.")

# === 결석자 ===
with st.expander("❌ 결석자 명단 보기", expanded=False):
    absented_display = safe_select(df_absented, [name_col, time_col, status_col])
    st.table(absented_display)

# === 미체크자 ===
with st.expander("⏳ 미체크자 명단 보기", expanded=False):
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

