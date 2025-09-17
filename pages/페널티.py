import streamlit as st
import gspread
from datetime import datetime
from google.oauth2.service_account import Credentials
import threading
import time
from gspread.exceptions import APIError
import pandas as pd
import os
from common_io import get_workbook, get_sheet

st.page_link("출석.py", label="⬅️ 돌아가기")

# 🔇 재실행 흐림/반투명 제거 + 상태 스피너 숨김 (본문+사이드바)
st.markdown("""
<style>
[data-stale="true"] { filter: none !important; opacity: 1 !important; }
[data-testid="stAppViewContainer"] [data-stale="true"],
[data-testid="stSidebar"] [data-stale="true"],
[data-testid="stAppViewBlockContainer"] [data-stale="true"] {
  filter: none !important; opacity: 1 !important;
}
[data-testid="stStatusWidget"] { visibility: hidden !important; }
[data-testid="stSidebar"] [data-testid="stStatusWidget"] { visibility: hidden !important; }
</style>
""", unsafe_allow_html=True)

# ================== 설정 ==================
SPREADSHEET_NAME = "출석"         # 구글 스프레드시트 파일명
WS_PENALTY = "페널티기록"          # 페널티 기록 탭
MEMBERS_CSV = "부원명단.csv"        # 이름/고유번호가 들어있는 CSV

sheet_penalty = get_sheet(SPREADSHEET_NAME, WS_PENALTY)

# 점수 규칙/기본 사유 (원하면 자유롭게 수정)
reasons_dict = {"홍길동": "지각", "김철수": "결석", "이영희": "무단조퇴"}
points_dict  = {"지각": -1, "결석": -3, "무단조퇴": -2}

# 관리자 비밀번호 (secrets.toml 권장)
ADMIN_PASS = st.secrets.get("admin_password", None)


# ================== 멤버 CSV 로드 ==================
@st.cache_data
def load_members_csv(path: str, mtime: float) -> pd.DataFrame:
    if not os.path.exists(path):
        return pd.DataFrame(columns=["이름", "고유번호"])
    try:
        df = pd.read_csv(path, encoding="utf-8-sig", dtype={"고유번호":str})
    except UnicodeDecodeError:
        df = pd.read_csv(path)
    for col in ["이름", "고유번호"]:
        if col not in df.columns:
            df[col] = ""
    df["이름"] = df["이름"].astype(str).str.strip()
    df["고유번호"] = df["고유번호"].astype(str).str.strip()
    return df[["이름", "고유번호"]]

members_mtime = os.path.getmtime(MEMBERS_CSV) if os.path.exists(MEMBERS_CSV) else 0.0
members_df = load_members_csv(MEMBERS_CSV, members_mtime)
name_options = sorted([n for n in members_df["이름"].dropna().astype(str).unique() if n])


if members_df.empty or not name_options:
    st.warning(f"'{MEMBERS_CSV}'에서 이름 목록을 불러오지 못했습니다. 파일과 컬럼(이름, 고유번호)을 확인하세요.")

# ================== 동시성 안전 append ==================
@st.cache_resource
def get_sheet_lock():
    return threading.Lock()
sheet_lock = get_sheet_lock()

_RETRY_HINTS = ("rate limit", "quota", "backenderror", "internal error", "timeout", "429", "503", "500")

def safe_append_row(ws, row_values, max_retries=12):
    """ append_row를 전역 락 + 지수 백오프(+지터)로 안정 처리 """
    delay = 0.6
    for attempt in range(1, max_retries + 1):
        try:
            with sheet_lock:
                ws.append_row(row_values, value_input_option="RAW")
            return True

        except APIError as e:
            msg = str(e).lower()
            transient = any(h in msg for h in _RETRY_HINTS) or \
                        "deadline" in msg or "socket" in msg or \
                        "ratelimitexceeded" in msg or "quotaexceeded" in msg
            if transient and attempt < max_retries:
                time.sleep(delay + __import__("random").random() * 0.5)  # 지터
                delay = min(delay * 1.8, 20.0)
                continue
            raise  # 비일시 오류는 상향

        except (TimeoutError,):
            if attempt < max_retries:
                time.sleep(delay + __import__("random").random() * 0.5)
                delay = min(delay * 1.8, 12.0)
                continue
            return False

        except Exception:
            if attempt < max_retries:
                time.sleep(delay + __import__("random").random() * 0.5)
                delay = min(delay * 1.8, 12.0)
                continue
            return False
    return False

# ================== 공용 유틸 ==================
@st.cache_data(ttl=60)
def load_penalties_df(_sheet=sheet_penalty) -> pd.DataFrame:
    rows = _sheet.get_all_records()
    df = pd.DataFrame(rows)
    if df.empty:
        df = pd.DataFrame(columns=["시간", "이름", "사유", "점수", "누적 점수"])
    if "점수" in df.columns:
        df["점수"] = pd.to_numeric(df["점수"], errors="coerce").fillna(0).astype(int)
    if "누적 점수" in df.columns:
        df["누적 점수"] = pd.to_numeric(df["누적 점수"], errors="coerce").fillna(0).astype(int)
    return df


def calc_total_for_name(name: str, base_df: pd.DataFrame | None = None) -> int:
    df = base_df if base_df is not None else load_penalties_df()
    if "이름" not in df.columns:
        return 0
    return int(df.loc[df["이름"] == name, "점수"].sum())

def verify_member(name: str, code: str) -> bool:
    """부원명단.csv 기반 검증. 고유번호 컬럼이 비어있으면 이름만으로 통과."""
    if not name:
        return False
    row = members_df[members_df["이름"] == str(name).strip()]
    if row.empty:
        return False
    expected_code = str(row.iloc[0]["고유번호"]).strip()
    # 고유번호가 CSV에 없거나 빈 값이면 이름만으로 통과
    if not expected_code:
        return True
    return str(code).strip() == expected_code

# ================== UI ==================
st.title("🔖 페널티 자동 기록 시스템")

# 탭 순서: 메인(내 페널티 조회) → 서브(관리자 입력)
tab_me, tab_admin = st.tabs(["내 페널티 조회", "관리자 입력"])

# -------- 내 페널티 조회 (메인) --------
with tab_me:
    st.subheader("👤 본인 페널티 조회")
    my_name = st.text_input("이름 입력").strip()
    my_code = st.text_input("고유번호", type="password", help=f"'{MEMBERS_CSV}'의 고유번호와 일치해야 합니다.")

    if st.button("조회"):
        if not my_name:
            st.error("이름을 입력해주세요.")
        elif not verify_member(my_name, my_code):
            st.error("이름 또는 고유번호가 일치하지 않습니다.")
        else:
            df = load_penalties_df()
            my_df = df[df["이름"] == my_name].copy()

            if my_df.empty:
                st.info("조회된 페널티 기록이 없습니다.")
            else:
                total = int(my_df["점수"].sum()) if "점수" in my_df.columns else 0
                st.metric("누적 페널티 점수", total)

                st.write("### 최근 기록")
                if "시간" in my_df.columns:
                    try:
                        my_df["_ts"] = pd.to_datetime(my_df["시간"], errors="coerce")
                        my_df = my_df.sort_values("_ts", ascending=False).drop(columns=["_ts"])
                    except Exception:
                        pass
                cols = ["시간", "사유", "점수", "누적 점수"]
                st.dataframe(my_df[cols] if set(cols).issubset(my_df.columns) else my_df.tail(20))

                if "사유" in my_df.columns and "점수" in my_df.columns:
                    st.write("### 사유별 합계")
                    by_reason = my_df.groupby("사유", as_index=False)["점수"].sum()
                    st.dataframe(by_reason)

# -------- 관리자 입력 (서브) --------
with tab_admin:
    st.subheader("🔐 관리자 입력")
    if ADMIN_PASS is None:
        st.error("secrets.toml에 `admin_password`를 설정해주세요.")
    admin_pw = st.text_input("관리자 비밀번호", type="password")

    if ADMIN_PASS is not None and admin_pw == ADMIN_PASS:
        st.success("관리자 인증 완료!")

        # ✅ 모든 입력을 '한 폼' 안에서 처리 (콜백 없음)
        with st.form("penalty_add_form", clear_on_submit=False):
            # 이름
            typed_name = st.text_input("이름 입력").strip()
            default_reason = reasons_dict.get(typed_name, "") if typed_name else ""
            reason = st.text_input("사유 입력", value=default_reason,
                                   placeholder="예: 무단 결석 / 뒷정리 안함 / 불화 조성 등")

            # 자동 계산 여부 + 점수 입력
            auto_calc = st.checkbox("사유 기반 자동 점수 사용", value=True,
                                    help="체크 시 제출할 때 사유→점수 매핑(points_dict)을 적용합니다. 해제하면 수동 입력값을 그대로 기록합니다.")
            suggested_point = points_dict.get(reason, -1) if reason else -1
            point = st.number_input("점수 입력", value=int(suggested_point), step=1)

            submitted = st.form_submit_button("➕ 페널티 추가")

        if submitted:
            name = typed_name
            reason_val = (reason or "").strip()
            point_val = int(points_dict.get(reason_val, point)) if auto_calc else int(point)

            if not name or not reason_val:
                st.error("이름과 사유를 모두 입력하세요.")
            else:
                now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                df_now = load_penalties_df()
                total_score = calc_total_for_name(name, df_now) + point_val
                try:
                    safe_append_row(
                        sheet_penalty,
                        [now, name, reason_val, point_val, int(total_score)]
                    )
                    st.success(f"✅ {name}님 페널티 기록 완료! (이번 {point_val}, 누적 {int(total_score)})")
                    st.cache_data.clear()   # ✅ 새 데이터 반영을 위해 데이터 캐시 비우기
                except Exception as e:
                    st.error(f"기록 중 오류가 발생했어요: {e}")

    else:
        st.info("관리자 비밀번호를 입력하면 기록 폼이 열립니다.")
