import os
import re
import json
from typing import Dict, List, Tuple, Optional, Any
from datetime import datetime

import gspread
import pandas as pd


# ==========================================================
# [환경변수 / 시트명 설정]
# - Google Sheets 접속용 ID / Credentials 로드
# - RAW / 유상증자 / 주식연계채권 / parse_log 시트명 설정
# - 특정 접수번호만 테스트할 때 RUN_ONLY_ACPTNO 사용
# ==========================================================
GOOGLE_SHEET_ID = os.environ.get("GOOGLE_SHEET_ID", "").strip()
GOOGLE_CREDENTIALS_JSON = (
    os.environ.get("GOOGLE_CREDENTIALS_JSON", "").strip()
    or os.environ.get("GOOGLE_CREDS", "").strip()
)

RAW_SHEET_NAME = os.getenv("DUMP_SHEET_NAME", "RAW_dump")
RIGHTS_SHEET_NAME = os.getenv("RIGHTS_SHEET_NAME", "유상증자")
BOND_SHEET_NAME = os.getenv("BOND_SHEET_NAME", "주식연계채권")
PARSE_LOG_SHEET_NAME = os.getenv("PARSE_LOG_SHEET_NAME", "parse_log")

RUN_ONLY_ACPTNO = os.getenv("RUN_ONLY_ACPTNO", "").strip()


# ==========================================================
# [유상증자 시트 헤더]
# - 최종 구조화 시트에 들어갈 컬럼 순서
# - upsert 시 반드시 이 순서대로 들어감
# ==========================================================
RIGHTS_HEADERS = [
    "회사명", "보고서명", "상장시장", "최초 이사회결의일", "증자방식", "발행상품",
    "신규발행주식수", "확정발행가(원)", "기준주가", "확정발행금액(억원)",
    "할인(할증률)", "증자전 주식수", "증자비율", "납입일", "신주의 배당기산일",
    "신주의 상장 예정일", "이사회결의일", "자금용도", "투자자", "링크", "접수번호"
]

# ==========================================================
# [주식연계채권 시트 헤더]
# - CB / EB / BW 공통 컬럼 구조
# - Put / Call / 리픽싱 / 행사조건 등을 포함
# ==========================================================
BOND_HEADERS = [
    "구분", "회사명", "보고서명", "상장시장", "최초 이사회결의일", "권면총액(원)",
    "Coupon", "YTM", "만기", "전환청구 시작", "전환청구 종료",
    "Put Option", "Call Option", "Call 비율", "YTC", "모집방식",
    "발행상품", "행사(전환)가액(원)", "전환주식수", "주식총수대비 비율",
    "Refixing Floor", "납입일", "자금용도", "투자자", "링크", "접수번호"
]

# ==========================================================
# [파싱 로그 시트 헤더]
# - 어떤 접수번호가 어떤 시트로 갔는지
# - 누락 컬럼 / 의심 컬럼 / 처리 상태 기록용
# ==========================================================
PARSE_LOG_HEADERS = [
    "접수번호", "보고서명", "대상시트", "상태", "누락컬럼", "의심컬럼", "처리시각"
]


# ==========================================================
# Google Sheets
# ==========================================================
# [구글시트 열기]
# - 서비스 계정 credentials로 Google Sheet open
# - 환경변수가 없으면 바로 에러 발생
def gs_open():
    if not GOOGLE_SHEET_ID or not GOOGLE_CREDENTIALS_JSON:
        raise RuntimeError("GOOGLE_SHEET_ID / GOOGLE_CREDS(또는 GOOGLE_CREDENTIALS_JSON)가 비어있습니다.")

    creds = json.loads(GOOGLE_CREDENTIALS_JSON)
    gc = gspread.service_account_from_dict(creds)
    sh = gc.open_by_key(GOOGLE_SHEET_ID)
    return sh


# [워크시트 보장]
# - 시트가 이미 있으면 가져오고
# - 없으면 새로 생성
def ensure_ws(sh, title: str, rows: int = 2000, cols: int = 60):
    try:
        return sh.worksheet(title)
    except gspread.WorksheetNotFound:
        return sh.add_worksheet(title=title, rows=rows, cols=cols)


# [헤더 보장]
# - 1행 헤더가 다르면 시트를 clear 후 새 헤더 입력
# - 구조가 달라졌을 때 맞춰주는 용도
def ensure_header(ws, headers: List[str]):
    current = ws.row_values(1)
    if current != headers:
        ws.clear()
        ws.update("A1", [headers])


# [안전한 셀 접근]
# - row 길이보다 큰 index 요청 시 에러 대신 "" 반환
def safe_cell(row: List[str], idx: int) -> str:
    return row[idx] if idx < len(row) else ""


# ==========================================================
# RAW loader
# ==========================================================
# [RAW_dump 로더]
# - RAW_dump에 쌓여있는 행들을 acpt_no 단위로 묶음
# - META / HEADER / DATA 구조를 읽어 DataFrame 리스트로 재구성
# - 최종적으로 rec = {acpt_no, title, src_url, tables...} 형태 반환
def load_raw_records(raw_ws) -> List[Dict[str, Any]]:
    values = raw_ws.get_all_values()
    if not values:
        return []

    by_acpt: Dict[str, List[List[str]]] = {}
    for row in values:
        acpt_no = safe_cell(row, 0).strip()
        if not acpt_no or not acpt_no.isdigit():
            continue
        by_acpt.setdefault(acpt_no, []).append(row)

    records = []
    for acpt_no, rows in by_acpt.items():
        meta = {"acpt_no": acpt_no, "category": "", "title": "", "src_url": "", "run_ts": ""}
        table_buckets: Dict[str, Dict[str, Any]] = {}

        for row in rows:
            row_type = safe_cell(row, 2).strip()

            # META 행: 공시 제목 / 링크 / 카테고리 등 저장
            if row_type == "META":
                meta["category"] = safe_cell(row, 3)
                meta["title"] = safe_cell(row, 4)
                meta["src_url"] = safe_cell(row, 5)
                meta["run_ts"] = safe_cell(row, 6)

            # HEADER 행: table index별 컬럼 헤더 저장
            elif row_type == "HEADER":
                tix = safe_cell(row, 1).strip()
                table_buckets.setdefault(tix, {"header": [], "data": []})
                table_buckets[tix]["header"] = row[3:]

            # DATA 행: 실제 테이블 body 저장
            elif row_type == "DATA":
                tix = safe_cell(row, 1).strip()
                table_buckets.setdefault(tix, {"header": [], "data": []})
                table_buckets[tix]["data"].append(row[3:])

        dfs = []
        for tix in sorted(table_buckets.keys(), key=lambda x: int(x) if str(x).isdigit() else 999999):
            header = table_buckets[tix]["header"]
            data = table_buckets[tix]["data"]

            width = max(len(header), max((len(r) for r in data), default=0))
            if width == 0:
                continue

            # 열 개수 맞추기
            header = header + [f"col_{i}" for i in range(len(header), width)]
            norm_data = [r + [""] * (width - len(r)) for r in data]
            dfs.append(pd.DataFrame(norm_data, columns=header))

        records.append({
            "acpt_no": meta["acpt_no"],
            "category": meta["category"],
            "title": meta["title"],
            "src_url": meta["src_url"],
            "run_ts": meta["run_ts"],
            "tables": dfs,
        })

    records.sort(key=lambda x: x["acpt_no"])
    return records


# ==========================================================
# Common utils
# ==========================================================
# [기본 문자열 정리]
# - None 방지
# - 줄바꿈/다중 공백 정리
def normalize_text(x: Any) -> str:
    if x is None:
        return ""
    s = str(x).replace("\xa0", " ")
    s = re.sub(r"\s+", " ", s)
    return s.strip()


# [강한 정규화]
# - 공백 제거 + 콜론 제거
# - label 비교용
def _norm(s: Any) -> str:
    return re.sub(r"\s+", "", str(s or "")).replace(":", "")


# [라벨 정리]
# - ①, (1), 1. 같은 앞번호 제거
# - 표 라벨 비교용
def _clean_label(s: Any) -> str:
    return re.sub(r"^([①-⑩]|\(\d+\)|\d+\.)+", "", _norm(s))


# [한 줄 문자열화]
def _single_line(s: Any) -> str:
    return re.sub(r"\s+", " ", str(s or "")).strip()


# [날짜 비교용 숫자화]
# - 2025년 03월 10일 -> 20250310
def _norm_date(s: Any) -> str:
    return re.sub(r"[^\d]", "", str(s or ""))


# [날짜 포맷 통일]
# - 다양한 날짜 표현을 "YYYY년 MM월 DD일" 형식으로 정리
def _format_date(s: Any) -> str:
    txt = _single_line(s)
    m = re.search(r'(\d{4})[-년\./\s]+(\d{1,2})[-월\./\s]+(\d{1,2})', txt)
    if m:
        return f"{m.group(1)}년 {int(m.group(2)):02d}월 {int(m.group(3)):02d}일"
    return txt


# [회사명 비교용 정규화]
# - (주), 주식회사, ㈜ 제거 후 비교
def norm_company_name(name: str) -> str:
    if not name:
        return ""
    n = name.replace("주식회사", "").replace("(주)", "").replace("㈜", "")
    return _norm(n)


# [첫 번째 비어있지 않은 값 반환]
def first_nonempty(*vals):
    for v in vals:
        if normalize_text(v):
            return normalize_text(v)
    return ""


# [키워드 포함 여부]
def contains_any(text: str, keywords: List[str]) -> bool:
    return any(k in text for k in keywords)


# [숫자형 문자열 -> float]
# - %, 원, 콤마 등 제거 후 float 변환
def parse_float_like(s):
    if s is None:
        return None
    t = re.sub(r"[^\d\.\-]", "", str(s).replace(",", ""))
    if t in ("", "-", "."):
        return None
    try:
        return float(t)
    except Exception:
        return None


# [문자열 -> int]
def parse_int(value: Any):
    s = normalize_text(value).replace(",", "")
    m = re.search(r"-?\d+", s)
    return int(m.group(0)) if m else None


# [문자열 -> float]
def parse_float(value: Any):
    s = normalize_text(value).replace(",", "")
    m = re.search(r"-?\d+(?:\.\d+)?", s)
    return float(m.group(0)) if m else None


# [안전 int 변환]
def _to_int(s: Any) -> Optional[int]:
    if s is None:
        return None
    t = re.sub(r"[^\d\-]", "", str(s).replace(",", ""))
    if t in ("", "-"):
        return None
    try:
        return int(t)
    except Exception:
        return None


# [안전 float 변환]
def _to_float(s: Any) -> Optional[float]:
    if s is None:
        return None
    t = re.sub(r"[^\d\.\-]", "", str(s).replace(",", ""))
    if t in ("", "-", "."):
        return None
    try:
        return float(t)
    except Exception:
        return None


# [문장 안 최대 정수 찾기]
# - 텍스트 안에 여러 숫자가 있을 때 가장 큰 숫자 반환
# - 주식수 / 금액 추정에 자주 사용
def _max_int_in_text(s: Any) -> Optional[int]:
    if not s:
        return None
    s_clean = re.sub(r'(^|\s)[\(①-⑩]?\s*\d+\s*[\.\)]\s+', ' ', str(s))
    nums = re.findall(r"\d{1,3}(?:[,.]\d{3})+(?!\d)|\d+", s_clean)
    vals = []
    for x in nums:
        t = re.sub(r"[,.]", "", x)
        if t.isdigit():
            vals.append(int(t))
    return max(vals) if vals else None


# [퍼센트 문자열 정리]
# - 10 -> 10%
# - 10.5 % -> 10.5%
def clean_percent(value: str) -> str:
    s = normalize_text(value)
    if not s:
        return ""
    if "%" in s:
        m = re.search(r"-?\d+(?:\.\d+)?\s*%", s)
        return m.group(0).replace(" ", "") if m else s
    m = re.search(r"-?\d+(?:\.\d+)?", s.replace(",", ""))
    return f"{m.group(0)}%" if m else s


# [숫자 포맷팅]
# - 정수는 1,234 형식
# - 소수면 2자리까지
def fmt_number(x):
    if x in (None, ""):
        return ""
    try:
        fx = float(x)
    except Exception:
        return str(x)
    if abs(fx - round(fx)) < 1e-9:
        return f"{int(round(fx)):,}"
    return f"{fx:,.2f}"


# [원화 -> 억원]
def fmt_eok_from_won(won):
    if won is None:
        return ""
    return f"{won / 100000000:.2f}"


# [보고서명 정리]
# - 자동복구 태그 제거
def clean_title(title: str) -> str:
    return _single_line(title).replace("[자동복구대상]", "").strip()


# [정정 공시 여부]
# - 제목에 [정정] 또는 정정 포함 여부 판단
def is_correction_title(title: str) -> bool:
    t = clean_title(title)
    return t.startswith("[정정]") or t.startswith("정정") or "[정정]" in t or "정정" in t


# [시장 문자열 표준화]
# - 다양한 표현을 코스닥 / 유가증권 / 코넥스 / 비상장으로 통일
def normalize_market_value(value: Any) -> str:
    s = normalize_text(value)
    n = _norm(s)

    if not s:
        return ""

    if (
        "코스닥" in s or "코스닥시장" in s or "[코]" in s or "KOSDAQ" in s.upper()
        or n in ["코", "코스닥", "코스닥시장"]
    ):
        return "코스닥"

    if (
        "유가증권" in s or "유가증권시장" in s or "코스피" in s or "[유]" in s or "KOSPI" in s.upper()
        or n in ["유", "유가증권", "유가증권시장", "코스피"]
    ):
        return "유가증권"

    if (
        "코넥스" in s or "코넥스시장" in s or "[코넥스]" in s or "[넥]" in s or "KONEX" in s.upper()
        or n in ["넥", "코넥스", "코넥스시장", "konex"]
    ):
        return "코넥스"

    if "비상장" in s or n == "비상장":
        return "비상장"

    return ""


# [제목에서 시장 추정]
# - [코] / [유] / [코넥스] 태그나 문구로 시장 인식
def detect_market_from_title(title: str) -> str:
    return normalize_market_value(title)


# [제목에서 공시 패밀리 인식]
# - 유상증자 / CB / EB / BW 구분용
def detect_report_family(title: str) -> str:
    for k in [
        "유상증자결정",
        "전환사채권발행결정",
        "교환사채권발행결정",
        "신주인수권부사채권발행결정",
    ]:
        if k in title.replace(" ", ""):
            return k
    return ""


# [제목에서 회사명 추출]
# - [코] [정정] 등을 제거하고 앞부분 회사명 추출
def extract_company_name_from_title(title: str) -> str:
    t = clean_title(title)
    t = re.sub(r"^\[(유|코|넥|코넥|KOSPI|KOSDAQ|KONEX)\]\s*", "", t).strip()
    t = re.sub(r"\[정정\]\s*", "", t).strip()
    for k in ["유상증자결정", "전환사채권발행결정", "교환사채권발행결정", "신주인수권부사채권발행결정"]:
        if k in t.replace(" ", ""):
            m = re.search(rf"^(.*?)\s*{k}", t)
            if m:
                return m.group(1).strip()
    parts = t.split()
    if not parts:
        return ""
    if len(parts) >= 2 and parts[0] in ("주식회사", "(주)", "㈜"):
        return f"{parts[0]} {parts[1]}".strip()
    return parts[0].strip()


# [유효한 날짜 문자열인지 판정]
# - 날짜처럼 보이지만 사실 설명문인 것들을 제외
def looks_like_valid_date(v: str) -> bool:
    v = _single_line(v)
    if not re.search(r"\d", v):
        return False
    bad_kws = [
        "정정", "변경", "요청", "사유", "기재", "오기",
        "추가상장", "상장주식", "총수", "교부예정일", "사항",
        "기준", "발행", "항목"
    ]
    if any(b in v for b in bad_kws):
        return False
    if not (re.search(r"\d{4}", v) or re.search(r"\d{2,4}[\.\-\/년]\s*\d{1,2}", v)):
        return False
    return True


# [모든 테이블을 한 줄 텍스트 리스트로 변환]
# - 옵션 본문 파싱 / 날짜 구간 파싱 등에 사용
def all_text_lines(tables: List[pd.DataFrame]) -> List[str]:
    lines = []
    for df in tables:
        arr = df.fillna("").astype(str).values.tolist()
        for row in arr:
            joined = " | ".join([normalize_text(x) for x in row if normalize_text(x)])
            if joined:
                lines.append(joined)
    return lines


# [테이블 전체 텍스트 평탄화]
# - 한 테이블의 모든 셀을 하나의 긴 문자열로 합침
def flatten_table_text(table: pd.DataFrame) -> str:
    parts = []
    arr = table.fillna("").astype(str).values.tolist()
    for row in arr:
        for cell in row:
            s = normalize_text(cell)
            if s:
                parts.append(s)
    return " ".join(parts)


# ==========================================================
# Pair helpers
# ==========================================================
# [2열 페어 추출]
# - 행 내 인접 셀들을 (left, right) 쌍으로 저장
# - 라벨-값 구조 탐색용
def df_to_pairs(df: pd.DataFrame) -> List[Tuple[str, str]]:
    pairs = []
    arr = df.fillna("").astype(str).values.tolist()
    for row in arr:
        row = [normalize_text(x) for x in row]
        if len(row) < 2:
            continue
        for i in range(len(row) - 1):
            left = row[i].strip()
            right = row[i + 1].strip()
            if left:
                pairs.append((left, right))
    return pairs


# [전체 테이블 페어 추출]
def all_pairs_from_tables(tables: List[pd.DataFrame]) -> List[Tuple[str, str]]:
    out = []
    for df in tables:
        out.extend(df_to_pairs(df))
    return out


# [왼쪽 라벨 키워드로 값 찾기]
def find_value_by_left_keywords(pairs: List[Tuple[str, str]], keywords: List[str]) -> str:
    for left, right in pairs:
        if contains_any(left, keywords) and normalize_text(right):
            return normalize_text(right)
    return ""


# [왼쪽 라벨 키워드로 숫자값 찾기]
def find_numeric_value_by_keywords(pairs: List[Tuple[str, str]], keywords: List[str]):
    return parse_float(find_value_by_left_keywords(pairs, keywords))


# ==========================================================
# Table scanners
# ==========================================================
# [정정사항 표의 '정정후' 맵 추출]
# - 정정 공시인 경우 '정정사항 / 정정전 / 정정후' 표를 우선 파싱
# - item -> 정정후값 형태의 dict 생성
# - 이후 실제 컬럼 추출 시 corr_after를 최우선으로 사용
def extract_correction_after_map(dfs: List[pd.DataFrame]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for df in dfs:
        try:
            arr = df.astype(str).values
        except Exception:
            continue

        R, C = arr.shape
        header_r = after_col = item_col = None

        for r in range(R):
            row_norm = [_norm(x) for x in arr[r].tolist()]
            has_before = any(w in x for w in ["정정전", "변경전"] for x in row_norm)
            has_after = any(w in x for w in ["정정후", "변경후"] for x in row_norm)
            if has_before and has_after:
                header_r = r
                after_col = next((i for i, x in enumerate(row_norm) if "정정후" in x or "변경후" in x), None)
                item_col = next((i for i, x in enumerate(row_norm) if ("정정사항" in x or "항목" in x or "구분" in x)), 0)
                break

        if header_r is None or after_col is None:
            continue

        last_item = ""
        for rr in range(header_r + 1, R):
            item = str(arr[rr][item_col]).strip() if item_col is not None and item_col < C else ""
            item = item if item and item.lower() != "nan" else last_item
            if not item:
                continue
            last_item = item

            after_val = ""
            if 0 <= after_col < C:
                v = str(arr[rr][after_col]).strip()
                if v and v.lower() != "nan" and _norm(v) not in ("정정후", "정정전", "항목", "변경사유", "정정사유", "-"):
                    after_val = _single_line(v)

            if after_val:
                out[_norm(item)] = after_val
                out[_clean_label(item)] = after_val

    return out


# [라벨 주변 값 탐색]
# - 표 안에서 label 후보를 찾고
# - 오른쪽 / 아래 / 같은 행에서 실제 값을 찾아 반환
def scan_label_value(dfs: List[pd.DataFrame], label_candidates: List[str]) -> str:
    cand_clean = {_clean_label(x) for x in label_candidates}
    for df in dfs:
        try:
            arr = df.astype(str).values
        except Exception:
            continue
        R, C = arr.shape

        for r in range(R):
            for c in range(C):
                if _clean_label(arr[r][c]) in cand_clean:
                    checks = []
                    for rr, cc in [(r, c + 1), (r, c + 2), (r + 1, c), (r + 1, c + 1)]:
                        if 0 <= rr < R and 0 <= cc < C:
                            checks.append(str(arr[rr][cc]).strip())

                    row_vals = [str(x).strip() for x in arr[r].tolist() if str(x).strip()]
                    for v in [v for v in checks + row_vals if v and v.lower() != "nan"]:
                        if _clean_label(v) in cand_clean:
                            continue
                        if re.fullmatch(r"([①-⑩]|\(\d+\)|\d+\.)", _norm(v)):
                            continue
                        return _single_line(v)
    return ""


# [라벨 값 탐색 - 정정후 우선]
# - 정정 공시면 corr_after에서 먼저 찾고
# - 없으면 일반 표 스캔으로 fallback
def scan_label_value_preferring_correction(dfs: List[pd.DataFrame], label_candidates: List[str], corr_after: Dict[str, str]) -> str:
    cand_clean = {_clean_label(x) for x in label_candidates}
    if corr_after:
        for c in cand_clean:
            if c in corr_after and str(corr_after[c]).strip():
                return _single_line(str(corr_after[c]))
        for k, v in corr_after.items():
            if str(v).strip() and any(c in k for c in cand_clean):
                return _single_line(str(v))
    return scan_label_value(dfs, label_candidates)


# [행 단위 최대 정수 탐색]
# - 특정 키워드들이 포함된 행에서 숫자 후보 중 가장 적절한 큰 값 선택
def find_row_best_int(dfs: List[pd.DataFrame], must_contain: List[str], min_val: int = 0) -> Optional[int]:
    keys = [_norm(x) for x in must_contain]
    best = None
    for df in dfs:
        try:
            arr = df.astype(str).values
        except Exception:
            continue

        for r in range(arr.shape[0]):
            row = [str(x).strip() for x in arr[r].tolist()]
            if all(k in _norm("".join(row)) for k in keys):
                valid_amts = []
                for cell in row:
                    if any(d in cell for d in ["년", "월", "일", "예정일", "납입일", "기일"]):
                        continue
                    amt = _max_int_in_text(cell)
                    if amt is not None and amt > min_val:
                        valid_amts.append(amt)
                if valid_amts:
                    best = valid_amts[-1]
    return best


# [행 단위 float 탐색]
def find_row_best_float(dfs: List[pd.DataFrame], must_contain: List[str]) -> Optional[float]:
    keys = [_norm(x) for x in must_contain]
    for df in dfs:
        try:
            arr = df.astype(str).values
        except Exception:
            continue

        for r in range(arr.shape[0]):
            row = [str(x).strip() for x in arr[r].tolist()]
            if all(k in _norm("".join(row)) for k in keys):
                vals = [x for x in [_to_float(x) for x in row] if x is not None]
                if vals:
                    return max(vals, key=lambda z: abs(z))
    return None


# [라벨 기반 날짜 추출]
# - 정정후 값 우선
# - 표 내 라벨 주변에서 유효한 날짜 찾아 포맷팅
def get_valid_date_by_labels(dfs: List[pd.DataFrame], labels: List[str], corr_after: Optional[Dict[str, str]] = None) -> str:
    cand_clean = {_clean_label(x) for x in labels}

    if corr_after:
        for k, v in corr_after.items():
            if any(c in k for c in cand_clean):
                if looks_like_valid_date(v):
                    return _format_date(v)

    for df in dfs:
        try:
            arr = df.astype(str).values
        except Exception:
            continue

        R, C = arr.shape
        for r in range(R):
            row_vals = [str(x).strip() for x in arr[r].tolist() if str(x).strip() and str(x).strip().lower() != "nan"]
            if any(_clean_label(x) in cand_clean for x in row_vals):
                possible_dates = []
                for v in row_vals:
                    if _clean_label(v) in cand_clean:
                        continue
                    if re.fullmatch(r"([①-⑩]|\(\d+\)|\d+\.)", _norm(v)):
                        continue
                    if looks_like_valid_date(v):
                        possible_dates.append(v)
                if possible_dates:
                    return _format_date(possible_dates[-1])

    val = scan_label_value(dfs, labels)
    if looks_like_valid_date(val):
        return _format_date(val)
    return ""


# [테이블에서 시장 인식]
def detect_market_from_tables(dfs: List[pd.DataFrame], corr_after: Optional[Dict[str, str]] = None) -> str:
    market_labels = [
        "상장시장", "시장구분", "주권상장구분", "상장구분",
        "주권상장시장", "상장 여부", "상장여부"
    ]
    label_set = {_clean_label(x) for x in market_labels}

    # 1순위: 정정공시 값 우선
    if corr_after:
        for k, v in corr_after.items():
            k_clean = _clean_label(k)
            k_norm = _norm(k)

            if k_clean in label_set or any(_norm(lb) in k_norm for lb in market_labels):
                market = normalize_market_value(v)
                if market:
                    return market

    # 2순위: 실제 표에서 라벨 주변 탐색
    for df in dfs:
        try:
            arr = df.fillna("").astype(str).values
        except Exception:
            continue

        R, C = arr.shape

        for r in range(R):
            for c in range(C):
                cell = normalize_text(arr[r][c])
                if not cell:
                    continue

                if _clean_label(cell) not in label_set:
                    continue

                candidates = []

                for rr, cc in [
                    (r, c + 1), (r, c + 2),
                    (r + 1, c), (r + 1, c + 1), (r + 1, c + 2)
                ]:
                    if 0 <= rr < R and 0 <= cc < C:
                        candidates.append(arr[rr][cc])

                row_text = " ".join([normalize_text(x) for x in arr[r].tolist() if normalize_text(x)])
                if row_text:
                    candidates.append(row_text)

                if r + 1 < R:
                    next_row_text = " ".join([normalize_text(x) for x in arr[r + 1].tolist() if normalize_text(x)])
                    if next_row_text:
                        candidates.append(next_row_text)

                for cand in candidates:
                    market = normalize_market_value(cand)
                    if market:
                        return market

    # 3순위: 표 전체 텍스트 보조 탐색
    for line in all_text_lines(dfs):
        line_norm = _norm(line)
        if any(_norm(lb) in line_norm for lb in market_labels):
            market = normalize_market_value(line)
            if market:
                return market

    return ""


# [테이블에서 회사명 인식]
# - 회사명 관련 라벨을 우선 탐색
# - 너무 긴 문장 / 비정상 텍스트는 제거
def detect_company_from_tables(dfs: List[pd.DataFrame], corr_after: Optional[Dict[str, str]] = None) -> str:
    labels = ["회사명", "회사 명", "발행회사", "발행회사명", "법인명", "종속회사명", "종속회사", "종속회사인"]
    v = scan_label_value_preferring_correction(dfs, labels, corr_after or {})
    if not v:
        return ""
    v = v.split("\n")[0].strip()
    bad_kws = ["상장여부", "여부", "해당사항", "해당없음", "본점", "소재지", "신고", "경영사항", "결정"]
    if len(v) > 40 or any(k in v.replace(" ", "") for k in bad_kws) or v in ("-", "."):
        return ""
    return v


# ==========================================================
# Rights-specific helpers
# ==========================================================
# [주식수 텍스트 파서]
# - 보통 / 기타(종류주) / 합계 숫자를 문장 안에서 분리 추출
# - 신규발행주식수 / 증자전주식수 파싱에 공통 사용
def parse_shares_from_text(text: str) -> Tuple[int, int, int]:
    text_norm = _norm(text)
    text_norm = re.sub(r'202\d[년월일\.]?', '', text_norm)
    text_norm = re.sub(r'\d+(?:\.\d+)?%', '', text_norm)

    boundaries = r'보통|기타|종류|우선|상환|합계|총계|총수|계|액면|자금|목적|발행가|할인'

    pattern_com = r'보통(?:(?!' + boundaries + r')[^0-9])*?((?:\d{1,3}(?:[,.]\d{3})+|\d+))'
    m_com = re.findall(pattern_com, text_norm)
    cvs = [int(re.sub(r'[,.]', '', x)) for x in m_com if int(re.sub(r'[,.]', '', x)) >= 50]
    cv = cvs[-1] if cvs else 0

    pattern_oth = r'(?:기타|종류|우선|상환)(?:(?!' + boundaries + r')[^0-9])*?((?:\d{1,3}(?:[,.]\d{3})+|\d+))'
    m_oth = re.findall(pattern_oth, text_norm)
    ovs = [int(re.sub(r'[,.]', '', x)) for x in m_oth if int(re.sub(r'[,.]', '', x)) >= 50]
    ov = ovs[-1] if ovs else 0

    pattern_tot = r'(?:합계|총계|총수|계)(?:(?!' + boundaries + r')[^0-9])*?((?:\d{1,3}(?:[,.]\d{3})+|\d+))'
    m_tot = re.findall(pattern_tot, text_norm)
    tvs = [int(re.sub(r'[,.]', '', x)) for x in m_tot if int(re.sub(r'[,.]', '', x)) >= 50]
    tv = tvs[-1] if tvs else 0

    # 위 패턴으로 못 잡은 경우 마지막 큰 숫자를 fallback으로 사용
    if cv == 0 and ov == 0 and tv == 0:
        text_clean = text_norm
        for kw in [
            "신주의종류와수", "발행예정주식", "발행예정주식수",
            "신주발행", "발행할주식", "증자전발행주식총수",
            "기발행주식총수", "발행주식총수", "증자전주식수", "증자전"
        ]:
            text_clean = text_clean.replace(kw, "")
        text_clean = re.sub(r'^([①-⑩]|\(\d+\)|\d+\.)+', '', text_clean)
        nums_str = re.findall(r"\d{1,3}(?:[,.]\d{3})+(?!\d)|\d+", text_clean)
        v_nums = [int(re.sub(r'[,.]', '', x)) for x in nums_str if int(re.sub(r'[,.]', '', x)) >= 50]
        if v_nums:
            cv = v_nums[-1]

    return cv, ov, tv


# [신규발행주식수 + 발행상품 추출]
# - 신주의 종류와 수 / 발행예정주식수 근처를 읽어서
# - 총 주식수와 발행상품(보통/우선/혼합) 추출
def extract_issue_shares_and_type(dfs: List[pd.DataFrame], corr_after: Dict[str, str]) -> Tuple[Optional[int], str]:
    target_kws = ["신주의종류와수", "발행예정주식수", "발행예정주식", "신주발행", "발행할주식"]
    stop_kws = ["증자전", "기발행", "총수", "발행가", "액면가", "자금조달", "증자방식", "일정", "목적"]

    stock_type = "보통주식"
    best_amt = 0

    # 1순위: 정정후 값에서 먼저 찾기
    if corr_after:
        for k, v in corr_after.items():
            k_norm = _norm(k)
            if any(t in k_norm for t in target_kws):
                if not any(s in k_norm for s in stop_kws):
                    cv, ov, tv = parse_shares_from_text(str(v))
                    calc = cv + ov
                    if tv > 0 and tv >= calc:
                        best_amt = tv
                    elif calc > 0:
                        best_amt = calc
                    elif cv > 0:
                        best_amt = cv
                    elif ov > 0:
                        best_amt = ov

                    if best_amt > 0:
                        v_norm = _norm(v)
                        if ov > 0 and cv == 0:
                            stock_type = "우선주식"
                        elif cv > 0 and ov == 0:
                            stock_type = "보통주식"
                        elif cv > 0 and ov > 0:
                            stock_type = "보통주식, 우선주식"
                        elif "우선" in v_norm or "종류" in v_norm or "기타" in v_norm:
                            stock_type = "보통주식, 우선주식" if "보통" in v_norm else "우선주식"
                        return best_amt, stock_type

    # 2순위: 일반 테이블 블록 스캔
    for df in dfs:
        try:
            arr = df.astype(str).values
        except Exception:
            continue

        R, C = arr.shape
        for r in range(R):
            row_str_norm = _norm("".join(arr[r]))
            combined_target = row_str_norm
            if r + 1 < R:
                combined_target += _norm("".join(arr[r + 1]))

            if any(t in combined_target for t in target_kws):
                if any(s in row_str_norm for s in stop_kws) and not any(t in row_str_norm for t in target_kws):
                    continue

                block_text = ""
                search_start = max(0, r - 1)

                for rr in range(search_start, min(r + 6, R)):
                    curr_row_norm = _norm("".join(arr[rr]))

                    if rr < r and any(s in curr_row_norm for s in stop_kws + ["액면", "자금", "방식"]):
                        continue

                    if rr > r + 1:
                        clean_next = _clean_label(curr_row_norm)
                        if len(curr_row_norm) != len(clean_next):
                            if any(k in curr_row_norm for k in ["액면", "자금", "가액", "증자", "목적", "방식", "총수", "예정"]):
                                break

                    for c in range(C):
                        cell_str = _norm(arr[rr][c])
                        if any(s in cell_str for s in stop_kws) and not any(t in cell_str for t in target_kws):
                            continue
                        block_text += " " + cell_str

                cv, ov, tv = parse_shares_from_text(block_text)

                calc_tot = cv + ov
                if tv > 0 and tv >= calc_tot:
                    best_amt = tv
                elif calc_tot > 0:
                    best_amt = calc_tot
                elif cv > 0:
                    best_amt = cv
                elif ov > 0:
                    best_amt = ov

                if best_amt > 0:
                    if ov > 0 and cv == 0:
                        stock_type = "우선주식"
                    elif cv > 0 and ov == 0:
                        stock_type = "보통주식"
                    elif cv > 0 and ov > 0:
                        stock_type = "보통주식, 우선주식"
                    elif "우선" in block_text or "종류" in block_text or "기타" in block_text:
                        stock_type = "보통주식, 우선주식" if "보통" in block_text else "우선주식"
                    return best_amt, stock_type

    # 3순위: 단순 라벨 값 fallback
    val = scan_label_value(dfs, ["신주의 종류와 수", "발행예정주식", "발행예정주식수"])
    amt = _max_int_in_text(val)
    if amt and amt > 100:
        stock_type = "우선주식" if any(x in _norm(val) for x in ["우선", "기타", "종류"]) else "보통주식"
        return amt, stock_type

    return None, "보통주식"


# [증자전 주식수 추출]
# - '증자전발행주식총수' 계열 표를 읽어서 총 발행주식수 추출
def get_prev_shares_sum(dfs: List[pd.DataFrame], corr_after: Dict[str, str]) -> Optional[int]:
    target_kws = ["증자전발행주식총수", "기발행주식총수", "발행주식총수", "증자전주식수", "증자전"]
    stop_kws = ["신주의종류", "발행예정", "자금조달", "증자방식", "신주발행", "액면가", "발행가", "목적", "일정"]

    if corr_after:
        for k, v in corr_after.items():
            k_norm = _norm(k)
            if any(t in k_norm for t in target_kws):
                if not any(s in k_norm for s in stop_kws):
                    cv, ov, tv = parse_shares_from_text(str(v))
                    calc = cv + ov
                    if tv > 0 and tv >= calc:
                        return tv
                    if calc > 0:
                        return calc
                    if cv > 0:
                        return cv

    for df in dfs:
        try:
            arr = df.astype(str).values
        except Exception:
            continue

        R, C = arr.shape
        for r in range(R):
            row_str_norm = _norm("".join(arr[r]))
            combined_target = row_str_norm
            if r + 1 < R:
                combined_target += _norm("".join(arr[r + 1]))

            if any(t in combined_target for t in target_kws):
                if any(s in row_str_norm for s in stop_kws) and not any(t in row_str_norm for t in target_kws):
                    continue

                block_text = ""
                search_start = max(0, r - 1)

                for rr in range(search_start, min(r + 7, R)):
                    curr_row_norm = _norm("".join(arr[rr]))

                    if rr < r and any(s in curr_row_norm for s in stop_kws + ["액면", "자금", "방식"]):
                        continue

                    if rr > r + 1:
                        clean_next = _clean_label(curr_row_norm)
                        if len(curr_row_norm) != len(clean_next):
                            if any(k in curr_row_norm for k in ["액면", "자금", "가액", "증자", "목적", "방식", "신주", "예정"]):
                                break

                    for c in range(C):
                        cell_str = _norm(arr[rr][c])
                        if any(s in cell_str for s in stop_kws) and not any(t in cell_str for t in target_kws):
                            continue
                        block_text += " " + cell_str

                cv, ov, tv = parse_shares_from_text(block_text)
                calc_tot = cv + ov
                if tv > 0 and tv >= calc_tot:
                    return tv
                if calc_tot > 0:
                    return calc_tot
                if cv > 0:
                    return cv

    return None


# [기준주가 추출]
# - 기준주가 / 기준발행가액 섹션만 정밀하게 읽음
# - 확정발행가나 날짜 숫자가 섞여 들어오는 문제를 최대한 방지
def get_base_price_by_exact_section(dfs: List[pd.DataFrame], corr_after: Dict[str, str]) -> Optional[int]:
    """
    기준주가는 반드시 '7. 기준주가' 섹션에서만 추출한다.
    - 정정공시는 corr_after에서 '7. 기준주가' 항목 우선
    - 일반 공시는 실제 표에서 '7. 기준주가' 섹션 블록만 읽음
    - 다른 라벨(기준발행가액 등)에서 넓게 찾지 않음
    """

    def _extract_valid_prices(text: str) -> List[int]:
        if not text:
            return []

        txt = str(text)
        txt = re.sub(r'202\d[년월일\.]?', '', txt)
        txt = re.sub(r'\d+(?:\.\d+)?%', '', txt)
        txt = re.sub(r'^([①-⑩]|\(\d+\)|\d+\.)+', '', txt)

        nums = re.findall(
            r"(?<![\d.])\d{1,3}(?:,\d{3})*(?:\.\d+)?(?![\d.])|(?<![\d.])\d+(?:\.\d+)?(?![\d.])",
            txt
        )

        vals = []
        for x in nums:
            try:
                val = int(float(x.replace(",", "")))
                if val >= 50 and val not in [2024, 2025, 2026, 2027]:
                    vals.append(val)
            except Exception:
                pass
        return vals

    def _first_nonempty_cell(row_vals) -> str:
        for x in row_vals:
            s = normalize_text(x)
            if s:
                return s
        return ""

    def _is_section7_heading(text: str) -> bool:
        raw = normalize_text(text)
        n = _norm(raw)
        if not raw:
            return False

        patterns = [
            r"^7[\.\)]?기준주가$",
            r"^7[\.\)]?기준발행가액$",
        ]
        if any(re.match(p, n) for p in patterns):
            return True

        if "7기준주가" in n or "7기준발행가액" in n:
            return True

        return False

    def _is_new_top_heading(text: str) -> bool:
        raw = normalize_text(text)
        if not raw:
            return False
        return bool(re.match(r"^\d+\s*[\.\)]\s*[가-힣A-Za-z]", raw))

    if corr_after:
        for k, v in corr_after.items():
            k_raw = normalize_text(k)
            k_norm = _norm(k_raw)

            if _is_section7_heading(k_raw) or "7기준주가" in k_norm or "7기준발행가액" in k_norm:
                vals = _extract_valid_prices(v)
                if vals:
                    return max(vals)

    for df in dfs:
        try:
            arr = df.astype(str).values
        except Exception:
            continue

        R, C = arr.shape

        for r in range(R):
            row_list = arr[r].tolist()
            first_cell = _first_nonempty_cell(row_list)
            row_join = " ".join([normalize_text(x) for x in row_list if normalize_text(x)])

            if _is_section7_heading(first_cell) or _is_section7_heading(row_join):
                block_texts = []

                for rr in range(r, min(r + 6, R)):
                    next_row_list = arr[rr].tolist()
                    next_first = _first_nonempty_cell(next_row_list)
                    next_join = " ".join([normalize_text(x) for x in next_row_list if normalize_text(x)])

                    if rr > r and _is_new_top_heading(next_first):
                        break

                    block_texts.append(next_join)

                vals = _extract_valid_prices(" ".join(block_texts))
                if vals:
                    return max(vals)

    return None


# [확정/예정 발행가 추출]
# - 유상증자 공시에서 "6. 신주 발행가액" 섹션을 최우선으로 읽음
# - 정정공시는 corr_after 우선
# - 못 찾으면 기존 일반 발행가액 라벨로 fallback
def get_price_by_exact_section(dfs: List[pd.DataFrame], corr_after: Dict[str, str]) -> Optional[int]:
    target_kws = ["신주발행가액", "예정발행가액", "확정발행가액", "발행가액"]
    stop_kws = [
        "자금", "증자방식", "기준", "할인", "할증", "증자전",
        "주식수", "납입", "방법", "산정", "일정", "발행목적"
    ]

    def _extract_valid_prices(text: str) -> List[int]:
        if not text:
            return []

        txt = str(text)
        txt = re.sub(r'202\d[년월일\.]?', '', txt)
        txt = re.sub(r'\d+(?:\.\d+)?%', '', txt)
        txt = re.sub(r'^([①-⑩]|\(\d+\)|\d+\.)+', '', txt)

        nums = re.findall(
            r"(?<![\d.])\d{1,3}(?:,\d{3})*(?:\.\d+)?(?![\d.])|(?<![\d.])\d+(?:\.\d+)?(?![\d.])",
            txt
        )

        vals = []
        for x in nums:
            try:
                val = int(float(x.replace(",", "")))
                if val >= 50 and val not in [2024, 2025, 2026, 2027]:
                    vals.append(val)
            except Exception:
                pass
        return vals

    def _first_nonempty_cell(row_vals) -> str:
        for x in row_vals:
            s = normalize_text(x)
            if s:
                return s
        return ""

    def _is_section6_heading(text: str) -> bool:
        raw = normalize_text(text)
        n = _norm(raw)
        if not raw:
            return False

        patterns = [
            r"^6[\.\)]?신주발행가액$",
            r"^6[\.\)]?신주의발행가액$",
            r"^6[\.\)]?1주당신주발행가액$",
            r"^6[\.\)]?발행가액$",
        ]
        if any(re.match(p, n) for p in patterns):
            return True

        if "6신주발행가액" in n or "6신주의발행가액" in n:
            return True

        return False

    def _is_new_top_heading(text: str) -> bool:
        raw = normalize_text(text)
        if not raw:
            return False
        return bool(re.match(r"^\d+\s*[\.\)]\s*[가-힣A-Za-z]", raw))

    if corr_after:
        for k, v in corr_after.items():
            k_raw = normalize_text(k)
            k_norm = _norm(k_raw)

            if _is_section6_heading(k_raw) or "6신주발행가액" in k_norm or "6신주의발행가액" in k_norm:
                vals = _extract_valid_prices(v)
                if vals:
                    return max(vals)

        for k, v in corr_after.items():
            k_norm = _norm(k)
            if any(t in k_norm for t in target_kws) and not any(s in k_norm for s in stop_kws):
                vals = _extract_valid_prices(v)
                if vals:
                    return max(vals)

    for df in dfs:
        try:
            arr = df.astype(str).values
        except Exception:
            continue

        R, C = arr.shape

        for r in range(R):
            row_list = arr[r].tolist()
            first_cell = _first_nonempty_cell(row_list)
            row_join = " ".join([normalize_text(x) for x in row_list if normalize_text(x)])

            if _is_section6_heading(first_cell) or _is_section6_heading(row_join):
                block_texts = []

                for rr in range(r, min(r + 6, R)):
                    next_row_list = arr[rr].tolist()
                    next_first = _first_nonempty_cell(next_row_list)
                    next_join = " ".join([normalize_text(x) for x in next_row_list if normalize_text(x)])

                    if rr > r and _is_new_top_heading(next_first):
                        break

                    block_texts.append(next_join)

                vals = _extract_valid_prices(" ".join(block_texts))
                if vals:
                    return max(vals)

    for df in dfs:
        try:
            arr = df.astype(str).values
        except Exception:
            continue

        R, C = arr.shape
        for r in range(R):
            row_str_norm = _norm("".join(arr[r]))
            if any(t in row_str_norm for t in target_kws):
                if any(s in row_str_norm for s in stop_kws) and not any(t in row_str_norm for t in target_kws):
                    continue

                all_nums = []
                for rr in range(r, min(r + 4, R)):
                    curr_row_norm = _norm("".join(arr[rr]))
                    if rr > r:
                        clean_next = _clean_label(curr_row_norm)
                        if len(curr_row_norm) != len(clean_next):
                            break
                        if any(s in curr_row_norm for s in stop_kws):
                            break

                    for c in range(C):
                        cell_norm = _norm(arr[rr][c])
                        if any(s in cell_norm for s in stop_kws) and not any(t in cell_norm for t in target_kws):
                            continue
                        all_nums.extend(_extract_valid_prices(arr[rr][c]))

                if all_nums:
                    return max(all_nums)

    return None


# [자금용도 + 자금합계 추출]
# - 운영자금 / 채무상환자금 등 6개 카테고리 읽기
# - 자금용도 텍스트와 합산 금액(won)을 함께 반환
def extract_fund_use_and_amount(dfs: List[pd.DataFrame], corr_after: Dict[str, str]) -> Tuple[str, Optional[int]]:
    keys_map = {
        "시설자금": "시설자금",
        "영업양수자금": "영업양수자금",
        "운영자금": "운영자금",
        "채무상환자금": "채무상환자금",
        "타법인증권취득자금": "타법인 증권 취득자금",
        "타법인증권": "타법인 증권 취득자금",
        "기타자금": "기타자금",
        "취득자금": "취득자금",
    }
    found_amts: Dict[str, int] = {}

    if corr_after:
        for itemk, v in corr_after.items():
            for k, std_name in keys_map.items():
                if _norm(k) in itemk:
                    amt = _max_int_in_text(v)
                    if amt and amt >= 100:
                        found_amts[std_name] = amt

    for df in dfs:
        try:
            arr = df.astype(str).values
        except Exception:
            continue
        for r in range(arr.shape[0]):
            row = [str(x).strip() for x in arr[r].tolist()]
            row_joined = _norm("".join(row))
            for k, std_name in keys_map.items():
                if _norm(k) in row_joined:
                    valid_amts = []
                    for cell in row:
                        amt = _max_int_in_text(cell)
                        if amt is not None and amt >= 100:
                            valid_amts.append(amt)
                    if valid_amts:
                        found_amts[std_name] = valid_amts[-1]

    std_order = ["시설자금", "영업양수자금", "운영자금", "채무상환자금", "타법인 증권 취득자금", "취득자금", "기타자금"]
    uses = [name for name in std_order if found_amts.get(name, 0) > 0]
    total_sum = sum(found_amts.get(name, 0) for name in uses)
    return ", ".join(uses), (total_sum if total_sum > 0 else None)


# [유상증자 투자자 추출]
# - 제3자배정대상자 / 배정대상자 / 법인명 컬럼 등에서 투자자명 추출
# - 관계/합계/비고 같은 잡음을 blacklist로 제거
def extract_investors_rights(dfs: List[pd.DataFrame], corr_after: Dict[str, str]) -> str:
    investors = []
    blacklist = [
        "관계", "지분", "%", "주식", "배정", "선정", "경위", "비고", "해당사항",
        "정정전", "정정후", "정정", "변경", "합계", "소계", "총계", "발행", "납입",
        "예정", "목적", "주1", "주2", "주)", "기타", "참고",
        "출자자수", "본점", "소재지", "(명)", "명"
    ]

    def is_valid_name(s: str) -> bool:
        sn = s.strip()
        if not sn or sn in ("-", ".", ",", "(", ")", "0", "1"):
            return False
        if len(sn) > 40:
            return False
        if re.fullmatch(r'[\d,\.\s]+', sn):
            return False
        sn_norm = _norm(sn)
        for bw in blacklist:
            if bw in sn_norm:
                return False
        return True

    for df in dfs:
        try:
            arr = df.astype(str).values
        except Exception:
            continue
        R, C = arr.shape
        target_col = -1
        start_row = -1

        for r in range(R):
            row_str = "".join([_norm(str(x)) for x in arr[r]])
            if any(kw in row_str for kw in ["제3자배정대상자", "배정대상자", "성명(법인명)", "출자자"]):
                for c in range(C):
                    cell_norm = _norm(str(arr[r][c]))
                    if any(kw in cell_norm for kw in ["성명", "법인명", "대상자", "출자자", "투자자"]) and "관계" not in cell_norm and "주식" not in cell_norm:
                        target_col = c
                        start_row = r
                        break
            if target_col != -1:
                break

        if target_col != -1:
            for rr in range(start_row + 1, R):
                val = str(arr[rr][target_col]).strip()
                val_norm = _norm(val)
                if "합계" in val_norm or "소계" in val_norm or "기타투자" in val_norm or val_norm.startswith("주1)"):
                    break
                chunks = [x.strip() for x in val.split('\n')]
                for chunk in chunks:
                    if is_valid_name(chunk) and chunk not in investors:
                        investors.append(chunk)

            if investors:
                return ", ".join(investors)

    if corr_after:
        for k, v in corr_after.items():
            if any(_norm(kw) in _norm(k) for kw in ["제3자배정대상자", "배정대상자", "투자자", "성명(법인명)"]):
                chunks = re.split(r'[\n,;/]', v)
                valid_chunks = []
                for chunk in chunks:
                    chunk = chunk.strip()
                    if is_valid_name(chunk) and chunk not in valid_chunks:
                        valid_chunks.append(chunk)
                if valid_chunks:
                    return ", ".join(valid_chunks)

    val = scan_label_value_preferring_correction(dfs, ["제3자배정대상자", "배정대상자", "투자자", "성명(법인명)"], corr_after)
    if val:
        chunks = re.split(r'[\n,;/]', val)
        valid_chunks = []
        for chunk in chunks:
            chunk = chunk.strip()
            if is_valid_name(chunk) and chunk not in valid_chunks:
                valid_chunks.append(chunk)
        if valid_chunks:
            return ", ".join(valid_chunks)

    return ""


# ==========================================================
# Bond-specific helpers
# ==========================================================
# [제목 기반 채권 구분 코드]
# - CB / EB / BW 반환
def bond_type_code(title: str) -> str:
    t = title.replace(" ", "")
    if "전환사채권발행결정" in t:
        return "CB"
    if "교환사채권발행결정" in t:
        return "EB"
    if "신주인수권부사채권발행결정" in t:
        return "BW"
    return ""


# [제목 기반 발행상품명]
# - 전환사채 / 교환사채 / 신주인수권부사채 반환
def bond_type_product_name(title: str) -> str:
    t = title.replace(" ", "")
    if "전환사채권발행결정" in t:
        return "전환사채"
    if "교환사채권발행결정" in t:
        return "교환사채"
    if "신주인수권부사채권발행결정" in t:
        return "신주인수권부사채"
    return ""


# [주식연계채권 발행상품 추출]
# - "사채의 종류" 같은 표 라벨에서 실제 상품명 추출
# - 못 찾으면 제목 기반 fallback
def extract_product_type_bond(dfs: List[pd.DataFrame], corr_after: Dict[str, str], title: str) -> str:
    """
    주식연계채권 발행상품 추출
    우선순위:
    1) 정정공시의 corr_after에서 '1. 사채의 종류' 계열 값
    2) 실제 표에서 '1. 사채의 종류' 라벨의 오른쪽 / 아래 값
    3) 같은 행 전체 문자열에서 추출
    4) 마지막 fallback으로 제목 기반 전환사채/교환사채/BW
    """

    primary_labels = [
        "1. 사채의 종류",
        "1.사채의종류",
        "사채의 종류",
        "사채의종류",
        "채권의 종류",
        "채권의종류",
        "증권의 종류",
        "증권의종류",
    ]

    fallback_labels = [
        "사채종류",
        "발행상품",
        "종류",
    ]

    def clean_candidate(text: str) -> str:
        if not text:
            return ""

        t = normalize_text(text)
        if not t:
            return ""

        t = re.sub(r'^\s*1\.\s*', '', t)
        t = re.sub(r'^(사채|채권|증권)의\s*종류\s*[:：]?\s*', '', t)
        t = re.sub(r'^사채종류\s*[:：]?\s*', '', t)
        t = re.sub(r'^발행상품\s*[:：]?\s*', '', t)
        t = t.strip()

        bad_exact = {
            "", "-", ".", "해당사항없음", "해당 없음", "없음", "해당사항 없음"
        }
        if t in bad_exact:
            return ""

        patterns = [
            r'((?:제\s*\d+\s*회\s*)?[^|,;/]{0,80}?(?:전환사채|교환사채|신주인수권부사채))',
            r'((?:무기명식|기명식|이권부|무보증|보증|사모|공모|비분리형|분리형|사모식)?[^|,;/]{0,80}?(?:전환사채|교환사채|신주인수권부사채))',
        ]
        for pat in patterns:
            m = re.search(pat, t)
            if m:
                val = normalize_text(m.group(1))
                if 3 <= len(val) <= 100:
                    return val

        for name in ["전환사채", "교환사채", "신주인수권부사채"]:
            if name in t:
                return t

        return ""

    if corr_after:
        for k, v in corr_after.items():
            k_clean = _clean_label(k)
            if any(_clean_label(lb) == k_clean for lb in primary_labels):
                cleaned = clean_candidate(v)
                if cleaned:
                    return cleaned

        for k, v in corr_after.items():
            k_norm = _norm(k)
            if any(_norm(lb) in k_norm for lb in primary_labels + fallback_labels):
                cleaned = clean_candidate(v)
                if cleaned:
                    return cleaned

    for df in dfs:
        try:
            arr = df.fillna("").astype(str).values
        except Exception:
            continue

        R, C = arr.shape

        for r in range(R):
            for c in range(C):
                cell = normalize_text(arr[r][c])
                if not cell:
                    continue

                cell_clean = _clean_label(cell)
                is_primary = any(_clean_label(lb) == cell_clean for lb in primary_labels)
                is_fallback = any(_clean_label(lb) == cell_clean for lb in fallback_labels)

                if not (is_primary or is_fallback):
                    continue

                for cc in range(c + 1, min(C, c + 5)):
                    candidate = clean_candidate(arr[r][cc])
                    if candidate:
                        return candidate

                for rr in range(r + 1, min(R, r + 4)):
                    candidate = clean_candidate(arr[rr][c])
                    if candidate:
                        return candidate

                for rr in range(r + 1, min(R, r + 4)):
                    for cc in range(c + 1, min(C, c + 4)):
                        candidate = clean_candidate(arr[rr][cc])
                        if candidate:
                            return candidate

                row_text = " ".join([normalize_text(x) for x in arr[r].tolist() if normalize_text(x)])
                candidate = clean_candidate(row_text)
                if candidate:
                    return candidate

    for df in dfs:
        try:
            arr = df.fillna("").astype(str).values
        except Exception:
            continue

        for r in range(min(12, arr.shape[0])):
            row_text = " ".join([normalize_text(x) for x in arr[r].tolist() if normalize_text(x)])
            if "사채의 종류" in row_text or "사채종류" in row_text:
                candidate = clean_candidate(row_text)
                if candidate:
                    return candidate

    return bond_type_product_name(title)


# [주식연계채권 납입일 추출]
# - '납입일'이 있는 줄 주변에서 날짜 추출
def extract_payment_date_bond(dfs: List[pd.DataFrame], corr_after: Dict[str, str]) -> str:
    if corr_after:
        for k, v in corr_after.items():
            if "납입" in k:
                pay_idx = v.find("납입") if "납입" in v else 0
                dates = re.findall(r'\d{4}[-년\.\s]+\d{1,2}[-월\.\s]+\d{1,2}', v[pay_idx:])
                if dates:
                    return _format_date(dates[-1])

    for df in dfs:
        try:
            arr = df.astype(str).values
        except Exception:
            continue
        R, C = arr.shape
        for r in range(R):
            row_str = " ".join([str(x) for x in arr[r] if str(x).lower() != 'nan'])
            if "납입일" in _norm(row_str) or "납입기일" in _norm(row_str):
                pay_idx = row_str.find("납입")
                dates = re.findall(r'\d{4}[-년\.\s]+\d{1,2}[-월\.\s]+\d{1,2}', row_str[pay_idx:])
                if dates:
                    return _format_date(dates[-1])
                dates = re.findall(r'\d{4}[-년\.\s]+\d{1,2}[-월\.\s]+\d{1,2}', row_str)
                if dates:
                    return _format_date(dates[-1])
                if r + 1 < R:
                    next_row = " ".join([str(x) for x in arr[r + 1] if str(x).lower() != 'nan'])
                    dates = re.findall(r'\d{4}[-년\.\s]+\d{1,2}[-월\.\s]+\d{1,2}', next_row)
                    if dates:
                        return _format_date(dates[-1])
    return ""


# [주식연계채권 자금용도 추출]
# - 금액이 실제 있는 자금항목만 남김
# - 타법인증권취득자금은 표준명으로 통일
def extract_fund_usage_bond(dfs: List[pd.DataFrame], corr_after: Dict[str, str]) -> str:
    target_keys = ["시설자금", "영업양수자금", "운영자금", "채무상환자금", "타법인 증권 취득자금", "타법인증권취득자금", "기타자금"]
    for df in dfs:
        found_funds = {}
        try:
            arr = df.astype(str).values
        except Exception:
            continue
        R, C = arr.shape
        for r in range(R):
            for c in range(C):
                cell_norm = _norm(str(arr[r][c]))
                for tk in target_keys:
                    if _norm(tk) in cell_norm:
                        amt = 0
                        for cc in range(c + 1, min(C, c + 3)):
                            a = _max_int_in_text(arr[r][cc])
                            if a and a > 100:
                                amt = max(amt, a)
                        if amt == 0 and r + 1 < R:
                            a = _max_int_in_text(arr[r + 1][c])
                            if a and a > 100:
                                amt = max(amt, a)
                        if amt > 0:
                            std_key = "타법인 증권 취득자금" if "타법인" in tk else tk
                            found_funds[std_key] = max(found_funds.get(std_key, 0), amt)
        if found_funds:
            result = [k for k, v in sorted(found_funds.items(), key=lambda x: x[1], reverse=True)]
            return _single_line(", ".join(result))

    if corr_after:
        found_funds = {}
        for k, v in corr_after.items():
            for tk in target_keys:
                if _norm(tk) in _norm(k):
                    amt = _max_int_in_text(v)
                    if amt and amt > 100:
                        std_key = "타법인 증권 취득자금" if "타법인" in tk else tk
                        found_funds[std_key] = amt
        if found_funds:
            result = [k for k, v in sorted(found_funds.items(), key=lambda x: x[1], reverse=True)]
            return _single_line(", ".join(result))

    return _single_line(scan_label_value_preferring_correction(dfs, ["조달자금의 구체적 사용 목적", "자금용도"], corr_after))


# [주식연계채권 투자자 추출]
# - 대상자명 / 법인명 / 인수인 등에서 투자자 이름 수집
# - 펀드명/조합명도 어느 정도 허용
def extract_investors_bond(dfs: List[pd.DataFrame], corr_after: Dict[str, str]) -> str:
    investors = []
    blacklist = [
        "관계", "배정", "비고", "합계", "소계", "해당사항", "내역", "금액", "주식수",
        "단위", "이사회", "총계", "주소", "근거", "선정경위", "거래내역", "목적",
        "취득내역", "잔고", "출자자수", "주요사항"
    ]

    def clean_investor_name(sn):
        if not sn or str(sn).lower() == 'nan':
            return ""
        s = str(sn).replace('\n', ' ').replace('\r', '').strip()
        s = re.sub(r'\([^)]*신탁업자[^)]*\)', '', s)
        s = re.sub(r'\([^)]*본건펀드[^)]*\)', '', s)
        s = re.sub(r'\([^)]*전문투자자[^)]*\)', '', s)
        s = re.sub(r'\([^)]*손익차등[^)]*\)', '', s)
        s = re.sub(r'주\s*\d+\)', '', s)
        return re.sub(r'\s+', ' ', s).strip()

    def is_valid_investor_name(sn):
        if not sn:
            return False
        sn_clean = sn.replace(" ", "")
        if not (2 <= len(sn_clean) <= 50):
            return False
        if re.fullmatch(r'[\d,\.\s\-]+', sn_clean):
            return False
        sn_norm = _norm(sn_clean)
        for bw in blacklist:
            if bw in sn_norm:
                return False
        return True

    target_col_kws = ["대상자명", "대상사명", "성명", "법인명", "인수인", "투자기구", "투자업자", "발행대상", "투자자"]

    for df in dfs:
        try:
            arr = df.astype(str).values
        except Exception:
            continue
        R, C = arr.shape
        found_cols = []
        start_row = 1

        for r in range(min(5, R)):
            for c in range(C):
                cell_v = _norm(arr[r][c])
                if any(kw in cell_v for kw in target_col_kws):
                    if "최대주주" in cell_v or "대표이사" in cell_v:
                        continue
                    found_cols.append(c)
            if found_cols:
                start_row = r + 1
                break

        for col_idx in found_cols:
            for rr in range(start_row, R):
                cell_data = str(arr[rr][col_idx])
                valid_found = False
                for line in cell_data.split('\n'):
                    c_line = clean_investor_name(line)
                    if is_valid_investor_name(c_line):
                        if c_line not in investors:
                            investors.append(c_line)
                        valid_found = True

                if not valid_found:
                    c_whole = clean_investor_name(cell_data.replace('\n', ' '))
                    if is_valid_investor_name(c_whole) and c_whole not in investors:
                        investors.append(c_whole)

    if not investors and corr_after:
        for k, v in corr_after.items():
            if any(_norm(kw) in _norm(k) for kw in ["발행대상자", "배정대상자", "투자자", "인수인", "대상자"]):
                for chunk in re.split(r'[,;/]', v.replace('\n', ',')):
                    c_name = clean_investor_name(chunk)
                    if is_valid_investor_name(c_name) and c_name not in investors:
                        investors.append(c_name)

    if not investors:
        val = scan_label_value_preferring_correction(dfs, ["발행대상자", "배정대상자", "투자자", "성명(법인명)", "인수인"], corr_after)
        if val:
            for chunk in re.split(r'[,;/]', val.replace('\n', ',')):
                c_name = clean_investor_name(chunk)
                if is_valid_investor_name(c_name) and c_name not in investors:
                    investors.append(c_name)

    if not investors:
        for df in dfs:
            try:
                arr = df.astype(str).values
            except Exception:
                continue
            for r in range(arr.shape[0]):
                for c in range(arr.shape[1]):
                    cell_val = clean_investor_name(str(arr[r][c]).replace('\n', ' '))
                    if re.search(r'(투자조합|사모투자|펀드|파트너스|인베스트먼트|자산운용|증권)', cell_val):
                        if is_valid_investor_name(cell_val) and cell_val not in investors:
                            investors.append(cell_val)

    final_investors = []
    for inv in investors:
        if inv and inv not in final_investors:
            final_investors.append(inv)

    return _single_line(", ".join(final_investors[:15]))


# ==========================================================
# [주식연계채권 시트] Put / Call Option 섹션 본문 추출 전용
# - Put Option: 실제 본문 앵커 "본 사채의 사채권자는 ..."
# - Call Option: 실제 본문 앵커 "발행회사 또는 발행회사가 지정하는 자 ..."
# - Put 종료: "지급하여야 한다"
# - Call 종료: "매도하여야 한다"
# ==========================================================
def _option_corpus_from_tables(tables: List[pd.DataFrame]) -> str:
    lines = []
    for line in all_text_lines(tables):
        s = normalize_text(line)
        if s:
            lines.append(s)

    corpus = "\n".join(lines)
    corpus = corpus.replace("\xa0", " ")
    corpus = re.sub(r"[ \t]+", " ", corpus)
    corpus = re.sub(r"\n{2,}", "\n", corpus)
    return corpus.strip()


# [옵션 대섹션만 자르기]
# - 9-1. 옵션에 관한 사항 우선
# - 9-1이 요약만 있고 실제 본문이 22/23/24. 기타 투자판단에 참고할 사항에 있는 경우까지 포함
def _slice_block_from_heading(corpus: str, heading_patterns: List[str], end_patterns: List[str]) -> str:
    if not corpus:
        return ""

    start_hit = None
    for pat in heading_patterns:
        m = re.search(pat, corpus, flags=re.I)
        if m and (start_hit is None or m.start() < start_hit.start()):
            start_hit = m

    if not start_hit:
        return ""

    sub = corpus[start_hit.start():]
    end_idx = len(sub)

    for pat in end_patterns:
        m = re.search(pat, sub[20:], flags=re.I)
        if m:
            end_idx = min(end_idx, 20 + m.start())

    return sub[:end_idx].strip()


def _slice_option_major_section(corpus: str) -> str:
    if not corpus:
        return ""

    common_end_patterns = [
        r"\n\s*9\s*-\s*2\s*\.",
        r"\n\s*10\s*\.",
        r"\n\s*24\s*[\.\)]\s*정관에\s*정한\s*신주인수권의\s*내용",
        r"\n\s*25\s*[\.\)]",
        r"\n\s*【",
        r"\n\s*금융위원회\s*/\s*한국거래소\s*귀중",
        #r"\n\s*\d+\s*[\.\)]\s*[가-힣A-Za-z]",
    ]

    sections = []

    # 1순위: 9-1 옵션 섹션
    block_91 = _slice_block_from_heading(
        corpus,
        [
            r"9\s*-\s*1\s*\.\s*옵션에\s*관한\s*사항",
            r"9\s*-\s*1\s*\)\s*옵션에\s*관한\s*사항",
        ],
        common_end_patterns,
    )
    if block_91:
        sections.append(block_91)

    # 2순위: 실제 상세 본문이 자주 들어가는 22/23/24 기타 투자판단 섹션
    for no in ["22", "23", "24"]:
        block = _slice_block_from_heading(
            corpus,
            [
                rf"{no}\s*[\.\)]\s*기타\s*투자판단에\s*참고할\s*사항",
                rf"{no}\s*[\.\)]\s*기타\s*투자판단에\s*참고할사항",
            ],
            common_end_patterns,
        )
        if block and re.search(
            r"Put\s*Option|Call\s*Option|조기상환청구권|매도청구권|중도상환청구권|풋옵션|콜옵션",
            block,
            flags=re.I,
        ):
            sections.append(block)

    if not sections:
        return corpus

    dedup = []
    seen = set()
    for sec in sections:
        key = _norm(sec)
        if key in seen:
            continue
        seen.add(key)
        dedup.append(sec)

    return "\n".join(dedup).strip()


# [종료 문구 위치 탐색]
# - 여러 종료 패턴 중 가장 먼저 나오는 위치를 반환
def _find_earliest_end(text: str, end_patterns: List[str], start_pos: int = 0) -> int:
    end_positions = []
    for pat in end_patterns:
        m = re.search(pat, text[start_pos:], flags=re.I)
        if m:
            end_positions.append(start_pos + m.end())
    return min(end_positions) if end_positions else -1


# [다음 섹션 경계 위치 탐색]
def _find_earliest_boundary(text: str, boundary_patterns: List[str], start_pos: int = 0) -> int:
    hit_positions = []
    for pat in boundary_patterns:
        m = re.search(pat, text[start_pos:], flags=re.I)
        if m:
            hit_positions.append(start_pos + m.start())
    return min(hit_positions) if hit_positions else -1


# [옵션 추출 결과 후처리]
# - 파이프 구분자, 각주, 중복 공백 제거
def _cleanup_option_result(text: str) -> str:
    if not text:
        return ""

    text = re.sub(r"\s*\|\s*", " ", text)
    text = re.sub(r"\(주\d+\)", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    text = re.sub(r"^(?:9\s*-\s*1\.\s*옵션에\s*관한\s*사항\s*)?", "", text, flags=re.I).strip()

    return text


# [옵션 본문 섹션 통추출]
# - Put/Call 섹션 제목부터 실제 종료문구까지 통째로 자름
# - 네가 말한 "original 버전"을 무조건 1순위(default)로 사용
def extract_option_section_from_tables(tables: List[pd.DataFrame], option_type: str) -> str:
    corpus = _option_corpus_from_tables(tables)
    if not corpus:
        return ""

    search_space = _slice_option_major_section(corpus)
    if not search_space:
        search_space = corpus

    option_key = (option_type or "").strip().lower()
    if option_key not in ("put", "call"):
        return ""

    primary_rules = {
        "put": {
            "headers": [
                r"\[\s*조기상환청구권\s*\(\s*Put\s*Option\s*\)\s*에\s*관한\s*사항\s*\]",
                r"조기상환청구권\s*\(\s*Put\s*Option\s*\)\s*에\s*관한\s*사항",
            ],
            "anchors": [
                r"본\s*사채의\s*사채권자는",
                r"조기상환을\s*청구할\s*수\s*있",
            ],
            "end_patterns": [
                r"지급하여야\s*한다\.?",
            ],
        },
        "call": {
            "headers": [
                r"\[\s*매도청구권\s*\(\s*Call\s*Option\s*\)\s*에\s*관한\s*사항\s*\]",
                r"매도청구권\s*\(\s*Call\s*Option\s*\)\s*에\s*관한\s*사항",
            ],
            "anchors": [
                r"발행회사\s*또는\s*발행회사가\s*지정하는\s*자",
                r"매도청구권을\s*행사",
            ],
            "end_patterns": [
                r"매도하여야\s*한다\.?",
            ],
        },
    }

    fallback_rules = {
        "put": {
            "headers": [
                r"\[\s*Put\s*Option\s*에\s*관한\s*사항\s*\]",
                r"Put\s*Option\s*에\s*관한\s*사항",
                r"가\.\s*조기상환청구권\s*\(\s*Put\s*Option\s*\)\s*에\s*관한\s*사항",
                r"1\.\s*사채권자\s*조기상환청구권\s*\(\s*Put\s*Option\s*\)",
                r"1\)\s*사채의\s*조기상환청구권\s*\(\s*Put\s*Option\s*\)\s*에\s*관한\s*사항",
                r"사채의\s*만기전\s*조기상환청구권\s*\(\s*Put[\-\s]*Option\s*\)",
                r"조기상환청구권\s*\(\s*PUT\s*OPTION\s*\)\s*에\s*관한\s*사항",
            ],
            "anchors": [
                r"사채권자는",
                r"인수인은",
                r"조기상환청구권",
                r"조기상환을\s*청구",
                r"중도상환을\s*요구할\s*수\s*없다",
            ],
            "end_patterns": [
                r"지급하여야\s*한다\.?",
                r"상환하고\s*조기상환지급기일\s*이후의\s*이자는\s*계산하지\s*아니한다\.?",
                r"상환하고\s*조기상환일\s*이후의\s*이자는\s*계산하지\s*아니한다\.?",
                r"요구할\s*수\s*없다\.?",
            ],
        },
        "call": {
            "headers": [
                r"\[\s*Call\s*option\s*에\s*관한\s*사항\s*\]",
                r"Call\s*option\s*에\s*관한\s*사항",
                r"나\.\s*매도청구권\s*\(\s*Call\s*Option\s*\)\s*에\s*관한\s*사항",
                r"나\.\s*매도청구권\s*\(\s*Call\s*option\s*\)\s*에\s*관한\s*사항",
                r"\[\s*중도상환청구권\s*\(\s*Call\s*Option\s*\)\s*에\s*관한\s*사항\s*\]",
                r"중도상환청구권\s*\(\s*Call\s*Option\s*\)\s*에\s*관한\s*사항",
                r"2\)\s*발행회사의\s*중도상환청구권\s*\(\s*Call\s*Option\s*\)\s*에\s*관한\s*사항",
                r"3\.\s*발행회사\s*매도청구권\s*\(\s*Call\s*Option\s*\)",
                r"콜옵션에\s*관한\s*사항",
            ],
            "anchors": [
                r"발행회사",
                r"발행회사가\s*지정하는\s*자",
                r"매도청구권",
                r"중도상환청구권",
                r"매수인에게\s*매도하여야\s*한다",
            ],
            "end_patterns": [
                r"매도하여야\s*한다\.?",
                r"행사를\s*보장하지\s*않는다\.?",
                r"중도상환지급기일\s*이후의\s*이자는\s*계산하지\s*아니한다\.?",
            ],
        },
    }

    if option_key == "put":
        opposite_patterns = [
            r"\[\s*매도청구권\s*\(\s*Call\s*Option\s*\)\s*에\s*관한\s*사항\s*\]",
            r"매도청구권\s*\(\s*Call\s*Option\s*\)\s*에\s*관한\s*사항",
            r"중도상환청구권\s*\(\s*Call\s*Option\s*\)\s*에\s*관한\s*사항",
            r"콜옵션에\s*관한\s*사항",
            r"Call\s*Option",
        ]
    else:
        opposite_patterns = [
            r"\[\s*조기상환청구권\s*\(\s*Put\s*Option\s*\)\s*에\s*관한\s*사항\s*\]",
            r"조기상환청구권\s*\(\s*Put\s*Option\s*\)\s*에\s*관한\s*사항",
            r"가\.\s*조기상환청구권\s*\(\s*Put\s*Option\s*\)\s*에\s*관한\s*사항",
            r"Put\s*Option",
            r"풋옵션",
        ]

    common_boundary_patterns = [
        r"\n\s*【[^】]+】",
        r"\n\s*\[[^\]]+\]",
        #r"\n\s*\d+\s*[\.\)]\s*[가-힣A-Za-z]",
        #r"\n\s*[가-하]\.\s*[가-힣A-Za-z]",
        r"【\s*특정인에\s*대한\s*대상자별\s*사채발행내역\s*】",
        r"\[\s*특정인에\s*대한\s*대상자별\s*사채발행내역\s*\]",
        r"【\s*사채발행\s*대상\s*법인\s*또는\s*단체가\s*권리\s*행사로\s*주주가\s*되는\s*경우\s*】",
        r"\[\s*사채발행\s*대상\s*법인\s*또는\s*단체가\s*권리\s*행사로\s*주주가\s*되는\s*경우\s*\]",
        r"【\s*조달자금의\s*구체적\s*사용\s*목적\s*】",
        r"\[\s*조달자금의\s*구체적\s*사용\s*목적\s*\]",
        r"\n\s*금융위원회\s*/\s*한국거래소\s*귀중",
    ]

    noise_patterns = [
        r"구분\s*조기상환\s*청구기간",
        r"구분\s*매도청구권\s*행사기간",
        r"From\s*To",
        r"시작일\s*종료일",
        r"\(주\d+\)",
    ]

    def _first_anchor_pos(text: str, patterns: List[str], limit: int) -> int:
        best = -1
        for pat in patterns:
            m = re.search(pat, text[:limit], flags=re.I)
            if m:
                if best == -1 or m.start() < best:
                    best = m.start()
        return best

    def _collect_candidates(rule_pack: Dict[str, List[str]], base_score: int, strict_anchor: bool) -> List[Tuple[int, str]]:
        out = []

        for hpat in rule_pack["headers"]:
            for hm in re.finditer(hpat, search_space, flags=re.I):
                start = hm.start()
                tail = search_space[start:start + 5000]

                head_250 = tail[:250]
                head_500 = tail[:500]

                anchor_250 = _first_anchor_pos(head_250, rule_pack["anchors"], 250)
                anchor_500 = _first_anchor_pos(head_500, rule_pack["anchors"], 500)

                if strict_anchor and anchor_250 == -1:
                    continue

                score = base_score

                if anchor_250 != -1:
                    score += 300
                    anchor_pos = anchor_250
                elif anchor_500 != -1:
                    score += 120
                    anchor_pos = anchor_500
                else:
                    score -= 220
                    anchor_pos = 0

                for npat in noise_patterns:
                    if re.search(npat, head_250, flags=re.I):
                        score -= 90

                end_pos = _find_earliest_end(tail, rule_pack["end_patterns"], start_pos=max(anchor_pos, 0))
                if end_pos != -1:
                    score += 60
                else:
                    boundary_pos = _find_earliest_boundary(
                        tail,
                        opposite_patterns + common_boundary_patterns,
                        start_pos=max(anchor_pos + 20, 20),
                    )
                    if boundary_pos != -1:
                        end_pos = boundary_pos
                        score += 20
                    else:
                        end_pos = min(len(tail), 1800)

                candidate = tail[:end_pos].strip()
                candidate = _cleanup_option_result(candidate)

                if len(candidate) < 40:
                    continue

                cand_norm = _norm(candidate)

                if option_key == "put":
                    if "매도청구권" in cand_norm and "조기상환청구권" not in cand_norm and "putoption" not in cand_norm:
                        score -= 500
                else:
                    if "조기상환청구권" in cand_norm and "매도청구권" not in cand_norm and "calloption" not in cand_norm:
                        score -= 500

                if len(candidate) > 1200:
                    score -= (len(candidate) - 1200) // 8

                out.append((score, candidate))

        return out

    candidates = []
    candidates.extend(_collect_candidates(primary_rules[option_key], base_score=1000, strict_anchor=True))
    candidates.extend(_collect_candidates(fallback_rules[option_key], base_score=700, strict_anchor=False))

    if not candidates:
        return ""

    candidates.sort(key=lambda x: (x[0], len(x[1])), reverse=True)
    best = candidates[0][1]
    return _cleanup_option_result(best)


# [옵션 본문 상세 추출 fallback]
# - 위 '섹션 통추출' 실패 시 보조적으로 사용하는 fallback
# - 키워드 주변 window를 잡아서 본문성 있는 부분만 최대한 남김
def extract_option_details_from_tables(tables: List[pd.DataFrame], option_type: str) -> str:
    if option_type == "put":
        my_kws = ["조기상환청구권", "Put Option", "풋옵션", "조기상환권", "사채권자의 조기상환청구권"]
        opp_kws = ["매도청구권", "Call Option", "콜옵션", "중도상환청구권"]
        anchor_regex = r'(본\s*사채의\s*사채권자는|본\s*사채의\s*인수인은|사채권자는|인수인은|투자자는)'
    else:
        my_kws = ["매도청구권", "Call Option", "콜옵션", "중도상환청구권", "발행회사의 매도청구권"]
        opp_kws = ["조기상환청구권", "Put Option", "풋옵션", "조기상환권"]
        anchor_regex = r'(발행회사\s*또는\s*발행회사가\s*지정하는\s*자(?:\([^)]*\))?(?:는|가)?|발행회사(?:는|가)|회사는\s*만기\s*전|본\s*사채는\s*만기\s*전)'

    corpus = " ".join(all_text_lines(tables))
    corpus = normalize_text(corpus)
    if not corpus:
        return ""

    candidates = []
    for kw in my_kws:
        for m in re.finditer(re.escape(kw), corpus, re.IGNORECASE):
            idx = m.start()
            window = corpus[max(0, idx - 50): idx + 1200]

            score = 0
            if option_type == "put":
                if re.search(r'사채권자|인수인|투자자', window):
                    score += 50
                if re.search(r'청구할\s*수\s*있다|조기상환을\s*청구', window):
                    score += 50
                if "의무보유" in window:
                    score -= 200
            else:
                if re.search(r'발행회사|매수|매도청구', window):
                    score += 50
                if re.search(r'매수할\s*수\s*있다|매도를\s*청구', window):
                    score += 50
                if "의무보유" in window and "사채권자" in window:
                    score -= 200

            if "매매일" in window and "상환율" in window:
                score -= 300
            if "from" in window.lower() and "to" in window.lower():
                score -= 300
            if "성명 및 관계" in window:
                score -= 300

            candidates.append((score, window))

    if not candidates:
        return ""

    candidates.sort(key=lambda x: x[0], reverse=True)
    best_score, best_window = candidates[0]
    if best_score < 0:
        return ""

    m = re.search(anchor_regex, best_window)
    if m and m.start() < 150:
        result = best_window[m.start():]
    else:
        result = best_window
        for _ in range(3):
            result = re.sub(r'^([\[【<\(]?\s*[①-⑩\d가-힣a-zA-Z][\.\)\]】>]\s*)+', '', result)
            prefix_pattern = r'^(?:본\s*사채의\s*|발행회사의\s*)?(?:조기상환청구권|매도청구권|중도상환청구권|콜옵션|풋옵션|Put\s*Option|Call\s*Option|PUT\s*OPTION|CALL\s*OPTION)[^가-힣]*?(?:에\s*관한\s*사항|청구권자|행사|부여|비율|한도)?\s*[:\]\-\>]*\s*'
            result = re.sub(prefix_pattern, '', result, flags=re.IGNORECASE)
            result = re.sub(r'^[:\-\]\s]+', '', result)

    stop_kws = opp_kws + [
        "기타사항", "합병 관련 사항", "청약일", "납입일",
        "기타 투자판단", "의무보유", "특정인", "발행결정 전후"
    ]

    cut_idx = len(result)
    lower_result = result.lower()
    for stop_kw in stop_kws:
        s_idx = lower_result.find(stop_kw.lower())
        if s_idx > 20 and s_idx < cut_idx:
            cut_idx = s_idx

    result = normalize_text(result[:cut_idx])

    if len(result) < 5:
        return ""

    return result[:300] + ("..." if len(result) > 300 else "")


# [Call Option 본문에서 Call 비율 / YTC 추출]
# - 표 라벨에서 못 찾았을 때 Call Option 본문에서 % 값을 재추출
# - 명시 패턴 우선, 없으면 퍼센트 후보를 heuristic으로 분류
def extract_call_ratio_and_ytc_from_text(text: str) -> Tuple[str, str]:
    text = normalize_text(text)
    if not text:
        return "", ""

    ratio = ""
    ytc = ""

    percent_matches = re.findall(r"-?\d+(?:\.\d+)?\s*%", text)
    percent_vals = []
    for p in percent_matches:
        val = parse_float_like(p)
        if val is not None:
            percent_vals.append((clean_percent(p), val))

    explicit_ratio_patterns = [
        r"(?:콜옵션\s*행사비율|매도청구권\s*행사비율|Call\s*비율|콜옵션\s*비율|매도청구권\s*비율|권면총액\s*대비\s*비율|행사비율)[^0-9\-]*(-?\d+(?:\.\d+)?)\s*%",
        r"(-?\d+(?:\.\d+)?)\s*%\s*(?:에\s*해당하는)?\s*(?:콜옵션|매도청구권)",
    ]
    for pat in explicit_ratio_patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            ratio = f"{m.group(1)}%"
            break

    explicit_ytc_patterns = [
        r"(?:YTC|Yield\s*To\s*Call|조기상환수익률|조기상환이율|연복리수익률)[^0-9\-]*(-?\d+(?:\.\d+)?)\s*%",
        r"(-?\d+(?:\.\d+)?)\s*%\s*(?:의\s*)?(?:조기상환수익률|연복리수익률|YTC)",
    ]
    for pat in explicit_ytc_patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            ytc = f"{m.group(1)}%"
            break

    if not ratio:
        for raw, val in percent_vals:
            if 0 < val <= 100:
                ratio = raw
                break

    if not ytc:
        for raw, val in percent_vals:
            if raw == ratio:
                continue
            if -50 <= val <= 50:
                ytc = raw
                break

    return ratio, ytc


# [정규식 패턴 중 가장 먼저 나오는 매치 찾기]
def _find_first_regex_match(text: str, patterns: List[str], start_pos: int = 0):
    best = None
    for pat in patterns:
        m = re.search(pat, text[start_pos:], flags=re.I)
        if not m:
            continue
        abs_start = start_pos + m.start()
        abs_end = start_pos + m.end()
        if best is None or abs_start < best[0]:
            best = (abs_start, abs_end, pat)
    return best


# [앵커 기준 옵션 블록 추출]
# - put: "Put Option" / "풋옵션"이 보이는 지점부터 "Call Option" / "콜옵션" 직전까지
# - call: "Call Option" / "콜옵션"이 보이는 지점부터 다음 큰 섹션 전까지
# - 기존 로직을 대체하는 게 아니라 마지막 fallback용으로 추가
def extract_option_block_by_anchor_range(tables: List[pd.DataFrame], option_type: str) -> str:
    corpus = _option_corpus_from_tables(tables)
    if not corpus:
        return ""

    corpus = _slice_option_major_section(corpus)
    if not corpus:
        return ""

    if option_type == "put":
        start_patterns = [
            r"조기상환청구권\s*\(\s*Put\s*Option\s*\)\s*에\s*관한\s*사항",
            r"조기상환청구권\s*\(\s*PUT\s*OPTION\s*\)\s*에\s*관한\s*사항",
            r"\[\s*조기상환청구권\s*\(\s*Put\s*Option\s*\)\s*에\s*관한\s*사항\s*\]",
            r"\[\s*조기상환청구권\s*\(\s*PUT\s*OPTION\s*\)\s*에\s*관한\s*사항\s*\]",
            r"Put\s*Option",
            r"PUT\s*OPTION",
            r"풋옵션",
        ]
        stop_patterns = [
            r"Call\s*Option",
            r"CALL\s*OPTION",
            r"콜옵션",
        ]
    else:
        start_patterns = [
            r"매도청구권\s*\(\s*Call\s*Option\s*\)\s*에\s*관한\s*사항",
            r"매도청구권\s*\(\s*CALL\s*OPTION\s*\)\s*에\s*관한\s*사항",
            r"\[\s*매도청구권\s*\(\s*Call\s*Option\s*\)\s*에\s*관한\s*사항\s*\]",
            r"\[\s*매도청구권\s*\(\s*CALL\s*OPTION\s*\)\s*에\s*관한\s*사항\s*\]",
            r"Call\s*Option",
            r"CALL\s*OPTION",
            r"콜옵션",
        ]
        stop_patterns = [
            #r"\n\s*\d+\s*-\s*\d+\s*\.",
            #r"\n\s*\d+\s*[\.\)]\s*[가-힣A-Za-z]",
            #r"\n\s*[가-하]\.\s*[가-힣A-Za-z]",
            r"\n\s*기타\s*투자판단에\s*참고할\s*사항",
            r"\n\s*기타사항",
            r"\n\s*합병\s*관련\s*사항",
            r"\n\s*청약일",
            r"\n\s*납입일",
            r"\n\s*【",
            r"\n\s*금융위원회\s*/\s*한국거래소\s*귀중",
        ]

    start_hit = _find_first_regex_match(corpus, start_patterns)
    if not start_hit:
        return ""

    # 매치 지점이 아니라 "그 줄의 시작"부터 가져오도록 보정
    start_pos = corpus.rfind("\n", 0, start_hit[0]) + 1
    sub = corpus[start_pos:]

    # start 이후부터만 stop 찾기
    local_anchor_pos = start_hit[0] - start_pos
    search_from = min(len(sub), max(0, local_anchor_pos + 1))

    end_pos = len(sub)
    stop_hit = _find_first_regex_match(sub, stop_patterns, start_pos=search_from)

    if stop_hit:
        stop_line_start = sub.rfind("\n", 0, stop_hit[0])
        if stop_line_start > 0:
            end_pos = stop_line_start
        else:
            end_pos = stop_hit[0]

    result = sub[:end_pos].strip()
    result = _cleanup_option_result(result)

    # 너무 짧으면 의미 없는 값으로 판단
    if len(result) < 10:
        return ""

    return result


# [옵션 후보 중 가장 좋은 값 선택]
# - 기존 primary / 기존 fallback / 새 anchor fallback 중에서
#   가장 본문성이 높은 값을 고른다
def _score_option_text(text: str, option_type: str) -> int:
    s = normalize_text(text)
    if not s:
        return -10**9

    n = _norm(s)
    score = min(len(s), 700)

    noise_kws = [
        "구분조기상환청구기간",
        "구분매도청구권행사기간",
        "fromto",
        "시작일종료일",
        "정정전",
        "정정후",
        "항목",
        "변경사유",
        "성명및관계",
    ]
    if any(k in n for k in noise_kws):
        score -= 300

    if option_type == "put":
        if "putoption" in n or "풋옵션" in n:
            score += 180
        if "조기상환청구권" in n or "사채권자" in n or "조기상환" in n or "청구" in n:
            score += 120
        if ("calloption" in n or "콜옵션" in n) and not ("putoption" in n or "풋옵션" in n):
            score -= 500

    else:
        if "calloption" in n or "콜옵션" in n:
            score += 180
        if "매도청구권" in n or "발행회사" in n or "지정하는자" in n or "매도" in n:
            score += 120
        if ("putoption" in n or "풋옵션" in n) and not ("calloption" in n or "콜옵션" in n):
            score -= 500

    if len(s) < 20:
        score -= 150

    return score


def choose_best_option_value(option_type: str, *candidates: str) -> str:
    best = ""
    best_score = -10**9
    seen = set()

    for cand in candidates:
        cleaned = _cleanup_option_result(cand)
        cleaned = normalize_text(cleaned)
        if not cleaned:
            continue

        dedup_key = _norm(cleaned)
        if dedup_key in seen:
            continue
        seen.add(dedup_key)

        score = _score_option_text(cleaned, option_type)
        if score > best_score:
            best_score = score
            best = cleaned

    return best


# [옵션 1차 추출]
# - "원래 방식" 우선:
#   1) corr_after / 라벨-값 구조에서 직접 추출
#   2) 표에서 해당 라벨 주변 값 추출
# - 값이 없을 때만 섹션 본문 파싱으로 fallback 하도록 설계
def extract_option_value_primary(
    dfs: List[pd.DataFrame],
    corr_after: Dict[str, str],
    option_type: str
) -> str:
    if option_type == "put":
        label_candidates = [
            "조기상환청구권(Put Option)에 관한 사항",
            "조기상환청구권 (Put Option)에 관한 사항",
            "가. 조기상환청구권(Put Option)에 관한 사항",
            "가. 조기상환청구권 (Put Option)에 관한 사항",
            "조기상환청구권에 관한 사항",
            "Put Option에 관한 사항",
            "조기상환청구권",
            "Put Option",
            "풋옵션",
            "사채권자 조기상환청구권(Put Option)",
            "사채권자 조기상환청구권 (Put Option)",
            "사채의 만기전 조기상환청구권(Put-Option)",
            "사채의 만기전 조기상환청구권 (Put-Option)",
            "만기전 조기상환청구권",
            "조기상환권",
        ]
        opposite_markers = ["매도청구권", "Call Option", "콜옵션"]
    else:
        label_candidates = [
            "매도청구권(Call Option)에 관한 사항",
            "매도청구권 (Call Option)에 관한 사항",
            "나. 매도청구권(Call Option)에 관한 사항",
            "나. 매도청구권 (Call Option)에 관한 사항",
            "매도청구권에 관한 사항",
            "Call Option에 관한 사항",
            "매도청구권",
            "Call Option",
            "콜옵션",
            "중도상환청구권(Call Option)에 관한 사항",
            "중도상환청구권 (Call Option)에 관한 사항",
            "발행회사의 중도상환청구권(Call Option)에 관한 사항",
            "발행회사의 중도상환청구권 (Call Option)에 관한 사항",
            "발행회사 매도청구권(Call Option)",
            "발행회사 매도청구권 (Call Option)",
            "콜옵션에 관한 사항",
            "중도상환청구권",
        ]
        opposite_markers = ["조기상환청구권", "Put Option", "풋옵션"]

    def _first_nonempty_cell(row_vals) -> str:
        for x in row_vals:
            s = normalize_text(x)
            if s:
                return s
        return ""

    def _clean_option_text(text: str) -> str:
        s = normalize_text(text)
        if not s:
            return ""

        s = re.sub(r"^(가|나|다|라|마|바)\.\s*", "", s)

        if option_type == "put":
            s = re.sub(
                r"^조기상환청구권\s*\(\s*Put\s*Option\s*\)\s*에\s*관한\s*사항\s*[:：]?\s*",
                "",
                s,
                flags=re.I,
            )
            s = re.sub(
                r"^조기상환청구권\s*[:：]?\s*",
                "",
                s,
                flags=re.I,
            )
        else:
            s = re.sub(
                r"^매도청구권\s*\(\s*Call\s*Option\s*\)\s*에\s*관한\s*사항\s*[:：]?\s*",
                "",
                s,
                flags=re.I,
            )
            s = re.sub(
                r"^매도청구권\s*[:：]?\s*",
                "",
                s,
                flags=re.I,
            )

        s = re.sub(r"\s*\|\s*", " ", s)
        s = re.sub(r"\s+", " ", s).strip()
        return s

    def _looks_like_noise(text: str) -> bool:
        s = normalize_text(text)
        if not s:
            return True

        s_norm = _norm(s)

        if s in ("-", ".", ","):
            return True

        if re.fullmatch(r"[\d,\.\-%\s]+", s):
            return True

        noise_kws = [
            "구분조기상환청구기간",
            "구분매도청구권행사기간",
            "fromto",
            "시작일종료일",
            "정정전",
            "정정후",
            "항목",
            "변경사유",
        ]
        if any(k in s_norm for k in noise_kws):
            return True

        return False
        
    def _is_new_heading(text: str) -> bool:
        raw = normalize_text(text)
        if not raw:
            return False
    
        return bool(re.match(r"^\d+\s*-\s*\d+\s*[\.\)]", raw))

    direct = scan_label_value_preferring_correction(dfs, label_candidates, corr_after)
    direct = _clean_option_text(direct)
    if direct and not _looks_like_noise(direct):
        return direct

    if corr_after:
        for k, v in corr_after.items():
            k_norm = _norm(k)
            if any(_norm(lb) in k_norm for lb in label_candidates):
                cleaned = _clean_option_text(v)
                if cleaned and not _looks_like_noise(cleaned):
                    return cleaned

    for df in dfs:
        try:
            arr = df.fillna("").astype(str).values
        except Exception:
            continue

        R, C = arr.shape

        for r in range(R):
            row_vals = arr[r].tolist()
            row_join = " ".join([normalize_text(x) for x in row_vals if normalize_text(x)])
            row_norm = _norm(row_join)

            if not any(_norm(lb) in row_norm for lb in label_candidates):
                continue

            block_lines = []
            for rr in range(r, min(r + 8, R)):
                next_row_vals = arr[rr].tolist()
                next_row_join = " ".join([normalize_text(x) for x in next_row_vals if normalize_text(x)])
                next_first = _first_nonempty_cell(next_row_vals)

                if not next_row_join:
                    continue

                if rr > r:
                    if any(k.lower() in next_row_join.lower() for k in opposite_markers):
                        break

                    if _is_new_heading(next_first):
                        break

                block_lines.append(next_row_join)

            candidate = _clean_option_text(" ".join(block_lines))
            if candidate and not _looks_like_noise(candidate):
                return candidate

    return ""


# [기간형 날짜 2개 추출]
# - 전환청구기간 / 교환청구기간 / 권리행사기간 블록에서 시작일/종료일 추출
def extract_period_dates_from_tables(
    tables: List[pd.DataFrame],
    corr_after: Dict[str, str],
    section_keywords: List[str]
) -> Tuple[str, str]:
    date_pat = r"\d{4}[.\-/년]\s*\d{1,2}[.\-/월]\s*\d{1,2}일?"

    def _extract_dates(text: Any) -> List[str]:
        if not text:
            return []
        return re.findall(date_pat, normalize_text(text))

    def _neighbor_dates(arr, rr: int, cc: int) -> List[str]:
        R, C = arr.shape
        out = []

        for r2, c2 in [
            (rr, cc + 1), (rr, cc + 2),
            (rr + 1, cc), (rr + 1, cc + 1), (rr + 1, cc + 2),
            (rr + 2, cc), (rr + 2, cc + 1), (rr + 2, cc + 2),
        ]:
            if 0 <= r2 < R and 0 <= c2 < C:
                out.extend(_extract_dates(arr[r2][c2]))

        row_join = " ".join([normalize_text(x) for x in arr[rr].tolist() if normalize_text(x)])
        out.extend(_extract_dates(row_join))

        return out

    if corr_after:
        for k, v in corr_after.items():
            if any(_norm(p) in _norm(k) for p in section_keywords):
                dates = _extract_dates(v)
                if len(dates) >= 2:
                    return _format_date(dates[0]), _format_date(dates[-1])

                if "시작일" in str(v) or "종료일" in str(v):
                    start_date = ""
                    end_date = ""

                    start_m = re.search(r"시작일.*?(" + date_pat + r")", str(v))
                    end_m = re.search(r"종료일.*?(" + date_pat + r")", str(v))

                    if start_m:
                        start_date = _format_date(start_m.group(1))
                    if end_m:
                        end_date = _format_date(end_m.group(1))

                    if start_date and end_date:
                        return start_date, end_date

    for df in tables:
        try:
            arr = df.fillna("").astype(str).values
        except Exception:
            continue

        R, C = arr.shape

        for r in range(R):
            row_vals = [normalize_text(x) for x in arr[r].tolist()]
            row_join = " ".join([x for x in row_vals if x])
            row_norm = _norm(row_join)

            if not any(_norm(k) in row_norm for k in section_keywords):
                continue

            start_date = ""
            end_date = ""

            for rr in range(r, min(r + 4, R)):
                local_vals = [normalize_text(x) for x in arr[rr].tolist()]
                local_join = " ".join([x for x in local_vals if x])

                if "시작일" in local_join and not start_date:
                    dates = _extract_dates(local_join)
                    if dates:
                        start_date = _format_date(dates[-1])

                if "종료일" in local_join and not end_date:
                    dates = _extract_dates(local_join)
                    if dates:
                        end_date = _format_date(dates[-1])

            if start_date and end_date:
                return start_date, end_date

            for rr in range(r, min(r + 4, R)):
                for cc in range(C):
                    cell = normalize_text(arr[rr][cc])

                    if "시작일" in cell and not start_date:
                        dates = _neighbor_dates(arr, rr, cc)
                        if dates:
                            start_date = _format_date(dates[0])

                    if "종료일" in cell and not end_date:
                        dates = _neighbor_dates(arr, rr, cc)
                        if dates:
                            end_date = _format_date(dates[0])

            if start_date and end_date:
                return start_date, end_date

            block_text = []
            for rr in range(r, min(r + 4, R)):
                block_text.append(
                    " ".join([normalize_text(x) for x in arr[rr].tolist() if normalize_text(x)])
                )

            dates = _extract_dates(" ".join(block_text))
            if len(dates) >= 2:
                return _format_date(dates[0]), _format_date(dates[1])

    return "", ""

# ==========================================================
# Parsers
# ==========================================================
# [유상증자 레코드 파서]
# - RAW record 하나를 RIGHTS_HEADERS 구조 row로 변환
# - 누락 컬럼 / 의심 컬럼도 함께 반환
def parse_rights_record(rec: Dict[str, Any]):
    title = clean_title(rec["title"])
    tables = rec["tables"]
    corr_after = extract_correction_after_map(tables) if is_correction_title(title) else {}

    row = {h: "" for h in RIGHTS_HEADERS}
    missing = []
    suspicious = []

    row["회사명"] = first_nonempty(
        detect_company_from_tables(tables, corr_after),
        extract_company_name_from_title(title)
    )

    row["보고서명"] = title
    row["상장시장"] = first_nonempty(
        detect_market_from_title(title),
        detect_market_from_tables(tables, corr_after)
    )

    row["최초 이사회결의일"] = get_valid_date_by_labels(
        tables, ["최초 이사회결의일", "최초이사회결의일"], corr_after
    )
    row["이사회결의일"] = get_valid_date_by_labels(
        tables, ["이사회결의일", "이사회 결의일", "이사회결의일(결정일)", "결정일"], corr_after
    )
    if not row["최초 이사회결의일"]:
        row["최초 이사회결의일"] = row["이사회결의일"]

    row["납입일"] = get_valid_date_by_labels(
        tables, ["납입일", "납입기일", "청약기일 및 납입일", "신주의 납입기일", "신주납입기일"], corr_after
    )
    row["신주의 배당기산일"] = get_valid_date_by_labels(
        tables, ["신주의 배당기산일", "배당기산일"], corr_after
    )
    row["신주의 상장 예정일"] = get_valid_date_by_labels(
        tables, ["신주의 상장예정일", "신주의 상장 예정일", "상장예정일", "신주 상장예정일", "상장 예정일", "신주상장예정일"], corr_after
    )

    row["증자방식"] = scan_label_value_preferring_correction(
        tables, ["증자방식", "배정방법", "배정방식", "발행방법"], corr_after
    )

    issue_shares, issue_type = extract_issue_shares_and_type(tables, corr_after)
    if issue_shares:
        row["신규발행주식수"] = fmt_number(issue_shares)
    if issue_type:
        row["발행상품"] = issue_type

    prev_shares = get_prev_shares_sum(tables, corr_after)
    if not prev_shares:
        prev_shares = _max_int_in_text(scan_label_value_preferring_correction(
            tables,
            [
                "증자전발행주식총수", "기발행주식총수", "발행주식총수",
                "증자전 주식수", "증자전발행주식총수(보통주식)"
            ],
            corr_after
        )) or find_row_best_int(tables, ["증자전발행주식총수", "보통주식"], 50) or find_row_best_int(tables, ["발행주식총수", "보통주식"], 50)
    if prev_shares:
        row["증자전 주식수"] = fmt_number(prev_shares)

    price = get_price_by_exact_section(tables, corr_after)
    if not price:
        price = _max_int_in_text(scan_label_value_preferring_correction(
            tables,
            ["신주 발행가액", "신주발행가액", "예정발행가액", "확정발행가액", "발행가액", "1주당 확정발행가액"],
            corr_after
        )) or find_row_best_int(tables, ["신주발행가액", "보통주식"], 50) or find_row_best_int(tables, ["예정발행가액"], 50) or find_row_best_int(tables, ["발행가액", "원"], 50)
    if price and price > 50:
        row["확정발행가(원)"] = fmt_number(price)

    base_price = get_base_price_by_exact_section(tables, corr_after)
    if base_price and base_price > 50:
        row["기준주가"] = fmt_number(base_price)

    disc = _to_float(scan_label_value_preferring_correction(
        tables,
        ["할인율", "할증률", "할인율 또는 할증률", "할인(할증)율", "할인(할증률)", "발행가액 산정시 할인율"],
        corr_after
    ))
    if disc is None:
        disc = find_row_best_float(tables, ["할인율또는할증율"]) or find_row_best_float(tables, ["할인율"])
    if disc is not None:
        row["할인(할증률)"] = f"{disc:g}%"

    use_text, use_total = extract_fund_use_and_amount(tables, corr_after)
    row["자금용도"] = use_text
    row["투자자"] = extract_investors_rights(tables, corr_after)
    row["링크"] = rec["src_url"]
    row["접수번호"] = rec["acpt_no"]

    if not row["발행상품"] and row["신규발행주식수"]:
        row["발행상품"] = "보통주식"

    new_shares = parse_float_like(row["신규발행주식수"])
    price_val = parse_float_like(row["확정발행가(원)"])
    pre_shares = parse_float_like(row["증자전 주식수"])

    if new_shares is not None and price_val is not None:
        row["확정발행금액(억원)"] = fmt_eok_from_won(new_shares * price_val)

    if new_shares is not None and pre_shares not in (None, 0):
        row["증자비율"] = f"{(new_shares / pre_shares) * 100:.2f}%"

    for h in RIGHTS_HEADERS:
        if h in ["링크", "접수번호"]:
            continue
        if not normalize_text(row[h]):
            missing.append(h)

    if row["회사명"] in ["유", "코", "넥"]:
        suspicious.append("회사명")
    if price_val is not None and price_val <= 50:
        suspicious.append("확정발행가(원)")
    if base_price is not None and base_price <= 50:
        suspicious.append("기준주가")
    if row["투자자"] and any(x in row["투자자"] for x in ["관계", "지분", "합계", "소계", "정정", "출자자수", "명"]):
        suspicious.append("투자자")
    if row["보고서명"] and len(row["보고서명"]) < 5:
        suspicious.append("보고서명")

    return row, missing, suspicious


# [주식연계채권 레코드 파서]
# - RAW record 하나를 BOND_HEADERS 구조 row로 변환
# - CB / EB / BW 공통 로직
def parse_bond_record(rec: Dict[str, Any]):
    title = clean_title(rec["title"])
    tables = rec["tables"]
    corr_after = extract_correction_after_map(tables) if is_correction_title(title) else {}

    row = {h: "" for h in BOND_HEADERS}
    missing = []
    suspicious = []

    row["구분"] = bond_type_code(title)
    row["회사명"] = first_nonempty(
        detect_company_from_tables(tables, corr_after),
        extract_company_name_from_title(title)
    )
    row["보고서명"] = title
    row["상장시장"] = first_nonempty(
        detect_market_from_title(title),
        detect_market_from_tables(tables, corr_after)
    )

    row["최초 이사회결의일"] = get_valid_date_by_labels(
        tables,
        ["최초 이사회결의일", "최초이사회결의일", "이사회결의일", "이사회결의일(결정일)", "결정일"],
        corr_after
    )

    def get_corr_num(labels, fallback_keys=None, min_val=-1, as_float=False):
        fallback_keys = fallback_keys or []
        val = scan_label_value_preferring_correction(tables, labels, corr_after)
        if as_float:
            num = _to_float(val)
            if num is None and fallback_keys:
                num = find_row_best_float(tables, fallback_keys)
            return f"{num:g}" if num is not None else ""
        num = _to_int(val)
        if (num is None or num <= min_val) and fallback_keys:
            num = find_row_best_int(tables, fallback_keys, min_val)
        if num is not None:
            if num == 0:
                return "0"
            if num > 0:
                return f"{num:,}"
        return ""

    row["권면총액(원)"] = get_corr_num(
        ["사채의권면(전자등록)총액(원)", "권면(전자등록)총액(원)", "사채의 권면총액", "권면총액", "사채의 총액"],
        ["권면총액", "원"],
        50
    )

    coupon = scan_label_value_preferring_correction(tables, ["표면이자율(%)", "표면이자율", "표면금리", "이표이자율"], corr_after)
    ytm = scan_label_value_preferring_correction(tables, ["만기이자율(%)", "만기이자율", "만기보장수익률", "만기수익률", "Yield To Maturity"], corr_after)
    row["Coupon"] = clean_percent(coupon) if coupon else get_corr_num(["표면이자율(%)", "표면이자율", "표면금리"], ["표면이자율"], -1, True)
    row["YTM"] = clean_percent(ytm) if ytm else get_corr_num(["만기이자율(%)", "만기이자율", "만기보장수익률"], ["만기이자율"], -1, True)

    row["만기"] = get_valid_date_by_labels(
        tables, ["사채만기일", "만기일", "상환기일", "만기"], corr_after
    )

    row["납입일"] = extract_payment_date_bond(tables, corr_after)
    row["모집방식"] = scan_label_value_preferring_correction(
        tables, ["사채발행방법", "모집방법", "모집방식", "발행방법", "공모여부"], corr_after
    )
    row["발행상품"] = extract_product_type_bond(tables, corr_after, title)

    row["행사(전환)가액(원)"] = get_corr_num(
        ["전환가액(원/주)", "교환가액(원/주)", "행사가액(원/주)", "권리행사가액(원/주)", "전환가액", "교환가액", "행사가액", "권리행사가액"],
        ["가액", "원"],
        50
    )
    row["전환주식수"] = get_corr_num(
        ["전환에 따라 발행할 주식수", "전환에 따라 발행할 주식의 수", "전환주식수", "교환대상 주식수", "교환대상주식수", "행사주식수", "권리행사로 발행할 주식수", "주식수"],
        ["주식수"],
        50
    )

    row["주식총수대비 비율"] = clean_percent(scan_label_value_preferring_correction(
        tables, ["주식총수 대비 비율(%)", "발행주식총수 대비 비율(%)", "주식총수 대비 비율", "총수대비 비율"], corr_after
    ))

    refixing_raw = scan_label_value_preferring_correction(
        tables, ["최저 조정가액", "조정가액 하한", "Refixing Floor", "하한가액", "최저 조정가액(원)", "최저조정가액", "리픽싱 하한", "리픽싱하한"],
        corr_after
    )
    if "%" in refixing_raw:
        row["Refixing Floor"] = clean_percent(refixing_raw)
    else:
        row["Refixing Floor"] = first_nonempty(
            fmt_number(_max_int_in_text(refixing_raw)) if _max_int_in_text(refixing_raw) else "",
            get_corr_num(["최저 조정가액", "조정가액 하한", "최저조정가액", "리픽싱하한"], ["최저조정가액", "원"], 50)
        )

    period_keywords = []

    if row["구분"] == "CB":
        period_keywords = ["전환청구기간"]
    elif row["구분"] == "EB":
        period_keywords = ["교환청구기간"]
    elif row["구분"] == "BW":
        period_keywords = ["권리행사기간"]

    s_date, e_date = extract_period_dates_from_tables(
        tables, corr_after, period_keywords
    )
    row["전환청구 시작"], row["전환청구 종료"] = s_date, e_date

    #0930에 2995~3019 고침
    put_section_val = extract_option_section_from_tables(tables, "put")
    put_primary_val = extract_option_value_primary(tables, corr_after, "put")
    put_anchor_val = extract_option_block_by_anchor_range(tables, "put")
    put_detail_val = extract_option_details_from_tables(tables, "put")
    
    row["Put Option"] = choose_best_option_value(
        "put",
        put_section_val,
        put_primary_val,
        put_anchor_val,
        put_detail_val,
    )
    
    call_section_val = extract_option_section_from_tables(tables, "call")
    call_primary_val = extract_option_value_primary(tables, corr_after, "call")
    call_anchor_val = extract_option_block_by_anchor_range(tables, "call")
    call_detail_val = extract_option_details_from_tables(tables, "call")
    
    row["Call Option"] = choose_best_option_value(
        "call",
        call_section_val,
        call_primary_val,
        call_anchor_val,
        call_detail_val,
    )

    row["Call 비율"] = clean_percent(scan_label_value_preferring_correction(
        tables,
        ["콜옵션 행사비율", "매도청구권 행사비율", "Call 비율", "콜옵션 비율", "매도청구권 비율", "권면총액 대비 비율", "행사비율"],
        corr_after
    ))
    row["YTC"] = clean_percent(scan_label_value_preferring_correction(
        tables, ["조기상환수익률", "YTC", "Yield To Call", "조기상환이율", "조기상환수익률(%)", "연복리수익률"],
        corr_after
    ))

    if not row["Call 비율"] or not row["YTC"]:
        ratio2, ytc2 = extract_call_ratio_and_ytc_from_text(row["Call Option"])
        if not row["Call 비율"]:
            row["Call 비율"] = ratio2
        if not row["YTC"]:
            row["YTC"] = ytc2

    row["투자자"] = extract_investors_bond(tables, corr_after)
    row["자금용도"] = extract_fund_usage_bond(tables, corr_after)
    row["링크"] = rec["src_url"]
    row["접수번호"] = rec["acpt_no"]

    for h in BOND_HEADERS:
        if h in ["링크", "접수번호"]:
            continue
        if not normalize_text(row[h]):
            missing.append(h)

    if not row["구분"]:
        suspicious.append("구분")
    if row["회사명"] in ["유", "코", "넥"]:
        suspicious.append("회사명")
    if row["보고서명"] and len(row["보고서명"]) < 5:
        suspicious.append("보고서명")

    return row, missing, suspicious


# ==========================================================
# Upsert helpers
# ==========================================================
# [특정 키 컬럼값으로 시트 행 찾기]
# - 현재는 접수번호 기반 update 여부 판단에 사용
def find_row_by_key(ws, key_header: str, key_value: str) -> Optional[int]:
    vals = ws.get_all_values()
    if not vals:
        return None

    headers = vals[0]
    if key_header not in headers:
        return None

    idx = headers.index(key_header)
    for i, row in enumerate(vals[1:], start=2):
        if idx < len(row) and str(row[idx]).strip() == str(key_value).strip():
            return i
    return None


# [이벤트 단위 기존 행 찾기]
# - 접수번호가 달라도 같은 이벤트라고 판단되면 update 가능하도록 보조 탐색
# - 기준:
#   1) 회사명 정규화 동일
#   2) 최초 이사회결의일 동일
#   3) bond인 경우 구분(CB/EB/BW)도 동일
def find_event_row(ws, headers: List[str], row_dict: Dict[str, Any], sheet_type: str) -> Optional[int]:
    vals = ws.get_all_values()
    if not vals or len(vals) <= 1:
        return None

    hidx = {h: i for i, h in enumerate(headers)}
    target_company = norm_company_name(row_dict.get("회사명", ""))
    target_first = _norm_date(row_dict.get("최초 이사회결의일", ""))

    if not target_company or not target_first:
        return None

    target_type = _norm(row_dict.get("구분", "")) if sheet_type == "bond" else ""

    for i, row in enumerate(vals[1:], start=2):
        comp = norm_company_name(safe_cell(row, hidx.get("회사명", 0)))
        first = _norm_date(safe_cell(row, hidx.get("최초 이사회결의일", 0)))
        if comp != target_company or first != target_first:
            continue

        if sheet_type == "bond":
            btype = _norm(safe_cell(row, hidx.get("구분", 0)))
            if btype != target_type:
                continue

        return i

    return None


# [구조화 row upsert]
# - 1순위: 접수번호로 기존 행 탐색
# - 2순위: 동일 이벤트(회사명+최초이사회결의일, bond면 구분 포함) 탐색
# - 있으면 UPDATE, 없으면 APPEND
def upsert_structured_row(ws, headers: List[str], row_dict: Dict[str, Any], sheet_type: str):
    row_values = [row_dict.get(h, "") for h in headers]

    # 동일 접수번호일 때만 UPDATE
    target_row = find_row_by_key(ws, "접수번호", str(row_dict.get("접수번호", "")))

    end_col = gspread.utils.rowcol_to_a1(1, len(headers)).rstrip("1")
    if target_row:
        ws.update(f"A{target_row}:{end_col}{target_row}", [row_values])
        return "UPDATE", target_row

    # 접수번호가 다르면 무조건 APPEND
    ws.append_row(row_values, value_input_option="RAW")
    return "APPEND", None


# ==========================================================
# Logging / Runner
# ==========================================================
# [parse_log 기록]
# - 처리 상태 / 누락 컬럼 / 의심 컬럼을 parse_log 시트에 append
def write_parse_log(
    log_ws,
    acpt_no: str,
    title: str,
    target_sheet: str,
    status: str,
    missing: List[str],
    suspicious: Optional[List[str]] = None,
):
    suspicious = suspicious or []
    log_ws.append_row([
        acpt_no,
        title,
        target_sheet,
        status,
        ", ".join(missing),
        ", ".join(suspicious),
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    ], value_input_option="RAW")


# [메인 실행 함수]
# - 시트 오픈 / 헤더 보장
# - RAW_dump 레코드 로드
# - 공시 타입별 parser 실행
# - 결과를 rights / bond 시트에 upsert
# - 처리 로그를 parse_log에 기록
def run_parser():
    sh = gs_open()

    raw_ws = ensure_ws(sh, RAW_SHEET_NAME, rows=5000, cols=250)
    rights_ws = ensure_ws(sh, RIGHTS_SHEET_NAME, rows=3000, cols=max(40, len(RIGHTS_HEADERS) + 5))
    bond_ws = ensure_ws(sh, BOND_SHEET_NAME, rows=3000, cols=max(40, len(BOND_HEADERS) + 5))
    log_ws = ensure_ws(sh, PARSE_LOG_SHEET_NAME, rows=3000, cols=max(20, len(PARSE_LOG_HEADERS) + 5))

    ensure_header(rights_ws, RIGHTS_HEADERS)
    ensure_header(bond_ws, BOND_HEADERS)
    ensure_header(log_ws, PARSE_LOG_HEADERS)

    records = load_raw_records(raw_ws)
    if RUN_ONLY_ACPTNO:
        records = [r for r in records if r["acpt_no"] == RUN_ONLY_ACPTNO]

    if not records:
        print("[INFO] RAW_dump에 파싱할 데이터가 없습니다.")
        return

    ok = 0
    skip = 0
    fail = 0

    for rec in records:
        acpt_no = rec["acpt_no"]
        title = clean_title(rec["title"] or "")

        try:
            if "유상증자결정" in title.replace(" ", ""):
                row, missing, suspicious = parse_rights_record(rec)
                mode, rownum = upsert_structured_row(rights_ws, RIGHTS_HEADERS, row, "rights")
                write_parse_log(log_ws, acpt_no, title, RIGHTS_SHEET_NAME, f"OK:{mode}", missing, suspicious)
                ok += 1
                print(f"[OK][RIGHTS][{mode}] {acpt_no} {title}")

            elif any(k in title.replace(" ", "") for k in [
                "전환사채권발행결정",
                "교환사채권발행결정",
                "신주인수권부사채권발행결정",
            ]):
                row, missing, suspicious = parse_bond_record(rec)
                mode, rownum = upsert_structured_row(bond_ws, BOND_HEADERS, row, "bond")
                write_parse_log(log_ws, acpt_no, title, BOND_SHEET_NAME, f"OK:{mode}", missing, suspicious)
                ok += 1
                print(f"[OK][BOND][{mode}] {acpt_no} {title}")

            else:
                write_parse_log(log_ws, acpt_no, title, "", "SKIP", [], [])
                skip += 1
                print(f"[SKIP] {acpt_no} {title}")

        except Exception as e:
            write_parse_log(log_ws, acpt_no, title, "", f"FAIL: {e}", [], [])
            fail += 1
            print(f"[FAIL] {acpt_no} {title} :: {e}")

    print(f"[DONE] ok={ok} skip={skip} fail={fail}")

    return {
        "rights_added": rights_added,
        "bond_added": bond_added,
        "total_added": rights_added + bond_added,
        "error_count": fail,
        "ok": ok,
        "skip": skip,
        "fail": fail,
    }

    def run_parser():
        ok = 0
        skip = 0
        fail = 0
        rights_added = 0
        bond_added = 0

# [직접 실행 진입점]
if __name__ == "__main__":
    run_parser()
