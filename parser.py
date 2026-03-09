import os
import re
import json
from typing import Dict, List, Tuple, Optional, Any
from datetime import datetime
import gspread
import pandas as pd

# ==========================================================
# 1. 설정 및 글로벌 변수
# ==========================================================
GOOGLE_SHEET_ID = os.environ.get("GOOGLE_SHEET_ID", "").strip()
GOOGLE_CREDENTIALS_JSON = (
    os.environ.get("GOOGLE_CREDENTIALS_JSON", "").strip() or 
    os.environ.get("GOOGLE_CREDS", "").strip()
)

RAW_SHEET_NAME = os.getenv("DUMP_SHEET_NAME", "RAW_dump")
RIGHTS_SHEET_NAME = os.getenv("RIGHTS_SHEET_NAME", "유상증자")
BOND_SHEET_NAME = os.getenv("BOND_SHEET_NAME", "주식연계채권")
PARSE_LOG_SHEET_NAME = os.getenv("PARSE_LOG_SHEET_NAME", "parse_log")
RUN_ONLY_ACPTNO = os.getenv("RUN_ONLY_ACPTNO", "").strip()

RIGHTS_HEADERS = [
    "회사명", "보고서명", "상장시장", "최초 이사회결의일", "증자방식", "발행상품", 
    "신규발행주식수", "확정발행가(원)", "기준주가", "확정발행금액(억원)", "할인(할증률)", 
    "증자전 주식수", "증자비율", "납입일", "신주의 배당기산일", "신주의 상장 예정일", 
    "이사회결의일", "자금용도", "투자자", "링크", "접수번호"
]

BOND_HEADERS = [
    "구분", "회사명", "보고서명", "상장시장", "최초 이사회결의일", "권면총액(원)", 
    "Coupon", "YTM", "만기", "전환청구 시작", "전환청구 종료", "Put Option", "Call Option", 
    "Call 비율", "YTC", "모집방식", "발행상품", "행사(전환)가액(원)", "전환주식수", 
    "주식총수대비 비율", "Refixing Floor", "납입일", "자금용도", "투자자", "링크", "접수번호"
]

PARSE_LOG_HEADERS = [
    "접수번호", "보고서명", "대상시트", "상태", "누락컬럼", "의심컬럼", "처리시각"
]

# ==========================================================
# 2. 구글 시트 및 데이터 로드 유틸리티 (기존 파이프라인 유지)
# ==========================================================
def gs_open():
    if not GOOGLE_SHEET_ID or not GOOGLE_CREDENTIALS_JSON:
        raise RuntimeError("GOOGLE_SHEET_ID / GOOGLE_CREDS가 비어있습니다.")
    creds = json.loads(GOOGLE_CREDENTIALS_JSON)
    gc = gspread.service_account_from_dict(creds)
    return gc.open_by_key(GOOGLE_SHEET_ID)

def ensure_ws(sh, title: str, rows: int = 2000, cols: int = 60):
    try:
        return sh.worksheet(title)
    except gspread.WorksheetNotFound:
        return sh.add_worksheet(title=title, rows=rows, cols=cols)

def ensure_header(ws, headers: List[str]):
    current = ws.row_values(1)
    if current != headers:
        ws.clear()
        ws.update("A1", [headers])

def find_row_by_key(ws, key_header: str, key_value: str) -> Optional[int]:
    vals = ws.get_all_values()
    if not vals: return None
    headers = vals[0]
    if key_header not in headers: return None
    idx = headers.index(key_header)
    for i, row in enumerate(vals[1:], start=2):
        if idx < len(row) and str(row[idx]).strip() == str(key_value).strip(): return i
    return None

def upsert_row(ws, headers: List[str], row_dict: Dict[str, Any], key_header: str):
    row_values = [row_dict.get(h, "") for h in headers]
    target_row = find_row_by_key(ws, key_header, str(row_dict.get(key_header, "")))
    if target_row:
        end_col = gspread.utils.rowcol_to_a1(1, len(headers)).rstrip("1")
        ws.update(f"A{target_row}:{end_col}{target_row}", [row_values])
    else:
        ws.append_row(row_values, value_input_option="RAW")

def safe_cell(row: List[str], idx: int) -> str:
    return row[idx] if idx < len(row) else ""

def load_raw_records(raw_ws) -> List[Dict[str, Any]]:
    values = raw_ws.get_all_values()
    if not values: return []
    by_acpt: Dict[str, List[List[str]]] = {}
    for row in values:
        acpt_no = safe_cell(row, 0).strip()
        if not acpt_no or not acpt_no.isdigit(): continue
        by_acpt.setdefault(acpt_no, []).append(row)
    
    records = []
    for acpt_no, rows in by_acpt.items():
        meta = {"acpt_no": acpt_no, "category": "", "title": "", "src_url": "", "run_ts": ""}
        table_buckets: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            row_type = safe_cell(row, 2).strip()
            if row_type == "META":
                meta["category"], meta["title"], meta["src_url"], meta["run_ts"] = safe_cell(row, 3), safe_cell(row, 4), safe_cell(row, 5), safe_cell(row, 6)
            elif row_type == "HEADER":
                tix = safe_cell(row, 1).strip()
                table_buckets.setdefault(tix, {"header": [], "data": []})["header"] = row[3:]
            elif row_type == "DATA":
                tix = safe_cell(row, 1).strip()
                table_buckets.setdefault(tix, {"header": [], "data": []})["data"].append(row[3:])
        
        dfs = []
        for tix in sorted(table_buckets.keys(), key=lambda x: int(x) if str(x).isdigit() else 999999):
            header = table_buckets[tix]["header"]
            data = table_buckets[tix]["data"]
            width = max(len(header), max((len(r) for r in data), default=0))
            if width == 0: continue
            header = header + [f"col_{i}" for i in range(len(header), width)]
            norm_data = [r + [""] * (width - len(r)) for r in data]
            dfs.append(pd.DataFrame(norm_data, columns=header))
        
        records.append({
            "acpt_no": meta["acpt_no"], "category": meta["category"], 
            "title": meta["title"], "src_url": meta["src_url"], 
            "tables": dfs,
        })
    return sorted(records, key=lambda x: x["acpt_no"])


# ==========================================================
# 3. 텍스트 정규화 및 [2D 스캐닝 & 정정 엔진]
# ==========================================================
def normalize_text(x: Any) -> str:
    if x is None: return ""
    return re.sub(r"\s+", " ", str(x).replace("\xa0", " ")).strip()

def _norm(s: str) -> str:
    return re.sub(r"\s+", "", str(s or "")).replace(":", "")

def _clean_label(s: str) -> str:
    return re.sub(r"^([①-⑩]|\(\d+\)|\d+\.)+", "", _norm(s))

def _single_line(s: str) -> str:
    return re.sub(r'\s+', ' ', str(s or "")).strip()

def build_corpus_from_tables(tables: List[pd.DataFrame]) -> str:
    """옵션 앵커 처리를 위해 표를 문장으로 병합"""
    return " ".join([" ".join([normalize_text(x) for x in df.fillna("").astype(str).values.flatten() if normalize_text(x)]) for df in tables])

def extract_correction_after_map(tables: List[pd.DataFrame]) -> Dict[str, str]:
    """정정공시 표에서 '정정후' 데이터를 매핑"""
    out: Dict[str, str] = {}
    for df in tables:
        arr = df.fillna("").astype(str).values
        R, C = arr.shape
        header_r = after_col = item_col = None
        
        for r in range(R):
            row_norm = [_norm(x) for x in arr[r].tolist()]
            if any(w in x for w in ["정정전", "변경전"] for x in row_norm) and any(w in x for w in ["정정후", "변경후"] for x in row_norm):
                header_r, after_col = r, next((i for i, x in enumerate(row_norm) if "정정후" in x or "변경후" in x), None)
                item_col = next((i for i, x in enumerate(row_norm) if ("정정사항" in x or "항목" in x or "구분" in x)), 0)
                break
                
        if header_r is None or after_col is None: continue
        
        last_item = ""
        for rr in range(header_r + 1, R):
            item = str(arr[rr][item_col]).strip() if item_col is not None and item_col < C else ""
            item = item if item and item.lower() != "nan" else last_item
            if not item: continue
            last_item = item
            
            if 0 <= after_col < C:
                v = str(arr[rr][after_col]).strip()
                if v and v.lower() != "nan" and _norm(v) not in ("정정후", "정정전", "항목", "-"):
                    after_val = _single_line(v)
                    if after_val:
                        out[_norm(item)] = after_val
                        out[_clean_label(item)] = after_val
    return out

def scan_label_value(tables: List[pd.DataFrame], label_candidates: List[str]) -> str:
    """표의 2D 구조를 파악해 키워드의 우측, 하단 값을 스마트하게 탐색"""
    cand_clean = {_clean_label(x) for x in label_candidates}
    for df in reversed(tables): # 최신 표 우선
        arr = df.fillna("").astype(str).values
        R, C = arr.shape
        for r in range(R):
            for c in range(C):
                if _clean_label(arr[r][c]) in cand_clean:
                    checks = [str(arr[rr][cc]).strip() for rr, cc in [(r, c+1), (r, c+2), (r+1, c), (r+1, c+1)] if 0 <= rr < R and 0 <= cc < C]
                    row_vals = [str(x).strip() for x in arr[r].tolist() if str(x).strip()]
                    for v in [v for v in checks + row_vals if v.lower() != "nan"]:
                        if _clean_label(v) in cand_clean or re.fullmatch(r"([①-⑩]|\(\d+\)|\d+\.)", _norm(v)): continue
                        return _single_line(v)
    return ""

def scan_label_value_preferring_correction(tables: List[pd.DataFrame], label_candidates: List[str], corr_after: Dict[str, str]) -> str:
    cand_clean = {_clean_label(x) for x in label_candidates}
    if corr_after:
        for c in cand_clean:
            if c in corr_after and str(corr_after[c]).strip(): return _single_line(str(corr_after[c]))
        for k, v in corr_after.items():
            if str(v).strip() and any(c in k for c in cand_clean): return _single_line(str(v))
    return scan_label_value(tables, label_candidates)


# ==========================================================
# 4. 세부 파싱 유틸리티 (숫자, 날짜, 주식수 등)
# ==========================================================
def looks_like_valid_date(v: str) -> bool:
    v = (v or "").strip()
    if not re.search(r"\d", v): return False
    bad_kws = ["정정", "변경", "요청", "사유", "기재", "오기", "추가상장", "상장주식", "총수", "교부예정일", "사항", "기준", "발행", "항목"]
    if any(b in v for b in bad_kws): return False
    return bool(re.search(r"\d{4}", v) or re.search(r"\d{2,4}[\.\-\/년]\s*\d{1,2}", v))

def clean_percent(value: str) -> str:
    s = normalize_text(value)
    if not s: return ""
    if "%" in s: return s
    m = re.search(r"-?\d+(?:\.\d+)?", s.replace(",", ""))
    return f"{m.group(0)}%" if m else s

def fmt_number(x):
    if x is None or str(x).strip() == "": return ""
    try:
        val = float(str(x).replace(",", ""))
        if abs(val - round(val)) < 1e-9: return f"{int(round(val)):,}"
        return f"{val:,.2f}"
    except: return str(x)

def parse_float_like(s):
    if s is None: return None
    t = re.sub(r"[^\d\.\-]", "", str(s).replace(",", ""))
    try: return float(t) if t not in ("", "-", ".") else None
    except: return None

def fmt_eok_from_won(won):
    return f"{won / 100000000:.2f}" if won is not None else ""

def max_int_in_text(s: str) -> Optional[int]:
    if not s: return None
    nums = re.findall(r"\d{1,3}(?:[,.]\d{3})+(?!\d)|\d+", re.sub(r'(^|\s)[\(①-⑩]?\s*\d+\s*[\.\)]\s+', ' ', str(s)))
    vals = [int(re.sub(r"[,.]", "", x)) for x in nums if re.sub(r"[,.]", "", x).isdigit()]
    return max(vals) if vals else None

def is_correction_title(title: str) -> bool:
    return "정정" in normalize_text(title)

def detect_market_from_title(title: str) -> str:
    if "[코]" in title or "코스닥" in title: return "코스닥"
    if "[유]" in title or "유가" in title: return "유가증권"
    if "코넥스" in title or "[넥]" in title: return "코넥스"
    return ""

def detect_report_type(title: str) -> str:
    for k in ["유상증자결정", "전환사채권발행결정", "교환사채권발행결정", "신주인수권부사채권발행결정"]:
        if k in title: return k
    return ""

def extract_company_name(title: str) -> str:
    t = re.sub(r"^\[[^\]]+\]", "", normalize_text(title)).strip()
    for k in ["유상증자결정", "전환사채권발행결정", "교환사채권발행결정", "신주인수권부사채권발행결정"]:
        if k in t: return normalize_text(t.split(k)[0]).replace("[정정]", "").strip()
    return ""

def parse_shares_from_text(text: str) -> Tuple[int, int, int]:
    text_norm = re.sub(r'\d+(?:\.\d+)?%', '', re.sub(r'202\d[년월일\.]?', '', _norm(text)))
    boundaries = r'보통|기타|종류|우선|상환|합계|총계|총수|계|액면|자금|목적|발행가|할인'
    
    cvs = [int(re.sub(r'[,.]', '', x)) for x in re.findall(r'보통(?:(?!' + boundaries + r')[^0-9])*?((?:\d{1,3}(?:[,.]\d{3})+|\d+))', text_norm) if int(re.sub(r'[,.]', '', x)) >= 50]
    ovs = [int(re.sub(r'[,.]', '', x)) for x in re.findall(r'(?:기타|종류|우선|상환)(?:(?!' + boundaries + r')[^0-9])*?((?:\d{1,3}(?:[,.]\d{3})+|\d+))', text_norm) if int(re.sub(r'[,.]', '', x)) >= 50]
    tvs = [int(re.sub(r'[,.]', '', x)) for x in re.findall(r'(?:합계|총계|총수|계)(?:(?!' + boundaries + r')[^0-9])*?((?:\d{1,3}(?:[,.]\d{3})+|\d+))', text_norm) if int(re.sub(r'[,.]', '', x)) >= 50]
    
    cv, ov, tv = cvs[-1] if cvs else 0, ovs[-1] if ovs else 0, tvs[-1] if tvs else 0
    if cv == 0 and ov == 0 and tv == 0:
        clean_t = text_norm
        for kw in ["신주의종류와수", "발행예정주식", "신주발행", "발행할주식", "증자전발행주식총수", "기발행주식총수", "발행주식총수", "증자전주식수", "증자전"]:
            clean_t = clean_t.replace(kw, "")
        v_nums = [int(re.sub(r'[,.]', '', x)) for x in re.findall(r"\d{1,3}(?:[,.]\d{3})+(?!\d)|\d+", clean_t) if int(re.sub(r'[,.]', '', x)) >= 50]
        if v_nums: cv = v_nums[-1]
    return cv, ov, tv


# ==========================================================
# 5. 핵심 엔진 (옵션 앵커, 발행가 정밀 타격, 자금/투자자)
# ==========================================================
def extract_bond_option_details(corpus: str, option_type: str, corr_after: Dict[str, str]) -> str:
    """정규식 앵커를 이용해 Put/Call Option 주어부터 발라내는 엔진"""
    my_kws = ["조기상환청구권", "put option", "풋옵션"] if option_type == 'put' else ["매도청구권", "call option", "콜옵션", "중도상환청구권"]
    opp_kws = ["매도청구권", "call option", "콜옵션", "중도상환청구권"] if option_type == 'put' else ["조기상환청구권", "put option", "풋옵션"]

    if corr_after:
        for k, v in corr_after.items():
            if any(_norm(kw).lower() in _norm(k).lower() for kw in my_kws) and len(v) > 10:
                corpus = v + " " + corpus

    candidates = []
    for kw in my_kws:
        for match in re.finditer(kw, corpus, re.IGNORECASE):
            idx = match.start()
            window = corpus[max(0, idx - 50) : idx + 1000]
            score = 0
            if option_type == 'put':
                if re.search(r'사채권자|인수인|투자자', window): score += 50
                if re.search(r'청구할\s*수\s*있다|조기상환을\s*청구', window): score += 50
                if "의무보유" in window or "콜옵션" in window: score -= 200
            else:
                if re.search(r'발행회사|매수|매도청구', window): score += 50
                if re.search(r'매수할\s*수\s*있다|매도를\s*청구', window): score += 50
                if "의무보유" in window and "사채권자" in window: score -= 200

            if "매매일" in window and "상환율" in window: score -= 300
            if "from" in window.lower() and "to" in window.lower(): score -= 300
            candidates.append((score, window))

    if not candidates: return ""
    candidates.sort(key=lambda x: x[0], reverse=True)
    best_score, best_window = candidates[0]
    if best_score < 0: return ""

    anchor_regex = r'(본\s*사채의\s*사채권자는|본\s*사채의\s*인수인은|사채권자는|인수인은|투자자는)' if option_type == 'put' else r'(발행회사\s*또는\s*발행회사가\s*지정하는\s*자(?:\([^)]*\))?(?:는|가)?|발행회사(?:는|가)|회사는\s*만기\s*전|본\s*사채는\s*만기\s*전)'
    match = re.search(anchor_regex, best_window)
    result = best_window[match.start():] if match and match.start() < 150 else best_window

    for _ in range(3):
        result = re.sub(r'^([\[【<\(]?\s*[①-⑩\d가-힣a-zA-Z][\.\)\]】>]\s*)+', '', result)
        prefix_pattern = r'^(?:본\s*사채의\s*|발행회사의\s*)?(?:조기상환청구권|매도청구권|중도상환청구권|콜옵션|풋옵션|Put\s*Option|Call\s*Option|PUT\s*OPTION|CALL\s*OPTION)[^가-힣]*?(?:에\s*관한\s*사항|청구권자|행사|부여|비율|한도)?\s*[:\]\-\>]*\s*'
        result = re.sub(prefix_pattern, '', result, flags=re.IGNORECASE)
        result = re.sub(r'^[:\-\]\s]+', '', result)

    cut_idx = len(result)
    stop_kws = opp_kws + ["기타사항", "합병 관련 사항", "청약일", "납입일", "기타 투자판단", "의무보유"]
    for stop_kw in stop_kws:
        s_idx = result.lower().find(stop_kw.lower())
        if 20 < s_idx < cut_idx: cut_idx = s_idx

    result = result[:cut_idx].strip()
    return result[:300] + ("..." if len(result) > 300 else "") if len(result) >= 5 else ""

def get_price_by_exact_section(tables: List[pd.DataFrame], corr_after: Dict[str, str], is_base_price=False) -> Optional[int]:
    """특정 키워드 주변 4줄 안에서만 발행가/기준주가를 스캔하는 정밀 타격 엔진"""
    target_kws = ["기준주가", "기준발행가액"] if is_base_price else ["신주발행가액", "예정발행가액", "확정발행가액", "발행가액"]
    stop_kws = ["자금", "증자방식", "할인", "할증", "증자전", "납입", "방법", "산정", "일정", "발행목적"]
    stop_kws.extend(["신주발행가", "확정발행가", "예정발행가"] if is_base_price else ["기준", "주식수"])

    if corr_after:
        for k, v in corr_after.items():
            k_norm = _norm(k)
            if any(t in k_norm for t in target_kws) and not any(s in k_norm for s in stop_kws):
                v_clean = re.sub(r'\d+(?:\.\d+)?%', '', re.sub(r'202\d[년월일\.]?', '', v))
                nums = [int(float(x.replace(',', ''))) for x in re.findall(r"(?<![\d.])\d{1,3}(?:,\d{3})*(?:\.\d+)?(?![\d.])|(?<![\d.])\d+(?:\.\d+)?(?![\d.])", v_clean)]
                all_vals = [val for val in nums if val >= 50 and val not in [2024, 2025, 2026, 2027]]
                if all_vals: return max(all_vals)

    for df in tables:
        arr = df.fillna("").astype(str).values
        R, C = arr.shape
        for r in range(R):
            row_str_norm = _norm("".join(arr[r]))
            if any(t in row_str_norm for t in target_kws):
                if any(s in row_str_norm for s in stop_kws) and not any(t in row_str_norm for t in target_kws): continue
                all_nums = []
                for rr in range(r, min(r+4, R)):
                    for c in range(C):
                        cell_norm = _norm(arr[rr][c])
                        if any(s in cell_norm for s in stop_kws) and not any(t in cell_norm for t in target_kws): continue
                        cell_clean = re.sub(r'\d+(?:\.\d+)?%', '', re.sub(r'202\d[년월일\.]?', '', cell_norm))
                        nums = [int(float(x.replace(',', ''))) for x in re.findall(r"(?<![\d.])\d{1,3}(?:,\d{3})*(?:\.\d+)?(?![\d.])|(?<![\d.])\d+(?:\.\d+)?(?![\d.])", cell_clean)]
                        all_nums.extend([val for val in nums if val >= 50 and val not in [2024, 2025, 2026, 2027]])
                if all_nums: return max(all_nums)
    return None

def extract_use_of_funds(tables: List[pd.DataFrame]) -> str:
    candidates = ["시설자금", "운영자금", "채무상환자금", "타법인증권취득자금", "타법인 증권 취득자금", "기타자금"]
    found_funds = {}
    for df in tables:
        arr = df.fillna("").astype(str).values
        for r in range(arr.shape[0]):
            for c in range(arr.shape[1]):
                cell_norm = _norm(arr[r][c])
                for tk in candidates:
                    if _norm(tk) in cell_norm:
                        amt = max((max_int_in_text(arr[r][cc]) or 0 for cc in range(c + 1, min(arr.shape[1], c + 3))), default=0)
                        if amt > 100: found_funds[tk] = max(found_funds.get(tk, 0), amt)
    return _single_line(", ".join([k for k, v in sorted(found_funds.items(), key=lambda x: x[1], reverse=True)]))

def extract_use_of_funds_total_won(tables: List[pd.DataFrame]):
    candidates = ["시설자금", "운영자금", "채무상환자금", "타법인증권취득자금", "기타자금"]
    total = 0
    hit = False
    for df in tables:
        for row in df.fillna("").astype(str).values.tolist():
            row_norm = [normalize_text(x) for x in row]
            if not any(c in " ".join(row_norm) for c in candidates): continue
            nums = [parse_float_like(cell) for cell in row_norm if parse_float_like(cell) is not None]
            if nums:
                biggest = max(nums)
                if biggest > 0:
                    total += biggest
                    hit = True
    return total if hit else None

def extract_investors(tables: List[pd.DataFrame], corr_after: Dict[str, str]) -> str:
    investors = []
    blacklist = ["관계", "배정", "비고", "합계", "소계", "해당사항", "주식수", "단위", "이사회", "총계", "출자자수", "주요사항", "지분"]
    def is_valid(sn):
        sn_clean = sn.replace(" ", "")
        if not (2 <= len(sn_clean) <= 50) or re.fullmatch(r'[\d,\.\s\-]+', sn_clean): return False
        if any(bw in _norm(sn_clean) for bw in blacklist): return False
        return True

    val = scan_label_value_preferring_correction(tables, ["제3자배정대상자", "배정대상자", "발행대상자", "투자자", "성명(법인명)", "인수인"], corr_after)
    if val:
        for chunk in re.split(r'[,;/]', val.replace('\n', ',')):
            c_name = re.sub(r'\([^)]*업자[^)]*\)|\([^)]*투자자[^)]*\)', '', chunk).strip()
            if is_valid(c_name) and c_name not in investors: investors.append(c_name)
    return _single_line(", ".join(investors[:5]))

def extract_call_ratio_and_ytc(call_text: str) -> Tuple[str, str]:
    ratio, ytc = "", ""
    call_text_clean = re.sub(r'\s+', ' ', str(call_text or ""))
    for p in [r'(?:비율|한도|초과하여).*?(\d{1,3}(?:\.\d+)?)\s*%', r'(\d{1,3}(?:\.\d+)?)\s*%\s*(?:를|을)\s*초과']:
        m = re.search(p, call_text_clean, re.IGNORECASE)
        if m and 5 <= float(m.group(1)) <= 100: ratio = f"{float(m.group(1)):g}%"; break
    for p in [r'(?:수익률|연복리|복리|이율)[^\d]{0,15}?\s*(\d{1,2}(?:\.\d+)?)\s*%']:
        m = re.search(p, call_text_clean, re.IGNORECASE)
        if m and 0 <= float(m.group(1)) <= 30: ytc = f"{float(m.group(1)):g}%"; break
    return ratio, ytc

# ==========================================================
# 6. 유상증자 / 채권 파싱 레코드 함수
# ==========================================================
def parse_rights_record(rec: Dict[str, Any]):
    title, tables = rec["title"], rec["tables"]
    corr_after = extract_correction_after_map(tables) if is_correction_title(title) else {}
    row = {h: "" for h in RIGHTS_HEADERS}
    missing, suspicious = [], []

    row["접수번호"] = rec["acpt_no"]
    row["링크"] = rec["src_url"]
    row["보고서명"] = detect_report_type(title) or title
    row["회사명"] = scan_label_value_preferring_correction(tables, ["회사명", "회사 명", "발행회사", "법인명"], corr_after).split('\n')[0].strip() or extract_company_name(title)
    if not row["회사명"] or row["회사명"] in ["유", "코", "넥"]: row["회사명"] = title
    
    mkt = scan_label_value_preferring_correction(tables, ["상장시장", "시장구분"], corr_after)
    row["상장시장"] = "코스닥" if "코스닥" in mkt else ("유가증권" if "유가" in mkt or "코스피" in mkt else ("코넥스" if "코넥스" in mkt else detect_market_from_title(title)))

    def get_valid_date(labels):
        d = scan_label_value_preferring_correction(tables, labels, corr_after)
        return normalize_text(d) if looks_like_valid_date(d) else ""

    row["이사회결의일"] = get_valid_date(["이사회결의일(결정일)", "이사회결의일", "결정일"])
    row["최초 이사회결의일"] = get_valid_date(["최초 이사회결의일", "최초이사회결의일"]) or row["이사회결의일"]
    row["납입일"] = get_valid_date(["납입일", "납입기일", "청약기일 및 납입일", "신주의 납입기일"])
    row["신주의 배당기산일"] = get_valid_date(["신주의 배당기산일", "배당기산일"])
    row["신주의 상장 예정일"] = get_valid_date(["신주의 상장 예정일", "상장예정일", "신주 상장예정일"])
    row["증자방식"] = scan_label_value_preferring_correction(tables, ["증자방식", "배정방법", "배정방식", "발행방법"], corr_after)

    # 보통주/우선주 로직
    issue_val = scan_label_value_preferring_correction(tables, ["신주의종류와수", "발행예정주식수", "발행예정주식", "신주발행", "발행할주식"], corr_after)
    cv, ov, tv = parse_shares_from_text(issue_val)
    best_amt = tv if tv > 0 and tv >= (cv + ov) else ((cv + ov) if (cv + ov) > 0 else (cv if cv > 0 else ov))
    if best_amt > 0:
        row["신규발행주식수"] = fmt_number(best_amt)
        row["발행상품"] = "보통주식" if cv > 0 and ov == 0 else ("우선주식" if ov > 0 and cv == 0 else "보통주식, 우선주식")

    prev_val = scan_label_value_preferring_correction(tables, ["증자전발행주식총수", "기발행주식총수", "발행주식총수", "증자전주식수", "증자전"], corr_after)
    pcv, pov, ptv = parse_shares_from_text(prev_val)
    best_prev = ptv if ptv > 0 and ptv >= (pcv + pov) else ((pcv + pov) if (pcv + pov) > 0 else (pcv if pcv > 0 else pov))
    if best_prev > 0: row["증자전 주식수"] = fmt_number(best_prev)

    row["확정발행가(원)"] = fmt_number(get_price_by_exact_section(tables, corr_after, is_base_price=False))
    row["기준주가"] = fmt_number(get_price_by_exact_section(tables, corr_after, is_base_price=True))
    row["할인(할증률)"] = clean_percent(scan_label_value_preferring_correction(tables, ["할인율", "할증률", "할인율 또는 할증률", "할인(할증률)"], corr_after))
    
    row["자금용도"] = extract_use_of_funds(tables)
    row["투자자"] = extract_investors(tables, corr_after)

    use_of_funds_total = extract_use_of_funds_total_won(tables)
    new_shares, price_val = parse_float_like(row["신규발행주식수"]), parse_float_like(row["확정발행가(원)"])
    
    if use_of_funds_total and use_of_funds_total > 0: row["확정발행금액(억원)"] = fmt_eok_from_won(use_of_funds_total)
    elif new_shares and price_val: row["확정발행금액(억원)"] = f"{(new_shares * price_val) / 100_000_000:,.2f}"

    pre_shares = parse_float_like(row["증자전 주식수"])
    if new_shares and pre_shares and pre_shares > 0: row["증자비율"] = f"{(new_shares / pre_shares) * 100:.2f}%"

    for h in RIGHTS_HEADERS:
        if h not in ["링크", "접수번호"] and not row[h]: missing.append(h)
    if price_val and price_val <= 50: suspicious.append("확정발행가(원)")
    
    return row, missing, suspicious

def parse_bond_record(rec: Dict[str, Any]):
    title, tables = rec["title"], rec["tables"]
    corr_after = extract_correction_after_map(tables) if is_correction_title(title) else {}
    corpus = build_corpus_from_tables(tables)
    
    row = {h: "" for h in BOND_HEADERS}
    missing = []

    row["접수번호"] = rec["acpt_no"]
    row["링크"] = rec["src_url"]
    row["보고서명"] = detect_report_type(title) or title
    row["구분"] = "EB" if "교환" in title else ("BW" if "신주인수권" in title else "CB")
    
    row["회사명"] = scan_label_value_preferring_correction(tables, ["회사명", "회사 명", "발행회사"], corr_after).split('\n')[0].strip() or extract_company_name(title)
    mkt = scan_label_value_preferring_correction(tables, ["상장시장", "시장구분"], corr_after)
    row["상장시장"] = "코스닥" if "코스닥" in mkt else ("유가증권" if "유가" in mkt or "코스피" in mkt else detect_market_from_title(title))

    def get_valid_date(labels):
        d = scan_label_value_preferring_correction(tables, labels, corr_after)
        return normalize_text(d) if looks_like_valid_date(d) else ""

    row["최초 이사회결의일"] = get_valid_date(["이사회결의일(결정일)", "이사회결의일", "최초이사회결의일"])
    row["만기"] = get_valid_date(["사채만기일", "만기일", "상환기일"])
    row["납입일"] = get_valid_date(["납입일", "납입기일", "발행일", "지급일"])
    row["모집방식"] = scan_label_value_preferring_correction(tables, ["사채발행방법", "모집방식", "발행방법"], corr_after)
    row["발행상품"] = scan_label_value_preferring_correction(tables, ["1. 사채의 종류", "사채의 종류", "발행상품"], corr_after) or row["구분"]

    def get_corr_num(labels):
        val = scan_label_value_preferring_correction(tables, labels, corr_after)
        num = max_int_in_text(val)
        return fmt_number(num) if num and num > 50 else ""

    row["권면총액(원)"] = get_corr_num(["권면(전자등록)총액(원)", "권면총액", "사채의 권면총액", "발행총액"])
    row["행사(전환)가액(원)"] = get_corr_num(["전환가액(원/주)", "교환가액(원/주)", "행사가액(원/주)", "전환가액", "교환가액", "행사가액"])
    row["전환주식수"] = get_corr_num(["전환에 따라 발행할 주식수", "교환대상 주식수", "전환주식수", "행사주식수"])

    row["Coupon"] = scan_label_value_preferring_correction(tables, ["표면이자율(%)", "표면이자율", "표면금리"], corr_after)
    row["YTM"] = scan_label_value_preferring_correction(tables, ["만기이자율(%)", "만기이자율", "만기보장수익률"], corr_after)
    row["주식총수대비 비율"] = clean_percent(scan_label_value_preferring_correction(tables, ["주식총수 대비 비율(%)", "총수대비 비율"], corr_after))
    row["Refixing Floor"] = clean_percent(scan_label_value_preferring_correction(tables, ["최저 조정가액(원)", "최저조정가액", "리픽싱하한"], corr_after))

    p_val = scan_label_value_preferring_correction(tables, ["전환청구기간", "교환청구기간", "권리행사기간"], corr_after)
    dates = re.findall(r'\d{4}[-년\.\s]+\d{1,2}[-월\.\s]+\d{1,2}', p_val)
    row["전환청구 시작"] = get_valid_date(["전환청구기간 시작일", "전환청구 시작일"]) or (normalize_text(dates[0]) if len(dates) >= 1 else "")
    row["전환청구 종료"] = get_valid_date(["전환청구기간 종료일", "전환청구 종료일"]) or (normalize_text(dates[-1]) if len(dates) >= 2 else "")

    row["Put Option"] = extract_bond_option_details(corpus, 'put', corr_after)
    row["Call Option"] = extract_bond_option_details(corpus, 'call', corr_after)
    
    ratio, ytc = extract_call_ratio(row["Call Option"])
    row["Call 비율"] = clean_percent(scan_label_value_preferring_correction(tables, ["콜옵션 행사비율", "Call 비율"], corr_after)) or ratio
    row["YTC"] = scan_label_value_preferring_correction(tables, ["조기상환수익률", "YTC", "연복리수익률"], corr_after) or ytc

    row["자금용도"] = extract_use_of_funds(tables)
    row["투자자"] = extract_investors(tables, corr_after)

    for h in BOND_HEADERS:
        if h not in ["링크", "접수번호"] and not row[h]: missing.append(h)
        
    return row, missing

# ==========================================================
# 7. 메인 실행 함수 (기존 로직 유지)
# ==========================================================
def write_parse_log(log_ws, acpt_no: str, title: str, target_sheet: str, status: str, missing: List[str], suspicious: Optional[List[str]] = None):
    suspicious = suspicious or []
    log_ws.append_row([
        acpt_no, title, target_sheet, status, ", ".join(missing), ", ".join(suspicious),
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    ], value_input_option="RAW")

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
        
    ok = skip = fail = 0
    for rec in records:
        acpt_no, title = rec["acpt_no"], rec["title"] or ""
        try:
            if "유상증자결정" in title:
                row, missing, suspicious = parse_rights_record(rec)
                upsert_row(rights_ws, RIGHTS_HEADERS, row, "접수번호")
                write_parse_log(log_ws, acpt_no, title, RIGHTS_SHEET_NAME, "OK", missing, suspicious)
                ok += 1
                print(f"[OK][RIGHTS] {acpt_no} {title}")
            elif any(k in title for k in ["전환사채권발행결정", "교환사채권발행결정", "신주인수권부사채권발행결정"]):
                row, missing = parse_bond_record(rec)
                upsert_row(bond_ws, BOND_HEADERS, row, "접수번호")
                write_parse_log(log_ws, acpt_no, title, BOND_SHEET_NAME, "OK", missing, [])
                ok += 1
                print(f"[OK][BOND] {acpt_no} {title}")
            else:
                write_parse_log(log_ws, acpt_no, title, "", "SKIP", [])
                skip += 1
                print(f"[SKIP] {acpt_no} {title}")
        except Exception as e:
            write_parse_log(log_ws, acpt_no, title, "", f"FAIL: {e}", [])
            fail += 1
            print(f"[FAIL] {acpt_no} {title} :: {e}")
            
    print(f"[DONE] ok={ok} skip={skip} fail={fail}")

if __name__ == "__main__":
    run_parser()
