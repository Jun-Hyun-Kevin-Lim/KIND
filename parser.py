# parser.py
import os
import re
import json
from typing import Dict, List, Tuple, Optional, Any
from datetime import datetime

import gspread
import pandas as pd


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


RIGHTS_HEADERS = [
    "회사명", "보고서명", "상장시장", "최초 이사회결의일", "증자방식", "발행상품",
    "신규발행주식수", "확정발행가(원)", "기준주가", "확정발행금액(억원)",
    "할인(할증률)", "증자전 주식수", "증자비율", "납입일", "신주의 배당기산일",
    "신주의 상장 예정일", "이사회결의일", "자금용도", "투자자", "링크", "접수번호"
]

BOND_HEADERS = [
    "구분", "회사명", "보고서명", "상장시장", "최초 이사회결의일", "권면총액(원)",
    "Coupon", "YTM", "만기", "전환청구 시작", "전환청구 종료",
    "Put Option", "Call Option", "Call 비율", "YTC", "모집방식",
    "발행상품", "행사(전환)가액(원)", "전환주식수", "주식총수대비 비율",
    "Refixing Floor", "납입일", "자금용도", "투자자", "링크", "접수번호"
]

PARSE_LOG_HEADERS = [
    "접수번호", "보고서명", "대상시트", "상태", "누락컬럼", "의심컬럼", "처리시각"
]


def gs_open():
    if not GOOGLE_SHEET_ID or not GOOGLE_CREDENTIALS_JSON:
        raise RuntimeError("GOOGLE_SHEET_ID / GOOGLE_CREDS(또는 GOOGLE_CREDENTIALS_JSON)가 비어있습니다.")

    creds = json.loads(GOOGLE_CREDENTIALS_JSON)
    gc = gspread.service_account_from_dict(creds)
    sh = gc.open_by_key(GOOGLE_SHEET_ID)
    return sh


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

            if row_type == "META":
                meta["category"] = safe_cell(row, 3)
                meta["title"] = safe_cell(row, 4)
                meta["src_url"] = safe_cell(row, 5)
                meta["run_ts"] = safe_cell(row, 6)

            elif row_type == "HEADER":
                tix = safe_cell(row, 1).strip()
                table_buckets.setdefault(tix, {"header": [], "data": []})
                table_buckets[tix]["header"] = row[3:]

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


def normalize_text(x: Any) -> str:
    if x is None:
        return ""
    s = str(x).replace("\xa0", " ")
    s = re.sub(r"\s+", " ", s)
    return s.strip()


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


def all_pairs_from_tables(tables: List[pd.DataFrame]) -> List[Tuple[str, str]]:
    out = []
    for df in tables:
        out.extend(df_to_pairs(df))
    return out


def all_text_lines(tables: List[pd.DataFrame]) -> List[str]:
    lines = []
    for df in tables:
        arr = df.fillna("").astype(str).values.tolist()
        for row in arr:
            joined = " | ".join([normalize_text(x) for x in row if normalize_text(x)])
            if joined:
                lines.append(joined)
    return lines


def contains_any(text: str, keywords: List[str]) -> bool:
    return any(k in text for k in keywords)


def first_nonempty(*vals):
    for v in vals:
        if normalize_text(v):
            return normalize_text(v)
    return ""


def parse_int(value: Any):
    s = normalize_text(value).replace(",", "")
    m = re.search(r"-?\d+", s)
    return int(m.group(0)) if m else None


def parse_float(value: Any):
    s = normalize_text(value).replace(",", "")
    m = re.search(r"-?\d+(?:\.\d+)?", s)
    return float(m.group(0)) if m else None


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


def clean_percent(value: str) -> str:
    s = normalize_text(value)
    if not s:
        return ""
    if "%" in s:
        return s
    m = re.search(r"-?\d+(?:\.\d+)?", s.replace(",", ""))
    return f"{m.group(0)}%" if m else s


def fmt_number(x):
    if x is None:
        return ""
    if isinstance(x, float) and abs(x - round(x)) < 1e-9:
        x = int(round(x))
    if isinstance(x, int):
        return f"{x:,}"
    return f"{x:,.2f}"


def fmt_eok_from_won(won):
    if won is None:
        return ""
    return f"{won / 100000000:.2f}"


def is_correction_title(title: str) -> bool:
    t = normalize_text(title)
    return t.startswith("[정정]") or t.startswith("정정") or "[정정]" in t


def find_value_by_left_keywords(pairs: List[Tuple[str, str]], keywords: List[str]) -> str:
    for left, right in pairs:
        if contains_any(left, keywords) and normalize_text(right):
            return normalize_text(right)
    return ""


def find_numeric_value_by_keywords(pairs: List[Tuple[str, str]], keywords: List[str]):
    return parse_float(find_value_by_left_keywords(pairs, keywords))


def extract_company_name(title: str) -> str:
    t = normalize_text(title)
    t = re.sub(r"^\[[^\]]+\]", "", t).strip()
    for k in ["유상증자결정", "전환사채권발행결정", "교환사채권발행결정", "신주인수권부사채권발행결정"]:
        if k in t:
            return normalize_text(t.split(k)[0]).replace("[정정]", "").strip()
    return ""


def detect_market_from_title(title: str) -> str:
    if "[코]" in title:
        return "코스닥"
    if "[유]" in title:
        return "유가증권"
    if "[코넥스]" in title or "[넥]" in title:
        return "코넥스"
    return ""


def detect_report_type(title: str) -> str:
    for k in [
        "유상증자결정",
        "전환사채권발행결정",
        "교환사채권발행결정",
        "신주인수권부사채권발행결정",
    ]:
        if k in title:
            return k
    return ""


def bond_type_kor(title: str) -> str:
    if "전환사채권발행결정" in title:
        return "전환사채"
    if "교환사채권발행결정" in title:
        return "교환사채"
    if "신주인수권부사채권발행결정" in title:
        return "신주인수권부사채"
    return ""


def extract_use_of_funds(tables: List[pd.DataFrame]) -> str:
    candidates = ["시설자금", "운영자금", "채무상환자금", "타법인증권취득자금", "기타자금", "취득자금"]
    found = []

    for df in tables:
        arr = df.fillna("").astype(str).values.tolist()
        for row in arr:
            row_text = " | ".join([normalize_text(x) for x in row if normalize_text(x)])
            if not row_text:
                continue
            for c in candidates:
                if c in row_text:
                    nums = [parse_int(x) for x in row if parse_int(x) is not None]
                    if nums and max(nums) > 0 and c not in found:
                        found.append(c)

    return ", ".join(found)


def extract_use_of_funds_total_won(tables: List[pd.DataFrame]):
    candidates = ["시설자금", "운영자금", "채무상환자금", "타법인증권취득자금", "기타자금", "취득자금"]
    total = 0
    hit = False

    for df in tables:
        arr = df.fillna("").astype(str).values.tolist()
        for row in arr:
            row_norm = [normalize_text(x) for x in row]
            row_join = " | ".join([x for x in row_norm if x])
            if not any(c in row_join for c in candidates):
                continue

            nums = [parse_int(cell) for cell in row_norm if parse_int(cell) is not None]
            if nums:
                biggest = max(nums)
                if biggest > 0:
                    total += biggest
                    hit = True

    return total if hit else None


def extract_investor_text(tables: List[pd.DataFrame]) -> str:
    investor_keys = [
        "제3자배정 대상자", "제3자배정대상자", "배정대상자",
        "인수인", "투자자", "상대방", "권리자", "취득자",
        "성명(법인명)", "출자자"
    ]
    lines = all_text_lines(tables)
    found = []

    for line in lines:
        if contains_any(line, investor_keys) and line not in found:
            found.append(line)

    return " / ".join(found[:5]) if found else ""


def correction_override_value(pairs: List[Tuple[str, str]], field_keywords: List[str]) -> str:
    for left, right in pairs:
        if "정정" in left and contains_any(left, field_keywords) and normalize_text(right):
            return normalize_text(right)
    return ""


def max_int_in_text(s: str) -> Optional[int]:
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


def looks_like_valid_date(v: str) -> bool:
    v = (v or "").strip()
    if not re.search(r"\d", v):
        return False
    bad_kws = ["정정", "변경", "요청", "사유", "기재", "오기", "추가상장", "상장주식", "총수", "교부예정일", "사항", "기준", "발행", "항목"]
    if any(b in v for b in bad_kws):
        return False
    if not (re.search(r"\d{4}", v) or re.search(r"\d{2,4}[\.\-\/년]\s*\d{1,2}", v)):
        return False
    return True


def get_valid_date_by_keywords(pairs, labels):
    for lab in labels:
        v = find_value_by_left_keywords(pairs, [lab])
        if looks_like_valid_date(v):
            return normalize_text(v)
    return ""


def flatten_table_text(table: pd.DataFrame) -> str:
    parts = []
    arr = table.fillna("").astype(str).values.tolist()
    for row in arr:
        for cell in row:
            s = normalize_text(cell)
            if s:
                parts.append(s)
    return " ".join(parts)


def parse_shares_from_text(text: str) -> Tuple[int, int, int]:
    text_norm = normalize_text(text or "")
    text_norm = re.sub(r"\s+", "", text_norm)
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

    if cv == 0 and ov == 0 and tv == 0:
        text_clean = text_norm
        for kw in [
            "신주의종류와수", "발행예정주식", "발행예정주식수",
            "신주발행", "발행할주식", "증자전발행주식총수",
            "기발행주식총수", "발행주식총수", "증자전주식수", "증자전"
        ]:
            text_clean = text_clean.replace(kw, "")
        nums_str = re.findall(r"\d{1,3}(?:[,.]\d{3})+(?!\d)|\d+", text_clean)
        v_nums = [int(re.sub(r'[,.]', '', x)) for x in nums_str if int(re.sub(r'[,.]', '', x)) >= 50]
        if v_nums:
            cv = v_nums[-1]

    return cv, ov, tv


def extract_issue_shares_and_type_from_tables(tables) -> Tuple[Optional[int], str]:
    target_kws = ["신주의종류와수", "발행예정주식수", "발행예정주식", "신주발행", "발행할주식"]

    best_amt = 0
    stock_type = ""

    for table in tables:
        table_text = re.sub(r"\s+", "", flatten_table_text(table))
        if not any(k in table_text for k in target_kws):
            continue

        cv, ov, tv = parse_shares_from_text(table_text)
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
            else:
                stock_type = "보통주식"
            return best_amt, stock_type

    return None, ""


def extract_prev_shares_from_tables(tables) -> Optional[int]:
    target_kws = [
        "증자전발행주식총수", "기발행주식총수", "발행주식총수",
        "증자전주식수", "증자전발행주식총수(보통주식)", "증자전"
    ]

    for table in tables:
        table_text = re.sub(r"\s+", "", flatten_table_text(table))
        if not any(k in table_text for k in target_kws):
            continue

        cv, ov, tv = parse_shares_from_text(table_text)
        calc_tot = cv + ov

        if tv > 0 and tv >= calc_tot:
            return tv
        if calc_tot > 0:
            return calc_tot
        if cv > 0:
            return cv

    return None


def find_numeric_value_by_keywords_expanded(pairs, keywords):
    return find_numeric_value_by_keywords(pairs, keywords)


def scan_price_like_from_pairs(pairs, target_kws, stop_kws):
    vals = []

    for left, right in pairs:
        l = re.sub(r"\s+", "", normalize_text(left or ""))
        r = str(right or "")

        if not any(t in l for t in target_kws):
            continue
        if any(s in l for s in stop_kws) and not any(t in l for t in target_kws):
            continue

        r_clean = re.sub(r'202\d[년월일\.]?', '', r)
        r_clean = re.sub(r'\d+(?:\.\d+)?%', '', r_clean)

        nums = re.findall(r"(?<![\d.])\d{1,3}(?:,\d{3})*(?:\.\d+)?(?![\d.])|(?<![\d.])\d+(?:\.\d+)?(?![\d.])", r_clean)
        for x in nums:
            try:
                val = int(float(x.replace(",", "")))
                if val >= 50 and val not in [2024, 2025, 2026, 2027]:
                    vals.append(val)
            except Exception:
                pass

    return max(vals) if vals else None


def detect_market_from_pairs(pairs):
    for lab in ["상장시장", "시장구분"]:
        v = find_value_by_left_keywords(pairs, [lab])
        if not v:
            continue
        if "코스닥" in v:
            return "코스닥"
        if "유가증권" in v or "코스피" in v:
            return "유가증권"
        if "코넥스" in v:
            return "코넥스"
        if "비상장" in v:
            return "비상장"
    return ""


def detect_company_from_pairs(pairs):
    labels = ["회사명", "회사 명", "발행회사", "발행회사명", "법인명", "종속회사명", "종속회사", "종속회사인"]
    for lab in labels:
        v = find_value_by_left_keywords(pairs, [lab])
        if v:
            return str(v).split("\n")[0].strip()
    return ""


def parse_rights_record(rec: Dict[str, Any]):
    title = rec["title"]
    tables = rec["tables"]
    pairs = all_pairs_from_tables(tables)

    row = {h: "" for h in RIGHTS_HEADERS}
    missing = []
    suspicious = []

    row["회사명"] = first_nonempty(
        detect_company_from_pairs(pairs),
        extract_company_name(title)
    )

    row["보고서명"] = detect_report_type(title) or title

    row["상장시장"] = first_nonempty(
        detect_market_from_pairs(pairs),
        detect_market_from_title(title)
    )

    row["최초 이사회결의일"] = get_valid_date_by_keywords(
        pairs, ["최초 이사회결의일", "최초이사회결의일"]
    )

    row["이사회결의일"] = get_valid_date_by_keywords(
        pairs, ["이사회결의일", "이사회 결의일", "이사회결의일(결정일)", "결정일"]
    )

    if not row["최초 이사회결의일"]:
        row["최초 이사회결의일"] = row["이사회결의일"]

    row["납입일"] = get_valid_date_by_keywords(
        pairs, ["납입일", "납입기일", "청약기일 및 납입일", "신주의 납입기일", "신주납입기일"]
    )

    row["신주의 배당기산일"] = get_valid_date_by_keywords(
        pairs, ["신주의 배당기산일", "배당기산일"]
    )

    row["신주의 상장 예정일"] = get_valid_date_by_keywords(
        pairs,
        ["신주의 상장예정일", "신주의 상장 예정일", "상장예정일", "신주 상장예정일", "상장 예정일", "신주상장예정일"]
    )

    row["증자방식"] = find_value_by_left_keywords(
        pairs, ["증자방식", "배정방법", "배정방식", "발행방법"]
    )

    issue_shares, issue_type = extract_issue_shares_and_type_from_tables(tables)

    if issue_shares:
        row["신규발행주식수"] = fmt_number(issue_shares)

    if issue_type:
        row["발행상품"] = issue_type

    if not row["신규발행주식수"]:
        row["신규발행주식수"] = fmt_number(find_numeric_value_by_keywords_expanded(
            pairs,
            [
                "신주발행수", "신규발행주식수", "발행주식수", "발행할 주식의 총수",
                "신주의 종류와 수", "신주의종류와수", "발행예정주식수",
                "발행예정주식", "신주발행", "발행할주식"
            ]
        ))

    if not row["발행상품"]:
        row["발행상품"] = first_nonempty(
            find_value_by_left_keywords(
                pairs, ["발행할 주식의 종류", "주식의 종류", "발행상품", "신주의 종류", "신주의 종류와 수"]
            ),
            "보통주식" if row["신규발행주식수"] else ""
        )

    row["확정발행가(원)"] = fmt_number(
        scan_price_like_from_pairs(
            pairs,
            target_kws=["신주발행가액", "신주 발행가액", "예정발행가액", "확정발행가액", "확정 발행가액", "확정발행가", "발행가액", "1주당 발행가액", "1주당 확정발행가액"],
            stop_kws=["자금", "증자방식", "기준", "할인", "할증", "증자전", "주식수", "납입", "방법", "산정", "일정", "발행목적"]
        )
    )

    row["기준주가"] = fmt_number(
        scan_price_like_from_pairs(
            pairs,
            target_kws=["기준주가", "산정기준주가", "기준발행가액"],
            stop_kws=["자금", "증자방식", "할인", "할증", "증자전", "납입", "방법", "산정", "일정", "신주발행가", "확정발행가", "예정발행가", "발행목적"]
        )
    )

    row["할인(할증률)"] = clean_percent(find_value_by_left_keywords(
        pairs,
        [
            "할인율", "할인(할증)율", "할인(할증률)", "할증률",
            "할인율 또는 할증률", "발행가액 산정시 할인율"
        ]
    ))

    prev_shares = extract_prev_shares_from_tables(tables)
    if prev_shares:
        row["증자전 주식수"] = fmt_number(prev_shares)

    if not row["증자전 주식수"]:
        row["증자전 주식수"] = fmt_number(find_numeric_value_by_keywords_expanded(
            pairs,
            [
                "증자전 발행주식총수", "증자전 주식수", "발행주식총수(증자전)",
                "기발행주식수", "기발행주식총수", "발행주식총수",
                "증자전발행주식총수", "증자전발행주식총수(보통주식)", "증자전주식수"
            ]
        ))

    row["자금용도"] = extract_use_of_funds(tables)
    row["투자자"] = extract_investor_text(tables)
    row["링크"] = rec["src_url"]
    row["접수번호"] = rec["acpt_no"]

    if is_correction_title(title):
        v = correction_override_value(
            pairs,
            [
                "확정발행가액", "확정발행가", "발행가액",
                "신주발행가액", "신주 발행가액", "예정발행가액"
            ]
        )
        if v:
            row["확정발행가(원)"] = fmt_number(parse_float_like(v))

        v = correction_override_value(pairs, ["납입일", "납입기일", "신주납입기일"])
        if v and looks_like_valid_date(v):
            row["납입일"] = v

        v = correction_override_value(
            pairs,
            ["신주의 상장예정일", "신주의 상장 예정일", "상장예정일", "신주상장예정일"]
        )
        if v and looks_like_valid_date(v):
            row["신주의 상장 예정일"] = v

        v = correction_override_value(
            pairs, ["이사회결의일", "이사회 결의일", "이사회결의일(결정일)", "결정일"]
        )
        if v and looks_like_valid_date(v):
            row["이사회결의일"] = v

    use_of_funds_total = extract_use_of_funds_total_won(tables)
    new_shares = parse_float_like(row["신규발행주식수"])
    price = parse_float_like(row["확정발행가(원)"])

    if use_of_funds_total is not None and use_of_funds_total > 0:
        row["확정발행금액(억원)"] = fmt_eok_from_won(use_of_funds_total)
    elif new_shares is not None and price is not None:
        row["확정발행금액(억원)"] = fmt_eok_from_won(new_shares * price)

    pre_shares = parse_float_like(row["증자전 주식수"])
    if new_shares is not None and pre_shares not in (None, 0):
        row["증자비율"] = f"{(new_shares / pre_shares) * 100:.2f}%"

    for h in RIGHTS_HEADERS:
        if h in ["링크", "접수번호"]:
            continue
        if not normalize_text(row[h]):
            missing.append(h)

    price_val = parse_float_like(row["확정발행가(원)"])
    if price_val is not None and price_val <= 50:
        suspicious.append("확정발행가(원)")

    base_val = parse_float_like(row["기준주가"])
    if base_val is not None and base_val <= 50:
        suspicious.append("기준주가")

    if row["납입일"] and not looks_like_valid_date(row["납입일"]):
        suspicious.append("납입일")

    if row["신주의 상장 예정일"] and not looks_like_valid_date(row["신주의 상장 예정일"]):
        suspicious.append("신주의 상장 예정일")

    if row["이사회결의일"] and not looks_like_valid_date(row["이사회결의일"]):
        suspicious.append("이사회결의일")

    if row["투자자"] and any(x in row["투자자"] for x in ["관계", "지분", "합계", "소계", "정정", "출자자수", "명"]):
        suspicious.append("투자자")

    if row["자금용도"] and "(원)" in row["자금용도"]:
        suspicious.append("자금용도")

    if row["회사명"] in ["유", "코", "넥"]:
        suspicious.append("회사명")

    return row, missing, suspicious


def extract_period_dates_from_tables(tables: List[pd.DataFrame], section_keywords: List[str]) -> Tuple[str, str]:
    text_lines = all_text_lines(tables)
    date_pat = r"\d{4}[.\-/년]\s*\d{1,2}[.\-/월]\s*\d{1,2}일?"
    for line in text_lines:
        if contains_any(line, section_keywords):
            dates = re.findall(date_pat, line)
            if len(dates) >= 2:
                return normalize_text(dates[0]), normalize_text(dates[1])
    return "", ""


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
        "기타 투자판단", "의무보유", "특정인"
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


def extract_product_type_from_pairs_and_tables(pairs, tables) -> str:
    labels = [
        "사채의 종류", "1. 사채의 종류", "1.사채의종류",
        "사채종류", "발행상품", "증권의 종류", "채권의 종류", "종류"
    ]

    v = find_value_by_left_keywords(pairs, labels)
    if v:
        t = normalize_text(v)
        t = re.sub(r"(?:^|\s)(사채의 종류|발행상품|증권의 종류|채권의 종류)\s*", "", t)
        if any(x in t for x in ["전환사채", "교환사채", "신주인수권부사채"]):
            return t

    full_text = " ".join(all_text_lines(tables))
    full_text = normalize_text(full_text)

    m = re.search(
        r'((?:제\s*\d+\s*회)?\s*(?:무기명식|기명식|이권부|무보증|보증|사모|공모|비분리형|분리형)?[\w\s,()\-]*?(?:전환사채|교환사채|신주인수권부사채))',
        full_text
    )
    if m:
        return normalize_text(m.group(1))

    for line in all_text_lines(tables):
        if "전환사채" in line:
            return "전환사채"
        if "교환사채" in line:
            return "교환사채"
        if "신주인수권부사채" in line:
            return "신주인수권부사채"

    return ""


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


def parse_bond_record(rec: Dict[str, Any]):
    title = rec["title"]
    tables = rec["tables"]
    pairs = all_pairs_from_tables(tables)

    row = {h: "" for h in BOND_HEADERS}
    missing = []

    row["구분"] = bond_type_kor(title)

    row["회사명"] = first_nonempty(
        detect_company_from_pairs(pairs),
        extract_company_name(title)
    )

    row["보고서명"] = detect_report_type(title) or title

    row["상장시장"] = first_nonempty(
        detect_market_from_pairs(pairs),
        detect_market_from_title(title)
    )

    row["최초 이사회결의일"] = get_valid_date_by_keywords(
        pairs,
        ["최초 이사회결의일", "최초이사회결의일", "이사회결의일", "이사회결의일(결정일)", "결정일"]
    )

    row["권면총액(원)"] = fmt_number(find_numeric_value_by_keywords(
        pairs,
        [
            "사채의 권면총액", "권면총액", "발행총액", "사채의 총액",
            "권면(전자등록)총액", "사채의 권면(전자등록)총액(원)",
            "권면(전자등록)총액(원)", "전자등록총액"
        ]
    ))

    row["Coupon"] = first_nonempty(
        find_value_by_left_keywords(pairs, ["표면이자율", "표면이자율(%)", "표면금리", "이표이자율"])
    )

    row["YTM"] = first_nonempty(
        find_value_by_left_keywords(pairs, ["만기이자율", "만기이자율(%)", "만기보장수익률", "만기수익률", "Yield To Maturity"])
    )

    row["만기"] = get_valid_date_by_keywords(
        pairs,
        ["만기일", "사채만기일", "상환기일", "만기"]
    )

    start_val = first_nonempty(
        find_value_by_left_keywords(pairs, [
            "전환청구기간 시작일", "전환청구 시작일", "권리행사 시작일",
            "교환청구기간 시작일", "교환청구 시작일", "권리행사기간 시작일"
        ])
    )
    end_val = first_nonempty(
        find_value_by_left_keywords(pairs, [
            "전환청구기간 종료일", "전환청구 종료일", "권리행사 종료일",
            "교환청구기간 종료일", "교환청구 종료일", "권리행사기간 종료일"
        ])
    )

    if not start_val or not end_val:
        p_start, p_end = extract_period_dates_from_tables(
            tables,
            ["전환청구기간", "교환청구기간", "권리행사기간"]
        )
        row["전환청구 시작"] = start_val or p_start
        row["전환청구 종료"] = end_val or p_end
    else:
        row["전환청구 시작"] = start_val
        row["전환청구 종료"] = end_val

    put_val = find_value_by_left_keywords(
        pairs,
        ["조기상환청구권", "Put Option", "풋옵션", "조기상환권", "사채권자의 조기상환청구권"]
    )
    call_val = find_value_by_left_keywords(
        pairs,
        ["매도청구권", "Call Option", "콜옵션", "중도상환청구권", "발행회사의 매도청구권"]
    )

    row["Put Option"] = put_val or extract_option_details_from_tables(tables, "put")
    row["Call Option"] = call_val or extract_option_details_from_tables(tables, "call")

    row["Call 비율"] = first_nonempty(
        clean_percent(find_value_by_left_keywords(pairs, [
            "콜옵션 행사비율", "매도청구권 행사비율", "Call 비율",
            "콜옵션 비율", "매도청구권 비율",
            "최대주주등에게 부여된 콜옵션 비율",
            "최대주주등에게 부여된 매도청구권 비율",
            "권면총액 대비 비율", "행사비율"
        ]))
    )

    row["YTC"] = first_nonempty(
        find_value_by_left_keywords(
            pairs,
            ["조기상환수익률", "YTC", "Yield To Call", "조기상환이율", "조기상환수익률(%)", "연복리수익률"]
        )
    )

    if not row["Call 비율"] or not row["YTC"]:
        ratio2, ytc2 = extract_call_ratio_and_ytc_from_text(row["Call Option"])
        if not row["Call 비율"]:
            row["Call 비율"] = ratio2
        if not row["YTC"]:
            row["YTC"] = ytc2

    row["모집방식"] = find_value_by_left_keywords(
        pairs,
        ["공모여부", "모집 또는 매출의 구분", "모집방법", "모집방식", "사채발행방법", "발행방법"]
    )

    row["발행상품"] = extract_product_type_from_pairs_and_tables(pairs, tables) or row["구분"]

    row["행사(전환)가액(원)"] = fmt_number(find_numeric_value_by_keywords(
        pairs,
        [
            "전환가액", "교환가액", "행사가액", "권리행사가액",
            "전환가액(원/주)", "교환가액(원/주)",
            "행사가액(원/주)", "권리행사가액(원/주)"
        ]
    ))

    row["전환주식수"] = fmt_number(find_numeric_value_by_keywords(
        pairs,
        [
            "전환에 따라 발행할 주식수", "전환에 따라 발행할 주식의 수",
            "전환주식수", "교환대상 주식수", "교환대상주식수",
            "행사주식수", "권리행사로 발행할 주식수", "주식수"
        ]
    ))

    row["주식총수대비 비율"] = clean_percent(find_value_by_left_keywords(
        pairs,
        [
            "주식총수 대비 비율", "발행주식총수 대비 비율", "총수대비 비율",
            "주식총수 대비 비율(%)", "발행주식총수 대비 비율(%)"
        ]
    ))

    row["Refixing Floor"] = clean_percent(find_value_by_left_keywords(
        pairs,
        [
            "최저 조정가액", "조정가액 하한", "Refixing Floor", "하한가액",
            "최저 조정가액(원)", "최저조정가액", "리픽싱 하한", "리픽싱하한"
        ]
    ))

    row["납입일"] = get_valid_date_by_keywords(
        pairs,
        ["납입일", "납입기일", "발행일", "지급일"]
    )

    row["자금용도"] = extract_use_of_funds(tables)
    row["투자자"] = extract_investor_text(tables)
    row["링크"] = rec["src_url"]
    row["접수번호"] = rec["acpt_no"]

    if is_correction_title(title):
        v = correction_override_value(
            pairs,
            [
                "전환가액", "교환가액", "행사가액", "권리행사가액",
                "전환가액(원/주)", "교환가액(원/주)",
                "행사가액(원/주)", "권리행사가액(원/주)"
            ]
        )
        if v:
            row["행사(전환)가액(원)"] = fmt_number(parse_float(v))

        v = correction_override_value(pairs, ["납입일", "납입기일"])
        if v and looks_like_valid_date(v):
            row["납입일"] = v

        v = correction_override_value(pairs, ["권면총액", "발행총액", "사채의 권면총액"])
        if v:
            row["권면총액(원)"] = fmt_number(parse_float(v))

        v = correction_override_value(pairs, ["전환주식수", "교환대상 주식수", "행사주식수"])
        if v:
            row["전환주식수"] = fmt_number(parse_float(v))

        v = correction_override_value(pairs, ["만기일", "사채만기일", "상환기일"])
        if v and looks_like_valid_date(v):
            row["만기"] = v

        v = correction_override_value(pairs, ["전환청구기간 시작일", "전환청구 시작일", "교환청구기간 시작일"])
        if v and looks_like_valid_date(v):
            row["전환청구 시작"] = v

        v = correction_override_value(pairs, ["전환청구기간 종료일", "전환청구 종료일", "교환청구기간 종료일"])
        if v and looks_like_valid_date(v):
            row["전환청구 종료"] = v

        v = correction_override_value(pairs, ["콜옵션 행사비율", "매도청구권 행사비율", "Call 비율", "콜옵션 비율"])
        if v:
            row["Call 비율"] = clean_percent(v)

        v = correction_override_value(pairs, ["조기상환수익률", "YTC", "Yield To Call", "연복리수익률"])
        if v:
            row["YTC"] = normalize_text(v)

    for h in BOND_HEADERS:
        if h in ["링크", "접수번호"]:
            continue
        if not normalize_text(row[h]):
            missing.append(h)

    return row, missing


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
        title = rec["title"] or ""

        try:
            if "유상증자결정" in title:
                row, missing, suspicious = parse_rights_record(rec)
                upsert_row(rights_ws, RIGHTS_HEADERS, row, "접수번호")
                write_parse_log(log_ws, acpt_no, title, RIGHTS_SHEET_NAME, "OK", missing, suspicious)
                ok += 1
                print(f"[OK][RIGHTS] {acpt_no} {title}")

            elif any(k in title for k in [
                "전환사채권발행결정",
                "교환사채권발행결정",
                "신주인수권부사채권발행결정",
            ]):
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
