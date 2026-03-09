#parser.py
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
    "접수번호", "보고서명", "대상시트", "상태", "누락컬럼", "처리시각"
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
        return "코스피"
    if "[코넥스]" in title:
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
    investor_keys = ["제3자배정 대상자", "배정대상자", "인수인", "투자자", "상대방", "권리자", "취득자"]
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


def parse_rights_record(rec: Dict[str, Any]):
    title = rec["title"]
    tables = rec["tables"]
    pairs = all_pairs_from_tables(tables)

    row = {h: "" for h in RIGHTS_HEADERS}
    missing = []

    row["회사명"] = extract_company_name(title)
    row["보고서명"] = detect_report_type(title) or title
    row["상장시장"] = detect_market_from_title(title)
    row["최초 이사회결의일"] = find_value_by_left_keywords(pairs, ["최초 이사회결의일"])
    row["증자방식"] = find_value_by_left_keywords(pairs, ["증자방식", "배정방법", "배정방식"])
    row["발행상품"] = find_value_by_left_keywords(pairs, ["발행할 주식의 종류", "주식의 종류", "발행상품"])
    row["신규발행주식수"] = fmt_number(find_numeric_value_by_keywords(pairs, ["신주발행수", "신규발행주식수", "발행주식수", "발행할 주식의 총수"]))
    row["확정발행가(원)"] = fmt_number(find_numeric_value_by_keywords(pairs, ["확정발행가액", "확정 발행가액", "확정발행가", "발행가액", "1주당 발행가액"]))
    row["기준주가"] = fmt_number(find_numeric_value_by_keywords(pairs, ["기준주가", "산정기준주가"]))
    row["할인(할증률)"] = clean_percent(find_value_by_left_keywords(pairs, ["할인율", "할인(할증)율", "할인(할증률)", "할증률"]))
    row["증자전 주식수"] = fmt_number(find_numeric_value_by_keywords(pairs, ["증자전 발행주식총수", "증자전 주식수", "발행주식총수(증자전)", "기발행주식수"]))
    row["납입일"] = find_value_by_left_keywords(pairs, ["납입일"])
    row["신주의 배당기산일"] = find_value_by_left_keywords(pairs, ["신주의 배당기산일"])
    row["신주의 상장 예정일"] = first_nonempty(
        find_value_by_left_keywords(pairs, ["신주의 상장예정일"]),
        find_value_by_left_keywords(pairs, ["신주의 상장 예정일"])
    )
    row["이사회결의일"] = first_nonempty(
        find_value_by_left_keywords(pairs, ["이사회결의일"]),
        find_value_by_left_keywords(pairs, ["이사회 결의일"])
    )
    row["자금용도"] = extract_use_of_funds(tables)
    row["투자자"] = extract_investor_text(tables)
    row["링크"] = rec["src_url"]
    row["접수번호"] = rec["acpt_no"]

    if is_correction_title(title):
        v = correction_override_value(pairs, ["확정발행가액", "확정발행가", "발행가액"])
        if v:
            row["확정발행가(원)"] = fmt_number(parse_float(v))

        v = correction_override_value(pairs, ["납입일"])
        if v:
            row["납입일"] = v

        v = correction_override_value(pairs, ["신주의 상장예정일", "신주의 상장 예정일"])
        if v:
            row["신주의 상장 예정일"] = v

    use_of_funds_total = extract_use_of_funds_total_won(tables)
    new_shares = parse_float(row["신규발행주식수"])
    price = parse_float(row["확정발행가(원)"])

    if use_of_funds_total is not None and use_of_funds_total > 0:
        row["확정발행금액(억원)"] = fmt_eok_from_won(use_of_funds_total)
    elif new_shares is not None and price is not None:
        row["확정발행금액(억원)"] = fmt_eok_from_won(new_shares * price)

    pre_shares = parse_float(row["증자전 주식수"])
    if new_shares is not None and pre_shares not in (None, 0):
        row["증자비율"] = f"{(new_shares / pre_shares) * 100:.2f}%"

    for h in RIGHTS_HEADERS:
        if h in ["링크", "접수번호"]:
            continue
        if not normalize_text(row[h]):
            missing.append(h)

    return row, missing


def parse_bond_record(rec: Dict[str, Any]):
    title = rec["title"]
    tables = rec["tables"]
    pairs = all_pairs_from_tables(tables)

    row = {h: "" for h in BOND_HEADERS}
    missing = []

    row["구분"] = bond_type_kor(title)
    row["회사명"] = extract_company_name(title)
    row["보고서명"] = detect_report_type(title) or title
    row["상장시장"] = detect_market_from_title(title)
    row["최초 이사회결의일"] = find_value_by_left_keywords(pairs, ["최초 이사회결의일"])
    row["권면총액(원)"] = fmt_number(find_numeric_value_by_keywords(pairs, ["사채의 권면총액", "권면총액", "발행총액"]))
    row["Coupon"] = first_nonempty(find_value_by_left_keywords(pairs, ["표면이자율"]), find_value_by_left_keywords(pairs, ["표면금리"]))
    row["YTM"] = first_nonempty(find_value_by_left_keywords(pairs, ["만기이자율"]), find_value_by_left_keywords(pairs, ["만기보장수익률"]), find_value_by_left_keywords(pairs, ["Yield To Maturity"]))
    row["만기"] = first_nonempty(find_value_by_left_keywords(pairs, ["만기일"]), find_value_by_left_keywords(pairs, ["사채만기일"]))
    row["전환청구 시작"] = first_nonempty(find_value_by_left_keywords(pairs, ["전환청구기간 시작일"]), find_value_by_left_keywords(pairs, ["전환청구 시작일"]), find_value_by_left_keywords(pairs, ["권리행사 시작일"]))
    row["전환청구 종료"] = first_nonempty(find_value_by_left_keywords(pairs, ["전환청구기간 종료일"]), find_value_by_left_keywords(pairs, ["전환청구 종료일"]), find_value_by_left_keywords(pairs, ["권리행사 종료일"]))
    row["Put Option"] = find_value_by_left_keywords(pairs, ["조기상환청구권", "Put Option", "풋옵션"])
    row["Call Option"] = find_value_by_left_keywords(pairs, ["매도청구권", "Call Option", "콜옵션"])
    row["Call 비율"] = first_nonempty(
        clean_percent(find_value_by_left_keywords(pairs, ["콜옵션 행사비율", "매도청구권 행사비율", "Call 비율"])),
        clean_percent(find_value_by_left_keywords(pairs, ["최대주주등에게 부여된 콜옵션 비율"]))
    )
    row["YTC"] = find_value_by_left_keywords(pairs, ["조기상환수익률", "YTC", "Yield To Call"])
    row["모집방식"] = find_value_by_left_keywords(pairs, ["공모여부", "모집 또는 매출의 구분", "모집방법", "모집방식"])
    row["발행상품"] = row["구분"]
    row["행사(전환)가액(원)"] = fmt_number(find_numeric_value_by_keywords(pairs, ["전환가액", "교환가액", "행사가액", "권리행사가액"]))
    row["전환주식수"] = fmt_number(find_numeric_value_by_keywords(pairs, ["전환에 따라 발행할 주식수", "전환주식수", "교환대상 주식수", "행사주식수"]))
    row["주식총수대비 비율"] = clean_percent(find_value_by_left_keywords(pairs, ["주식총수 대비 비율", "발행주식총수 대비 비율", "총수대비 비율"]))
    row["Refixing Floor"] = clean_percent(find_value_by_left_keywords(pairs, ["최저 조정가액", "조정가액 하한", "Refixing Floor", "하한가액"]))
    row["납입일"] = find_value_by_left_keywords(pairs, ["납입일"])
    row["자금용도"] = extract_use_of_funds(tables)
    row["투자자"] = extract_investor_text(tables)
    row["링크"] = rec["src_url"]
    row["접수번호"] = rec["acpt_no"]

    if is_correction_title(title):
        v = correction_override_value(pairs, ["전환가액", "교환가액", "행사가액", "권리행사가액"])
        if v:
            row["행사(전환)가액(원)"] = fmt_number(parse_float(v))
        v = correction_override_value(pairs, ["납입일"])
        if v:
            row["납입일"] = v

    for h in BOND_HEADERS:
        if h in ["링크", "접수번호"]:
            continue
        if not normalize_text(row[h]):
            missing.append(h)

    return row, missing


def write_parse_log(log_ws, acpt_no: str, title: str, target_sheet: str, status: str, missing: List[str]):
    log_ws.append_row([
        acpt_no,
        title,
        target_sheet,
        status,
        ", ".join(missing),
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
                row, missing = parse_rights_record(rec)
                upsert_row(rights_ws, RIGHTS_HEADERS, row, "접수번호")
                write_parse_log(log_ws, acpt_no, title, RIGHTS_SHEET_NAME, "OK", missing)
                ok += 1
                print(f"[OK][RIGHTS] {acpt_no} {title}")

            elif any(k in title for k in [
                "전환사채권발행결정",
                "교환사채권발행결정",
                "신주인수권부사채권발행결정",
            ]):
                row, missing = parse_bond_record(rec)
                upsert_row(bond_ws, BOND_HEADERS, row, "접수번호")
                write_parse_log(log_ws, acpt_no, title, BOND_SHEET_NAME, "OK", missing)
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
