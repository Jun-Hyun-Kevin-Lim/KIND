# validator.py
import os
import json
import re
from datetime import datetime
from typing import List, Dict, Any, Optional

import gspread


GOOGLE_SHEET_ID = os.environ.get("GOOGLE_SHEET_ID", "").strip()
GOOGLE_CREDENTIALS_JSON = (
    os.environ.get("GOOGLE_CREDENTIALS_JSON", "").strip()
    or os.environ.get("GOOGLE_CREDS", "").strip()
)

RIGHTS_SHEET_NAME = os.getenv("RIGHTS_SHEET_NAME", "유상증자")
BOND_SHEET_NAME = os.getenv("BOND_SHEET_NAME", "주식연계채권")
REVIEW_SHEET_NAME = os.getenv("REVIEW_SHEET_NAME", "review_queue")


REVIEW_HEADERS = [
    "접수번호",
    "대상시트",
    "회사명",
    "보고서명",
    "검토등급",
    "의심사유",
    "링크",
    "검토시각",
]


def gs_open():
    if not GOOGLE_SHEET_ID or not GOOGLE_CREDENTIALS_JSON:
        raise RuntimeError("GOOGLE_SHEET_ID / GOOGLE_CREDS(또는 GOOGLE_CREDENTIALS_JSON)가 비어있습니다.")

    creds = json.loads(GOOGLE_CREDENTIALS_JSON)
    gc = gspread.service_account_from_dict(creds)
    sh = gc.open_by_key(GOOGLE_SHEET_ID)
    return sh


def ensure_ws(sh, title: str, rows: int = 2000, cols: int = 30):
    try:
        return sh.worksheet(title)
    except gspread.WorksheetNotFound:
        return sh.add_worksheet(title=title, rows=rows, cols=cols)


def ensure_header(ws, headers: List[str]):
    current = ws.row_values(1)
    if current != headers:
        ws.clear()
        ws.update("A1", [headers])


def normalize_text(x: Any) -> str:
    if x is None:
        return ""
    s = str(x).replace("\xa0", " ")
    s = re.sub(r"\s+", " ", s)
    return s.strip()


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


def _norm(s: Any) -> str:
    return re.sub(r"\s+", "", str(s or "")).replace(":", "")


def _norm_date(s: Any) -> str:
    return re.sub(r"[^\d]", "", str(s or ""))


def get_records(ws) -> List[Dict[str, str]]:
    values = ws.get_all_records()
    out = []
    for row in values:
        clean_row = {str(k): normalize_text(v) for k, v in row.items()}
        out.append(clean_row)
    return out


def add_flag(flags: List[str], level: str, field: str, reason: str, value: Any = ""):
    value_txt = normalize_text(value)
    msg = f"{level}|{field}|{reason}"
    if value_txt:
        msg += f"|값={value_txt[:120]}"
    if msg not in flags:
        flags.append(msg)


def judge_level(flags: List[str]) -> str:
    if not flags:
        return ""
    if any(x.startswith("HIGH|") for x in flags):
        return "REVIEW_HIGH"
    return "REVIEW"


def validate_rights_row(row: Dict[str, str]) -> List[str]:
    flags: List[str] = []

    title = row.get("보고서명", "")
    company = row.get("회사명", "")
    market = row.get("상장시장", "")
    method = row.get("증자방식", "")
    investors = row.get("투자자", "")

    issue_price = parse_float_like(row.get("확정발행가(원)", ""))
    base_price = parse_float_like(row.get("기준주가", ""))
    discount = parse_float_like(row.get("할인(할증률)", ""))
    new_shares = parse_float_like(row.get("신규발행주식수", ""))
    prev_shares = parse_float_like(row.get("증자전 주식수", ""))

    if company and len(company) <= 1:
        add_flag(flags, "HIGH", "회사명", "회사명이 비정상적으로 짧음", company)

    if "[코]" in title and market and market != "코스닥":
        add_flag(flags, "MED", "상장시장", "제목 [코]와 값 불일치", market)
    if "[유]" in title and market and market != "유가증권":
        add_flag(flags, "MED", "상장시장", "제목 [유]와 값 불일치", market)
    if "[넥]" in title and market and market != "코넥스":
        add_flag(flags, "MED", "상장시장", "제목 [넥]와 값 불일치", market)

    if issue_price is not None and issue_price <= 50:
        add_flag(flags, "HIGH", "확정발행가(원)", "50 이하", row.get("확정발행가(원)", ""))

    if base_price is not None and base_price <= 50:
        add_flag(flags, "HIGH", "기준주가", "50 이하", row.get("기준주가", ""))

    if issue_price is not None and base_price not in (None, 0):
        ratio = issue_price / base_price
        if ratio < 0.1 or ratio > 2.5:
            add_flag(
                flags,
                "MED",
                "확정발행가(원)",
                "기준주가 대비 과도하게 벗어남",
                f"확정발행가={row.get('확정발행가(원)', '')}, 기준주가={row.get('기준주가', '')}"
            )

    if discount is not None and issue_price is not None and base_price not in (None, 0):
        if discount > 0 and issue_price > base_price:
            add_flag(flags, "HIGH", "할인(할증률)", "할인인데 확정발행가가 기준주가보다 큼", row.get("할인(할증률)", ""))
        if discount < 0 and issue_price < base_price:
            add_flag(flags, "MED", "할인(할증률)", "할증인데 확정발행가가 기준주가보다 작음", row.get("할인(할증률)", ""))

    if new_shares is not None and prev_shares not in (None, 0):
        ratio_pct = (new_shares / prev_shares) * 100
        if ratio_pct > 300:
            add_flag(flags, "HIGH", "증자비율", "300% 초과", f"{ratio_pct:.2f}%")
        elif ratio_pct > 100:
            add_flag(flags, "MED", "증자비율", "100% 초과", f"{ratio_pct:.2f}%")

    board_date = _norm_date(row.get("이사회결의일", "")) or _norm_date(row.get("최초 이사회결의일", ""))
    pay_date = _norm_date(row.get("납입일", ""))
    list_date = _norm_date(row.get("신주의 상장 예정일", ""))

    if board_date and pay_date and pay_date < board_date:
        add_flag(flags, "HIGH", "납입일", "이사회결의일보다 빠름", row.get("납입일", ""))

    if pay_date and list_date and list_date < pay_date:
        add_flag(flags, "HIGH", "신주의 상장 예정일", "납입일보다 빠름", row.get("신주의 상장 예정일", ""))

    if "제3자배정" in method and not investors:
        add_flag(flags, "HIGH", "투자자", "제3자배정인데 투자자 비어있음", "")

    if investors and any(x in investors for x in ["관계", "합계", "소계", "출자자수", "명"]):
        add_flag(flags, "MED", "투자자", "집계/설명 문구 포함 가능성", investors)

    return flags


def looks_like_option_noise_text(text: str) -> bool:
    s = normalize_text(text)
    if not s:
        return False

    n = _norm(s)

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
        return True

    if len(s) < 25:
        return True

    return False


def validate_bond_row(row: Dict[str, str]) -> List[str]:
    flags: List[str] = []

    bond_type = row.get("구분", "")
    product = row.get("발행상품", "")
    put_text = row.get("Put Option", "")
    call_text = row.get("Call Option", "")

    conv_price = parse_float_like(row.get("행사(전환)가액(원)", ""))
    conv_shares = parse_float_like(row.get("전환주식수", ""))
    call_ratio = parse_float_like(row.get("Call 비율", ""))
    ytc = parse_float_like(row.get("YTC", ""))

    if bond_type == "CB" and product and "전환사채" not in product:
        add_flag(flags, "HIGH", "발행상품", "CB인데 전환사채가 아님", product)
    if bond_type == "EB" and product and "교환사채" not in product:
        add_flag(flags, "HIGH", "발행상품", "EB인데 교환사채가 아님", product)
    if bond_type == "BW" and product and "신주인수권부사채" not in product:
        add_flag(flags, "HIGH", "발행상품", "BW인데 신주인수권부사채가 아님", product)

    if conv_price is not None and conv_price <= 50:
        add_flag(flags, "HIGH", "행사(전환)가액(원)", "50 이하", row.get("행사(전환)가액(원)", ""))

    if conv_shares not in (None, 0) and conv_price in (None, 0):
        add_flag(flags, "MED", "행사(전환)가액(원)", "전환주식수는 있는데 가액 비어있음", row.get("행사(전환)가액(원)", ""))

    start_date = _norm_date(row.get("전환청구 시작", ""))
    end_date = _norm_date(row.get("전환청구 종료", ""))
    pay_date = _norm_date(row.get("납입일", ""))
    maturity_date = _norm_date(row.get("만기", ""))

    if start_date and end_date and start_date > end_date:
        add_flag(flags, "HIGH", "전환청구 시작", "시작일이 종료일보다 늦음", f"{row.get('전환청구 시작', '')} ~ {row.get('전환청구 종료', '')}")

    if pay_date and maturity_date and pay_date > maturity_date:
        add_flag(flags, "HIGH", "납입일", "납입일이 만기보다 늦음", row.get("납입일", ""))

    if call_ratio is not None and not (0 <= call_ratio <= 100):
        add_flag(flags, "HIGH", "Call 비율", "0~100% 범위 벗어남", row.get("Call 비율", ""))

    if ytc is not None and abs(ytc) > 50:
        add_flag(flags, "MED", "YTC", "절대값 50% 초과", row.get("YTC", ""))

    if put_text and looks_like_option_noise_text(put_text):
        add_flag(flags, "MED", "Put Option", "표머리/잡음일 가능성", put_text)

    if call_text and looks_like_option_noise_text(call_text):
        add_flag(flags, "MED", "Call Option", "표머리/잡음일 가능성", call_text)

    return flags


def build_review_rows(sheet_name: str, records: List[Dict[str, str]]) -> List[List[str]]:
    rows: List[List[str]] = []
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for row in records:
        if sheet_name == RIGHTS_SHEET_NAME:
            flags = validate_rights_row(row)
        else:
            flags = validate_bond_row(row)

        if not flags:
            continue

        rows.append([
            row.get("접수번호", ""),
            sheet_name,
            row.get("회사명", ""),
            row.get("보고서명", ""),
            judge_level(flags),
            " || ".join(flags),
            row.get("링크", ""),
            now,
        ])

    return rows


def dedupe_review_rows(rows: List[List[str]]) -> List[List[str]]:
    seen = set()
    out = []
    for r in rows:
        key = (r[0], r[1], r[4], r[5])
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out


def run_validator():
    sh = gs_open()

    rights_ws = ensure_ws(sh, RIGHTS_SHEET_NAME, rows=3000, cols=50)
    bond_ws = ensure_ws(sh, BOND_SHEET_NAME, rows=3000, cols=50)
    review_ws = ensure_ws(sh, REVIEW_SHEET_NAME, rows=5000, cols=20)

    ensure_header(review_ws, REVIEW_HEADERS)

    rights_records = get_records(rights_ws)
    bond_records = get_records(bond_ws)

    rows = []
    rows.extend(build_review_rows(RIGHTS_SHEET_NAME, rights_records))
    rows.extend(build_review_rows(BOND_SHEET_NAME, bond_records))
    rows = dedupe_review_rows(rows)

    review_ws.clear()
    review_ws.update("A1", [REVIEW_HEADERS] + rows)

    print(f"[DONE] review_rows={len(rows)}")


if __name__ == "__main__":
    run_validator()
