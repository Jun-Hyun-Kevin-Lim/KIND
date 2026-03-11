import os
import json
import re
from datetime import datetime, timedelta
from typing import List, Dict, Any, Set, Tuple

import gspread


GOOGLE_SHEET_ID = os.environ.get("GOOGLE_SHEET_ID", "").strip()
GOOGLE_CREDENTIALS_JSON = (
    os.environ.get("GOOGLE_CREDENTIALS_JSON", "").strip()
    or os.environ.get("GOOGLE_CREDS", "").strip()
)

RIGHTS_SHEET_NAME = os.getenv("RIGHTS_SHEET_NAME", "K_유상증자")
BOND_SHEET_NAME = os.getenv("BOND_SHEET_NAME", "K_주식연계채권")
SEEN_SHEET_NAME = os.getenv("SEEN_SHEET_NAME", "seen")
REVIEW_SHEET_NAME = os.getenv("REVIEW_SHEET_NAME", "review_queue")
REVIEW_LOOKBACK_MINUTES = int(os.getenv("REVIEW_LOOKBACK_MINUTES", "20"))

REVIEW_HEADERS = [
    "접수번호",
    "대상시트",
    "회사명",
    "보고서명",
    "검토등급",
    "의심사유",
    "누락컬럼",
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
    """
    헤더가 바뀌어도 기존 데이터를 최대한 보존하면서 새 헤더로 맞춤.
    (기존 ensure_header처럼 clear만 하면 review_queue 이력이 날아갈 수 있어서 개선)
    """
    values = ws.get_all_values()

    if not values:
        ws.update("A1", [headers])
        return

    current_headers = values[0]
    if current_headers == headers:
        return

    old_idx = {h: i for i, h in enumerate(current_headers)}
    migrated_rows = []

    for row in values[1:]:
        new_row = []
        for h in headers:
            if h in old_idx and old_idx[h] < len(row):
                new_row.append(row[old_idx[h]])
            else:
                new_row.append("")
        migrated_rows.append(new_row)

    ws.clear()
    ws.update("A1", [headers] + migrated_rows)


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


def parse_dt(s: str):
    s = normalize_text(s)
    if not s:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y.%m.%d %H:%M:%S"):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            pass
    return None


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


def get_recent_acptnos_from_seen(seen_ws, lookback_minutes: int) -> Set[str]:
    values = seen_ws.get_all_values()
    if not values:
        return set()

    cutoff = datetime.now() - timedelta(minutes=lookback_minutes)
    recent: Set[str] = set()

    for row in values[1:]:
        acpt_no = row[0].strip() if len(row) > 0 else ""
        processed_at = row[2].strip() if len(row) > 2 else ""

        if not acpt_no.isdigit():
            continue

        dt = parse_dt(processed_at)
        if not dt:
            continue

        if dt >= cutoff:
            recent.add(acpt_no)

    return recent


def get_sheet_records_by_acpt(ws, target_acptnos: Set[str]) -> List[Dict[str, str]]:
    all_rows = ws.get_all_records()
    out = []
    for row in all_rows:
        clean_row = {str(k): normalize_text(v) for k, v in row.items()}
        if clean_row.get("접수번호", "") in target_acptnos:
            out.append(clean_row)
    return out


def get_existing_review_keys(review_ws) -> Set[Tuple[str, str, str]]:
    vals = review_ws.get_all_records()
    keys = set()
    for row in vals:
        acpt = normalize_text(row.get("접수번호", ""))
        sheet_name = normalize_text(row.get("대상시트", ""))
        reason = normalize_text(row.get("의심사유", ""))
        if acpt and sheet_name and reason:
            keys.add((acpt, sheet_name, reason))
    return keys


def get_missing_columns_rights(row: Dict[str, str]) -> List[str]:
    missing: List[str] = []

    required_common = [
        "접수번호",
        "회사명",
        "보고서명",
        "상장시장",
        "증자방식",
        "발행상품",
        "신규발행주식수",
        "납입일",
        "링크",
    ]

    for field in required_common:
        if not normalize_text(row.get(field, "")):
            missing.append(field)

    if not (
        _norm_date(row.get("이사회결의일", ""))
        or _norm_date(row.get("최초 이사회결의일", ""))
    ):
        missing.append("이사회결의일/최초 이사회결의일")

    if not normalize_text(row.get("확정발행가(원)", "")):
        missing.append("확정발행가(원)")

    if not normalize_text(row.get("기준주가", "")):
        missing.append("기준주가")

    if not normalize_text(row.get("신주의 상장 예정일", "")):
        missing.append("신주의 상장 예정일")

    method = normalize_text(row.get("증자방식", ""))
    if "제3자배정" in method and not normalize_text(row.get("투자자", "")):
        missing.append("투자자")

    return list(dict.fromkeys(missing))


def get_missing_columns_bond(row: Dict[str, str]) -> List[str]:
    missing: List[str] = []

    required_common = [
        "접수번호",
        "회사명",
        "보고서명",
        "구분",
        "발행상품",
        "납입일",
        "만기",
        "링크",
    ]

    for field in required_common:
        if not normalize_text(row.get(field, "")):
            missing.append(field)

    bond_type = normalize_text(row.get("구분", ""))

    # 공통적으로 중요하게 보는 값
    for field in ["행사(전환)가액(원)", "전환청구 시작", "전환청구 종료"]:
        if not normalize_text(row.get(field, "")):
            missing.append(field)

    # CB / BW는 전환주식수도 핵심
    if bond_type in ("CB", "BW"):
        if not normalize_text(row.get("전환주식수", "")):
            missing.append("전환주식수")

    return list(dict.fromkeys(missing))


def get_missing_columns(sheet_name: str, row: Dict[str, str]) -> List[str]:
    if sheet_name == RIGHTS_SHEET_NAME:
        return get_missing_columns_rights(row)
    return get_missing_columns_bond(row)


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


def build_review_rows(sheet_name: str, records: List[Dict[str, str]], existing_keys: Set[Tuple[str, str, str]]) -> List[List[str]]:
    rows: List[List[str]] = []
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for row in records:
        if sheet_name == RIGHTS_SHEET_NAME:
            flags = validate_rights_row(row)
        else:
            flags = validate_bond_row(row)

        missing_cols = get_missing_columns(sheet_name, row)
        missing_cols_text = ", ".join(missing_cols)

        if missing_cols:
            add_flag(flags, "MED", "누락컬럼", "필수값 비어있음", missing_cols_text)

        if not flags:
            continue

        reason = " || ".join(flags)
        key = (row.get("접수번호", ""), sheet_name, reason)
        if key in existing_keys:
            continue

        rows.append([
            row.get("접수번호", ""),
            sheet_name,
            row.get("회사명", ""),
            row.get("보고서명", ""),
            judge_level(flags),
            reason,
            missing_cols_text,
            row.get("링크", ""),
            now,
        ])

    return rows


def run_validator():
    sh = gs_open()

    seen_ws = ensure_ws(sh, SEEN_SHEET_NAME, rows=2000, cols=5)
    rights_ws = ensure_ws(sh, RIGHTS_SHEET_NAME, rows=3000, cols=50)
    bond_ws = ensure_ws(sh, BOND_SHEET_NAME, rows=3000, cols=50)
    review_ws = ensure_ws(sh, REVIEW_SHEET_NAME, rows=5000, cols=20)

    ensure_header(review_ws, REVIEW_HEADERS)

    recent_acptnos = get_recent_acptnos_from_seen(seen_ws, REVIEW_LOOKBACK_MINUTES)

    if not recent_acptnos:
        print("[INFO] 최근 유입된 접수번호가 없어 validator 종료")
        return

    rights_records = get_sheet_records_by_acpt(rights_ws, recent_acptnos)
    bond_records = get_sheet_records_by_acpt(bond_ws, recent_acptnos)
    existing_keys = get_existing_review_keys(review_ws)

    rows = []
    rows.extend(build_review_rows(RIGHTS_SHEET_NAME, rights_records, existing_keys))
    rows.extend(build_review_rows(BOND_SHEET_NAME, bond_records, existing_keys))

    if rows:
        review_ws.append_rows(rows, value_input_option="RAW")
        print(f"[DONE] recent_acptnos={len(recent_acptnos)} review_added={len(rows)}")
    else:
        print(f"[DONE] recent_acptnos={len(recent_acptnos)} review_added=0")


if __name__ == "__main__":
    run_validator()
