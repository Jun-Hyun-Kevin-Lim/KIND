import os
import time
import random
from typing import Dict, Any, Optional

from gspread.exceptions import APIError
from gspread.utils import rowcol_to_a1

from parser import gs_open, ensure_ws, load_raw_records, clean_title
from bond_option_parser import parse_bond_option_record


# ==========================================================
# [환경변수]
# ==========================================================
RAW_SHEET_NAME = os.getenv("DUMP_SHEET_NAME", "RAW_dump")
BOND_SHEET_NAME = os.getenv("BOND_SHEET_NAME", "K_주식연계채권")
RUN_ONLY_ACPTNO = os.getenv("RUN_ONLY_ACPTNO", "").strip()


# ==========================================================
# [헤더 후보]
# ==========================================================
BOND_ACPTNO_CANDIDATES = ["접수번호", "acptNo", "acptno"]

PUT_COL_CANDIDATES = ["Put Option", "Put옵션", "Put"]
CALL_COL_CANDIDATES = ["Call Option", "Call옵션", "Call"]
CALL_RATIO_COL_CANDIDATES = ["Call 비율", "콜옵션 비율"]
YTC_COL_CANDIDATES = ["YTC"]


# ==========================================================
# [Google Sheets retry]
# ==========================================================
def gs_retry(fn, *args, **kwargs):
    last_err = None
    for attempt in range(6):
        try:
            return fn(*args, **kwargs)
        except APIError as e:
            last_err = e
            msg = str(e)
            if "429" in msg or "Quota exceeded" in msg:
                sleep_s = (2 ** attempt) + random.uniform(0.3, 1.2)
                time.sleep(sleep_s)
                continue
            raise
    raise last_err if last_err else RuntimeError("Unknown Google Sheets error")


# ==========================================================
# [공통 유틸]
# ==========================================================
def _normalize_header(s: Any) -> str:
    return str(s).strip()


def _header_to_col_map(header_row):
    out = {}
    for i, h in enumerate(header_row, start=1):
        key = _normalize_header(h)
        if key:
            out[key] = i
    return out


def _find_col(header_map: Dict[str, int], candidates) -> Optional[int]:
    for c in candidates:
        if c in header_map:
            return header_map[c]
    return None


def _truncate_sheet_text(value: Any, limit: int = 49000) -> str:
    s = "" if value is None else str(value)
    if len(s) <= limit:
        return s
    return s[: limit - 20] + " ...[TRUNCATED]"


# ==========================================================
# [워크시트 열기]
# ==========================================================
def open_worksheets():
    sh = gs_open()

    raw_ws = ensure_ws(sh, RAW_SHEET_NAME, rows=5000, cols=250)
    bond_ws = ensure_ws(sh, BOND_SHEET_NAME, rows=3000, cols=60)

    return raw_ws, bond_ws


# ==========================================================
# [주식연계채권 시트 전체 읽기 + row map]
# ==========================================================
def build_bond_sheet_context(bond_ws):
    values = gs_retry(bond_ws.get_all_values)
    if not values:
        raise RuntimeError(f"{BOND_SHEET_NAME} 시트가 비어 있습니다.")

    header = values[0]
    rows = values[1:]
    header_map = _header_to_col_map(header)

    acptno_col = _find_col(header_map, BOND_ACPTNO_CANDIDATES)
    put_col = _find_col(header_map, PUT_COL_CANDIDATES)
    call_col = _find_col(header_map, CALL_COL_CANDIDATES)
    ratio_col = _find_col(header_map, CALL_RATIO_COL_CANDIDATES)
    ytc_col = _find_col(header_map, YTC_COL_CANDIDATES)

    missing = []
    if not acptno_col:
        missing.append("접수번호")
    if not put_col:
        missing.append("Put Option")
    if not call_col:
        missing.append("Call Option")
    if not ratio_col:
        missing.append("Call 비율")
    if not ytc_col:
        missing.append("YTC")

    if missing:
        raise RuntimeError(
            f"{BOND_SHEET_NAME} 시트 헤더 누락: {', '.join(missing)}"
        )

    row_map: Dict[str, int] = {}
    for i, row in enumerate(rows, start=2):
        acptno = ""
        if len(row) >= acptno_col:
            acptno = str(row[acptno_col - 1]).strip()
        if acptno:
            row_map[acptno] = i

    return {
        "row_map": row_map,
        "put_col": put_col,
        "call_col": call_col,
        "ratio_col": ratio_col,
        "ytc_col": ytc_col,
    }


# ==========================================================
# [1행 업데이트]
# ==========================================================
def update_option_row(
    ws,
    row_num: int,
    put_col: int,
    call_col: int,
    ratio_col: int,
    ytc_col: int,
    parsed: Dict[str, str],
):
    put_val = _truncate_sheet_text(parsed.get("Put Option", ""))
    call_val = _truncate_sheet_text(parsed.get("Call Option", ""))
    ratio_val = _truncate_sheet_text(parsed.get("Call 비율", ""))
    ytc_val = _truncate_sheet_text(parsed.get("YTC", ""))

    data = [
        {
            "range": rowcol_to_a1(row_num, put_col),
            "values": [[put_val]],
        },
        {
            "range": rowcol_to_a1(row_num, call_col),
            "values": [[call_val]],
        },
        {
            "range": rowcol_to_a1(row_num, ratio_col),
            "values": [[ratio_val]],
        },
        {
            "range": rowcol_to_a1(row_num, ytc_col),
            "values": [[ytc_val]],
        },
    ]

    gs_retry(ws.batch_update, data)


# ==========================================================
# [주식연계채권 공시 여부]
# ==========================================================
def is_bond_title(title: str) -> bool:
    t = (title or "").replace(" ", "")
    return any(
        k in t
        for k in [
            "전환사채권발행결정",
            "교환사채권발행결정",
            "신주인수권부사채권발행결정",
        ]
    )


# ==========================================================
# [메인]
# ==========================================================
def main():
    raw_ws, bond_ws = open_worksheets()

    # parser.py의 RAW 구조 로더를 그대로 사용
    raw_records = load_raw_records(raw_ws)
    raw_records = [
        r for r in raw_records
        if is_bond_title(clean_title(r.get("title", "")))
    ]

    if RUN_ONLY_ACPTNO:
        raw_records = [
            r for r in raw_records
            if str(r.get("acpt_no", "")).strip() == RUN_ONLY_ACPTNO
        ]

    ctx = build_bond_sheet_context(bond_ws)
    row_map = ctx["row_map"]

    print(f"[DEBUG] RAW bond records = {len(raw_records)}")
    print(f"[DEBUG] Bond sheet rows  = {len(row_map)}")
    print(f"[DEBUG] Target sheet     = {BOND_SHEET_NAME}")

    ok = 0
    skip = 0
    fail = 0

    for rec in raw_records:
        acptno = str(rec.get("acpt_no", "")).strip()
        title = clean_title(rec.get("title", "") or "")

        if not acptno:
            skip += 1
            print(f"[SKIP][NO_ACPTNO] {title}")
            continue

        row_num = row_map.get(acptno)
        if not row_num:
            skip += 1
            print(f"[SKIP][NO_ROW_IN_BOND] {acptno} {title}")
            continue

        try:
            parsed = parse_bond_option_record(rec)

            if not str(parsed.get("Put Option", "")).strip():
                parsed["Put Option"] = "공시 확인 바람"

            update_option_row(
                bond_ws,
                row_num=row_num,
                put_col=ctx["put_col"],
                call_col=ctx["call_col"],
                ratio_col=ctx["ratio_col"],
                ytc_col=ctx["ytc_col"],
                parsed=parsed,
            )

            put_found = parsed.get("Put Option", "") != "공시 확인 바람"
            call_found = bool(str(parsed.get("Call Option", "")).strip())

            print(
                f"[OK][OPTION][UPDATE] {acptno} {title} "
                f"(row={row_num}, put={'Y' if put_found else 'N'}, call={'Y' if call_found else 'N'})"
            )
            ok += 1
            time.sleep(0.15)

        except Exception as e:
            print(f"[FAIL][OPTION] {acptno} {title} :: {e}")
            fail += 1

    print(f"[DONE][OPTION] ok={ok} skip={skip} fail={fail}")


if __name__ == "__main__":
    main()
