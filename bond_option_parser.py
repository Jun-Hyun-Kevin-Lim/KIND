import os
import json
import time
import random
from typing import Dict, List, Any, Optional

import gspread
import pandas as pd
from gspread.exceptions import APIError
from gspread.utils import rowcol_to_a1

# ==========================================================
# [환경변수]
# ==========================================================
GOOGLE_SHEET_ID = os.environ.get("GOOGLE_SHEET_ID", "").strip()
GOOGLE_CREDENTIALS_JSON = (
    os.environ.get("GOOGLE_CREDENTIALS_JSON", "").strip()
    or os.environ.get("GOOGLE_CREDS", "").strip()
)

RAW_SHEET_NAME = os.getenv("DUMP_SHEET_NAME", "RAW_dump")
BOND_SHEET_NAME = os.getenv("BOND_SHEET_NAME", "주식연계채권")

RUN_ONLY_ACPTNO = os.getenv("RUN_ONLY_ACPTNO", "").strip()


# ==========================================================
# [헤더 후보]
# - 네 시트 헤더명이 다르면 여기만 수정
# ==========================================================
RAW_ACPTNO_CANDIDATES = ["접수번호", "acptNo", "acptno"]
RAW_TITLE_CANDIDATES = ["보고서명", "title", "공시명"]
RAW_TABLES_CANDIDATES = ["tables", "테이블"]

BOND_ACPTNO_CANDIDATES = ["접수번호", "acptNo", "acptno"]

PUT_COL_CANDIDATES = ["Put Option", "Put옵션", "Put"]
CALL_COL_CANDIDATES = ["Call Option", "Call옵션", "Call"]
CALL_RATIO_COL_CANDIDATES = ["Call 비율", "콜옵션 비율"]
YTC_COL_CANDIDATES = ["YTC"]


# ==========================================================
# [Google Sheets retry]
# - 429 쿼터 초과 시 지수 백오프
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
def _require_env(name: str, value: str):
    if not value:
        raise RuntimeError(f"환경변수 누락: {name}")


def _normalize_header(s: Any) -> str:
    return str(s).strip()


def _header_to_col_map(header_row: List[Any]) -> Dict[str, int]:
    """
    header -> 1-based column index
    """
    out = {}
    for i, h in enumerate(header_row, start=1):
        key = _normalize_header(h)
        if key:
            out[key] = i
    return out


def _find_col(header_map: Dict[str, int], candidates: List[str]) -> Optional[int]:
    for c in candidates:
        if c in header_map:
            return header_map[c]
    return None


def _row_to_dict(header_row: List[Any], row: List[Any]) -> Dict[str, Any]:
    out = {}
    max_len = max(len(header_row), len(row))
    for i in range(max_len):
        k = header_row[i] if i < len(header_row) else f"__extra_{i}"
        v = row[i] if i < len(row) else ""
        out[str(k).strip()] = v
    return out


def _first_nonempty_from_dict(d: Dict[str, Any], keys: List[str]) -> str:
    for k in keys:
        v = d.get(k, "")
        if v is None:
            continue
        s = str(v).strip()
        if s:
            return s
    return ""


# ==========================================================
# [tables 파싱]
# - RAW_dump의 tables 셀(JSON 문자열)을 List[pd.DataFrame]로 변환
# - 네 RAW_dump 구조가 조금 달라도 최대한 유연하게 받도록 작성
# ==========================================================
def _to_dataframe(obj: Any) -> Optional[pd.DataFrame]:
    try:
        if isinstance(obj, pd.DataFrame):
            return obj

        if isinstance(obj, dict):
            if "data" in obj and isinstance(obj["data"], list):
                return pd.DataFrame(obj["data"])
            if "rows" in obj and isinstance(obj["rows"], list):
                return pd.DataFrame(obj["rows"])
            return pd.DataFrame(obj)

        if isinstance(obj, list):
            return pd.DataFrame(obj)
    except Exception:
        return None

    return None


def _parse_tables_cell(cell: Any) -> List[pd.DataFrame]:
    if cell is None:
        return []

    if isinstance(cell, list):
        raw = cell
    else:
        s = str(cell).strip()
        if not s:
            return []

        try:
            raw = json.loads(s)
        except Exception:
            return []

    if not isinstance(raw, list):
        return []

    out: List[pd.DataFrame] = []
    for item in raw:
        df = _to_dataframe(item)
        if df is not None:
            out.append(df)

    return out


# ==========================================================
# [워크시트 열기]
# ==========================================================
def open_worksheets():
    _require_env("GOOGLE_SHEET_ID", GOOGLE_SHEET_ID)
    _require_env("GOOGLE_CREDENTIALS_JSON", GOOGLE_CREDENTIALS_JSON)

    creds = json.loads(GOOGLE_CREDENTIALS_JSON)
    gc = gspread.service_account_from_dict(creds)
    sh = gs_retry(gc.open_by_key, GOOGLE_SHEET_ID)

    raw_ws = gs_retry(sh.worksheet, RAW_SHEET_NAME)
    bond_ws = gs_retry(sh.worksheet, BOND_SHEET_NAME)

    return raw_ws, bond_ws


# ==========================================================
# [RAW_dump 전체 읽기]
# - 한 번만 읽는다 (중요)
# ==========================================================
def load_raw_records(raw_ws) -> List[Dict[str, Any]]:
    values = gs_retry(raw_ws.get_all_values)
    if not values:
        return []

    header = values[0]
    rows = values[1:]

    records = []
    for row in rows:
        d = _row_to_dict(header, row)

        acptno = _first_nonempty_from_dict(d, RAW_ACPTNO_CANDIDATES)
        title = _first_nonempty_from_dict(d, RAW_TITLE_CANDIDATES)
        tables_cell = d.get(RAW_TABLES_CANDIDATES[0], "")

        if not acptno or not title:
            continue

        tables = _parse_tables_cell(tables_cell)

        records.append(
            {
                "acptNo": acptno,
                "title": title,
                "tables": tables,
            }
        )

    return records


# ==========================================================
# [주식연계채권 시트 전체 읽기 + row map 생성]
# - 이것도 한 번만 읽는다 (중요)
# ==========================================================
def build_bond_sheet_context(bond_ws):
    values = gs_retry(bond_ws.get_all_values)
    if not values:
        raise RuntimeError(f"{BOND_SHEET_NAME} 시트가 비어 있음")

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
        raise RuntimeError(f"{BOND_SHEET_NAME} 시트 헤더 누락: {', '.join(missing)}")

    row_map: Dict[str, int] = {}
    for i, row in enumerate(rows, start=2):
        acptno = ""
        if len(row) >= acptno_col:
            acptno = str(row[acptno_col - 1]).strip()
        if acptno:
            row_map[acptno] = i

    return {
        "header_map": header_map,
        "row_map": row_map,
        "acptno_col": acptno_col,
        "put_col": put_col,
        "call_col": call_col,
        "ratio_col": ratio_col,
        "ytc_col": ytc_col,
    }


# ==========================================================
# [1행 업데이트]
# - ws.update 대신 batch_update 사용
# - 그래서 DeprecationWarning도 사라짐
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
    data = [
        {
            "range": rowcol_to_a1(row_num, put_col),
            "values": [[parsed.get("Put Option", "")]],
        },
        {
            "range": rowcol_to_a1(row_num, call_col),
            "values": [[parsed.get("Call Option", "")]],
        },
        {
            "range": rowcol_to_a1(row_num, ratio_col),
            "values": [[parsed.get("Call 비율", "")]],
        },
        {
            "range": rowcol_to_a1(row_num, ytc_col),
            "values": [[parsed.get("YTC", "")]],
        },
    ]
    gs_retry(ws.batch_update, data)


# ==========================================================
# [메인]
# ==========================================================
def main():
    raw_ws, bond_ws = open_worksheets()

    # 1) RAW_dump 한 번 읽기
    raw_records = load_raw_records(raw_ws)

    # 2) 주식연계채권 시트 한 번 읽기 + row map 생성
    ctx = build_bond_sheet_context(bond_ws)
    row_map = ctx["row_map"]

    # 3) RUN_ONLY_ACPTNO 필터
    if RUN_ONLY_ACPTNO:
        raw_records = [r for r in raw_records if r.get("acptNo") == RUN_ONLY_ACPTNO]

    ok = 0
    skip = 0
    fail = 0

    for rec in raw_records:
        acptno = str(rec.get("acptNo", "")).strip()
        title = str(rec.get("title", "")).strip()

        if not acptno:
            continue

        # 대상 시트에 없는 접수번호면 스킵
        row_num = row_map.get(acptno)
        if not row_num:
            continue

        try:
            parsed = parse_bond_option_record(rec)

            # 혹시 파서가 비어 있어도 안전하게 처리
            put_val = str(parsed.get("Put Option", "") or "").strip()
            call_val = str(parsed.get("Call Option", "") or "").strip()
            ratio_val = str(parsed.get("Call 비율", "") or "").strip()
            ytc_val = str(parsed.get("YTC", "") or "").strip()

            if not put_val:
                parsed["Put Option"] = "공시 확인 바람"
            if not call_val:
                parsed["Call Option"] = "공시 확인 바람"

            update_option_row(
                bond_ws,
                row_num=row_num,
                put_col=ctx["put_col"],
                call_col=ctx["call_col"],
                ratio_col=ctx["ratio_col"],
                ytc_col=ctx["ytc_col"],
                parsed=parsed,
            )

            put_found = parsed["Put Option"] != "공시 확인 바람"
            call_found = parsed["Call Option"] != "공시 확인 바람"

            print(
                f"[OK][OPTION][UPDATE] {acptno} {title} "
                f"(row={row_num}, put={'Y' if put_found else 'N'}, call={'Y' if call_found else 'N'})"
            )
            ok += 1

            # 쓰기 간격을 아주 조금만 줘도 안정적
            time.sleep(0.15)

        except Exception as e:
            print(f"[FAIL][OPTION] {acptno} {title} :: {e}")
            fail += 1

    print(f"[DONE][OPTION] ok={ok} skip={skip} fail={fail}")


if __name__ == "__main__":
    main()
