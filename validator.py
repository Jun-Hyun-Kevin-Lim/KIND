import os
import re
import json
import time
import random
from functools import lru_cache
from typing import Dict, List, Optional, Any

import gspread
from gspread.exceptions import APIError
import pandas as pd


# ==========================================================
# [환경변수 / 시트명 설정]
# ==========================================================
GOOGLE_SHEET_ID = os.environ.get("GOOGLE_SHEET_ID", "").strip()
GOOGLE_CREDENTIALS_JSON = (
    os.environ.get("GOOGLE_CREDENTIALS_JSON", "").strip()
    or os.environ.get("GOOGLE_CREDS", "").strip()
)

RIGHTS_SHEET_NAME = os.getenv("RIGHTS_SHEET_NAME", "K_유상증자")
BOND_SHEET_NAME = os.getenv("BOND_SHEET_NAME", "K_주식연계채권")
RAW_SHEET_NAME = os.getenv("DUMP_SHEET_NAME", "RAW_dump")
REVIEW_SHEET_NAME = os.getenv("REVIEW_QUEUE_SHEET_NAME", "review_queue")


# ==========================================================
# [Google Sheets quota-safe helpers]
# ==========================================================
def _is_quota_error(e: Exception) -> bool:
    s = str(e)
    return (
        "429" in s
        or "Quota exceeded" in s
        or "Read requests per minute per user" in s
        or "RESOURCE_EXHAUSTED" in s
    )


def _gs_call(func, *args, max_retries=6, max_backoff=32, **kwargs):
    for attempt in range(max_retries):
        try:
            return func(*args, **kwargs)
        except APIError as e:
            if not _is_quota_error(e):
                raise

            if attempt == max_retries - 1:
                raise

            sleep_sec = min((2 ** attempt) + random.uniform(0.3, 1.0), max_backoff)
            print(f"[quota retry] {sleep_sec:.1f}s 후 재시도: {e}")
            time.sleep(sleep_sec)


@lru_cache(maxsize=1)
def gs_client():
    if not GOOGLE_CREDENTIALS_JSON:
        raise RuntimeError("GOOGLE_CREDENTIALS_JSON 또는 GOOGLE_CREDS 가 비어 있습니다.")
    if not GOOGLE_SHEET_ID:
        raise RuntimeError("GOOGLE_SHEET_ID 가 비어 있습니다.")

    info = json.loads(GOOGLE_CREDENTIALS_JSON)
    return gspread.service_account_from_dict(info)


@lru_cache(maxsize=1)
def gs_open():
    gc = gs_client()
    return _gs_call(gc.open_by_key, GOOGLE_SHEET_ID)


@lru_cache(maxsize=None)
def gs_ws(sheet_name: str):
    sh = gs_open()
    return _gs_call(sh.worksheet, sheet_name)


def gs_get_all_records(ws, **kwargs):
    return _gs_call(ws.get_all_records, **kwargs)


def gs_get_all_values(ws):
    return _gs_call(ws.get_all_values)


def gs_clear(ws):
    return _gs_call(ws.clear)


def gs_update(ws, values, range_name=None):
    if range_name:
        return _gs_call(ws.update, range_name, values)
    return _gs_call(ws.update, values)


# ==========================================================
# [공통 유틸]
# ==========================================================
def _s(x: Any) -> str:
    if x is None:
        return ""
    return str(x).strip()


def _norm(x: Any) -> str:
    s = _s(x)
    s = s.replace("\xa0", " ")
    s = re.sub(r"\s+", "", s)
    return s.lower()


def first_nonempty(*vals) -> str:
    for v in vals:
        s = _s(v)
        if s:
            return s
    return ""


def parse_float_like(x: Any) -> Optional[float]:
    s = _s(x)
    if not s:
        return None

    s = s.replace(",", "")
    m = re.search(r"-?\d+(?:\.\d+)?", s)
    if not m:
        return None

    try:
        return float(m.group(0))
    except Exception:
        return None


def now_kst_str() -> str:
    from datetime import datetime, timezone, timedelta
    kst = timezone(timedelta(hours=9))
    return datetime.now(kst).strftime("%Y-%m-%d %H:%M:%S")


def dedupe_review_rows(rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    seen = set()
    out = []

    for r in rows:
        key = (
            _s(r.get("사명")),
            _s(r.get("보고서명")),
            _s(r.get("검토등급")),
            _s(r.get("의심사유")),
            _s(r.get("누락컬럼")),
            _s(r.get("링크")),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(r)

    return out


# ==========================================================
# [review_queue 출력 헤더]
# ==========================================================
REVIEW_HEADERS = [
    "사명",
    "보고서명",
    "검토등급",
    "의심사유",
    "누락컬럼",
    "링크",
    "검토시각",
]


# ==========================================================
# [RAW_dump -> 링크 매핑]
# - review_queue 의 링크 컬럼 채우기용
# ==========================================================
def build_raw_link_map(raw_rows: List[Dict[str, Any]]) -> Dict[str, str]:
    out: Dict[str, str] = {}

    for r in raw_rows:
        acptno = first_nonempty(
            r.get("acptNo"),
            r.get("접수번호"),
            r.get("AcptNo"),
        )

        link = first_nonempty(
            r.get("link"),
            r.get("링크"),
            r.get("url"),
            r.get("URL"),
            r.get("href"),
        )

        if acptno and link and acptno not in out:
            out[acptno] = link

    return out


def find_link(rec: Dict[str, Any], raw_link_map: Dict[str, str]) -> str:
    direct = first_nonempty(
        rec.get("link"),
        rec.get("링크"),
        rec.get("url"),
        rec.get("URL"),
        rec.get("href"),
    )
    if direct:
        return direct

    acptno = first_nonempty(
        rec.get("acptNo"),
        rec.get("접수번호"),
        rec.get("AcptNo"),
    )
    if acptno:
        return raw_link_map.get(acptno, "")

    return ""


# ==========================================================
# [필수 컬럼 누락 체크]
# - 네 시트 컬럼 구조에 맞춰 여기만 추가 수정하면 됨
# ==========================================================
RIGHTS_REQUIRED_COLUMNS = [
    "회사명",
    "보고서명",
]

BOND_REQUIRED_COLUMNS = [
    "회사명",
    "보고서명",
]


def collect_missing_columns(rec: Dict[str, Any], required_cols: List[str]) -> List[str]:
    missing = []
    for col in required_cols:
        if not _s(rec.get(col)):
            missing.append(col)
    return missing


# ==========================================================
# [기본 review row 생성]
# ==========================================================
def make_review_row(
    rec: Dict[str, Any],
    raw_link_map: Dict[str, str],
    reason: str,
    grade: str = "REVIEW",
    missing_cols: Optional[List[str]] = None,
) -> Dict[str, str]:
    return {
        "사명": first_nonempty(rec.get("회사명"), rec.get("사명"), rec.get("corp_name")),
        "보고서명": first_nonempty(rec.get("보고서명"), rec.get("title"), rec.get("공시명")),
        "검토등급": grade,
        "의심사유": reason,
        "누락컬럼": ", ".join(missing_cols or []),
        "링크": find_link(rec, raw_link_map),
        "검토시각": now_kst_str(),
    }


# ==========================================================
# [유상증자 커스텀 검증]
# - 기존 validator 규칙이 있으면 여기에 넣으면 됨
# ==========================================================
def custom_validate_rights_row(
    rec: Dict[str, Any],
    raw_link_map: Dict[str, str],
) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []

    # ------------------------------------------------------
    # 1) 필수값 누락
    # ------------------------------------------------------
    missing = collect_missing_columns(rec, RIGHTS_REQUIRED_COLUMNS)
    if missing:
        out.append(
            make_review_row(
                rec=rec,
                raw_link_map=raw_link_map,
                reason="필수 컬럼 누락",
                grade="REVIEW",
                missing_cols=missing,
            )
        )

    # ------------------------------------------------------
    # 2) 예시: 할인율 이상치
    #    - 네 기존 validator 규칙 있으면 이 부분 교체
    # ------------------------------------------------------
    discount = first_nonempty(
        rec.get("할인율"),
        rec.get("할인율(%)"),
        rec.get("할인비율"),
    )
    final_price = first_nonempty(
        rec.get("확정발행가"),
        rec.get("확정발행가(원)"),
        rec.get("확정발행가격"),
    )
    base_price = first_nonempty(
        rec.get("기준주가"),
        rec.get("기준주가(원)"),
    )

    d = parse_float_like(discount)
    f = parse_float_like(final_price)
    b = parse_float_like(base_price)

    if d is not None:
        if d >= 30:
            out.append(
                make_review_row(
                    rec=rec,
                    raw_link_map=raw_link_map,
                    reason=f"HIGH|할인율 과다|할인율={d}",
                    grade="REVIEW_HIGH",
                )
            )
        elif d >= 15:
            out.append(
                make_review_row(
                    rec=rec,
                    raw_link_map=raw_link_map,
                    reason=f"MED|할인율 높음|할인율={d}",
                    grade="REVIEW",
                )
            )

    if f is not None and b is not None and b > 0:
        gap_pct = ((f - b) / b) * 100.0
        if gap_pct >= 30:
            out.append(
                make_review_row(
                    rec=rec,
                    raw_link_map=raw_link_map,
                    reason=f"HIGH|확정발행가가 기준주가보다 큼|값={gap_pct:.2f}%",
                    grade="REVIEW_HIGH",
                )
            )
        elif gap_pct <= -30:
            out.append(
                make_review_row(
                    rec=rec,
                    raw_link_map=raw_link_map,
                    reason=f"HIGH|확정발행가가 기준주가보다 작음|값={gap_pct:.2f}%",
                    grade="REVIEW_HIGH",
                )
            )

    return out


# ==========================================================
# [주식연계채권 커스텀 검증]
# - 기존 validator 규칙 있으면 여기에 넣으면 됨
# ==========================================================
def custom_validate_bond_row(
    rec: Dict[str, Any],
    raw_link_map: Dict[str, str],
) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []

    missing = collect_missing_columns(rec, BOND_REQUIRED_COLUMNS)
    if missing:
        out.append(
            make_review_row(
                rec=rec,
                raw_link_map=raw_link_map,
                reason="필수 컬럼 누락",
                grade="REVIEW",
                missing_cols=missing,
            )
        )

    # 예시: 권면총액 / 만기이자율 / 표면이자율 누락
    important_cols = []
    for col in ["권면총액", "표면이자율", "만기이자율", "전환가액", "행사가액", "교환가액"]:
        if col in rec and not _s(rec.get(col)):
            important_cols.append(col)

    if important_cols:
        out.append(
            make_review_row(
                rec=rec,
                raw_link_map=raw_link_map,
                reason="주요 채권 컬럼 누락",
                grade="REVIEW",
                missing_cols=important_cols,
            )
        )

    return out


# ==========================================================
# [시트 로드]
# ==========================================================
def load_rights_rows() -> List[Dict[str, Any]]:
    ws = gs_ws(RIGHTS_SHEET_NAME)
    return gs_get_all_records(ws)


def load_bond_rows() -> List[Dict[str, Any]]:
    ws = gs_ws(BOND_SHEET_NAME)
    return gs_get_all_records(ws)


def load_raw_rows() -> List[Dict[str, Any]]:
    ws = gs_ws(RAW_SHEET_NAME)
    return gs_get_all_records(ws)


# ==========================================================
# [review_queue 생성]
# ==========================================================
def build_review_rows(
    rights_rows: List[Dict[str, Any]],
    bond_rows: List[Dict[str, Any]],
    raw_rows: List[Dict[str, Any]],
) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []

    raw_link_map = build_raw_link_map(raw_rows)

    for rec in rights_rows:
        out.extend(custom_validate_rights_row(rec, raw_link_map))

    for rec in bond_rows:
        out.extend(custom_validate_bond_row(rec, raw_link_map))

    out = dedupe_review_rows(out)
    return out


def rows_to_values(rows: List[Dict[str, Any]], headers: List[str]) -> List[List[Any]]:
    values = [headers]
    for r in rows:
        values.append([r.get(h, "") for h in headers])
    return values


# ==========================================================
# [review_queue 저장]
# ==========================================================
def write_review_queue(rows: List[Dict[str, Any]]):
    ws = gs_ws(REVIEW_SHEET_NAME)
    values = rows_to_values(rows, REVIEW_HEADERS)

    gs_clear(ws)
    gs_update(ws, values)

    print(f"[validator] 저장 완료: {len(rows)} rows -> {REVIEW_SHEET_NAME}")


# ==========================================================
# [메인 실행]
# ==========================================================
def run_validator():
    raw_rows = load_raw_rows()
    rights_rows = load_rights_rows()
    bond_rows = load_bond_rows()

    print(f"[validator] RAW rows    : {len(raw_rows)}")
    print(f"[validator] RIGHTS rows : {len(rights_rows)}")
    print(f"[validator] BOND rows   : {len(bond_rows)}")

    review_rows = build_review_rows(
        rights_rows=rights_rows,
        bond_rows=bond_rows,
        raw_rows=raw_rows,
    )

    write_review_queue(review_rows)


if __name__ == "__main__":
    run_validator()
