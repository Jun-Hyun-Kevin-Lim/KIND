import gspread
from typing import Dict, Any

from parser import (
    gs_open,
    ensure_ws,
    ensure_header,
    load_raw_records,
    clean_title,
    normalize_text,
    safe_cell,
    find_row_by_key,
    RAW_SHEET_NAME,
    BOND_SHEET_NAME,
    BOND_HEADERS,
    RUN_ONLY_ACPTNO,
)
from bond_option_parser import parse_bond_option_record


# ==========================================================
# [옵션 전용 업데이트 대상 컬럼]
# - Put / Call 관련 컬럼만 부분 업데이트
# ==========================================================
OPTION_HEADERS = [
    "Put Option",
    "Call Option",
    "Call 비율",
    "YTC",
]


# ==========================================================
# [옵션 컬럼만 부분 업데이트]
# - 접수번호 기준으로 기존 행을 찾아
# - Put Option ~ YTC 4개 컬럼만 업데이트
# - 새 값이 비어 있으면 기존 값을 유지
# ==========================================================
def update_option_fields_only(
    ws,
    headers,
    acpt_no: str,
    option_row: Dict[str, Any],
):
    target_row = find_row_by_key(ws, "접수번호", str(acpt_no))
    if not target_row:
        return "SKIP_NO_BASE_ROW", None

    for h in OPTION_HEADERS:
        if h not in headers:
            raise RuntimeError(f"BOND_HEADERS에 '{h}' 컬럼이 없습니다.")

    existing = ws.row_values(target_row)

    final_values = []
    for h in OPTION_HEADERS:
        idx = headers.index(h)
        old_val = safe_cell(existing, idx)
        new_val = normalize_text(option_row.get(h, ""))

        # 새 값이 있으면 새 값으로, 없으면 기존 값 유지
        final_values.append(new_val if new_val else old_val)

    start_col = headers.index("Put Option") + 1
    end_col = headers.index("YTC") + 1

    start_a1 = gspread.utils.rowcol_to_a1(target_row, start_col)
    end_a1 = gspread.utils.rowcol_to_a1(target_row, end_col)

    ws.update(f"{start_a1}:{end_a1}", [final_values])
    return "UPDATE", target_row


# ==========================================================
# [주식연계채권 공시 여부 판단]
# ==========================================================
def is_bond_title(title: str) -> bool:
    t = title.replace(" ", "")
    return any(
        k in t
        for k in [
            "전환사채권발행결정",
            "교환사채권발행결정",
            "신주인수권부사채권발행결정",
        ]
    )


# ==========================================================
# [옵션 전용 러너]
# - RAW_dump 로드
# - bond 공시만 대상으로 Put / Call 로직 실행
# - 기존 bond 시트의 옵션 4개 컬럼만 갱신
# ==========================================================
def run_option_parser():
    sh = gs_open()

    raw_ws = ensure_ws(sh, RAW_SHEET_NAME, rows=5000, cols=250)
    bond_ws = ensure_ws(sh, BOND_SHEET_NAME, rows=3000, cols=max(40, len(BOND_HEADERS) + 5))

    ensure_header(bond_ws, BOND_HEADERS)

    records = load_raw_records(raw_ws)
    if RUN_ONLY_ACPTNO:
        records = [r for r in records if r["acpt_no"] == RUN_ONLY_ACPTNO]

    if not records:
        print("[INFO] RAW_dump에 옵션 파싱할 데이터가 없습니다.")
        return

    ok = 0
    skip = 0
    fail = 0

    for rec in records:
        acpt_no = rec["acpt_no"]
        title = clean_title(rec.get("title", "") or "")

        if not is_bond_title(title):
            continue

        try:
            option_row = parse_bond_option_record(rec)

            has_any_option_value = any(
                normalize_text(option_row.get(h, ""))
                for h in OPTION_HEADERS
            )

            if not has_any_option_value:
                skip += 1
                print(f"[SKIP][OPTION][EMPTY] {acpt_no} {title}")
                continue

            mode, rownum = update_option_fields_only(
                bond_ws,
                BOND_HEADERS,
                acpt_no,
                option_row,
            )

            if mode == "SKIP_NO_BASE_ROW":
                skip += 1
                print(f"[SKIP][OPTION][NO_BASE_ROW] {acpt_no} {title}")
                continue

            ok += 1
            print(
                f"[OK][OPTION][{mode}] {acpt_no} {title} "
                f"(row={rownum}, put={'Y' if option_row.get('Put Option') else 'N'}, "
                f"call={'Y' if option_row.get('Call Option') else 'N'})"
            )

        except Exception as e:
            fail += 1
            print(f"[FAIL][OPTION] {acpt_no} {title} :: {e}")

    print(f"[DONE][OPTION] ok={ok} skip={skip} fail={fail}")


# ==========================================================
# [직접 실행 진입점]
# ==========================================================
if __name__ == "__main__":
    run_option_parser()
