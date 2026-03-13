import re
from typing import Dict, List, Tuple, Any

import pandas as pd

from parser import (
    normalize_text,
    all_text_lines,
    clean_title,
    is_correction_title,
    extract_correction_after_map,
    clean_percent,
    parse_float_like,
    scan_label_value_preferring_correction,
)


# ==========================================================
# [기본 정리]
# ==========================================================
def _clean_line(text: Any) -> str:
    if text is None:
        return ""
    s = str(text).replace("\xa0", " ")
    s = re.sub(r"\s*\|\s*", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def _n(s: Any) -> str:
    return re.sub(r"\s+", "", str(s or "")).replace(":", "")


def _lines_from_tables(tables: List[pd.DataFrame]) -> List[str]:
    out = []
    for line in all_text_lines(tables):
        s = _clean_line(normalize_text(line))
        if s:
            out.append(s)
    return out


def _corpus_from_lines(lines: List[str]) -> str:
    return "\n".join([x for x in lines if x]).strip()


def _safe_percent(value: Any) -> str:
    if value is None:
        return ""

    s = str(value).strip()
    if not s:
        return ""

    s2 = clean_percent(s)
    if s2:
        return s2

    f = parse_float_like(s)
    if f is None:
        return ""

    if float(f).is_integer():
        return f"{int(f)}%"
    return f"{f}%"


def _is_top_heading(text: str) -> bool:
    s = normalize_text(text)
    if not s:
        return False
    return bool(re.match(r"^\d+\s*[\.\)]\s*[가-힣A-Za-z]", s))


# ==========================================================
# [Call / Put 시작 / 종료 패턴]
# ==========================================================
CALL_START_PATTERNS = [
    r"\[\s*Call Option에 관한 사항\s*\]",
    r"\[\s*call option에 관한 사항\s*\]",
    r"\[\s*매도청구권\s*\(\s*Call Option\s*\)\s*에 관한 사항\s*\]",
    r"\[\s*매도청구권\s*\(\s*CALL OPTION\s*\)\s*에 관한 사항\s*\]",
    r"\[\s*중도상환청구권\s*\(\s*Call Option\s*\)\s*에 관한 사항\s*\]",
    r"^\s*매도청구권\s*\(\s*Call Option\s*\)\s*$",
    r"^\s*매도청구권\s*\(\s*CALL OPTION\s*\)\s*$",
    r"^\s*<\s*Call Option\s*>\s*",
    r"^\s*\d+\.\s*발행회사의\s*중도상환청구권\s*\(\s*Call Option\s*\)\s*에 관한 사항",
]

CALL_PREFIX_PATTERNS = [
    r"^\s*\[\s*Call Option에 관한 사항\s*\]\s*",
    r"^\s*\[\s*call option에 관한 사항\s*\]\s*",
    r"^\s*\[\s*매도청구권\s*\(\s*Call Option\s*\)\s*에 관한 사항\s*\]\s*",
    r"^\s*\[\s*매도청구권\s*\(\s*CALL OPTION\s*\)\s*에 관한 사항\s*\]\s*",
    r"^\s*\[\s*중도상환청구권\s*\(\s*Call Option\s*\)\s*에 관한 사항\s*\]\s*",
    r"^\s*매도청구권\s*\(\s*Call Option\s*\)\s*",
    r"^\s*매도청구권\s*\(\s*CALL OPTION\s*\)\s*",
    r"^\s*<\s*Call Option\s*>\s*",
    r"^\s*\d+\.\s*발행회사의\s*중도상환청구권\s*\(\s*Call Option\s*\)\s*에 관한 사항\s*",
]

PUT_START_PATTERNS = [
    r"\[\s*Put Option에 관한 사항\s*\]",
    r"\[\s*조기상환청구권\s*\(\s*Put Option\s*\)\s*에 관한 사항\s*\]",
    r"\[\s*사채권자의 조기상환청구권\s*\]",
    r"^\s*Put Option\s*$",
    r"^\s*조기상환청구권\s*\(\s*Put Option\s*\)",
    r"^\s*사채권자의 조기상환청구권",
]

PUT_PREFIX_PATTERNS = [
    r"^\s*\[\s*Put Option에 관한 사항\s*\]\s*",
    r"^\s*\[\s*조기상환청구권\s*\(\s*Put Option\s*\)\s*에 관한 사항\s*\]\s*",
    r"^\s*\[\s*사채권자의 조기상환청구권\s*\]\s*",
    r"^\s*Put Option\s*",
    r"^\s*조기상환청구권\s*\(\s*Put Option\s*\)\s*",
    r"^\s*사채권자의 조기상환청구권\s*",
]

NEXT_MAJOR_PATTERNS = [
    r"^\s*9\s*[\.\-]?\s*2",
    r"^\s*9\s*[\.\-]?\s*3",
    r"^\s*10\s*[\.\)]",
    r"^\s*11\s*[\.\)]",
    r"^\s*12\s*[\.\)]",
    r"^\s*13\s*[\.\)]",
    r"^\s*20\s*[\.\)]",
    r"^\s*21\s*[\.\)]",
    r"^\s*22\s*[\.\)]",
    r"^\s*23\s*[\.\)]",
    r"^\s*24\s*[\.\)]",
    r"^\s*25\s*[\.\)]",
]


# ==========================================================
# [패턴 판별]
# ==========================================================
def _matches_any(text: str, patterns: List[str]) -> bool:
    s = _clean_line(text)
    return any(re.search(p, s, flags=re.IGNORECASE) for p in patterns)


def is_call_start_line(line: str) -> bool:
    return _matches_any(line, CALL_START_PATTERNS)


def is_put_start_line(line: str) -> bool:
    return _matches_any(line, PUT_START_PATTERNS)


def is_next_major_line(line: str) -> bool:
    return _matches_any(line, NEXT_MAJOR_PATTERNS)


def strip_prefix(text: str, patterns: List[str]) -> str:
    s = _clean_line(text)
    for p in patterns:
        s2 = re.sub(p, "", s, flags=re.IGNORECASE).strip()
        if s2 != s:
            return s2
    return s


# ==========================================================
# [끝부분 참고문장 제거]
# ==========================================================
def trim_reference_sentences(text: str) -> str:
    s = _clean_line(text)

    ref_patterns = [
        r'(이 외 .*?기타 투자판단에 참고할 사항.*)$',
        r'(세부내용은 .*?기타 투자판단에 참고할 사항.*)$',
        r'(.*?기타 투자판단에 참고할 사항을 참고.*)$',
        r'(.*?참고하시기 바랍니다\.)$',
        r'(.*?참고하여 주시기 바랍니다\.)$',
    ]

    for p in ref_patterns:
        s = re.sub(p, "", s, flags=re.IGNORECASE).strip()

    return s


# ==========================================================
# [Call / Put 블록 추출]
# ==========================================================
def extract_call_option_text_from_lines(lines: List[str]) -> str:
    started = False
    bucket = []

    for line in lines:
        s = _clean_line(line)
        if not s:
            continue

        if not started:
            if is_call_start_line(s):
                started = True
                body = strip_prefix(s, CALL_PREFIX_PATTERNS)
                if body:
                    bucket.append(body)
            continue

        # Call 시작 후 Put 시작이 나오면 종료
        if is_put_start_line(s):
            break

        # 다음 대목차 나오면 종료
        if is_next_major_line(s):
            break

        bucket.append(s)

    text = " ".join(bucket).strip()
    text = trim_reference_sentences(text)
    text = re.sub(r"\s{2,}", " ", text)
    return text


def extract_put_option_text_from_lines(lines: List[str]) -> str:
    started = False
    bucket = []

    for line in lines:
        s = _clean_line(line)
        if not s:
            continue

        if not started:
            if is_put_start_line(s):
                started = True
                body = strip_prefix(s, PUT_PREFIX_PATTERNS)
                if body:
                    bucket.append(body)
            continue

        # Put 시작 후 Call 시작이 나오면 종료
        if is_call_start_line(s):
            break

        # 다음 대목차 나오면 종료
        if is_next_major_line(s):
            break

        bucket.append(s)

    text = " ".join(bucket).strip()
    text = trim_reference_sentences(text)
    text = re.sub(r"\s{2,}", " ", text)
    return text


# ==========================================================
# [표 grid에서 Call 비율 / YTC 읽기]
# ==========================================================
def _to_pct_text(cell: Any, min_v: float = None, max_v: float = None) -> str:
    s = normalize_text(cell)
    if not s:
        return ""

    if s in ["구분", "-", ".", "해당없음", "해당사항없음"]:
        return ""

    m = re.search(r"(-?\d+(?:\.\d+)?)\s*%", s)
    if not m:
        raw = s.replace(",", "")
        m = re.fullmatch(r"(-?\d+(?:\.\d+)?)", raw)
        if not m:
            return ""

    try:
        val = float(m.group(1))
    except Exception:
        return ""

    if min_v is not None and val < min_v:
        return ""
    if max_v is not None and val > max_v:
        return ""

    if float(val).is_integer():
        return f"{int(val)}%"
    return f"{val}%"


def extract_call_ratio_ytc_from_table_grid(
    tables: List[pd.DataFrame],
) -> Tuple[str, str]:
    call_header_kws = [
        "Call비율",
        "콜옵션비율",
        "행사비율",
        "매도청구권행사비율",
    ]
    ytc_header_kws = [
        "YTC",
        "조기상환수익률",
        "연복리수익률",
        "매도청구권보장수익률",
        "매도청구수익률",
    ]

    pairs = []

    for df in tables:
        try:
            arr = df.fillna("").astype(str).values
        except Exception:
            continue

        R, C = arr.shape
        if R == 0 or C == 0:
            continue

        header_row = None
        call_col = None
        ytc_col = None

        for r in range(R):
            row_norm = [_n(x) for x in arr[r].tolist()]

            tmp_call = None
            tmp_ytc = None

            for c, cell in enumerate(row_norm):
                if tmp_call is None and any(k in cell for k in call_header_kws):
                    tmp_call = c
                if tmp_ytc is None and any(k in cell for k in ytc_header_kws):
                    tmp_ytc = c

            if tmp_call is not None and tmp_ytc is not None:
                header_row = r
                call_col = tmp_call
                ytc_col = tmp_ytc
                break

        if header_row is None:
            continue

        for rr in range(header_row + 1, R):
            row_vals = [normalize_text(x) for x in arr[rr].tolist()]
            row_join = " ".join([x for x in row_vals if x])

            if not row_join:
                continue

            first_nonempty = next((x for x in row_vals if x), "")
            if _is_top_heading(first_nonempty):
                break

            call_val = _to_pct_text(arr[rr][call_col], min_v=0, max_v=100) if call_col is not None else ""
            ytc_val = _to_pct_text(arr[rr][ytc_col], min_v=0, max_v=30) if ytc_col is not None else ""

            if call_val or ytc_val:
                pairs.append((call_val, ytc_val))

    uniq = []
    for p in pairs:
        if p not in uniq:
            uniq.append(p)

    for call_val, ytc_val in uniq:
        if call_val and ytc_val:
            return call_val, ytc_val

    for call_val, ytc_val in uniq:
        if call_val or ytc_val:
            return call_val, ytc_val

    return "", ""


# ==========================================================
# [본문 fallback]
# ==========================================================
def extract_call_ratio_and_ytc_from_text(text: str) -> Tuple[str, str]:
    if not text:
        return "", ""

    ratio = ""
    ytc = ""

    ratio_patterns = [
        r"(?:행사비율|콜옵션비율|매도청구권\s*행사비율|Call\s*비율)\s*[:=]?\s*(\d+(?:\.\d+)?)\s*%",
        r"(?:권면총액|권면액|전자등록총액|전자등록금액|인수금액|발행금액|사채원금)\s*(?:의|중)\s*(\d+(?:\.\d+)?)\s*%",
    ]
    ytc_patterns = [
        r"(?:YTC|매도청구권보장수익률|매도청구수익률|조기상환수익률|조기상환이율|연복리수익률)\s*[:=]?\s*(\d+(?:\.\d+)?)\s*%",
        r"연\s*(\d+(?:\.\d+)?)\s*%\s*\(\s*3개월\s*단위\s*복리계산\s*\)",
        r"연복리\s*(\d+(?:\.\d+)?)\s*%",
        r"IRR.*?연\s*(\d+(?:\.\d+)?)\s*%",
    ]

    for pat in ratio_patterns:
        m = re.search(pat, text, flags=re.IGNORECASE)
        if m:
            ratio = f"{m.group(1)}%"
            break

    for pat in ytc_patterns:
        m = re.search(pat, text, flags=re.IGNORECASE)
        if m:
            ytc = f"{m.group(1)}%"
            break

    return ratio, ytc


# ==========================================================
# [최종 파서]
# ==========================================================
def parse_bond_option_record(rec: Dict[str, Any]) -> Dict[str, str]:
    title = clean_title(rec.get("title", "") or "")
    tables = rec.get("tables", [])
    corr_after = extract_correction_after_map(tables) if is_correction_title(title) else {}

    row = {
        "Put Option": "",
        "Call Option": "",
        "Call 비율": "",
        "YTC": "",
    }

    lines = _lines_from_tables(tables)
    corpus = _corpus_from_lines(lines)

    # 1) 텍스트 블록 추출
    put_text = extract_put_option_text_from_lines(lines)
    call_text = extract_call_option_text_from_lines(lines)

    row["Put Option"] = put_text if put_text else "공시 확인 바람"
    row["Call Option"] = call_text if call_text else "공시 확인 바람"

    # 2) 표 key-value 우선
    row["Call 비율"] = _safe_percent(
        scan_label_value_preferring_correction(
            tables,
            ["콜옵션 행사비율", "매도청구권 행사비율", "Call 비율", "행사비율"],
            corr_after,
        )
    )

    row["YTC"] = _safe_percent(
        scan_label_value_preferring_correction(
            tables,
            ["조기상환수익률", "YTC", "Yield To Call", "연복리수익률", "매도청구권보장수익률"],
            corr_after,
        )
    )

    # 3) 표 grid fallback
    if not row["Call 비율"] or not row["YTC"]:
        table_ratio, table_ytc = extract_call_ratio_ytc_from_table_grid(tables)

        if not row["Call 비율"]:
            row["Call 비율"] = table_ratio
        if not row["YTC"]:
            row["YTC"] = table_ytc

    # 4) Call 본문 fallback
    if (not row["Call 비율"] or not row["YTC"]) and call_text and call_text != "공시 확인 바람":
        txt_ratio, txt_ytc = extract_call_ratio_and_ytc_from_text(call_text)

        if not row["Call 비율"]:
            row["Call 비율"] = txt_ratio
        if not row["YTC"]:
            row["YTC"] = txt_ytc

    return row
