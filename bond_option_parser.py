import re
from typing import Dict, List, Tuple, Any, Optional

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
# [9.1 섹션 추출]
# - Put Option의 원본 텍스트가 되는 9.1 전체 섹션
# - 단, "9-1. 옵션에 관한 사항" 제목 자체는 제거
# ==========================================================
def _is_91_heading(line: str) -> bool:
    s = _clean_line(line)
    if not s:
        return False

    patterns = [
        r"^9\s*[\.\-]?\s*1\s*[\)\.]?\s*옵션에\s*관한\s*사항",
        r"^9\s*[\.\-]?\s*1\s*[\)\.]?\s*옵션사항",
    ]
    return any(re.search(p, s, flags=re.IGNORECASE) for p in patterns)


def _is_next_major_heading(line: str) -> bool:
    s = _clean_line(line)
    if not s:
        return False

    stop_patterns = [
        r"^9\s*[\.\-]?\s*2\s*[\)\.]?",
        r"^9\s*[\.\-]?\s*3\s*[\)\.]?",
        r"^10\s*[\)\.]?",
        r"^11\s*[\)\.]?",
        r"^12\s*[\)\.]?",
        r"^13\s*[\)\.]?",
        r"^20\s*[\)\.]?",
        r"^21\s*[\)\.]?",
        r"^22\s*[\)\.]?",
        r"^23\s*[\)\.]?",
        r"^24\s*[\)\.]?",
        r"^25\s*[\)\.]?",
    ]
    return any(re.search(p, s, flags=re.IGNORECASE) for p in stop_patterns)


def _strip_91_heading_prefix(text: str) -> str:
    s = _clean_line(text)
    if not s:
        return ""

    patterns = [
        r"^\s*9\s*[\.\-]?\s*1\s*[\)\.]?\s*옵션에\s*관한\s*사항\s*[:：]?\s*",
        r"^\s*9\s*[\.\-]?\s*1\s*[\)\.]?\s*옵션사항\s*[:：]?\s*",
    ]

    for pat in patterns:
        new_s = re.sub(pat, "", s, flags=re.IGNORECASE).strip()
        if new_s != s:
            return new_s

    return s


def extract_91_option_section_from_lines(lines: List[str]) -> str:
    if not lines:
        return ""

    started = False
    bucket = []

    for line in lines:
        s = _clean_line(line)
        if not s:
            continue

        if not started:
            if _is_91_heading(s):
                started = True
                first_body = _strip_91_heading_prefix(s)
                if first_body:
                    bucket.append(first_body)
            continue

        if _is_next_major_heading(s):
            break

        bucket.append(s)

    text = " ".join(bucket).strip()
    text = re.sub(r"\s{2,}", " ", text)
    return text


def extract_91_option_section_from_corpus(corpus: str) -> str:
    if not corpus:
        return ""

    start_patterns = [
        r"(?:^|\n)\s*9\s*[\.\-]?\s*1\s*[\)\.]?\s*옵션에\s*관한\s*사항",
        r"(?:^|\n)\s*9\s*[\.\-]?\s*1\s*[\)\.]?\s*옵션사항",
    ]

    start_match = None
    for pat in start_patterns:
        m = re.search(pat, corpus, flags=re.IGNORECASE | re.MULTILINE)
        if m:
            if start_match is None or m.start() < start_match.start():
                start_match = m

    if not start_match:
        return ""

    start_idx = start_match.end()
    sub = corpus[start_idx:]

    end_patterns = [
        r"(?:^|\n)\s*9\s*[\.\-]?\s*2\s*[\)\.]?",
        r"(?:^|\n)\s*9\s*[\.\-]?\s*3\s*[\)\.]?",
        r"(?:^|\n)\s*10\s*[\)\.]?",
        r"(?:^|\n)\s*11\s*[\)\.]?",
        r"(?:^|\n)\s*12\s*[\)\.]?",
        r"(?:^|\n)\s*13\s*[\)\.]?",
        r"(?:^|\n)\s*20\s*[\)\.]?",
        r"(?:^|\n)\s*21\s*[\)\.]?",
        r"(?:^|\n)\s*22\s*[\)\.]?",
        r"(?:^|\n)\s*23\s*[\)\.]?",
        r"(?:^|\n)\s*24\s*[\)\.]?",
        r"(?:^|\n)\s*25\s*[\)\.]?",
    ]

    cut = len(sub)
    for pat in end_patterns:
        m = re.search(pat, sub, flags=re.IGNORECASE | re.MULTILINE)
        if m and m.start() > 0:
            cut = min(cut, m.start())

    text = sub[:cut].strip()
    text = text.replace("\n", " ")
    text = re.sub(r"\s*\|\s*", " ", text)
    text = re.sub(r"\s{2,}", " ", text)
    return text.strip()


# ==========================================================
# [9.1 예외 처리]
# 1) 9.1이 참조 문장만 있는 경우:
#    "조기상환청구권(Put Option), 매도청구권(Call Option)에 관한 사항,
#     23. 기타 투자판단에 참고할 사항을 참고하시기 바랍니다."
#    -> Put/Call 둘 다 "공시 확인 바람"
#
# 2) 9.1 안에 "22. 기타 투자판단에 참고할 사항"이 들어오면
#    -> Put/Call 둘 다 "공시 확인 바람"
# ==========================================================
def _is_reference_only_91_section(text: str) -> bool:
    s = _clean_line(text)
    if not s:
        return False

    patterns = [
        r"^\s*조기상환청구권\s*\(\s*Put\s*Option\s*\)\s*,\s*매도청구권\s*\(\s*Call\s*Option\s*\)\s*에\s*관한\s*사항\s*,\s*23\s*[\.\)]\s*기타\s*투자판단에\s*참고할\s*사항(?:을)?\s*참고(?:하여)?\s*주시기\s*바랍니다\.?\s*$",
        r"^\s*조기상환청구권\s*\(\s*Put\s*Option\s*\)\s*[,，]?\s*매도청구권\s*\(\s*Call\s*Option\s*\)\s*에\s*관한\s*사항\s*[,，]?\s*23\s*[\.\)]\s*기타\s*투자판단에\s*참고할\s*사항(?:을)?\s*참고(?:하여)?\s*주시기\s*바랍니다\.?\s*$",
    ]
    return any(re.search(p, s, flags=re.IGNORECASE) for p in patterns)


def _contains_invalid_22_reference_in_91(text: str) -> bool:
    s = _clean_line(text)
    if not s:
        return False

    patterns = [
        r"(?:^|[\s\]])22\s*[\.\)]\s*기타\s*투자판단에\s*참고할\s*사항",
        r"(?:^|[\s\]])22\s*[\.\)]\s*기타투자판단에참고할사항",
    ]
    return any(re.search(p, s, flags=re.IGNORECASE) for p in patterns)


# ==========================================================
# [Call Option 헤딩 / 종료 패턴]
# - Call은 Put Option 텍스트 안에서 잘라낸다
# - Call 헤딩은 삭제하지 않고 같이 가져간다
# ==========================================================
CALL_START_PATTERNS = [
    r"\[\s*Call Option에 관한 사항\s*\]",
    r"\[\s*call option에 관한 사항\s*\]",
    r"\[\s*매도청구권\s*\(\s*Call Option\s*\)\s*에\s*관한\s*사항\s*\]",
    r"\[\s*매도청구권\s*\(\s*CALL OPTION\s*\)\s*에\s*관한\s*사항\s*\]",
    r"\[\s*중도상환청구권\s*\(\s*Call Option\s*\)\s*에\s*관한\s*사항\s*\]",
    r"\[\s*중도상환청구권\s*\(\s*CALL OPTION\s*\)\s*에\s*관한\s*사항\s*\]",
    r"<\s*Call Option\s*>",
    r"매도청구권\s*\(\s*Call Option\s*\)",
    r"매도청구권\s*\(\s*CALL OPTION\s*\)",
    r"중도상환청구권\s*\(\s*Call Option\s*\)",
    r"중도상환청구권\s*\(\s*CALL OPTION\s*\)",
    r"발행회사의\s*중도상환청구권\s*\(\s*Call Option\s*\)\s*에\s*관한\s*사항",
    r"발행회사의\s*중도상환청구권\s*\(\s*CALL OPTION\s*\)\s*에\s*관한\s*사항",
]

CALL_END_PATTERNS = [
    r"이\s*외\s*Put Option",
    r"조기상환청구권\s*\(\s*Put Option\s*\)",
    r"사채권자의\s*조기상환청구권",
    r"(?:^|[\s\]])9\s*[-\.]?\s*2\s*[\)\.]?\s*[가-힣A-Za-z\(]",
    r"(?:^|[\s\]])9\s*[-\.]?\s*3\s*[\)\.]?\s*[가-힣A-Za-z\(]",
    r"(?:^|[\s\]])10\.\s*[가-힣A-Za-z\(]",
    r"(?:^|[\s\]])11\.\s*[가-힣A-Za-z\(]",
    r"(?:^|[\s\]])12\.\s*[가-힣A-Za-z\(]",
    r"(?:^|[\s\]])13\.\s*[가-힣A-Za-z\(]",
    r"(?:^|[\s\]])20\.\s*[가-힣A-Za-z\(]",
    r"(?:^|[\s\]])21\.\s*[가-힣A-Za-z\(]",
    r"(?:^|[\s\]])22\.\s*[가-힣A-Za-z\(]",
    r"(?:^|[\s\]])23\.\s*[가-힣A-Za-z\(]",
    r"(?:^|[\s\]])24\.\s*[가-힣A-Za-z\(]",
    r"(?:^|[\s\]])25\.\s*[가-힣A-Za-z\(]",
]

REFERENCE_TAIL_PATTERNS = [
    r'\s*(?:이\s*외|이외)\s*.*?기타 투자판단에 참고할 사항.*$',
    r'\s*세부내용은\s*.*?기타 투자판단에 참고할 사항.*$',
    r'\s*상세내용은\s*.*?기타 투자판단에 참고할 사항.*$',
    r'\s*".*?기타 투자판단에 참고할 사항".*$',
    r'\s*을\s*참고(?:하여)?\s*주시기\s*바랍니다\.?$',
]


def _find_earliest_match(text: str, patterns: List[str], start_pos: int = 0):
    best = None
    for pat in patterns:
        m = re.search(pat, text[start_pos:], flags=re.IGNORECASE)
        if m:
            abs_start = start_pos + m.start()
            abs_end = start_pos + m.end()
            if best is None or abs_start < best[0]:
                best = (abs_start, abs_end, pat)
    return best


def locate_call_option_span(text: str) -> Optional[Tuple[int, int]]:
    raw = _clean_line(text)
    if not raw:
        return None

    start_match = _find_earliest_match(raw, CALL_START_PATTERNS)
    if not start_match:
        return None

    start_idx = start_match[0]
    sub = raw[start_idx:]

    cut = len(sub)
    end_match = _find_earliest_match(sub[1:], CALL_END_PATTERNS)
    if end_match:
        cut = min(cut, end_match[0] + 1)

    end_idx = start_idx + cut
    if end_idx <= start_idx:
        return None

    return start_idx, end_idx


def _trim_reference_tail(text: str) -> str:
    s = _clean_line(text)
    for pat in REFERENCE_TAIL_PATTERNS:
        s = re.sub(pat, "", s, flags=re.IGNORECASE).strip()
    return s


def extract_call_option_text_from_section(section_text: str) -> str:
    if not section_text:
        return ""

    raw = _clean_line(section_text)
    span = locate_call_option_span(raw)
    if not span:
        return ""

    start_idx, end_idx = span
    result = raw[start_idx:end_idx].strip()
    result = _trim_reference_tail(result)
    result = re.sub(r"\s{2,}", " ", result)
    return result.strip()


def remove_call_option_text_from_section(section_text: str) -> str:
    if not section_text:
        return ""

    raw = _clean_line(section_text)
    span = locate_call_option_span(raw)
    if not span:
        return raw

    start_idx, end_idx = span
    kept = (raw[:start_idx] + " " + raw[end_idx:]).strip()

    kept = _trim_reference_tail(kept)
    kept = re.sub(r"\s{2,}", " ", kept)
    kept = re.sub(r"\s+([,\.\)])", r"\1", kept)
    kept = re.sub(r"(\(\s+)", "(", kept)
    return kept.strip()


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
) -> Tuple[str, str, List[Tuple[str, str]]]:
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

    pairs: List[Tuple[str, str]] = []

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

        blank_streak = 0
        for rr in range(header_row + 1, R):
            row_vals = [normalize_text(x) for x in arr[rr].tolist()]
            row_join = " ".join([x for x in row_vals if x])

            if not row_join:
                blank_streak += 1
                if blank_streak >= 2:
                    break
                continue
            blank_streak = 0

            first_nonempty = next((x for x in row_vals if x), "")
            if _is_top_heading(first_nonempty):
                break

            call_val = ""
            ytc_val = ""

            if call_col is not None and call_col < C:
                call_val = _to_pct_text(arr[rr][call_col], min_v=0, max_v=100)

            if ytc_col is not None and ytc_col < C:
                ytc_val = _to_pct_text(arr[rr][ytc_col], min_v=0, max_v=30)

            if not call_val and call_col is not None:
                for cc in range(max(0, call_col - 1), min(C, call_col + 2)):
                    call_val = _to_pct_text(arr[rr][cc], min_v=0, max_v=100)
                    if call_val:
                        break

            if not ytc_val and ytc_col is not None:
                for cc in range(max(0, ytc_col - 1), min(C, ytc_col + 2)):
                    ytc_val = _to_pct_text(arr[rr][cc], min_v=0, max_v=30)
                    if ytc_val:
                        break

            if call_val or ytc_val:
                pairs.append((call_val, ytc_val))

    uniq_pairs = []
    for p in pairs:
        if p not in uniq_pairs:
            uniq_pairs.append(p)

    for call_val, ytc_val in uniq_pairs:
        if call_val and ytc_val:
            return call_val, ytc_val, uniq_pairs

    for call_val, ytc_val in uniq_pairs:
        if call_val or ytc_val:
            return call_val, ytc_val, uniq_pairs

    return "", "", []


# ==========================================================
# [Call 본문에서 Call 비율 / YTC 추출]
# ==========================================================
def extract_call_ratio_and_ytc_from_text(text: str) -> Tuple[str, str]:
    if not text:
        return "", ""

    ratio = ""
    ytc = ""

    ratio_patterns = [
        r"(?:행사비율|콜옵션비율|매도청구권\s*행사비율|Call\s*비율)\s*[:=]?\s*(\d+(?:\.\d+)?)\s*%",
        r"(?:권면총액|권면액|전자등록총액|전자등록금액|인수금액|발행금액|사채원금)\s*(?:의|중)\s*(\d+(?:\.\d+)?)\s*%",
        r"(\d+(?:\.\d+)?)\s*%\s*(?:에\s*해당하는|이내의\s*범위|총\s*한도로)",
    ]
    ytc_patterns = [
        r"(?:YTC|매도청구권보장수익률|매도청구수익률|조기상환수익률|조기상환이율|연복리수익률)\s*[:=]?\s*(\d+(?:\.\d+)?)\s*%",
        r"연\s*복리\s*(\d+(?:\.\d+)?)\s*%",
        r"연복리\s*(\d+(?:\.\d+)?)\s*%",
        r"IRR.*?연\s*(\d+(?:\.\d+)?)\s*%",
        r"내부수익률.*?연\s*(\d+(?:\.\d+)?)\s*%",
        r"연\s*(\d+(?:\.\d+)?)\s*%\s*\(\s*3개월\s*단위\s*복리계산\s*\)",
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
# - Put Option = 9.1 전체 - Call block 제거
# - Call Option = Put 원문 안에서 잘라낸 Call block (헤딩 포함)
# - 단, 9.1이 참조 문장만 있거나 22.기타 투자판단...이 들어오면
#   Put/Call 둘 다 "공시 확인 바람"
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

    # 1) 9.1 전체 섹션 추출
    section_91 = extract_91_option_section_from_lines(lines)
    if not section_91:
        section_91 = extract_91_option_section_from_corpus(corpus)

    section_91 = _clean_line(section_91)

    # 2) 예외 처리
    force_disclosure_check = False
    if section_91:
        if _is_reference_only_91_section(section_91):
            force_disclosure_check = True
        elif _contains_invalid_22_reference_in_91(section_91):
            force_disclosure_check = True

    # 3) Put / Call 추출
    if force_disclosure_check:
        row["Put Option"] = "공시 확인 바람"
        row["Call Option"] = "공시 확인 바람"
        call_text = ""
    else:
        # section_91에서 Call block 추출 (헤딩 포함)
        call_text = extract_call_option_text_from_section(section_91)

        # 9.1 안에서 못 찾으면 전체 corpus에서 fallback
        if not call_text:
            call_text = extract_call_option_text_from_section(corpus)

        # Put Option에서는 Call block 제거
        put_text = remove_call_option_text_from_section(section_91) if call_text else section_91

        row["Put Option"] = put_text if put_text else "공시 확인 바람"
        row["Call Option"] = call_text if call_text else "공시 확인 바람"

    # 4) Call 비율 / YTC : 표 key-value 우선
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

    # 5) 표 grid fallback
    if not row["Call 비율"] or not row["YTC"]:
        table_ratio, table_ytc, _ = extract_call_ratio_ytc_from_table_grid(tables)

        if not row["Call 비율"]:
            row["Call 비율"] = table_ratio
        if not row["YTC"]:
            row["YTC"] = table_ytc

    # 6) Call 본문 fallback
    if (not row["Call 비율"] or not row["YTC"]) and call_text and call_text != "공시 확인 바람":
        ext_ratio, ext_ytc = extract_call_ratio_and_ytc_from_text(call_text)

        if not row["Call 비율"]:
            row["Call 비율"] = ext_ratio
        if not row["YTC"]:
            row["YTC"] = ext_ytc

    return row
