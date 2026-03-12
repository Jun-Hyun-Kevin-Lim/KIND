import re
from typing import Dict, List, Tuple, Any

import pandas as pd

from parser import (
    normalize_text,
    _norm,
    all_text_lines,
    clean_title,
    is_correction_title,
    extract_correction_after_map,
    clean_percent,
    parse_float_like,
    scan_label_value_preferring_correction,
)


# ==========================================================
# [옵션 본문용 corpus 생성]
# - RAW tables를 줄 단위 텍스트로 펴서 하나의 문자열로 만듦
# ==========================================================
def _option_corpus_from_tables(tables: List[pd.DataFrame]) -> str:
    lines = []
    for line in all_text_lines(tables):
        s = normalize_text(line)
        if s:
            lines.append(s)

    corpus = "\n".join(lines)
    corpus = corpus.replace("\xa0", " ")
    corpus = re.sub(r"[ \t]+", " ", corpus)
    corpus = re.sub(r"\n{2,}", "\n", corpus)
    return corpus.strip()


# ==========================================================
# [정규식 매치 helper]
# ==========================================================
def _find_first_match(text: str, patterns: List[str], flags=re.I | re.M):
    matches = []
    for pat in patterns:
        m = re.search(pat, text, flags=flags)
        if m:
            matches.append(m)
    if not matches:
        return None
    matches.sort(key=lambda x: x.start())
    return matches[0]


def _find_earliest_start(text: str, patterns: List[str], flags=re.I | re.M) -> int:
    starts = []
    for pat in patterns:
        m = re.search(pat, text, flags=flags)
        if m:
            starts.append(m.start())
    return min(starts) if starts else -1


# ==========================================================
# [9-1 옵션 대섹션 자르기]
# - 있으면 9-1 옵션 파트만 남기고
# - 없으면 전체 corpus 그대로 사용
# ==========================================================
def _slice_option_major_section(corpus: str) -> str:
    if not corpus:
        return ""

    start_patterns = [
        r"\n?\s*9\s*[-\.]?\s*1\s*[\.\)]?\s*옵션에\s*관한\s*사항",
        r"\n?\s*9\s*[-\.]?\s*1\s*[\.\)]?\s*기타\s*투자판단에\s*참고할\s*사항",
        r"\n?\s*옵션에\s*관한\s*사항",
    ]

    end_patterns = [
        r"\n\s*9\s*[-\.]?\s*2\s*[\.\)]\s*[가-힣A-Za-z]",
        r"\n\s*10\s*[\.\)]\s*[가-힣A-Za-z]",
        r"\n\s*11\s*[\.\)]\s*[가-힣A-Za-z]",
        r"\n\s*12\s*[\.\)]\s*[가-힣A-Za-z]",
        r"\n\s*【",
        r"\n\s*금융위원회\s*/\s*한국거래소\s*귀중",
    ]

    m = _find_first_match(corpus, start_patterns)
    if not m:
        return corpus

    sub = corpus[m.start():]
    end_idx = _find_earliest_start(sub[1:], end_patterns)
    if end_idx == -1:
        return sub.strip()

    return sub[: 1 + end_idx].strip()


# ==========================================================
# [옵션 결과 텍스트 정리]
# - 표 헤더/잡행 제거
# - 파이프 제거
# - 공백 정리
# ==========================================================
def _cleanup_option_result(text: str) -> str:
    if not text:
        return ""

    text = text.replace("\xa0", " ")
    text = re.sub(r"\s*\|\s*", " ", text)

    cleaned_lines = []
    for raw in re.split(r"\n+", text):
        s = normalize_text(raw)
        if not s:
            continue

        s_norm = _norm(s)

        if s in ["From", "To"]:
            continue
        if s_norm in ["from", "to", "구분", "비율"]:
            continue
        if "구분" in s and ("청구기간" in s or "행사기간" in s):
            continue
        if "From To" in s:
            continue

        cleaned_lines.append(s)

    out = " ".join(cleaned_lines)
    out = re.sub(r"\(주\d+\)", " ", out)
    out = re.sub(r"\s+", " ", out).strip()
    out = re.sub(r"^[\s:：\-–]+", "", out).strip()

    return out


# ==========================================================
# [Put / Call 헤더 패턴]
# ==========================================================
PUT_HEADER_PATTERNS = [
    r"\[\s*조기상환청구권\s*\(\s*Put\s*Option\s*\)\s*에\s*관한\s*사항\s*\]",
    r"\[\s*조기상환청구권\s*\(\s*PUT\s*OPTION\s*\)\s*에\s*관한\s*사항\s*\]",
    r"조기상환청구권\s*\(\s*Put\s*Option\s*\)\s*에\s*관한\s*사항",
    r"사채권자의\s*조기상환청구권.*?에\s*관한\s*사항",
]

CALL_HEADER_PATTERNS = [
    r"\[\s*매도청구권\s*\(\s*Call\s*Option\s*\)\s*에\s*관한\s*사항\s*\]",
    r"\[\s*중도상환청구권\s*\(\s*Call\s*Option\s*\)\s*에\s*관한\s*사항\s*\]",
    r"\[\s*매도청구권\s*\(\s*CALL\s*OPTION\s*\)\s*에\s*관한\s*사항\s*\]",
    r"\[\s*중도상환청구권\s*\(\s*CALL\s*OPTION\s*\)\s*에\s*관한\s*사항\s*\]",
    r"매도청구권\s*\(\s*Call\s*Option\s*\)\s*에\s*관한\s*사항",
    r"중도상환청구권\s*\(\s*Call\s*Option\s*\)\s*에\s*관한\s*사항",
    r"발행회사의\s*(?:매도청구권|중도상환청구권).*?에\s*관한\s*사항",
]

NEXT_SECTION_PATTERNS = [
    r"\n\s*\[[^\n\]]{1,120}?에\s*관한\s*사항\s*\]",
    r"\n\s*9\s*[-\.]?\s*2\s*[\.\)]\s*[가-힣A-Za-z]",
    r"\n\s*10\s*[\.\)]\s*[가-힣A-Za-z]",
    r"\n\s*11\s*[\.\)]\s*[가-힣A-Za-z]",
    r"\n\s*12\s*[\.\)]\s*[가-힣A-Za-z]",
    r"\n\s*【",
]


# ==========================================================
# [섹션 자르기 공통]
# - start 헤더 뒤부터 시작
# - stop 패턴 나오기 직전까지 자름
# ==========================================================
def _extract_section_by_headers(
    base_text: str,
    start_patterns: List[str],
    stop_patterns: List[str],
) -> str:
    if not base_text:
        return ""

    m = _find_first_match(base_text, start_patterns)
    if not m:
        return ""

    sub = base_text[m.end():]
    end_idx = _find_earliest_start(sub, stop_patterns)
    if end_idx != -1:
        sub = sub[:end_idx]

    return _cleanup_option_result(sub)


# ==========================================================
# [Put 추출]
# - Put 헤더 뒤 ~ Call 헤더 전
# - 9-1 섹션에서 먼저 찾고, 없으면 전체 corpus에서 찾음
# ==========================================================
def extract_put_option_text(tables: List[pd.DataFrame]) -> str:
    corpus = _option_corpus_from_tables(tables)
    if not corpus:
        return ""

    option_area = _slice_option_major_section(corpus)

    stop_patterns = [
        *CALL_HEADER_PATTERNS,
        *NEXT_SECTION_PATTERNS,
    ]

    put_text = _extract_section_by_headers(option_area, PUT_HEADER_PATTERNS, stop_patterns)
    if put_text:
        return put_text

    return _extract_section_by_headers(corpus, PUT_HEADER_PATTERNS, stop_patterns)


# ==========================================================
# [Call 추출]
# - Call 헤더 뒤 ~ 다음 섹션 전
# - 9-1 섹션에서 먼저 찾고, 없으면 전체 corpus에서 찾음
# ==========================================================
def extract_call_option_text(tables: List[pd.DataFrame]) -> str:
    corpus = _option_corpus_from_tables(tables)
    if not corpus:
        return ""

    option_area = _slice_option_major_section(corpus)

    call_text = _extract_section_by_headers(option_area, CALL_HEADER_PATTERNS, NEXT_SECTION_PATTERNS)
    if call_text:
        return call_text

    return _extract_section_by_headers(corpus, CALL_HEADER_PATTERNS, NEXT_SECTION_PATTERNS)


# ==========================================================
# [Call 비율 / YTC 추출]
# - Call 본문에서 우선 추출
# - 못 찾으면 라벨 기반 fallback 가능
# ==========================================================
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


# ==========================================================
# [옵션 전용 레코드 파서]
# - Put / Call / Call 비율 / YTC만 반환
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

    put_text = extract_put_option_text(tables)
    call_text = extract_call_option_text(tables)

    row["Put Option"] = put_text
    row["Call Option"] = call_text

    # 1순위: 라벨 기반
    row["Call 비율"] = clean_percent(
        scan_label_value_preferring_correction(
            tables,
            [
                "콜옵션 행사비율",
                "매도청구권 행사비율",
                "Call 비율",
                "콜옵션 비율",
                "매도청구권 비율",
                "권면총액 대비 비율",
                "행사비율",
            ],
            corr_after,
        )
    )

    row["YTC"] = clean_percent(
        scan_label_value_preferring_correction(
            tables,
            [
                "조기상환수익률",
                "YTC",
                "Yield To Call",
                "조기상환이율",
                "조기상환수익률(%)",
                "연복리수익률",
            ],
            corr_after,
        )
    )

    # 2순위: Call 본문에서 복구
    if not row["Call 비율"] or not row["YTC"]:
        ratio2, ytc2 = extract_call_ratio_and_ytc_from_text(call_text)
        if not row["Call 비율"]:
            row["Call 비율"] = ratio2
        if not row["YTC"]:
            row["YTC"] = ytc2

    return row
