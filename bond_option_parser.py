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
# [라인 정리]
# ==========================================================
def _clean_line(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\xa0", " ")
    text = re.sub(r"\s*\|\s*", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _lines_from_tables(tables: List[pd.DataFrame]) -> List[str]:
    out = []
    for line in all_text_lines(tables):
        s = _clean_line(normalize_text(line))
        if s:
            out.append(s)
    return out


def _corpus_from_lines(lines: List[str]) -> str:
    return "\n".join([x for x in lines if x]).strip()


# ==========================================================
# [% 안전 정리]
# ==========================================================
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


# ==========================================================
# [9.1 섹션 헤딩 판별]
# ==========================================================
def _is_91_heading(line: str) -> bool:
    s = _clean_line(line)
    if not s:
        return False

    patterns = [
        r"^9\s*[\.\-]?\s*1\s*[\)\.]?\s*옵션에\s*관한\s*사항",
        r"^9\s*[\.\-]?\s*1\s*[\)\.]?\s*옵션사항",
        r"^9\s*[\.\-]?\s*1\s*[\)\.]?\s*조기상환청구권",
        r"^9\s*[\.\-]?\s*1\s*[\)\.]?\s*매도청구권",
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


# ==========================================================
# [9.1 섹션 추출 - 라인 기반]
# ==========================================================
def extract_91_option_section_from_lines(lines: List[str]) -> str:
    if not lines:
        return ""

    started = False
    bucket = []

    for line in lines:
        if not started:
            if _is_91_heading(line):
                started = True
                bucket.append(line)
            continue

        if _is_next_major_heading(line):
            break

        bucket.append(line)

    text = " ".join(bucket).strip()
    text = re.sub(r"\s{2,}", " ", text)
    return text


# ==========================================================
# [9.1 섹션 추출 - corpus fallback]
# ==========================================================
def extract_91_option_section_from_corpus(corpus: str) -> str:
    if not corpus:
        return ""

    start_patterns = [
        r"(?:^|\n)\s*9\s*[\.\-]?\s*1\s*[\)\.]?\s*옵션에\s*관한\s*사항",
        r"(?:^|\n)\s*9\s*[\.\-]?\s*1\s*[\)\.]?\s*옵션사항",
        r"(?:^|\n)\s*9\s*[\.\-]?\s*1\s*[\)\.]?\s*조기상환청구권",
        r"(?:^|\n)\s*9\s*[\.\-]?\s*1\s*[\)\.]?\s*매도청구권",
    ]

    start_match = None
    for pat in start_patterns:
        m = re.search(pat, corpus, flags=re.IGNORECASE | re.MULTILINE)
        if m:
            if start_match is None or m.start() < start_match.start():
                start_match = m

    if not start_match:
        return ""

    start_idx = start_match.start()
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
    return text


# ==========================================================
# [Call 비율 / YTC 추출]
# ==========================================================
def extract_call_ratio_and_ytc_from_text(text: str) -> Tuple[str, str]:
    if not text:
        return "", ""

    ratio = ""
    ytc = ""

    ratio_patterns = [
        r"(?:행사비율|콜옵션비율|매도청구권\s*행사비율|Call\s*비율)\s*[:=]?\s*(\d+(?:\.\d+)?)\s*%",
        r"(?:권면총액|권면액|전자등록총액|전자등록금액|인수금액|발행금액|사채원금)\s*(?:의|중)\s*(\d+(?:\.\d+)?)\s*%",
        r"(\d+(?:\.\d+)?)\s*%\s*(?:이내의\s*범위|에\s*해당하는\s*금액|까지\s*매도청구)",
    ]
    for pat in ratio_patterns:
        m = re.search(pat, text, flags=re.IGNORECASE)
        if m:
            ratio = f"{m.group(1)}%"
            break

    ytc_patterns = [
        r"(?:YTC|매도청구권보장수익률|매도청구수익률|조기상환수익률|조기상환이율|연복리수익률)\s*[:=]?\s*(\d+(?:\.\d+)?)\s*%",
        r"연\s*(\d+(?:\.\d+)?)\s*%\s*\(\s*3개월\s*단위\s*복리계산\s*\)",
        r"연\s*(\d+(?:\.\d+)?)\s*%\s*(?:의\s*이율|를\s*가산|로\s*계산)",
        r"연복리\s*(\d+(?:\.\d+)?)\s*%",
    ]
    for pat in ytc_patterns:
        m = re.search(pat, text, flags=re.IGNORECASE)
        if m:
            ytc = f"{m.group(1)}%"
            break

    if not ratio or not ytc:
        percent_matches = re.findall(r"(\d+(?:\.\d+)?)\s*%", text)
        for p in percent_matches:
            try:
                val = float(p)
            except Exception:
                continue

            if not ratio and 10 <= val <= 100:
                ratio = f"{p}%"
                continue

            if not ytc and 0 < val < 15:
                ytc = f"{p}%"

    return ratio, ytc


# ==========================================================
# [옵션 전용 레코드 파서]
# - 9.1 섹션 전체를 Put Option에 저장
# - Call Option은 이번 버전에서는 분리하지 않음
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

    # 1순위: 라인 기반
    section_91 = extract_91_option_section_from_lines(lines)

    # 2순위: corpus fallback
    if not section_91:
        section_91 = extract_91_option_section_from_corpus(corpus)

    # 최종 Put Option
    row["Put Option"] = section_91 if section_91 else "공시 확인 바람"

    # 이번 버전은 Call Option 분리 안 함
    row["Call Option"] = ""

    # 표 key-value 우선
    row["Call 비율"] = _safe_percent(
        scan_label_value_preferring_correction(
            tables,
            [
                "콜옵션 행사비율",
                "매도청구권 행사비율",
                "Call 비율",
                "행사비율",
            ],
            corr_after,
        )
    )

    row["YTC"] = _safe_percent(
        scan_label_value_preferring_correction(
            tables,
            [
                "조기상환수익률",
                "YTC",
                "Yield To Call",
                "연복리수익률",
                "매도청구권보장수익률",
            ],
            corr_after,
        )
    )

    # 표에서 못 찾으면 9.1 텍스트에서 fallback
    if not row["Call 비율"] or not row["YTC"]:
        ext_ratio, ext_ytc = extract_call_ratio_and_ytc_from_text(section_91)

        if not row["Call 비율"]:
            row["Call 비율"] = ext_ratio

        if not row["YTC"]:
            row["YTC"] = ext_ytc

    return row
