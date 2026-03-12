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
# [옵션 본문용 corpus 생성]
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
# [텍스트 정리]
# ==========================================================
def _cleanup_text(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\xa0", " ")
    text = re.sub(r"\s*\|\s*", " ", text)
    text = re.sub(r"\n+", " ", text)
    text = re.sub(r"\s{2,}", " ", text)
    return text.strip()


# ==========================================================
# [% 안전 정리]
# ==========================================================
def _safe_percent(value: Any) -> str:
    if value is None:
        return ""

    s = str(value).strip()
    if not s:
        return ""

    s = clean_percent(s)
    if s:
        return s

    f = parse_float_like(s)
    if f is None:
        return ""

    if float(f).is_integer():
        return f"{int(f)}%"
    return f"{f}%"


# ==========================================================
# [정규식 공통]
# ==========================================================
def _find_first_match(text: str, patterns: List[str]):
    best = None
    for pat in patterns:
        m = re.search(pat, text, flags=re.IGNORECASE | re.MULTILINE)
        if m:
            if best is None or m.start() < best.start():
                best = m
    return best


def _find_next_heading_start(text: str, start_pos: int) -> int:
    """
    9.1 섹션 이후 다음 큰 목차 시작점을 찾는다.
    """
    heading_patterns = [
        r"(?:^|\n)\s*9\s*[-\.]?\s*2\s*[\)\.]?\s+",
        r"(?:^|\n)\s*9\s*[-\.]?\s*3\s*[\)\.]?\s+",
        r"(?:^|\n)\s*10\s*[\)\.]?\s+",
        r"(?:^|\n)\s*11\s*[\)\.]?\s+",
        r"(?:^|\n)\s*12\s*[\)\.]?\s+",
        r"(?:^|\n)\s*13\s*[\)\.]?\s+",
        r"(?:^|\n)\s*20\s*[\)\.]?\s+",
        r"(?:^|\n)\s*21\s*[\)\.]?\s+",
        r"(?:^|\n)\s*22\s*[\)\.]?\s+",
        r"(?:^|\n)\s*23\s*[\)\.]?\s+",
        r"(?:^|\n)\s*24\s*[\)\.]?\s+",
        r"(?:^|\n)\s*25\s*[\)\.]?\s+",
    ]

    cut = len(text)
    sub = text[start_pos:]

    for pat in heading_patterns:
        m = re.search(pat, sub, flags=re.IGNORECASE | re.MULTILINE)
        if m and m.start() > 0:
            cut = min(cut, start_pos + m.start())

    return cut


# ==========================================================
# [9.1 / 9-1 옵션에 관한 사항 섹션 전체 추출]
# - 찾은 섹션 전체를 그대로 반환
# ==========================================================
def extract_91_option_section(corpus: str) -> str:
    if not corpus:
        return ""

    start_patterns = [
        r"(?:^|\n)\s*9\s*[-\.]?\s*1\s*[\)\.]?\s*옵션에\s*관한\s*사항",
        r"(?:^|\n)\s*9\s*[-\.]?\s*1\s*[\)\.]?\s*옵션사항",
    ]

    m = _find_first_match(corpus, start_patterns)
    if not m:
        return ""

    start_idx = m.start()
    end_idx = _find_next_heading_start(corpus, start_idx + 1)

    text = corpus[start_idx:end_idx].strip()
    return _cleanup_text(text)


# ==========================================================
# [Call 비율 / YTC 추출]
# - 이제 9.1 전체 텍스트에서 그냥 찾는다
# ==========================================================
def extract_call_ratio_and_ytc_from_text(text: str) -> Tuple[str, str]:
    if not text:
        return "", ""

    ratio = ""
    ytc = ""

    ratio_patterns = [
        r"(?:행사비율|콜옵션비율|매도청구권비율|Call\s*비율)\s*[:=]?\s*(\d+(?:\.\d+)?)\s*%",
        r"(?:권면총액|권면액|전자등록총액|전자등록금액|인수금액|발행금액|사채원금)\s*(?:의|중)\s*(\d+(?:\.\d+)?)\s*%",
        r"(\d+(?:\.\d+)?)\s*%\s*(?:이내의\s*범위|에\s*해당하는|에\s*대하여\s*매도)",
    ]
    for pat in ratio_patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            ratio = f"{m.group(1)}%"
            break

    ytc_patterns = [
        r"(?:YTC|매도청구권보장수익률|매도청구수익률|조기상환수익률|조기상환이율|연복리수익률)\s*[:=]?\s*(\d+(?:\.\d+)?)\s*%",
        r"내부수익률\s*\(?IRR\)?\s*(?:이|은)?\s*연\s*(\d+(?:\.\d+)?)\s*%",
        r"연\s*복리\s*(\d+(?:\.\d+)?)\s*%",
        r"연\s*(\d+(?:\.\d+)?)\s*%\s*(?:의\s*이율|를\s*적용|로\s*계산)",
    ]
    for pat in ytc_patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            ytc = f"{m.group(1)}%"
            break

    if not ratio or not ytc:
        percent_matches = re.findall(r"(\d+(?:\.\d+)?)\s*%", text)
        for p in percent_matches:
            val = float(p)

            if not ratio and 10 <= val <= 100:
                ratio = f"{p}%"
                continue

            if not ytc and 0 <= val < 15:
                ytc = f"{p}%"

    return ratio, ytc


# ==========================================================
# [옵션 전용 레코드 파서]
# - 9.1 전체를 Put Option에 넣는다
# - Call Option은 일단 비워둔다
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

    corpus = _option_corpus_from_tables(tables)

    # 핵심: 9.1 섹션 전체를 Put Option에 넣기
    section_91 = extract_91_option_section(corpus)
    row["Put Option"] = section_91 if section_91 else "공시 확인 바람"

    # Call Option은 이번 버전에서는 분리 안 함
    row["Call Option"] = ""

    # 표(Key-Value) 우선 추출
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

    # 표에서 못 찾으면 9.1 전체 텍스트에서 fallback
    if not row["Call 비율"] or not row["YTC"]:
        extracted_ratio, extracted_ytc = extract_call_ratio_and_ytc_from_text(section_91)

        if not row["Call 비율"]:
            row["Call 비율"] = extracted_ratio

        if not row["YTC"]:
            row["YTC"] = extracted_ytc

    return row
