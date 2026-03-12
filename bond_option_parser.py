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
# [옵션 결과 텍스트 정리]
# ==========================================================
def _cleanup_option_result(text: str) -> str:
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
    다음 큰 섹션 시작점을 찾는다.
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
# [9-1. 옵션에 관한 사항 추출]
# ==========================================================
def _extract_91_option_section(corpus: str) -> str:
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
    return corpus[start_idx:end_idx].strip()


# ==========================================================
# [22 / 23 기타 투자판단 참고사항 섹션 추출]
# ==========================================================
def _extract_22_23_reference_sections(corpus: str) -> List[str]:
    sections = []

    start_patterns_map = {
        "22": [
            r"(?:^|\n)\s*22\s*[\)\.]?\s*기타\s*투자판단에\s*참고(?:할)?\s*사항",
            r"(?:^|\n)\s*22\s*[\)\.]?\s*기타\s*투자판단에\s*관한\s*사항",
        ],
        "23": [
            r"(?:^|\n)\s*23\s*[\)\.]?\s*기타\s*투자판단에\s*참고(?:할)?\s*사항",
            r"(?:^|\n)\s*23\s*[\)\.]?\s*기타\s*투자판단에\s*관한\s*사항",
        ],
    }

    for sec_no in ["22", "23"]:
        m = _find_first_match(corpus, start_patterns_map[sec_no])
        if not m:
            continue

        start_idx = m.start()
        end_idx = _find_next_heading_start(corpus, start_idx + 1)
        block = corpus[start_idx:end_idx].strip()
        if block:
            sections.append(block)

    return sections


# ==========================================================
# [9-1 섹션 안에 22/23 참조가 있는지 확인]
# ==========================================================
def _has_reference_to_22_23(text: str) -> bool:
    if not text:
        return False

    ref_patterns = [
        r"22\s*[\)\.]?\s*기타\s*투자판단에\s*참고(?:할)?\s*사항\s*참조",
        r"23\s*[\)\.]?\s*기타\s*투자판단에\s*참고(?:할)?\s*사항\s*참조",
        r"22\s*[\)\.]?\s*기타\s*투자판단에\s*참고(?:할)?\s*사항(?:을)?\s*참고",
        r"23\s*[\)\.]?\s*기타\s*투자판단에\s*참고(?:할)?\s*사항(?:을)?\s*참고",
        r"22\s*[\)\.]?\s*기타\s*투자판단에\s*참고(?:할)?\s*사항을\s*참조",
        r"23\s*[\)\.]?\s*기타\s*투자판단에\s*참고(?:할)?\s*사항을\s*참조",
    ]
    return any(re.search(pat, text, flags=re.IGNORECASE) for pat in ref_patterns)


# ==========================================================
# [Put / Call 헤더 패턴]
# ==========================================================
def _put_heading_patterns() -> List[str]:
    return [
        r"조기상환청구권\s*\(\s*Put\s*Option\s*\)\s*에\s*관한\s*사항",
        r"조기상환청구권\s*에\s*관한\s*사항",
        r"Put\s*Option\s*에\s*관한\s*사항",
        r"\[\s*PUT\s*OPTION\s*\]",
        r"풋옵션\s*에\s*관한\s*사항",
    ]


def _call_heading_patterns() -> List[str]:
    return [
        r"매도청구권\s*\(\s*Call\s*Option\s*\)\s*에\s*관한\s*사항",
        r"중도상환청구권\s*\(\s*Call\s*Option\s*\)\s*에\s*관한\s*사항",
        r"매도청구권\s*에\s*관한\s*사항",
        r"중도상환청구권\s*에\s*관한\s*사항",
        r"Call\s*Option\s*에\s*관한\s*사항",
        r"\[\s*CALL\s*OPTION\s*\]",
        r"콜옵션\s*에\s*관한\s*사항",
    ]


# ==========================================================
# [추출 후 앞껍질 제거]
# ==========================================================
def _strip_option_shell(text: str) -> str:
    if not text:
        return ""

    result = text.strip()

    for _ in range(3):
        result = re.sub(
            r"^\s*(?:[\[【(<]?\s*[①-⑩\d가-힣a-zA-Z]+\s*[\].)】>: -]*\s*)+",
            "",
            result,
            flags=re.IGNORECASE,
        )

        result = re.sub(
            r"^\s*(?:본\s*사채의\s*|발행회사의\s*)?"
            r"(?:조기상환청구권|매도청구권|중도상환청구권|풋옵션|콜옵션|Put\s*Option|Call\s*Option|PUT\s*OPTION|CALL\s*OPTION)"
            r"[^가-힣A-Za-z0-9]{0,40}"
            r"(?:에\s*관한\s*사항|청구권자|행사|부여|비율|한도)?"
            r"\s*[:>\-]\s*",
            "",
            result,
            flags=re.IGNORECASE,
        )

        result = re.sub(r"^\s*[:>\-\]\)]+\s*", "", result)

    return _cleanup_option_result(result)


# ==========================================================
# [옵션 텍스트 뒤 노이즈 제거]
# ==========================================================
def _cut_option_noise(text: str, option_type: str) -> str:
    if not text:
        return ""

    if option_type == "put":
        opp_kws = ["매도청구권", "중도상환청구권", "Call Option", "CALL OPTION", "콜옵션"]
    else:
        opp_kws = ["조기상환청구권", "Put Option", "PUT OPTION", "풋옵션"]

    stop_patterns = [
        *[re.escape(x) for x in opp_kws],
        r"\n\s*Call\s*Option\s*행사",
        r"\n\s*Put\s*Option\s*행사",
        r"(?:^|\n)\s*9\s*[-\.]?\s*2\s*[\)\.]?",
        r"(?:^|\n)\s*10\s*[\)\.]?",
        r"(?:^|\n)\s*11\s*[\)\.]?",
        r"(?:^|\n)\s*12\s*[\)\.]?",
        r"(?:^|\n)\s*22\s*[\)\.]?\s*기타\s*투자판단에\s*참고(?:할)?\s*사항",
        r"(?:^|\n)\s*23\s*[\)\.]?\s*기타\s*투자판단에\s*참고(?:할)?\s*사항",
        r"(?:^|\n)\s*24\s*[\)\.]?",
        r"【\s*특정인",
        r"\[\s*특정인",
        r"사채권자의\s*본\s*사채\s*의무보유",
        r"\b의무보유\b",
    ]

    cut_idx = len(text)
    for pat in stop_patterns:
        m = re.search(pat, text, flags=re.IGNORECASE | re.MULTILINE)
        if m and m.start() > 20:
            cut_idx = min(cut_idx, m.start())

    text = text[:cut_idx]
    return _cleanup_option_result(text)


# ==========================================================
# [한 블록 안에서 Put / Call 분리]
# ==========================================================
def _extract_put_call_from_block(block: str) -> Tuple[str, str]:
    if not block:
        return "", ""

    put_m = _find_first_match(block, _put_heading_patterns())
    call_m = _find_first_match(block, _call_heading_patterns())

    put_text = ""
    call_text = ""

    if put_m and call_m:
        if put_m.start() < call_m.start():
            put_text = block[put_m.end():call_m.start()]
            call_text = block[call_m.end():]
        else:
            call_text = block[call_m.end():put_m.start()]
            put_text = block[put_m.end():]
    elif put_m:
        put_text = block[put_m.end():]
    elif call_m:
        call_text = block[call_m.end():]

    put_text = _cut_option_noise(_strip_option_shell(put_text), "put")
    call_text = _cut_option_noise(_strip_option_shell(call_text), "call")

    return put_text, call_text


# ==========================================================
# [옵션 본문 추출 메인]
# - 1순위: 9-1
# - 2순위: 9-1 안에 22/23 참조가 있으면 해당 섹션
# - 3순위: 더 이상 fallback 없음
# ==========================================================
def extract_put_call_texts(corpus: str) -> Tuple[str, str]:
    if not corpus:
        return "", ""

    put_text = ""
    call_text = ""

    # 1) 9-1 우선
    section_91 = _extract_91_option_section(corpus)
    if section_91:
        p91, c91 = _extract_put_call_from_block(section_91)
        if p91:
            put_text = p91
        if c91:
            call_text = c91

        # 2) 9-1에 22/23 참조가 있으면 그쪽도 탐색
        if _has_reference_to_22_23(section_91):
            ref_sections = _extract_22_23_reference_sections(corpus)
            for sec in ref_sections:
                p_ref, c_ref = _extract_put_call_from_block(sec)

                if not put_text and p_ref:
                    put_text = p_ref
                if not call_text and c_ref:
                    call_text = c_ref

                if put_text and call_text:
                    break

    # 3) 9-1에서 못 찾았으면 22/23 단독 탐색
    if not put_text or not call_text:
        ref_sections = _extract_22_23_reference_sections(corpus)
        for sec in ref_sections:
            p_ref, c_ref = _extract_put_call_from_block(sec)

            if not put_text and p_ref:
                put_text = p_ref
            if not call_text and c_ref:
                call_text = c_ref

            if put_text and call_text:
                break

    return _cleanup_option_result(put_text), _cleanup_option_result(call_text)


# ==========================================================
# [Call 비율 / YTC 추출]
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
# - Put / Call 못 찾으면 "공시 확인 바람"
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

    # 1. Put / Call 본문 추출
    put_text, call_text = extract_put_call_texts(corpus)

    row["Put Option"] = put_text if put_text else "공시 확인 바람"
    row["Call Option"] = call_text if call_text else "공시 확인 바람"

    # 2. 표(Key-Value) 우선 추출
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

    # 3. 표에서 못 찾았으면 Call 본문에서 fallback
    #    단, "공시 확인 바람"으로 바뀌기 전의 raw call_text 기준으로 본다.
    if not row["Call 비율"] or not row["YTC"]:
        extracted_ratio, extracted_ytc = extract_call_ratio_and_ytc_from_text(call_text)

        if not row["Call 비율"]:
            row["Call 비율"] = extracted_ratio

        if not row["YTC"]:
            row["YTC"] = extracted_ytc

    return row
