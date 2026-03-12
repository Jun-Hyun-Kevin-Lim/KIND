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
# [옵션 섹션 헤더 패턴]
# ==========================================================
def _option_heading_patterns(option_type: str) -> List[str]:
    if option_type == "put":
        return [
            r"(?:^|\n)\s*[\[【(]?\s*조기상환청구권\s*\(\s*Put\s*Option\s*\)\s*에\s*관한\s*사항\s*[\]】)]?",
            r"(?:^|\n)\s*[\[【(]?\s*조기상환청구권\s*에\s*관한\s*사항\s*[\]】)]?",
            r"(?:^|\n)\s*[\[【(]?\s*Put\s*Option\s*에\s*관한\s*사항\s*[\]】)]?",
            r"(?:^|\n)\s*[\[【(]?\s*풋옵션\s*에\s*관한\s*사항\s*[\]】)]?",
        ]

    return [
        r"(?:^|\n)\s*[\[【(]?\s*매도청구권\s*\(\s*Call\s*Option\s*\)\s*에\s*관한\s*사항\s*[\]】)]?",
        r"(?:^|\n)\s*[\[【(]?\s*중도상환청구권\s*\(\s*Call\s*Option\s*\)\s*에\s*관한\s*사항\s*[\]】)]?",
        r"(?:^|\n)\s*[\[【(]?\s*매도청구권\s*에\s*관한\s*사항\s*[\]】)]?",
        r"(?:^|\n)\s*[\[【(]?\s*중도상환청구권\s*에\s*관한\s*사항\s*[\]】)]?",
        r"(?:^|\n)\s*[\[【(]?\s*Call\s*Option\s*에\s*관한\s*사항\s*[\]】)]?",
        r"(?:^|\n)\s*[\[【(]?\s*콜옵션\s*에\s*관한\s*사항\s*[\]】)]?",
    ]


def _find_first_pattern(text: str, patterns: List[str]) -> Tuple[int, int]:
    best = None
    for pat in patterns:
        m = re.search(pat, text, flags=re.IGNORECASE | re.MULTILINE)
        if m:
            if best is None or m.start() < best[0]:
                best = (m.start(), m.end())
    return best if best else (-1, -1)


def _find_first_stop_after(text: str, start_idx: int, stop_patterns: List[str]) -> int:
    cut_idx = len(text)
    for pat in stop_patterns:
        for m in re.finditer(pat, text, flags=re.IGNORECASE | re.MULTILINE):
            if m.start() > start_idx + 5:
                cut_idx = min(cut_idx, m.start())
                break
    return cut_idx


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
            r"(?:조기상환청구권|매도청구권|중도상환청구권|풋옵션|콜옵션|Put\s*Option|Call\s*Option)"
            r"[^가-힣A-Za-z0-9]{0,30}"
            r"(?:에\s*관한\s*사항|청구권자|행사|부여|비율|한도)?"
            r"\s*[:>\-]\s*",
            "",
            result,
            flags=re.IGNORECASE,
        )

        result = re.sub(r"^\s*[:>\-\]\)]+\s*", "", result)

    return _cleanup_option_result(result)


def _cut_option_noise(text: str, option_type: str) -> str:
    if not text:
        return ""

    if option_type == "put":
        opp_kws = ["매도청구권", "중도상환청구권", "Call Option", "콜옵션"]
    else:
        opp_kws = ["조기상환청구권", "Put Option", "풋옵션"]

    stop_patterns = [
        *[re.escape(x) for x in opp_kws],
        r"\n\s*\d{1,2}\.\s*합병\s*관련\s*사항",
        r"\n\s*\d{1,2}\.\s*청약일",
        r"\n\s*\d{1,2}\.\s*납입일",
        r"\n\s*\d{1,2}\.\s*기타\s*투자판단",
        r"\n\s*\d{1,2}\.\s*기타사항",
        r"【\s*특정인",
        r"\[\s*특정인",
        r"사채권자의\s*본\s*사채\s*의무보유",
        r"\b의무보유\b",
    ]

    cut_idx = len(text)
    for pat in stop_patterns:
        m = re.search(pat, text, flags=re.IGNORECASE)
        if m and m.start() > 20:
            cut_idx = min(cut_idx, m.start())

    text = text[:cut_idx]
    return _cleanup_option_result(text)


def _extract_option_by_heading(corpus: str, option_type: str) -> str:
    if not corpus:
        return ""

    my_heads = _option_heading_patterns(option_type)
    opp_heads = _option_heading_patterns("call" if option_type == "put" else "put")

    generic_stops = [
        r"(?:^|\n)\s*\d{1,2}\.\s*합병\s*관련\s*사항",
        r"(?:^|\n)\s*\d{1,2}\.\s*청약일",
        r"(?:^|\n)\s*\d{1,2}\.\s*납입일",
        r"(?:^|\n)\s*\d{1,2}\.\s*기타\s*투자판단",
        r"(?:^|\n)\s*\d{1,2}\.\s*기타사항",
        r"(?:^|\n)\s*[【\[]\s*특정인",
    ]

    start_idx, _ = _find_first_pattern(corpus, my_heads)
    if start_idx < 0:
        return ""

    end_idx = _find_first_stop_after(corpus, start_idx, opp_heads + generic_stops)
    result = corpus[start_idx:end_idx]
    result = _strip_option_shell(result)
    result = _cut_option_noise(result, option_type)

    if len(result) < 5:
        return ""
    return result


# ==========================================================
# [Put / Call 본문 추출 - 헤더 우선 + 문맥 fallback]
# ==========================================================
def extract_option_details_from_corpus(corpus: str, option_type: str) -> str:
    if not corpus:
        return ""

    # 1) 섹션 제목 기준 우선 추출
    by_heading = _extract_option_by_heading(corpus, option_type)
    if by_heading:
        return by_heading

    # 2) fallback: 키워드 주변 window를 점수화
    if option_type == "put":
        my_kws = ["조기상환청구권", "Put Option", "풋옵션"]
        opp_kws = ["매도청구권", "중도상환청구권", "Call Option", "콜옵션"]
        anchor_regex = (
            r"(본\s*사채의\s*사채권자는|본\s*사채의\s*인수인은|사채권자는|인수인은|"
            r"투자자는|본\s*전환사채의\s*사채권자는)"
        )
    else:
        my_kws = ["매도청구권", "중도상환청구권", "Call Option", "콜옵션"]
        opp_kws = ["조기상환청구권", "Put Option", "풋옵션"]
        anchor_regex = (
            r"(발행회사\s*또는\s*발행회사가\s*지정하는\s*자(?:\([^)]*\))?(?:는|가)?|"
            r"발행회사(?:는|가)|회사는\s*만기\s*전|본\s*사채는\s*만기\s*전)"
        )

    candidates = []

    for kw in my_kws:
        for match in re.finditer(re.escape(kw), corpus, flags=re.IGNORECASE):
            idx = match.start()
            window = corpus[max(0, idx - 80): idx + 1600]

            score = 0
            if option_type == "put":
                if re.search(r"사채권자|인수인|투자자", window, re.IGNORECASE):
                    score += 50
                if re.search(r"청구할\s*수\s*있다|조기상환을\s*청구", window, re.IGNORECASE):
                    score += 50
                if re.search(r"\b의무보유\b", window, re.IGNORECASE):
                    score -= 200
                if re.search(r"매도청구권|중도상환청구권|Call\s*Option|콜옵션", window, re.IGNORECASE):
                    score -= 60
            else:
                if re.search(r"발행회사|발행회사가\s*지정하는\s*자|회사", window, re.IGNORECASE):
                    score += 50
                if re.search(r"매수할\s*수\s*있다|매도를\s*청구|매도청구", window, re.IGNORECASE):
                    score += 50
                if re.search(r"사채권자.*의무보유|\b의무보유\b", window, re.IGNORECASE):
                    score -= 150

            # 요약표 / 헤더 낚시 방지
            if re.search(r"매매일", window, re.IGNORECASE):
                score -= 300
            if re.search(r"상환율", window, re.IGNORECASE):
                score -= 300
            if re.search(r"\bfrom\b.*\bto\b", window, re.IGNORECASE):
                score -= 300
            if re.search(r"성명\s*및\s*관계", window, re.IGNORECASE):
                score -= 300

            candidates.append((score, window))

    if not candidates:
        return ""

    candidates.sort(key=lambda x: x[0], reverse=True)
    best_score, best_window = candidates[0]

    if best_score < 0:
        return ""

    # 3) 진짜 본문 시작 anchor부터 다시 자르기
    m = re.search(anchor_regex, best_window, re.IGNORECASE)
    if m and m.start() < 180:
        result = best_window[m.start():]
    else:
        result = _strip_option_shell(best_window)

    # 4) 반대 옵션 / 다음 목차에서 컷
    stop_patterns = [
        *[re.escape(x) for x in opp_kws],
        r"\n\s*\d{1,2}\.\s*합병\s*관련\s*사항",
        r"\n\s*\d{1,2}\.\s*청약일",
        r"\n\s*\d{1,2}\.\s*납입일",
        r"\n\s*\d{1,2}\.\s*기타\s*투자판단",
        r"\n\s*\d{1,2}\.\s*기타사항",
        r"【\s*특정인",
        r"\[\s*특정인",
        r"사채권자의\s*본\s*사채\s*의무보유",
        r"\b의무보유\b",
    ]

    cut_idx = len(result)
    for pat in stop_patterns:
        m = re.search(pat, result, flags=re.IGNORECASE)
        if m and m.start() > 20:
            cut_idx = min(cut_idx, m.start())

    result = result[:cut_idx]
    result = _cleanup_option_result(result)

    if len(result) < 5:
        return ""

    if re.fullmatch(r"\d{4}년\s*\d{1,2}월\s*\d{1,2}일.*", result) and len(result) < 30:
        return ""

    return result


# ==========================================================
# [Call 비율 / YTC 추출 핵심 로직]
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
            elif not ytc and 0 <= val < 15:
                ytc = f"{p}%"

    return ratio, ytc


# ==========================================================
# [옵션 전용 레코드 파서]
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
    row["Put Option"] = extract_option_details_from_corpus(corpus, "put")
    row["Call Option"] = extract_option_details_from_corpus(corpus, "call")

    put_text = row["Put Option"]
    call_text = row["Call Option"]

    # 2. 정형화된 표(Key-Value)에서 먼저 스캔
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

    # 3. 표에서 못 찾았으면 Call 본문에서 정규식 추출
    if not row["Call 비율"] or not row["YTC"]:
        extracted_ratio, extracted_ytc = extract_call_ratio_and_ytc_from_text(call_text)
        if not row["Call 비율"]:
            row["Call 비율"] = extracted_ratio
        if not row["YTC"]:
            row["YTC"] = extracted_ytc

    return row
