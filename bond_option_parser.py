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
# [옵션 결과 텍스트 정리 (불필요한 공백 및 파이프 제거)]
# ==========================================================
def _cleanup_option_result(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\xa0", " ")
    text = re.sub(r"\s*\|\s*", " ", text)
    text = re.sub(r"\n+", " ", text)  # 줄바꿈을 공백으로 펴기
    text = re.sub(r"\s{2,}", " ", text)
    return text.strip()


# ==========================================================
# [Put / Call 텍스트 지능형 분리 추출]
# - 하나의 코퍼스 내에서 Put과 Call의 위치를 찾아 영리하게 분리합니다.
# ==========================================================
def extract_put_call_texts(corpus: str) -> Tuple[str, str]:
    if not corpus:
        return "", ""

    # DART 공시에서 자주 쓰이는 Put/Call 키워드
    put_regex = re.compile(r"(?:조기상환청구권|Put\s*Option|풋옵션)", re.IGNORECASE)
    call_regex = re.compile(r"(?:매도청구권|중도상환청구권|Call\s*Option|콜옵션)", re.IGNORECASE)

    put_match = put_regex.search(corpus)
    call_match = call_regex.search(corpus)

    put_text = ""
    call_text = ""

    # 둘 다 있는 경우, 먼저 나온 것부터 다음 것이 나오기 전까지 자름
    if put_match and call_match:
        if put_match.start() < call_match.start():
            put_text = corpus[put_match.start() : call_match.start()]
            call_text = corpus[call_match.start() :]
        else:
            call_text = corpus[call_match.start() : put_match.start()]
            put_text = corpus[put_match.start() :]
    elif put_match:
        put_text = corpus[put_match.start() :]
    elif call_match:
        call_text = corpus[call_match.start() :]

    # 뒷부분에 엉뚱한 다음 목차가 딸려오는 것 방지 (예: 10. 합병 관련 사항)
    stop_patterns = [
        r"\s*10\.?\s*합병\s*관련\s*사항",
        r"\s*11\.?\s*청약일",
        r"\s*24\.?\s*기타\s*투자판단에"
    ]
    
    for stop_pat in stop_patterns:
        m_put = re.search(stop_pat, put_text)
        if m_put:
            put_text = put_text[:m_put.start()]
            
        m_call = re.search(stop_pat, call_text)
        if m_call:
            call_text = call_text[:m_call.start()]

    return _cleanup_option_result(put_text), _cleanup_option_result(call_text)


# ==========================================================
# [Call 비율 / YTC 추출 핵심 로직 (실무 정규식 적용)]
# ==========================================================
def extract_call_ratio_and_ytc_from_text(text: str) -> Tuple[str, str]:
    if not text:
        return "", ""

    ratio = ""
    ytc = ""

    # 1. Call 비율 (행사비율) 추출
    # 실무 표현: "권면총액의 50%", "전자등록총액 중 30%", "행사비율 : 50%" 등
    ratio_patterns = [
        r"(?:행사비율|콜옵션비율|매도청구권비율|Call\s*비율)\s*[:=]?\s*(\d+(?:\.\d+)?)\s*%",
        r"(?:권면총액|권면액|전자등록총액|전자등록금액|인수금액|발행금액|사채원금)\s*(?:의|중)\s*(\d+(?:\.\d+)?)\s*%",
        r"(\d+(?:\.\d+)?)\s*%\s*(?:이내의\s*범위|에\s*해당하는|에\s*대하여\s*매도)"
    ]
    for pat in ratio_patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            ratio = f"{m.group(1)}%"
            break

    # 2. YTC (수익률) 추출
    # 실무 표현: "YTC : 7.0%", "연 복리 4.0%", "연 1%를 적용", "내부수익률(IRR)이 연 9.5%" 등
    ytc_patterns = [
        r"(?:YTC|매도청구권보장수익률|매도청구수익률|조기상환수익률|조기상환이율|연복리수익률)\s*[:=]?\s*(\d+(?:\.\d+)?)\s*%",
        r"내부수익률\s*\(?IRR\)?\s*(?:이|은)?\s*연\s*(\d+(?:\.\d+)?)\s*%",
        r"연\s*복리\s*(\d+(?:\.\d+)?)\s*%",
        r"연\s*(\d+(?:\.\d+)?)\s*%\s*(?:의\s*이율|를\s*적용|로\s*계산)"
    ]
    for pat in ytc_patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            ytc = f"{m.group(1)}%"
            break

    # 3. 못 찾았을 경우의 Fallback (일반적인 % 기호 매칭 중 논리적인 값)
    if not ratio or not ytc:
        percent_matches = re.findall(r"(\d+(?:\.\d+)?)\s*%", text)
        for p in percent_matches:
            val = float(p)
            # 보통 Call 비율은 10% ~ 100% 사이의 큰 값
            if not ratio and 10 <= val <= 100:
                ratio = f"{p}%"
            # 보통 YTC는 0% ~ 20% 사이의 작은 값
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

    # 전체 코퍼스를 평탄화하여 가져옴
    corpus = _option_corpus_from_tables(tables)
    
    # 1. Put / Call 본문 분리 추출
    put_text, call_text = extract_put_call_texts(corpus)
    row["Put Option"] = put_text
    row["Call Option"] = call_text

    # 2. 정형화된 표(Key-Value)에서 먼저 스캔 시도 (우선순위 1)
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

    # 3. 표에서 못 찾았으면 Call Option 본문 텍스트에서 정규식으로 지능형 추출 (우선순위 2)
    if not row["Call 비율"] or not row["YTC"]:
        extracted_ratio, extracted_ytc = extract_call_ratio_and_ytc_from_text(call_text)
        if not row["Call 비율"]:
            row["Call 비율"] = extracted_ratio
        if not row["YTC"]:
            row["YTC"] = extracted_ytc

    return row
