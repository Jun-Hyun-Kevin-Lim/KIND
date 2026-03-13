import os
import re
import json
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple, Set

import feedparser
import pandas as pd
from bs4 import BeautifulSoup
import gspread
from playwright.sync_api import sync_playwright


BASE = "https://kind.krx.co.kr"
DEFAULT_RSS = (
    "http://kind.krx.co.kr:80/disclosure/rsstodaydistribute.do"
    "?method=searchRssTodayDistribute&mktTpCd=0&currentPageSize=100"
)

RSS_URL = os.getenv("RSS_URL", DEFAULT_RSS)

KEYWORDS = [
    x.strip()
    for x in os.getenv(
        "KEYWORDS",
        "유상증자결정,전환사채권발행결정,교환사채권발행결정,신주인수권부사채권발행결정"
    ).split(",")
    if x.strip()
]

HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"
LIMIT = int(os.getenv("LIMIT", "30"))
RUN_ONE_ACPTNO = os.getenv("RUN_ONE_ACPTNO", "").strip()

GOOGLE_SHEET_ID = os.environ.get("GOOGLE_SHEET_ID", "").strip()
GOOGLE_CREDENTIALS_JSON = (
    os.environ.get("GOOGLE_CREDENTIALS_JSON", "").strip()
    or os.environ.get("GOOGLE_CREDS", "").strip()
)

DUMP_SHEET_NAME = os.getenv("DUMP_SHEET_NAME", "RAW_dump")
SEEN_SHEET_NAME = os.getenv("SEEN_SHEET_NAME", "seen")

OUTDIR = Path(os.getenv("OUTDIR", "out"))
DEBUGDIR = OUTDIR / "debug"


@dataclass
class Target:
    acpt_no: str
    title: str
    link: str


def extract_acpt_no(text: str) -> Optional[str]:
    if not text:
        return None
    m = re.search(r"acptNo=(\d{14})", text or "", flags=re.I)
    if not m:
        m = re.search(r"acptno=(\d{14})", text or "", flags=re.I)
    return m.group(1) if m else None


def match_keyword(title: str) -> bool:
    return bool(title) and any(k in title for k in KEYWORDS)


def detect_category(title: str) -> str:
    for k in KEYWORDS:
        if k in (title or ""):
            return k
    return ""


def viewer_url(acpt_no: str, docno: str = "") -> str:
    url = f"{BASE}/common/disclsviewer.do?method=search&acptno={acpt_no}"
    if docno:
        url += f"&docno={docno}"
    return url


def ensure_sheet_size(ws, extra_rows_needed: int, min_cols: int):
    if ws.col_count < min_cols:
        ws.add_cols(min_cols - ws.col_count)

    target_rows = ws.row_count + max(extra_rows_needed, 0) + 50
    if ws.row_count < target_rows:
        ws.add_rows(target_rows - ws.row_count)


def parse_rss_targets() -> List[Target]:
    feed = feedparser.parse(RSS_URL)
    items = feed.entries or []
    targets: List[Target] = []

    for it in items:
        title = getattr(it, "title", "") or ""
        link = getattr(it, "link", "") or ""
        guid = getattr(it, "guid", "") or ""

        if not match_keyword(title):
            continue

        acpt_no = extract_acpt_no(link) or extract_acpt_no(guid)
        if not acpt_no:
            continue

        targets.append(Target(acpt_no=acpt_no, title=title, link=link))

    uniq = {}
    for t in targets:
        if t.acpt_no not in uniq:
            uniq[t.acpt_no] = t
    return list(uniq.values())


def is_block_page(html: str) -> bool:
    if not html:
        return True
    lower = html.lower()
    suspects = [
        "비정상", "접근이 제한", "차단", "권한", "error", "에러", "오류",
        "서비스를 이용", "잠시 후", "관리자에게"
    ]
    return any(s in lower for s in suspects) and ("<table" not in lower)


def frame_score(html: str) -> int:
    if not html:
        return -1
    lower = html.lower()
    tcnt = lower.count("<table")
    if tcnt == 0:
        return -1

    bonus_words = [
        "기준주가", "납입", "이사회", "할인", "할증", "발행", "청약",
        "사채", "교환", "전환", "유상", "신주인수권"
    ]
    bonus = sum(1 for w in bonus_words if w in lower)
    length_bonus = min(len(lower) // 2000, 50)

    return tcnt * 100 + bonus * 30 + length_bonus


def collect_candidate_htmls(page) -> List[str]:
    htmls = []

    try:
        htmls.append(page.content())
    except Exception:
        pass

    for fr in page.frames:
        try:
            html = fr.content()
            if html:
                htmls.append(html)
        except Exception:
            continue

    uniq = []
    seen = set()
    for html in htmls:
        key = re.sub(r"\s+", " ", (html or "")[:5000])
        if key not in seen:
            seen.add(key)
            uniq.append(html)

    return uniq


def extract_tables_from_html_robust(html: str) -> List[pd.DataFrame]:
    html = (html or "").replace("\x00", "")

    try:
        dfs = pd.read_html(html)
        return [df.where(pd.notnull(df), "") for df in dfs]
    except Exception:
        pass

    soup = BeautifulSoup(html, "lxml")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    tables = soup.find_all("table")
    results: List[pd.DataFrame] = []

    for tbl in tables:
        try:
            one = pd.read_html(str(tbl))
            if one:
                df = one[0].where(pd.notnull(one[0]), "")
                results.append(df)
                continue
        except Exception:
            pass

        rows = []
        for tr in tbl.find_all("tr"):
            cells = tr.find_all(["th", "td"])
            row = [c.get_text(" ", strip=True) for c in cells]
            if row:
                rows.append(row)

        if rows:
            max_len = max(len(r) for r in rows)
            norm = [r + [""] * (max_len - len(r)) for r in rows]
            results.append(pd.DataFrame(norm))

    if not results:
        raise ValueError("No tables parsed (robust).")

    return results


def clean_text_line(s: str) -> str:
    s = (s or "").replace("\xa0", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def extract_text_blocks_as_df(html: str) -> List[pd.DataFrame]:
    soup = BeautifulSoup(html or "", "lxml")

    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    texts = []

    for tag in soup.find_all(["p", "div", "span", "li", "td", "th"]):
        txt = clean_text_line(tag.get_text(" ", strip=True))
        if len(txt) < 8:
            continue
        texts.append(txt)

    uniq = []
    seen = set()
    for t in texts:
        key = re.sub(r"\s+", " ", t)
        if key not in seen:
            seen.add(key)
            uniq.append(t)

    important_keywords = [
        "옵션에 관한 사항",
        "Put Option",
        "Call Option",
        "조기상환청구권",
        "매도청구권",
        "전환청구기간",
        "교환청구기간",
        "권리행사기간",
        "발행회사 또는 발행회사가 지정하는 자",
        "본 사채의 사채권자는",
        "지급하여야 한다",
        "매도하여야 한다",
    ]

    important = [t for t in uniq if any(k in t for k in important_keywords)]

    if not important:
        return []

    return [pd.DataFrame({"text": important})]


def dedupe_dataframes(dfs: List[pd.DataFrame]) -> List[pd.DataFrame]:
    uniq = []
    seen = set()

    for df in dfs:
        try:
            sig = (
                tuple(str(c) for c in df.columns.tolist()),
                tuple(tuple(str(x) for x in row) for row in df.fillna("").astype(str).values.tolist()[:30])
            )
        except Exception:
            continue

        if sig not in seen:
            seen.add(sig)
            uniq.append(df)

    return uniq


def gs_open():
    if not GOOGLE_SHEET_ID or not GOOGLE_CREDENTIALS_JSON:
        raise RuntimeError("GOOGLE_SHEET_ID / GOOGLE_CREDS(또는 GOOGLE_CREDENTIALS_JSON)가 비어있습니다.")

    creds = json.loads(GOOGLE_CREDENTIALS_JSON)
    gc = gspread.service_account_from_dict(creds)
    sh = gc.open_by_key(GOOGLE_SHEET_ID)

    try:
        dump_ws = sh.worksheet(DUMP_SHEET_NAME)
    except gspread.WorksheetNotFound:
        dump_ws = sh.add_worksheet(title=DUMP_SHEET_NAME, rows=3000, cols=250)

    try:
        seen_ws = sh.worksheet(SEEN_SHEET_NAME)
    except gspread.WorksheetNotFound:
        seen_ws = sh.add_worksheet(title=SEEN_SHEET_NAME, rows=2000, cols=3)
        seen_ws.update("A1:C2", [
            ["acptNo", "title", "processed_at"],
            ["(do not edit manually)", "", ""]
        ])

    return sh, dump_ws, seen_ws


def load_seen_from_sheet(seen_ws) -> Set[str]:
    col = seen_ws.col_values(1)
    vals = [x.strip() for x in col if x and x.strip().isdigit()]
    return set(vals)


def append_seen(seen_ws, acpt_no: str, title: str):
    seen_ws.append_row(
        [acpt_no, title, datetime.now().strftime("%Y-%m-%d %H:%M:%S")],
        value_input_option="RAW"
    )


def df_to_rowlists(df: pd.DataFrame) -> Tuple[List[str], List[List[str]]]:
    cols = [str(c) for c in list(df.columns)]
    values = []
    for _, row in df.iterrows():
        values.append([str(x) if x != "" else "" for x in row.tolist()])
    return cols, values


def build_dump_rows(acpt_no: str, title: str, src_url: str, category: str, dfs: List[pd.DataFrame], run_ts: str) -> List[List[str]]:
    rows: List[List[str]] = []

    rows.append([acpt_no, "", "META", category, title, src_url, run_ts])
    rows.append([acpt_no, "", "BLANK"])

    for i, df in enumerate(dfs):
        cols, data_rows = df_to_rowlists(df)

        rows.append([acpt_no, str(i), "TABLE_LABEL", f"tableIndex: {i}"])
        rows.append([acpt_no, str(i), "HEADER"] + cols)

        width = max(len(cols), max((len(r) for r in data_rows), default=0))
        for r in data_rows:
            r = r + [""] * (width - len(r))
            rows.append([acpt_no, str(i), "DATA"] + r)

        rows.append([acpt_no, "", "BLANK"])

    return rows


def append_rows_chunked(ws, rows: List[List[str]], min_cols: int = 220, chunk: int = 200):
    max_len = max((len(r) for r in rows), default=0)
    ensure_sheet_size(ws, extra_rows_needed=len(rows), min_cols=max(min_cols, max_len + 5))

    for i in range(0, len(rows), chunk):
        ws.append_rows(rows[i:i + chunk], value_input_option="RAW")
        time.sleep(0.2)


def save_debug(acpt_no: str, page, html: str, reason: str):
    try:
        OUTDIR.mkdir(parents=True, exist_ok=True)
        DEBUGDIR.mkdir(parents=True, exist_ok=True)

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        (DEBUGDIR / f"{acpt_no}_{ts}_{reason}.html").write_text(html or "", encoding="utf-8")

        try:
            page.screenshot(path=str(DEBUGDIR / f"{acpt_no}_{ts}_{reason}.png"), full_page=True)
        except Exception:
            pass
    except Exception:
        pass


def scrape_one(context, t: Target) -> Tuple[List[pd.DataFrame], str]:
    url = viewer_url(t.acpt_no)
    page = context.new_page()
    try:
        page.goto(url, wait_until="networkidle", timeout=60000)
        page.wait_for_timeout(2000)

        html_candidates = collect_candidate_htmls(page)

        all_dfs: List[pd.DataFrame] = []

        for html in html_candidates:
            if not html or is_block_page(html):
                continue

            try:
                dfs = extract_tables_from_html_robust(html)
                all_dfs.extend(dfs)
            except Exception:
                pass

            try:
                text_dfs = extract_text_blocks_as_df(html)
                all_dfs.extend(text_dfs)
            except Exception:
                pass

        all_dfs = dedupe_dataframes(all_dfs)

        if not all_dfs:
            save_debug(t.acpt_no, page, page.content(), "no_tables_no_text")
            raise RuntimeError("테이블/본문 텍스트를 모두 못 찾음")

        return all_dfs, url

    finally:
        try:
            page.close()
        except Exception:
            pass


def run():
    _, dump_ws, seen_ws = gs_open()
    seen_set = load_seen_from_sheet(seen_ws)

    if RUN_ONE_ACPTNO:
        targets = [Target(acpt_no=RUN_ONE_ACPTNO, title=f"MANUAL_{RUN_ONE_ACPTNO}", link="")]
    else:
        targets = parse_rss_targets()
        targets = [t for t in targets if t.acpt_no not in seen_set]
        targets = targets[:LIMIT] if LIMIT > 0 else targets

    if not targets:
        print("[INFO] 처리할 대상이 없습니다.")
        return

    run_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=HEADLESS,
            args=["--disable-blink-features=AutomationControlled", "--no-sandbox"],
        )
        context = browser.new_context(
            locale="ko-KR",
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
            viewport={"width": 1400, "height": 900},
        )

        ok = 0
        for t in targets:
            try:
                dfs, src = scrape_one(context, t)
                category = detect_category(t.title)

                rows = build_dump_rows(
                    acpt_no=t.acpt_no,
                    title=t.title,
                    src_url=src,
                    category=category,
                    dfs=dfs,
                    run_ts=run_ts,
                )

                append_rows_chunked(dump_ws, rows)
                append_seen(seen_ws, t.acpt_no, t.title)
                ok += 1
                print(f"[OK] {t.acpt_no} tables={len(dfs)}")

            except Exception as e:
                print(f"[FAIL] {t.acpt_no} {t.title} :: {e}")

            time.sleep(0.5)

        context.close()
        browser.close()

    print(f"[DONE] ok={ok} / total_seen={len(load_seen_from_sheet(seen_ws))}")


if __name__ == "__main__":
    run()
