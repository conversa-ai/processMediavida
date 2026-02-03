#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Rehydrate Mediavida dialogue IDs into text by scraping thread pages at runtime.

Input: dehydrated IDs-only JSON with fields:
  - thread_url (required)
  - dialogues: dict[str, list[int]]

Output: rehydrated JSON with dialogue texts (NOT FOR REDISTRIBUTION)

Turn representation:
  - Each turn is either:
      [ "<SPEAKER_LETTER>", "<TURN_TEXT>" ]
    or null if missing.
  - Speaker letters are assigned per thread based on the post author as observed on the page.
  - No usernames/handles are emitted; authors are mapped internally to letters A, B, C...

Important:
- This produces user-generated content retrieved at runtime from Mediavida.
- Do not redistribute the rehydrated output.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup


POST_ID_RE = re.compile(r"^post-(\d+)$")
_WS_RE = re.compile(r"\s+")


def _index_to_letters(i: int) -> str:
    # 0->A, 25->Z, 26->AA, ...
    if i < 0:
        raise ValueError("Index must be non-negative")
    letters: List[str] = []
    while True:
        i, rem = divmod(i, 26)
        letters.append(chr(ord("A") + rem))
        if i == 0:
            break
        i -= 1
    return "".join(reversed(letters))


def _assign_speaker(raw_author: str, mapping: Dict[str, str]) -> str:
    a = (raw_author or "").strip()
    if not a:
        a = "__unknown__"
    if a in mapping:
        return mapping[a]
    label = _index_to_letters(len(mapping))
    mapping[a] = label
    return label


def _clean_text(text: str) -> str:
    if text is None:
        return ""
    t = str(text).replace("\r\n", "\n").replace("\r", "\n")
    # Normalize whitespace but keep line breaks reasonably
    t = _WS_RE.sub(" ", t).strip()
    return t


def _get_soup(url: str, sess: requests.Session, timeout_s: int) -> BeautifulSoup:
    r = sess.get(url, timeout=timeout_s)
    r.raise_for_status()
    return BeautifulSoup(r.text, "html.parser")


def _extract_posts(soup: BeautifulSoup, debug: bool) -> Dict[int, Tuple[str, str]]:
    """
    Extract mapping:
      post_number -> (author_token, post_text)

    Mediavida uses div id="post-<n>" where <n> is the visible post number (#n).
    We DO NOT output usernames; we only use an internal author token to map to letters.
    """
    out: Dict[int, Tuple[str, str]] = {}

    for d in soup.find_all("div", id=POST_ID_RE):
        m = POST_ID_RE.match(d.get("id", ""))
        if not m:
            continue
        pid = int(m.group(1))

        contents = d.find("div", {"class": "post-contents"})
        if contents is None:
            continue

        # Try to get an author token from common Mediavida structures.
        # We keep it flexible (site markup can vary). We do NOT emit it.
        author = ""
        # Common patterns: elements with classes containing "nick"/"author"/"user"
        author_el = (
            d.find(attrs={"class": re.compile(r"(nick|author|user)", re.IGNORECASE)})
            or d.find("a", attrs={"class": re.compile(r"(nick|author|user)", re.IGNORECASE)})
        )
        if author_el is not None:
            author = author_el.get_text(" ", strip=True) or ""

        # Fallback: sometimes author is in a header/meta section
        if not author:
            header = d.find(attrs={"class": re.compile(r"(post-header|post-info|post-meta)", re.IGNORECASE)})
            if header is not None:
                # pick first non-empty link text as a weak heuristic
                for a in header.find_all("a"):
                    txt = a.get_text(" ", strip=True) or ""
                    if txt:
                        author = txt
                        break

        txt = contents.get_text("\n").strip()

        # Important: do not preserve any direct "user>" quoting patterns in author;
        # only used for mapping internally.
        out[pid] = (author, txt)

    if debug:
        print(f"[debug] extracted posts on page: {len(out)}", file=sys.stderr)

    return out


def _normalize_href(base_url: str, href: str) -> Optional[str]:
    if not href:
        return None
    href = href.strip()
    if not href:
        return None
    return urljoin(base_url, href)


def _find_next_url(current_url: str, soup: BeautifulSoup, debug: bool) -> Optional[str]:
    """
    Find the next page URL.

    Priority:
    1) <a rel="next" href="...">
    2) an <a> whose visible text is "Siguiente"
    3) pagination fallback: choose the smallest page number > current page among links like "/.../<n>"
    """
    a = soup.find("a", attrs={"rel": "next"})
    if a and a.get("href"):
        nxt = _normalize_href(current_url, a["href"])
        if debug:
            print(f"[debug] next(rel=next) -> {nxt}", file=sys.stderr)
        return nxt

    for a in soup.find_all("a"):
        txt = (a.get_text(" ", strip=True) or "").lower()
        if txt == "siguiente" and a.get("href"):
            nxt = _normalize_href(current_url, a["href"])
            if debug:
                print(f"[debug] next(text='Siguiente') -> {nxt}", file=sys.stderr)
            return nxt

    parsed = urlparse(current_url)
    path = parsed.path.rstrip("/")
    m_cur = re.search(r"/(\d+)$", path)
    cur_page = int(m_cur.group(1)) if m_cur else 1

    candidates: Dict[int, str] = {}
    for a in soup.find_all("a"):
        href = a.get("href")
        if not href:
            continue
        absu = _normalize_href(current_url, href)
        if not absu:
            continue
        p = urlparse(absu).path.rstrip("/")
        m = re.search(r"/(\d+)$", p)
        if not m:
            continue
        page = int(m.group(1))
        candidates[page] = absu

    higher_pages = sorted([p for p in candidates.keys() if p > cur_page])
    if higher_pages:
        nxt = candidates[higher_pages[0]]
        if debug:
            print(f"[debug] next(pagination fallback cur={cur_page}) -> {nxt}", file=sys.stderr)
        return nxt

    if debug:
        print("[debug] next -> None (no next page link found)", file=sys.stderr)
    return None


def scrape_thread_posts(
    thread_url: str,
    user_agent: str,
    sleep_s: float,
    timeout_s: int,
    max_pages: int,
    debug: bool,
) -> Dict[int, Tuple[str, str]]:
    """
    Crawl pages in a thread and collect all posts keyed by post number (#1, #2, ...).

    Returns:
      post_number -> (author_token, post_text)
    """
    sess = requests.Session()
    sess.headers.update({"User-Agent": user_agent})

    seen: Set[str] = set()
    url: Optional[str] = thread_url
    all_posts: Dict[int, Tuple[str, str]] = {}

    pages = 0
    while url and url not in seen and pages < max_pages:
        seen.add(url)
        pages += 1
        if debug:
            print(f"[debug] fetching page {pages}: {url}", file=sys.stderr)

        soup = _get_soup(url, sess=sess, timeout_s=timeout_s)
        posts = _extract_posts(soup, debug=debug)
        all_posts.update(posts)

        url = _find_next_url(url, soup, debug=debug)
        time.sleep(sleep_s)

    if debug:
        print(f"[debug] pages={pages} posts_collected={len(all_posts)}", file=sys.stderr)
    return all_posts


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="Dehydrated Mediavida JSON (IDs-only).")
    ap.add_argument("--output", required=True, help="Rehydrated JSON with text (local use only).")
    ap.add_argument("--user-agent", required=True, help="User-Agent string for polite requests.")
    ap.add_argument("--sleep", type=float, default=1.0)
    ap.add_argument("--timeout", type=int, default=30)
    ap.add_argument("--max-pages", type=int, default=2000)
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    inp = Path(args.input)
    obj = json.loads(inp.read_text(encoding="utf-8"))

    thread_url = obj.get("thread_url")
    dialogues = obj.get("dialogues")

    if not isinstance(thread_url, str) or not thread_url.strip():
        raise ValueError("Missing 'thread_url' in dehydrated input. Rehydration is not possible.")
    if not isinstance(dialogues, dict):
        raise ValueError("Missing 'dialogues' dict in dehydrated input.")

    posts = scrape_thread_posts(
        thread_url=thread_url,
        user_agent=args.user_agent,
        sleep_s=args.sleep,
        timeout_s=args.timeout,
        max_pages=args.max_pages,
        debug=args.debug,
    )

    # Internal per-thread mapping: author_token -> speaker_letter
    speaker_map: Dict[str, str] = {}

    rehydrated_dialogues: Dict[str, List[Optional[List[str]]]] = {}
    missing: Dict[str, dict] = {}

    for did, chain in dialogues.items():
        if not isinstance(chain, list):
            continue

        out_turns: List[Optional[List[str]]] = []
        miss = 0

        for cid in chain:
            try:
                cid_int = int(cid)
            except Exception:
                cid_int = None

            if cid_int is None or cid_int not in posts:
                miss += 1
                out_turns.append(None)
                continue

            author_token, raw_txt = posts[cid_int]
            speaker = _assign_speaker(author_token, speaker_map)

            txt = _clean_text(raw_txt)
            if not txt:
                miss += 1
                out_turns.append(None)
                continue

            out_turns.append([speaker, txt])

        rehydrated_dialogues[str(did)] = out_turns
        missing[str(did)] = {"n_turns": len(chain), "n_missing": miss}

    out = {
        "format": "mediavida_dialogue_text_v2_tuples",
        "source": "mediavida",
        "thread_id": obj.get("thread_id"),
        "thread_url": thread_url,
        "snapshot_date": obj.get("snapshot_date"),
        "rehydrated_at": time.strftime("%Y-%m-%d"),
        "turn_representation": "[speaker_letter, text] (null if missing)",
        "dialogues": rehydrated_dialogues,
        "missing": missing,
        "notice": "This file contains user-generated content retrieved at runtime from Mediavida. Do not redistribute.",
    }

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[ok] wrote {args.output}", file=sys.stderr)


if __name__ == "__main__":
    main()
