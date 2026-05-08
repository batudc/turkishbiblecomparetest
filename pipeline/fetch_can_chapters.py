#!/usr/bin/env python3
"""
Fetch missing CAN (Kutsal Incil, Bünyamin Candemir) chapters from incil.info
and save them as JSON files in the correct directory structure.

URL pattern: https://incil.info/CAN/kitap/{turkish_book_name}/{chapter}
"""

import urllib.request
import re
import json
import os
import time

BASE_URL = "https://incil.info/CAN/kitap"
OUTPUT_BASE = "/Users/batuhandemircan/website building/data/translations/CAN"
DELAY = 0.4  # seconds between requests

# Map from canonical book abbreviation -> Turkish book name (URL slug) and display title
BOOK_MAP = {
    "MAT": ("matta",   "Matta"),
    "MRK": ("markos",  "Markos"),
    "LUK": ("luka",    "Luka"),
    "JHN": ("yuhanna", "Yuhanna"),
}

# Missing chapters to fetch
MISSING = {
    "MAT": list(range(4, 29)),   # 4-28  (25 chapters)
    "MRK": list(range(4, 17)),   # 4-16  (13 chapters)
    "LUK": list(range(4, 25)),   # 4-24  (21 chapters)
    "JHN": list(range(4, 22)),   # 4-21  (18 chapters)
}


def fetch_html(url: str) -> str:
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "Mozilla/5.0 (compatible; BibleFetcher/1.0)"},
    )
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read().decode("utf-8")


def clean_text(raw: str) -> str:
    """Remove HTML tags, decode entities, collapse whitespace."""
    # Remove inline HTML tags (bolumno span, sup, title subType, indent spans, br)
    text = re.sub(r"<[^>]+>", "", raw)
    # Decode common HTML entities
    text = text.replace("&nbsp;", " ")
    text = text.replace("&amp;", "&")
    text = text.replace("&lt;", "<")
    text = text.replace("&gt;", ">")
    text = text.replace("&quot;", '"')
    text = text.replace("&#39;", "'")
    # Collapse whitespace
    text = re.sub(r"\s+", " ", text).strip()
    # Strip footnote markers like * at end of words / sentences
    # (keep them as part of text to be faithful; incil.info uses * for footnotes)
    return text


def parse_chapter(html: str, book_abbr: str, chap: int) -> dict:
    """
    Parse the HTML page and return the JSON data dict for one chapter.
    """
    # Locate the CAN column
    col_start = html.find('<div class="lookup column c1 CAN"')
    if col_start == -1:
        raise ValueError("CAN column not found in HTML")

    # Extract column content up to the closing tag of the viewport div
    col_html = html[col_start:col_start + 200_000]

    content = []

    # ---------- section headings ----------
    # h4 tags with class 'baslik'
    # Pattern: <h4 class='baslik'>TEXT</h4>  OR  <h4 class="baslik">TEXT</h4>
    # We need to interleave headings and verses in document order, so we collect
    # all items with their positions.

    items = []  # list of (position, kind, data)

    # Find all <h4 class='baslik'> headings
    for m in re.finditer(r"<h4 class=['\"]baslik['\"]>(.*?)</h4>", col_html, re.DOTALL):
        heading_text = clean_text(m.group(1))
        items.append((m.start(), "heading", heading_text))

    # Find all verse spans: <span class="verse CAN" data-incil-ayet="vC_V">...</span>
    # The closing </span> is right before the next <span or </div>
    verse_pattern = re.compile(
        r'<span class="verse CAN" data-incil-ayet="v\d+_(\d+)">(.*?)'
        r'(?=<span class="verse CAN"|</div>)',
        re.DOTALL,
    )

    for m in verse_pattern.finditer(col_html):
        verse_html = m.group(2)

        # Extract verse label from <sup> tag (may be "1-3", "4", etc.)
        sup_match = re.search(
            r"<sup[^>]*>.*?<a [^>]*>([^<]+)</a>",
            verse_html,
            re.DOTALL,
        )
        if sup_match:
            verse_label_raw = sup_match.group(1).strip()
        else:
            # First verse in chapter has <sup class='ilk'> with bolumno before it
            ilk_match = re.search(
                r"<sup class=['\"]ilk['\"]>.*?<a [^>]*>([^<]+)</a>",
                verse_html,
                re.DOTALL,
            )
            verse_label_raw = ilk_match.group(1).strip() if ilk_match else str(m.group(1))

        # Convert to int if single verse, keep as string if range like "1-3"
        if re.match(r"^\d+$", verse_label_raw):
            verse_label = int(verse_label_raw)
        else:
            verse_label = verse_label_raw  # e.g. "1-3" or "69-75"

        # Remove footer h6 (e.g. <h6 class="translation CAN">İncil — Bünyamin Candemir</h6>)
        verse_html = re.sub(r"<h6[^>]*>.*?</h6>", "", verse_html, flags=re.DOTALL)
        # Remove the bolumno span (chapter number) before cleaning text
        verse_html_clean = re.sub(r"<span class=['\"]bolumno['\"]>.*?</span>", "", verse_html, flags=re.DOTALL)
        # Remove the sup tag entirely (contains verse label, not text)
        verse_html_clean = re.sub(r"<sup[^>]*>.*?</sup>", "", verse_html_clean, flags=re.DOTALL)
        # Clean the verse text
        verse_text = clean_text(verse_html_clean)

        items.append((m.start(), "verse", (verse_label, verse_text)))

    # Sort by document position
    items.sort(key=lambda x: x[0])

    # Determine chapter title from first heading (if any)
    title = None
    for _, kind, data in items:
        if kind == "heading":
            title = data
            break

    # Build content list
    last_heading_emitted = None
    for _, kind, data in items:
        if kind == "heading":
            # Don't emit the same heading twice (it may appear again if it was
            # extracted both as a standalone h4 AND embedded in a verse's preverse title)
            if data != last_heading_emitted:
                content.append({"section": data})
                last_heading_emitted = data
        else:
            v_label, v_text = data
            if v_text:
                content.append({"v": v_label, "text": v_text})

    return {
        "t": "CAN",
        "b": book_abbr,
        "c": chap,
        "title": title or "",
        "content": content,
    }


def fetch_and_save(book_abbr: str, chap: int, turkish_name: str) -> bool:
    out_dir = os.path.join(OUTPUT_BASE, book_abbr)
    out_path = os.path.join(out_dir, f"{chap}.json")

    if os.path.exists(out_path):
        print(f"  SKIP (exists): {book_abbr}/{chap}.json")
        return True

    url = f"{BASE_URL}/{turkish_name}/{chap}"
    try:
        html = fetch_html(url)
        data = parse_chapter(html, book_abbr, chap)
        os.makedirs(out_dir, exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"  OK: {book_abbr}/{chap}.json  ({len(data['content'])} items)")
        return True
    except Exception as e:
        print(f"  ERROR: {book_abbr}/{chap} — {e}")
        return False


def main():
    total = sum(len(v) for v in MISSING.values())
    done = 0
    failed = []

    print(f"Fetching {total} missing CAN chapters from incil.info ...\n")

    for book_abbr, chapters in MISSING.items():
        turkish_name, _ = BOOK_MAP[book_abbr]
        print(f"=== {book_abbr} ({turkish_name}) — {len(chapters)} chapters ===")
        for chap in chapters:
            success = fetch_and_save(book_abbr, chap, turkish_name)
            if success:
                done += 1
            else:
                failed.append(f"{book_abbr}/{chap}")
            time.sleep(DELAY)
        print()

    print("=" * 50)
    print(f"Done. {done}/{total} chapters saved successfully.")
    if failed:
        print(f"Failed ({len(failed)}): {', '.join(failed)}")


if __name__ == "__main__":
    main()
