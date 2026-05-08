#!/usr/bin/env python3
"""
add_tcl02_headings.py — Fetches section headings for every TCL02 chapter from
bible.com (version 170) and injects them into the local JSON files.

Each chapter JSON gains:
  - "title"  : text of the first section heading (if it precedes verse 1)
  - {section: "..."} items inserted into content[] before the correct verse

Usage:
    python3 pipeline/add_tcl02_headings.py
    python3 pipeline/add_tcl02_headings.py --resume      # skip already-done chapters
    python3 pipeline/add_tcl02_headings.py --book MAT    # single book test
"""

import json
import os
import re
import time
import argparse
import html as htmlmod
import urllib.request
from pathlib import Path

PROJECT  = Path(__file__).resolve().parent.parent
TCL_DIR  = PROJECT / 'data' / 'translations' / 'TCL02'
LOG_PATH = PROJECT / 'output' / 'tcl02_headings.log'

DELAY = 0.35   # seconds between requests
UA    = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'

# ---------------------------------------------------------------------------

def fetch_headings(book: str, chap: int) -> list[dict] | None:
    """
    Returns list of {'verse': int, 'section': str} for the given chapter,
    or None on error.
    """
    url = f'https://www.bible.com/bible/170/{book}.{chap}.TCL02'
    req = urllib.request.Request(url, headers={'User-Agent': UA})
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            raw = r.read().decode('utf-8')
    except Exception as e:
        print(f'    fetch error: {e}')
        return None

    m = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', raw, re.DOTALL)
    if not m:
        print('    no __NEXT_DATA__')
        return None

    data = json.loads(m.group(1))
    chapter_info = data['props']['pageProps'].get('chapterInfo') or {}
    raw_content = chapter_info.get('content')
    if not raw_content:
        print('    no chapterInfo.content')
        return None
    content_html = htmlmod.unescape(raw_content)

    # Section headings: only from <div class="s1"> or <div class="s2">
    # Cross-ref divs (<div class="r">) also contain <span class="heading"> but
    # we deliberately exclude them by matching only the s1/s2 div pattern.
    heading_pat = re.compile(r'<div class="s[12]"><span class="heading">(.*?)</span>')
    # Only "real" verse spans (those with a label child = non-bridge spans)
    verse_pat   = re.compile(r'<span class="verse [^"]*" data-usfm="([^"]+)"><span class="label">')

    headings = [(m2.start(), m2.group(1)) for m2 in heading_pat.finditer(content_html)]
    verses   = [
        (m2.start(), int(m2.group(1).split('+')[0].split('.')[-1]))
        for m2 in verse_pat.finditer(content_html)
    ]

    result = []
    for hpos, htxt in headings:
        next_v = next((v for pos, v in verses if pos > hpos), None)
        if next_v is not None:
            result.append({'verse': next_v, 'section': htxt})

    return result


def patch_chapter(json_path: Path, headings: list[dict]) -> bool:
    """
    Inserts section headings into the chapter JSON.
    Returns True if the file was modified.
    """
    data = json.loads(json_path.read_text(encoding='utf-8'))
    content = data.get('content', [])

    # Remove any existing section items first (idempotent)
    content = [item for item in content if 'v' in item]

    # Build a mapping: verse_number -> list of section texts to insert before it
    insert_map: dict[int, list[str]] = {}
    for h in headings:
        insert_map.setdefault(h['verse'], []).append(h['section'])

    # Determine first verse number in chapter
    first_v = content[0]['v'] if content else 1

    # Identify sections that appear before verse 1 (or before the first verse)
    # → these set the chapter-level title
    title_texts = []
    for v_num in sorted(insert_map.keys()):
        if v_num <= first_v:
            title_texts.extend(insert_map[v_num])

    # Set chapter title to the first heading that appears at/before verse 1
    data['title'] = title_texts[0] if title_texts else None
    if data['title'] is None:
        data.pop('title', None)

    # Inject section items into content
    new_content = []
    verse_nums = [item['v'] for item in content]

    for item in content:
        v_num = item['v']
        # Find sections whose target verse == v_num (exact) or whose
        # target verse is between this verse and the previous one
        for target_v in sorted(insert_map.keys()):
            # Insert before this item if target_v <= v_num and
            # (this is the first verse OR target_v > previous verse)
            prev_v = verse_nums[verse_nums.index(v_num) - 1] if verse_nums.index(v_num) > 0 else 0
            if prev_v < target_v <= v_num:
                for sec_text in insert_map[target_v]:
                    new_content.append({'section': sec_text})
        new_content.append(item)

    data['content'] = new_content
    json_path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding='utf-8',
    )
    return True


# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--book',   help='Process only this book (e.g. MAT)')
    parser.add_argument('--resume', action='store_true',
                        help='Skip chapters that already have section headings')
    args = parser.parse_args()

    LOG_PATH.parent.mkdir(exist_ok=True)

    # Collect work items
    work = []
    for book_dir in sorted(TCL_DIR.iterdir()):
        if not book_dir.is_dir():
            continue
        book = book_dir.name
        if args.book and book != args.book:
            continue
        for f in sorted(book_dir.glob('*.json'), key=lambda p: int(p.stem)):
            if args.resume:
                existing = json.loads(f.read_text(encoding='utf-8'))
                has_sections = any('section' in item for item in existing.get('content', []))
                has_title    = 'title' in existing
                if has_sections or has_title:
                    continue
            work.append((book, int(f.stem), f))

    total = len(work)
    print(f'Processing {total} chapters')
    print('─' * 55)

    done = ok = 0
    log_lines = []

    for book, chap, path in work:
        done += 1
        print(f'[{done:4d}/{total}] {book} {chap:3d} … ', end='', flush=True)

        headings = fetch_headings(book, chap)
        time.sleep(DELAY)

        if headings is None:
            print('FETCH ERROR')
            log_lines.append(f'FAIL {book} {chap}')
            continue

        if not headings:
            print('no headings')
            log_lines.append(f'NONE {book} {chap}')
            continue

        patch_chapter(path, headings)
        ok += 1
        labels = ', '.join(f'v{h["verse"]}:{h["section"][:20]}' for h in headings)
        print(f'✓ {len(headings)} headings  ({labels})')
        log_lines.append(f'OK   {book} {chap}: {len(headings)} headings')

    LOG_PATH.write_text('\n'.join(log_lines), encoding='utf-8')
    print()
    print('─' * 55)
    print(f'Done: {ok}/{total} chapters updated')
    print(f'Log: {LOG_PATH}')


if __name__ == '__main__':
    main()
