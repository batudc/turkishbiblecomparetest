#!/usr/bin/env python3
"""
fetch_bhs.py — Downloads the Westminster Leningrad Codex (WLC / BHS)
from bolls.life API and saves as chapter JSON files.

Source: bolls.life WLC (Westminster Leningrad Codex) — public domain Hebrew OT.
All 39 OT books, pointed text with cantillation marks.

Data saved to: data/translations/BHS/{USFM_BOOK}/{chapter}.json
Format matches all other translation files:
  {"content": [{"v": 1, "text": "בְּרֵאשִׁ֖ית..."}, ...]}

Usage:
    python3 pipeline/fetch_bhs.py             # all books
    python3 pipeline/fetch_bhs.py --book GEN  # single book
    python3 pipeline/fetch_bhs.py --resume    # skip existing files
"""

import json
import os
import re
import time
import argparse
import urllib.request
from pathlib import Path

PROJECT = Path(__file__).resolve().parent.parent
OUT_DIR = PROJECT / 'data' / 'translations' / 'BHS'

UA = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
# bolls.life serves WLC (Westminster Leningrad Codex) as 'WLC' slug.
# If this returns 404, check https://bolls.life/api/ for available translations
# and try 'BHS', 'BHSA', or 'WLCHebrew'.
BOLLS_SLUG = 'WLC'
BASE_URL   = f'https://bolls.life/get-text/{BOLLS_SLUG}'
DELAY = 0.45  # seconds between requests

# USFM code → bolls.life canonical book number (OT = 1–39)
BOOK_NUM = {
    'GEN': 1,  'EXO': 2,  'LEV': 3,  'NUM': 4,  'DEU': 5,
    'JOS': 6,  'JDG': 7,  'RUT': 8,  '1SA': 9,  '2SA': 10,
    '1KI': 11, '2KI': 12, '1CH': 13, '2CH': 14, 'EZR': 15,
    'NEH': 16, 'EST': 17, 'JOB': 18, 'PSA': 19, 'PRO': 20,
    'ECC': 21, 'SNG': 22, 'ISA': 23, 'JER': 24, 'LAM': 25,
    'EZK': 26, 'DAN': 27, 'HOS': 28, 'JOL': 29, 'AMO': 30,
    'OBA': 31, 'JON': 32, 'MIC': 33, 'NAH': 34, 'HAB': 35,
    'ZEP': 36, 'HAG': 37, 'ZEC': 38, 'MAL': 39,
}

CHAPTERS = {
    'GEN': 50, 'EXO': 40, 'LEV': 27, 'NUM': 36, 'DEU': 34,
    'JOS': 24, 'JDG': 21, 'RUT':  4, '1SA': 31, '2SA': 24,
    '1KI': 22, '2KI': 25, '1CH': 29, '2CH': 36, 'EZR': 10,
    'NEH': 13, 'EST': 10, 'JOB': 42, 'PSA': 150,'PRO': 31,
    'ECC': 12, 'SNG':  8, 'ISA': 66, 'JER': 52, 'LAM':  5,
    'EZK': 48, 'DAN': 12, 'HOS': 14, 'JOL':  3, 'AMO':  9,
    'OBA':  1, 'JON':  4, 'MIC':  7, 'NAH':  3, 'HAB':  3,
    'ZEP':  3, 'HAG':  2, 'ZEC': 14, 'MAL':  4,  # 4 ch per English convention; ch4 split from Hebrew 3:19-24
}

BOOK_ORDER = [
    'GEN','EXO','LEV','NUM','DEU','JOS','JDG','RUT','1SA','2SA',
    '1KI','2KI','1CH','2CH','EZR','NEH','EST','JOB','PSA','PRO',
    'ECC','SNG','ISA','JER','LAM','EZK','DAN','HOS','JOL','AMO',
    'OBA','JON','MIC','NAH','HAB','ZEP','HAG','ZEC','MAL',
]


def fetch_chapter(book_num: int, chap: int) -> list | None:
    url = f'{BASE_URL}/{book_num}/{chap}/'
    req = urllib.request.Request(url, headers={'User-Agent': UA})
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            return json.loads(r.read().decode('utf-8'))
    except Exception as e:
        print(f'    fetch error: {e}')
        return None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--book',   help='Process only this USFM book code (e.g. GEN)')
    parser.add_argument('--resume', action='store_true',
                        help='Skip chapters that already exist')
    args = parser.parse_args()

    books = [args.book.upper()] if args.book else BOOK_ORDER

    work = []
    for code in books:
        bnum = BOOK_NUM.get(code)
        if not bnum:
            print(f'Unknown or non-OT book: {code}')
            continue
        total_ch = CHAPTERS.get(code, 1)
        for ch in range(1, total_ch + 1):
            out = OUT_DIR / code / f'{ch}.json'
            if args.resume and out.exists():
                continue
            work.append((code, bnum, ch, out))

    total = len(work)
    print(f'Fetching {total} chapters of WLC Hebrew OT from bolls.life …')
    print(f'Translation slug: {BOLLS_SLUG}')
    print('─' * 55)

    ok = fail = 0
    for i, (code, bnum, ch, out) in enumerate(work, 1):
        print(f'[{i:4d}/{total}] {code} {ch:3d} … ', end='', flush=True)

        raw = fetch_chapter(bnum, ch)
        time.sleep(DELAY)

        if raw is None:
            print('FETCH ERROR')
            fail += 1
            continue

        if not isinstance(raw, list) or not raw:
            print('EMPTY')
            fail += 1
            continue

        # bolls.life returns [{"pk":..., "verse":1, "text":"..."}, ...]
        content = []
        for item in raw:
            v = item.get('verse') or item.get('v')
            t = item.get('text', '').strip()
            if v and t:
                content.append({'v': int(v), 'text': t})

        if not content:
            print('NO VERSES')
            fail += 1
            continue

        data = {'content': content}
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')
        print(f'✓ {len(content)} verses')
        ok += 1

    print()
    print('─' * 55)
    print(f'Done: {ok} saved, {fail} errors, {total} total')
    if fail:
        print(f'Note: If you see 404 errors, check https://bolls.life/api/ for the correct '
              f'Hebrew translation slug and update BOLLS_SLUG in this script.')


if __name__ == '__main__':
    main()
