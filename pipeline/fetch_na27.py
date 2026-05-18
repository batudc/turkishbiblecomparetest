#!/usr/bin/env python3
"""
fetch_na27.py — Downloads the Nestle-Aland 27th edition (NA27) Greek NT
from bolls.life API and saves as chapter JSON files.

Source: bolls.life NA27 — standard critical Greek NT text.
All 27 NT books, with diacritics (polytonic Greek Unicode).

Data saved to: data/translations/NA27/{USFM_BOOK}/{chapter}.json
Format: {"content": [{"v": 1, "text": "Ἐν ἀρχῇ..."}, ...]}

Usage:
    python3 pipeline/fetch_na27.py             # all books
    python3 pipeline/fetch_na27.py --book MAT  # single book
    python3 pipeline/fetch_na27.py --resume    # skip existing files
"""

import json
import time
import argparse
import urllib.request
from pathlib import Path

PROJECT = Path(__file__).resolve().parent.parent
OUT_DIR = PROJECT / 'data' / 'translations' / 'NA27'

UA = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
BOLLS_SLUG = 'NA27'
BASE_URL   = f'https://bolls.life/get-text/{BOLLS_SLUG}'
DELAY = 0.45

# USFM code → bolls.life canonical book number (NT = 40–66)
BOOK_NUM = {
    'MAT': 40, 'MRK': 41, 'LUK': 42, 'JHN': 43, 'ACT': 44,
    'ROM': 45, '1CO': 46, '2CO': 47, 'GAL': 48, 'EPH': 49,
    'PHP': 50, 'COL': 51, '1TH': 52, '2TH': 53, '1TI': 54,
    '2TI': 55, 'TIT': 56, 'PHM': 57, 'HEB': 58, 'JAS': 59,
    '1PE': 60, '2PE': 61, '1JN': 62, '2JN': 63, '3JN': 64,
    'JUD': 65, 'REV': 66,
}

CHAPTERS = {
    'MAT': 28, 'MRK': 16, 'LUK': 24, 'JHN': 21, 'ACT': 28,
    'ROM': 16, '1CO': 16, '2CO': 13, 'GAL':  6, 'EPH':  6,
    'PHP':  4, 'COL':  4, '1TH':  5, '2TH':  3, '1TI':  6,
    '2TI':  4, 'TIT':  3, 'PHM':  1, 'HEB': 13, 'JAS':  5,
    '1PE':  5, '2PE':  3, '1JN':  5, '2JN':  1, '3JN':  1,
    'JUD':  1, 'REV': 22,
}

BOOK_ORDER = [
    'MAT','MRK','LUK','JHN','ACT','ROM','1CO','2CO','GAL','EPH',
    'PHP','COL','1TH','2TH','1TI','2TI','TIT','PHM','HEB','JAS',
    '1PE','2PE','1JN','2JN','3JN','JUD','REV',
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
    parser.add_argument('--book',   help='Process only this USFM book code (e.g. MAT)')
    parser.add_argument('--resume', action='store_true',
                        help='Skip chapters that already exist')
    args = parser.parse_args()

    books = [args.book.upper()] if args.book else BOOK_ORDER

    work = []
    for code in books:
        bnum = BOOK_NUM.get(code)
        if not bnum:
            print(f'Unknown or non-NT book: {code}')
            continue
        for ch in range(1, CHAPTERS[code] + 1):
            out = OUT_DIR / code / f'{ch}.json'
            if args.resume and out.exists():
                continue
            work.append((code, bnum, ch, out))

    total = len(work)
    print(f'Fetching {total} chapters of NA27 Greek NT from bolls.life …')
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

        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps({'content': content}, ensure_ascii=False, indent=2), encoding='utf-8')
        print(f'✓ {len(content)} verses')
        ok += 1

    print()
    print('─' * 55)
    print(f'Done: {ok} saved, {fail} errors, {total} total')


if __name__ == '__main__':
    main()
