#!/usr/bin/env python3
"""
fetch_vulgata.py — Downloads the Clementine Vulgate from bolls.life API
and saves as chapter JSON files.

Source: bolls.life VULG — Clementine Vulgate (1592 edition).
Full Bible (73 books via Protestant 66-book numbering + deuterocanonical
extensions within Daniel ch.13-14 and Esther ch.11-16).

Data saved to: data/translations/VULG/{USFM_BOOK}/{chapter}.json
Format: {"content": [{"v": 1, "text": "In principio…"}, ...]}

Usage:
    python3 pipeline/fetch_vulgata.py             # all books
    python3 pipeline/fetch_vulgata.py --book GEN  # single book
    python3 pipeline/fetch_vulgata.py --resume    # skip existing files
"""

import json
import time
import argparse
import urllib.request
from pathlib import Path

PROJECT = Path(__file__).resolve().parent.parent
OUT_DIR = PROJECT / 'data' / 'translations' / 'VULG'

UA = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
BOLLS_SLUG = 'VULG'
BASE_URL   = f'https://bolls.life/get-text/{BOLLS_SLUG}'
DELAY = 0.45

BOOK_NUM = {
    'GEN': 1,  'EXO': 2,  'LEV': 3,  'NUM': 4,  'DEU': 5,
    'JOS': 6,  'JDG': 7,  'RUT': 8,  '1SA': 9,  '2SA': 10,
    '1KI': 11, '2KI': 12, '1CH': 13, '2CH': 14, 'EZR': 15,
    'NEH': 16, 'EST': 17, 'JOB': 18, 'PSA': 19, 'PRO': 20,
    'ECC': 21, 'SNG': 22, 'ISA': 23, 'JER': 24, 'LAM': 25,
    'EZK': 26, 'DAN': 27, 'HOS': 28, 'JOL': 29, 'AMO': 30,
    'OBA': 31, 'JON': 32, 'MIC': 33, 'NAH': 34, 'HAB': 35,
    'ZEP': 36, 'HAG': 37, 'ZEC': 38, 'MAL': 39,
    'MAT': 40, 'MRK': 41, 'LUK': 42, 'JHN': 43, 'ACT': 44,
    'ROM': 45, '1CO': 46, '2CO': 47, 'GAL': 48, 'EPH': 49,
    'PHP': 50, 'COL': 51, '1TH': 52, '2TH': 53, '1TI': 54,
    '2TI': 55, 'TIT': 56, 'PHM': 57, 'HEB': 58, 'JAS': 59,
    '1PE': 60, '2PE': 61, '1JN': 62, '2JN': 63, '3JN': 64,
    'JUD': 65, 'REV': 66,
}

# Vulgate chapter counts — EST and DAN have deuterocanonical extensions
CHAPTERS = {
    'GEN': 50, 'EXO': 40, 'LEV': 27, 'NUM': 36, 'DEU': 34,
    'JOS': 24, 'JDG': 21, 'RUT':  4, '1SA': 31, '2SA': 24,
    '1KI': 22, '2KI': 25, '1CH': 29, '2CH': 36, 'EZR': 10,
    'NEH': 13, 'EST': 16, 'JOB': 42, 'PSA': 150, 'PRO': 31,
    'ECC': 12, 'SNG':  8, 'ISA': 66, 'JER': 52, 'LAM':  5,
    'EZK': 48, 'DAN': 14, 'HOS': 14, 'JOL':  3, 'AMO':  9,
    'OBA':  1, 'JON':  4, 'MIC':  7, 'NAH':  3, 'HAB':  3,
    'ZEP':  3, 'HAG':  2, 'ZEC': 14, 'MAL':  4,
    'MAT': 28, 'MRK': 16, 'LUK': 24, 'JHN': 21, 'ACT': 28,
    'ROM': 16, '1CO': 16, '2CO': 13, 'GAL':  6, 'EPH':  6,
    'PHP':  4, 'COL':  4, '1TH':  5, '2TH':  3, '1TI':  6,
    '2TI':  4, 'TIT':  3, 'PHM':  1, 'HEB': 13, 'JAS':  5,
    '1PE':  5, '2PE':  3, '1JN':  5, '2JN':  1, '3JN':  1,
    'JUD':  1, 'REV': 22,
}

BOOK_ORDER = [
    'GEN','EXO','LEV','NUM','DEU','JOS','JDG','RUT','1SA','2SA',
    '1KI','2KI','1CH','2CH','EZR','NEH','EST','JOB','PSA','PRO',
    'ECC','SNG','ISA','JER','LAM','EZK','DAN','HOS','JOL','AMO',
    'OBA','JON','MIC','NAH','HAB','ZEP','HAG','ZEC','MAL',
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
    parser.add_argument('--book',   help='Process only this USFM book code (e.g. GEN)')
    parser.add_argument('--resume', action='store_true',
                        help='Skip chapters that already exist')
    args = parser.parse_args()

    books = [args.book.upper()] if args.book else BOOK_ORDER

    work = []
    for code in books:
        bnum = BOOK_NUM.get(code)
        if not bnum:
            print(f'Unknown book: {code}')
            continue
        for ch in range(1, CHAPTERS[code] + 1):
            out = OUT_DIR / code / f'{ch}.json'
            if args.resume and out.exists():
                continue
            work.append((code, bnum, ch, out))

    total = len(work)
    print(f'Fetching {total} chapters of Clementine Vulgate from bolls.life …')
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
