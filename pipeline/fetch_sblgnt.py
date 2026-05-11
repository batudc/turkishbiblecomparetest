#!/usr/bin/env python3
"""
fetch_sblgnt.py — Downloads the SBL Greek New Testament (SBLGNT) from the
MorphGNT project on GitHub and saves as chapter JSON files.

Source: https://github.com/morphgnt/sblgnt
Free scholarly critical Greek NT with full polytonic diacritics.
Format: word-by-word morphological tagging; verse text reconstructed by
joining col-4 (surface form) tokens per BBCCVV reference.

Data saved to: data/translations/NA27/{USFM_BOOK}/{chapter}.json
(We store SBLGNT under the NA27 folder — both are modern critical editions
and differences between them are minimal for reading purposes.)

Usage:
    python3 pipeline/fetch_sblgnt.py             # all books
    python3 pipeline/fetch_sblgnt.py --book MAT  # single book
    python3 pipeline/fetch_sblgnt.py --resume    # skip existing files
"""

import json
import time
import argparse
import urllib.request
from pathlib import Path
from collections import defaultdict

PROJECT = Path(__file__).resolve().parent.parent
OUT_DIR = PROJECT / 'data' / 'translations' / 'NA27'

UA = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
BASE_URL = 'https://raw.githubusercontent.com/morphgnt/sblgnt/master'
DELAY = 0.3

# MorphGNT book number → (filename_stem, USFM code)
BOOK_MAP = [
    (61, '61-Mt-morphgnt',  'MAT'),
    (62, '62-Mk-morphgnt',  'MRK'),
    (63, '63-Lk-morphgnt',  'LUK'),
    (64, '64-Jn-morphgnt',  'JHN'),
    (65, '65-Ac-morphgnt',  'ACT'),
    (66, '66-Ro-morphgnt',  'ROM'),
    (67, '67-1Co-morphgnt', '1CO'),
    (68, '68-2Co-morphgnt', '2CO'),
    (69, '69-Ga-morphgnt',  'GAL'),
    (70, '70-Eph-morphgnt', 'EPH'),
    (71, '71-Php-morphgnt', 'PHP'),
    (72, '72-Col-morphgnt', 'COL'),
    (73, '73-1Th-morphgnt', '1TH'),
    (74, '74-2Th-morphgnt', '2TH'),
    (75, '75-1Ti-morphgnt', '1TI'),
    (76, '76-2Ti-morphgnt', '2TI'),
    (77, '77-Tit-morphgnt', 'TIT'),
    (78, '78-Phm-morphgnt', 'PHM'),
    (79, '79-Heb-morphgnt', 'HEB'),
    (80, '80-Jas-morphgnt', 'JAS'),
    (81, '81-1Pe-morphgnt', '1PE'),
    (82, '82-2Pe-morphgnt', '2PE'),
    (83, '83-1Jn-morphgnt', '1JN'),
    (84, '84-2Jn-morphgnt', '2JN'),
    (85, '85-3Jn-morphgnt', '3JN'),
    (86, '86-Jud-morphgnt', 'JUD'),
    (87, '87-Re-morphgnt',  'REV'),
]

USFM_TO_FILE = {usfm: fn for _, fn, usfm in BOOK_MAP}


def fetch_book_file(filename: str) -> str | None:
    url = f'{BASE_URL}/{filename}.txt'
    req = urllib.request.Request(url, headers={'User-Agent': UA})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.read().decode('utf-8')
    except Exception as e:
        print(f'    fetch error: {e}')
        return None


def parse_morphgnt(raw: str) -> dict:
    """
    Parse MorphGNT format into {chapter: {verse: [words]}}.
    Each line: BBCCVV POS MORPH WORD NORMALIZED ACCENTUATED LEMMA
    Column 4 (index 3) is the surface word as printed (may have punctuation).
    """
    chapters = defaultdict(lambda: defaultdict(list))
    for line in raw.splitlines():
        parts = line.split()
        if len(parts) < 4:
            continue
        ref = parts[0]   # BBCCVV
        word = parts[3]  # surface form with punctuation
        ch = int(ref[2:4])
        v  = int(ref[4:6])
        chapters[ch][v].append(word)
    return chapters


def build_verse_text(words: list[str]) -> str:
    """Join words with spaces; strip trailing punctuation from each token
    that's not the last, to avoid doubled punct. Actually keep as-is:
    punctuation is attached to the preceding word in MorphGNT."""
    return ' '.join(words)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--book',   help='Process only this USFM book (e.g. MAT)')
    parser.add_argument('--resume', action='store_true',
                        help='Skip books where all chapter files already exist')
    args = parser.parse_args()

    if args.book:
        books = [(fn, args.book.upper()) for fn, usfm in
                 [(fn, usfm) for _, fn, usfm in BOOK_MAP] if usfm == args.book.upper()]
        if not books:
            print(f'Unknown book: {args.book}')
            return
        work_books = [(USFM_TO_FILE[args.book.upper()], args.book.upper())]
    else:
        work_books = [(fn, usfm) for _, fn, usfm in BOOK_MAP]

    total_books = len(work_books)
    print(f'Fetching {total_books} NT books from MorphGNT/SBLGNT …')
    print(f'Output: {OUT_DIR}')
    print('─' * 55)

    ok_books = fail_books = ok_ch = 0
    for i, (filename, usfm) in enumerate(work_books, 1):
        print(f'[{i:2d}/{total_books}] {usfm} ({filename}) … ', end='', flush=True)

        raw = fetch_book_file(filename)
        time.sleep(DELAY)

        if raw is None:
            print('FETCH ERROR')
            fail_books += 1
            continue

        chapters = parse_morphgnt(raw)
        if not chapters:
            print('PARSE ERROR — no chapters found')
            fail_books += 1
            continue

        book_dir = OUT_DIR / usfm
        book_dir.mkdir(parents=True, exist_ok=True)

        ch_written = 0
        for ch_num in sorted(chapters.keys()):
            out = book_dir / f'{ch_num}.json'
            if args.resume and out.exists():
                continue
            verse_dict = chapters[ch_num]
            content = []
            for v_num in sorted(verse_dict.keys()):
                text = build_verse_text(verse_dict[v_num])
                if text:
                    content.append({'v': v_num, 'text': text})
            if content:
                out.write_text(json.dumps({'content': content}, ensure_ascii=False, indent=2),
                               encoding='utf-8')
                ch_written += 1

        print(f'✓ {len(chapters)} chapters, {ch_written} written')
        ok_books += 1
        ok_ch += ch_written

    print()
    print('─' * 55)
    print(f'Done: {ok_books} books, {ok_ch} chapters saved, {fail_books} errors')


if __name__ == '__main__':
    main()
