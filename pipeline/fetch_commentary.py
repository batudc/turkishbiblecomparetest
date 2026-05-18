#!/usr/bin/env python3
"""
fetch_commentary.py — Scrapes Turkish Bible commentary from kutsalkitap.info.tr
All rights to use granted by the website owner.

Data saved to: data/commentary/{USFM_BOOK}/{chapter}.json
Format:
  {
    "intro": "Book introduction text (only on chapter 1)",
    "notes": [
      {"ref": "1:1", "text": "Commentary text..."},
      ...
    ]
  }

Usage:
    python3 pipeline/fetch_commentary.py             # all books
    python3 pipeline/fetch_commentary.py --book JHN  # single book
    python3 pipeline/fetch_commentary.py --resume    # skip existing files
"""

import json
import os
import re
import time
import argparse
import html as htmlmod
import urllib.request
from pathlib import Path

PROJECT = Path(__file__).resolve().parent.parent
OUT_DIR = PROJECT / 'data' / 'commentary'

UA = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
BASE = 'https://kutsalkitap.info.tr/?q='
DELAY = 0.4  # seconds between requests

# USFM code → kutsalkitap.info.tr abbreviation (URL-safe)
BOOK_ABBREV = {
    'GEN':'Yar',    'EXO':'%C3%87%C4%B1k', 'LEV':'Lev',    'NUM':'Say',
    'DEU':'Yas',    'JOS':'Y%C5%9Fu',       'JDG':'Hak',    'RUT':'Rut',
    '1SA':'1Sa',    '2SA':'2Sa',            '1KI':'1Kr',    '2KI':'2Kr',
    '1CH':'1Ta',    '2CH':'2Ta',            'EZR':'Ezr',    'NEH':'Neh',
    'EST':'Est',    'JOB':'Ey%C3%BC',       'PSA':'Mez',    'PRO':'%C3%96zd',
    'ECC':'Vai',    'SNG':'Ezg',            'ISA':'Y%C5%9Fa', 'JER':'Yer',
    'LAM':'A%C4%9F%C4%B1', 'EZK':'Hez',    'DAN':'Dan',    'HOS':'Ho%C5%9F',
    'JOL':'Yoe',   'AMO':'Amo',            'OBA':'Ova',    'JON':'Yun',
    'MIC':'Mik',   'NAH':'Nah',            'HAB':'Hab',    'ZEP':'Sef',
    'HAG':'Hag',   'ZEC':'Zek',            'MAL':'Mal',    'MAT':'Mat',
    'MRK':'Mar',   'LUK':'Luk',            'JHN':'Yu',     'ACT':'El%C3%A7',
    'ROM':'Rom',   '1CO':'1Ko',            '2CO':'2Ko',    'GAL':'Gal',
    'EPH':'Ef',    'PHP':'Flp',            'COL':'Kol',    '1TH':'1Se',
    '2TH':'2Se',   '1TI':'1Ti',            '2TI':'2ti',    'TIT':'Tit',
    'PHM':'Flm',   'HEB':'%C4%B0br',       'JAS':'Yak',    '1PE':'1Pe',
    '2PE':'2Pe',   '1JN':'1Yu',            '2JN':'2Yu',    '3JN':'3Yu',
    'JUD':'Yah',   'REV':'Va',
}

CHAPTERS = {
    'GEN':50,'EXO':40,'LEV':27,'NUM':36,'DEU':34,'JOS':24,'JDG':21,'RUT':4,
    '1SA':31,'2SA':24,'1KI':22,'2KI':25,'1CH':29,'2CH':36,
    'EZR':10,'NEH':13,'EST':10,'JOB':42,'PSA':150,'PRO':31,'ECC':12,'SNG':8,
    'ISA':66,'JER':52,'LAM':5,'EZK':48,'DAN':12,'HOS':14,'JOL':3,'AMO':9,
    'OBA':1,'JON':4,'MIC':7,'NAH':3,'HAB':3,'ZEP':3,'HAG':2,'ZEC':14,'MAL':4,
    'MAT':28,'MRK':16,'LUK':24,'JHN':21,'ACT':28,'ROM':16,'1CO':16,'2CO':13,
    'GAL':6,'EPH':6,'PHP':4,'COL':4,'1TH':5,'2TH':3,'1TI':6,'2TI':4,
    'TIT':3,'PHM':1,'HEB':13,'JAS':5,'1PE':5,'2PE':3,'1JN':5,'2JN':1,
    '3JN':1,'JUD':1,'REV':22,
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


def fetch_page(book_abbrev: str, chap: int) -> str | None:
    url = f'{BASE}{book_abbrev}.{chap}'
    req = urllib.request.Request(url, headers={'User-Agent': UA})
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            return r.read().decode('utf-8')
    except Exception as e:
        print(f'    fetch error: {e}')
        return None


def clean_text(html_fragment: str) -> str:
    """Strip HTML tags, decode entities, normalize whitespace."""
    text = re.sub(r'<[^>]+>', ' ', html_fragment)
    text = htmlmod.unescape(text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def parse_chapter(raw: str, chap: int) -> dict:
    """Extract notes and optional book intro from a raw HTML page."""
    # Book intro paragraphs (iq1 class) — present on chapter 1 pages
    intro_parts = re.findall(r'<p[^>]*class="iq1"[^>]*>(.*?)</p>', raw, re.DOTALL)
    intro = ' '.join(clean_text(p) for p in intro_parts).strip() if intro_parts else ''

    # Commentary notes (.notp divs)
    # Structure: <div class="notp"><strong>REF</strong> body text with <em> and <a> tags</div>
    note_divs = re.findall(r'<div class="notp">(.*?)</div>', raw, re.DOTALL)

    notes = []
    for div in note_divs:
        # Extract ref from <strong> tag
        ref_match = re.match(r'\s*<strong>(.*?)</strong>\s*', div, re.DOTALL)
        if ref_match:
            ref = clean_text(ref_match.group(1))
            body_html = div[ref_match.end():]
        else:
            ref = str(chap)
            body_html = div

        body = clean_text(body_html)
        if body:
            notes.append({'ref': ref, 'text': body})

    result = {'notes': notes}
    if intro:
        result['intro'] = intro
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--book',   help='Process only this book (e.g. JHN)')
    parser.add_argument('--resume', action='store_true',
                        help='Skip chapters that already have commentary files')
    args = parser.parse_args()

    books = [args.book.upper()] if args.book else BOOK_ORDER

    # Build work list
    work = []
    for code in books:
        abbrev = BOOK_ABBREV.get(code)
        if not abbrev:
            print(f'Unknown book code: {code}')
            continue
        total = CHAPTERS.get(code, 1)
        for ch in range(1, total + 1):
            out = OUT_DIR / code / f'{ch}.json'
            if args.resume and out.exists():
                continue
            work.append((code, abbrev, ch, out))

    total = len(work)
    print(f'Fetching {total} chapters of commentary …')
    print('─' * 55)

    ok = fail = 0
    for i, (code, abbrev, ch, out) in enumerate(work, 1):
        print(f'[{i:4d}/{total}] {code} {ch:3d} … ', end='', flush=True)
        raw = fetch_page(abbrev, ch)
        time.sleep(DELAY)

        if raw is None:
            print('FETCH ERROR')
            fail += 1
            continue

        data = parse_chapter(raw, ch)
        if not data['notes']:
            print('no notes')
            # Still save an empty file so resume skips it
        else:
            print(f'✓ {len(data["notes"])} notes' +
                  (' + intro' if data.get('intro') else ''))

        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')
        ok += 1

    print()
    print('─' * 55)
    print(f'Done: {ok} saved, {fail} errors, {total} total')


if __name__ == '__main__':
    main()
