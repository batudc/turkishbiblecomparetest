#!/usr/bin/env python3
"""
Extract Hebrew interlinear data from strong/hsb.bbli (HSB = Hebrew Study Bible)
and save 929 chapter JSON files to data/interlinear/{BOOK_CODE}/{chapter}.json

Word fields: w (Hebrew surface), st (H number), tr (transliteration), cx (gloss)

Usage:
    python3 pipeline/extract_ot_interlinear.py
"""
import json, re, sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / 'strong/hsb.bbli'
OUT_DIR = Path(__file__).parent.parent / 'data/interlinear'

BOOK_MAP = {
     1:'GEN',  2:'EXO',  3:'LEV',  4:'NUM',  5:'DEU',
     6:'JOS',  7:'JDG',  8:'RUT',  9:'1SA', 10:'2SA',
    11:'1KI', 12:'2KI', 13:'1CH', 14:'2CH', 15:'EZR',
    16:'NEH', 17:'EST', 18:'JOB', 19:'PSA', 20:'PRO',
    21:'ECC', 22:'SNG', 23:'ISA', 24:'JER', 25:'LAM',
    26:'EZK', 27:'DAN', 28:'HOS', 29:'JOL', 30:'AMO',
    31:'OBA', 32:'JON', 33:'MIC', 34:'NAH', 35:'HAB',
    36:'ZEP', 37:'HAG', 38:'ZEC', 39:'MAL',
}

_TAG = re.compile(r'<[^>]+>')
def strip(s): return _TAG.sub('', s).strip()


def parse_verse(html):
    words = []
    for part in re.split(r'(?=<heb>)', html):
        m_heb = re.match(r'<heb>(.*?)</heb>', part, re.DOTALL)
        if not m_heb:
            continue
        w = m_heb.group(1).strip()
        rest = part[m_heb.end():]

        m_num = re.search(r'<num>(H\d+[a-z]?)</num>', rest)
        st = m_num.group(1) if m_num else None

        m_tr = re.search(r'color:#757575[^>]*><sup>(.*?)</sup>', rest, re.DOTALL)
        tr = strip(m_tr.group(1)) if m_tr else None

        m_cx = re.search(r'color:#2E78C2[^>]*><sup>(.*?)</sup>', rest, re.DOTALL)
        if not m_cx:
            m_cx = re.search(r'<tvm>(.*?)</tvm>', rest, re.DOTALL)
        cx = strip(m_cx.group(1)) if m_cx else None

        entry = {'w': w}
        if st: entry['st'] = st
        if tr: entry['tr'] = tr
        if cx: entry['cx'] = cx
        words.append(entry)
    return words


def main():
    con = sqlite3.connect(DB_PATH)
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    total_ch = 0
    for book_num, book_code in BOOK_MAP.items():
        book_dir = OUT_DIR / book_code
        book_dir.mkdir(exist_ok=True)

        chapters = con.execute(
            'SELECT DISTINCT Chapter FROM Bible WHERE Book=? ORDER BY Chapter',
            (book_num,)
        ).fetchall()

        for (ch_num,) in chapters:
            rows = con.execute(
                'SELECT Verse, Scripture FROM Bible WHERE Book=? AND Chapter=? ORDER BY Verse',
                (book_num, ch_num)
            ).fetchall()

            verses = []
            for v_num, html in rows:
                words = parse_verse(html)
                if words:
                    verses.append({'v': v_num, 'words': words})

            out = {'b': book_code, 'c': ch_num, 'lang': 'heb', 'verses': verses}
            (book_dir / f'{ch_num}.json').write_text(
                json.dumps(out, ensure_ascii=False, separators=(',', ':')), 'utf-8'
            )

        total_ch += len(chapters)
        print(f'  {book_code}: {len(chapters)} chapters')

    con.close()
    print(f'\nDone. {total_ch} chapter files → {OUT_DIR}')


if __name__ == '__main__':
    main()
