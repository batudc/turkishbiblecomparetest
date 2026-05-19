#!/usr/bin/env python3
"""
Extract Greek NT interlinear data from greekntinttvm+.bbli (SQLite) into per-chapter JSON files.
Output: data/interlinear/{BOOK_CODE}/{chapter}.json

Usage:
    python3 pipeline/extract_interlinear.py
"""
import json
import re
import sqlite3
from pathlib import Path

DB = Path(__file__).parent.parent / 'data/strong/greekntinttvm+.bbli'
OUT = Path(__file__).parent.parent / 'data/interlinear'

BOOK_MAP = {
    40: 'MAT', 41: 'MRK', 42: 'LUK', 43: 'JHN', 44: 'ACT',
    45: 'ROM', 46: '1CO', 47: '2CO', 48: 'GAL', 49: 'EPH',
    50: 'PHP', 51: 'COL', 52: '1TH', 53: '2TH', 54: '1TI',
    55: '2TI', 56: 'TIT', 57: 'PHM', 58: 'HEB', 59: 'JAS',
    60: '1PE', 61: '2PE', 62: '1JN', 63: '2JN', 64: '3JN',
    65: 'JUD', 66: 'REV',
}

# Regex to split individual word blocks
RE_WORD_DIV = re.compile(r'<div\s+style="([^"]*)">(.*?)</div>', re.DOTALL)
RE_TAG      = re.compile(r'<([a-z]+)>(.*?)</\1>', re.DOTALL)

def strip_tags(s):
    return re.sub(r'<[^>]+>', '', s).strip()

def parse_word(style, body):
    """Parse one word <div> into a dict."""
    is_variant = 'background-color:#CCCCCC' in style or 'background-color: #CCCCCC' in style

    fields = RE_TAG.findall(body)
    # Expected order: grk(text), gray, num(tvm_code), num(strongs), tvm, grk(lexical), gra, blu, red
    grk_vals  = [strip_tags(v) for t, v in fields if t == 'grk']
    gray_vals = [strip_tags(v) for t, v in fields if t == 'gray']
    num_vals  = [strip_tags(v) for t, v in fields if t == 'num']
    tvm_vals  = [strip_tags(v) for t, v in fields if t == 'tvm']
    gra_vals  = [strip_tags(v) for t, v in fields if t == 'gra']
    blu_vals  = [strip_tags(v) for t, v in fields if t == 'blu']
    red_vals  = [strip_tags(v) for t, v in fields if t == 'red']

    # variant marker text is a small <grk> inside a <red> — skip those from grk_vals
    # First <grk> is the surface form; second is the lexical/accented form
    text    = grk_vals[0] if len(grk_vals) > 0 else ''
    lexical = grk_vals[1] if len(grk_vals) > 1 else text

    translit = gray_vals[0] if gray_vals else ''

    # Two <num> tags: first may be TVM Strong's code (empty for non-verbs), second is lexical Strong's
    tvm_strongs = num_vals[0] if len(num_vals) > 0 else ''
    strongs     = num_vals[1] if len(num_vals) > 1 else (num_vals[0] if num_vals else '')

    morph   = tvm_vals[0] if tvm_vals else ''
    gloss   = gra_vals[0] if gra_vals else ''
    trans   = blu_vals[0] if blu_vals else ''
    context = red_vals[0] if red_vals else ''

    word = {
        'w': text,
        'l': lexical,
        'tr': translit,
        'st': strongs,
        'mp': morph,
        'gl': gloss,
    }
    if tvm_strongs:
        word['stv'] = tvm_strongs
    if trans:
        word['tx'] = trans
    if context:
        word['cx'] = context
    if is_variant:
        word['var'] = True
    return word


def parse_verse(scripture):
    """Parse a verse's Scripture HTML into a list of word dicts."""
    words = []
    for m in RE_WORD_DIV.finditer(scripture):
        style, body = m.group(1), m.group(2)
        w = parse_word(style, body)
        if w['w']:
            words.append(w)
    return words


def main():
    conn = sqlite3.connect(DB)
    cur  = conn.cursor()

    cur.execute("SELECT DISTINCT Book FROM Bible ORDER BY Book")
    books = [r[0] for r in cur.fetchall()]

    total_chapters = 0
    for book_id in books:
        code = BOOK_MAP.get(book_id)
        if not code:
            print(f'  skipping book {book_id}')
            continue

        book_dir = OUT / code
        book_dir.mkdir(parents=True, exist_ok=True)

        cur.execute("SELECT DISTINCT Chapter FROM Bible WHERE Book=? ORDER BY Chapter", (book_id,))
        chapters = [r[0] for r in cur.fetchall()]

        for ch in chapters:
            cur.execute(
                "SELECT Verse, Scripture FROM Bible WHERE Book=? AND Chapter=? ORDER BY Verse",
                (book_id, ch)
            )
            rows = cur.fetchall()

            verses = []
            for verse_num, scripture in rows:
                words = parse_verse(scripture)
                verses.append({'v': verse_num, 'words': words})

            out_path = book_dir / f'{ch}.json'
            out_path.write_text(
                json.dumps({'b': code, 'c': ch, 'verses': verses}, ensure_ascii=False, separators=(',', ':')),
                encoding='utf-8'
            )
            total_chapters += 1

        print(f'{code}: {len(chapters)} chapters')

    conn.close()
    print(f'\nDone. {total_chapters} chapter files written to {OUT}')


if __name__ == '__main__':
    main()
