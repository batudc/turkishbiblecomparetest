#!/usr/bin/env python3
"""
Fix ESV TR chapters where note text fields are empty strings.
Reads from EN source and translates into TR.
"""

import json, re, time, sys
from pathlib import Path
from deep_translator import GoogleTranslator

ROOT   = Path(__file__).parent.parent
EN_DIR = ROOT / 'data' / 'commentary-en'
TR_DIR = ROOT / 'data' / 'commentary-tr-esv'
DELAY  = 0.4

CHAPTERS = {
    'ACT': 28, 'ROM': 16, '1CO': 16, '2CO': 13,
    'GAL':  6, 'EPH':  6, '1TH':  5, '2TH':  3,
    '1TI':  6, '2TI':  4, 'TIT':  3, 'PHM':  1,
    'JAS':  5, '1PE':  5, '2PE':  3, 'JUD':  1,
    'REV': 22,
}

# CLI: optional --book BOOK to run a single book
args = sys.argv[1:]
def get_arg(name):
    try: return args[args.index(f'--{name}') + 1]
    except: return None

specific_book = get_arg('book')
books_to_process = [specific_book.upper()] if specific_book else list(CHAPTERS.keys())

translator = GoogleTranslator(source='en', target='tr')
_cache: dict = {}

def translate(text: str) -> str:
    if not text or not text.strip():
        return text
    t = text.strip()
    if t in _cache:
        return _cache[t]
    parts = t.split('**')
    html = ''
    for i, part in enumerate(parts):
        html += f'<b>{part}</b>' if i % 2 == 1 else part

    for attempt in range(5):
        try:
            result = translator.translate(html) or html
            time.sleep(DELAY)
            result = re.sub(r'<b>(.*?)</b>', r'**\1**', result, flags=re.DOTALL)
            _cache[t] = result
            return result
        except Exception as e:
            wait = 2 ** (attempt + 1)
            print(f'    [retry {attempt+1}/5, {wait}s] {str(e)[:60]}', flush=True)
            time.sleep(wait)

    _cache[t] = t
    return t

def has_empty_notes(notes: list) -> bool:
    return any(not n.get('text', '').strip() for n in notes)

def process_chapter(book: str, ch: int) -> bool:
    tr_path = TR_DIR / book / f'{ch}.json'
    en_path = EN_DIR / book / f'{ch}.json'

    if not tr_path.exists():
        print(f'  [{ch}]skip-no-tr ', end='', flush=True)
        return False

    tr = {}
    try:
        tr = json.loads(tr_path.read_text('utf-8'))
    except Exception as e:
        print(f'  [{ch}]err-read ', end='', flush=True)
        return False

    if not has_empty_notes(tr.get('notes', [])):
        print(f'  [{ch}]ok ', end='', flush=True)
        return False

    if not en_path.exists():
        print(f'  [{ch}]no-en ', end='', flush=True)
        return False

    en = json.loads(en_path.read_text('utf-8'))

    tr_notes = []
    for note in en.get('notes', []):
        text = translate(note.get('text', ''))
        tr_notes.append({'ref': note.get('ref', ''), 'text': text})

    tr_passages = []
    for p in en.get('passages', []):
        tr_passages.append({
            'ref':   p.get('ref', ''),
            'title': translate(p.get('title', '')),
            'text':  translate(p.get('text', '')),
        })

    merged = {
        'source': 'ESV Global Study Bible (Crossway) — Türkçe çeviri — Google Translate — Lisans kapsamında kullanılmaktadır',
        **({k: tr[k] for k in ('book_intro', 'chapter_intro') if k in tr and tr[k]}),
        'passages': tr_passages,
        'notes':    tr_notes,
    }

    tr_path.write_text(json.dumps(merged, ensure_ascii=False, indent=2), 'utf-8')
    print(f'  [{ch}]✓{len(tr_notes)}n ', end='', flush=True)
    return True

total_done = 0
for book in books_to_process:
    max_ch = CHAPTERS.get(book, 0)
    if not max_ch:
        print(f'\nBilinmeyen kitap: {book}')
        continue
    print(f'\n── {book} ({max_ch} ch) ──', flush=True)
    for ch in range(1, max_ch + 1):
        if process_chapter(book, ch):
            total_done += 1
    print()

print(f'\n─── Bitti: {total_done} bölüm çevrildi ───')
