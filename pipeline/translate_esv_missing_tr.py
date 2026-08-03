#!/usr/bin/env python3
"""
Translates missing ESV TR note chapters from ESV EN → Turkish using GoogleTranslator.
Preserves **bold** markers and Bible cross-references.

Usage:
    python3 pipeline/translate_esv_missing_tr.py
"""

import json, re, time
from pathlib import Path
from deep_translator import GoogleTranslator

ROOT    = Path(__file__).parent.parent
EN_DIR  = ROOT / 'data' / 'commentary-en'
TR_DIR  = ROOT / 'data' / 'commentary-tr-esv'
DELAY   = 0.3  # seconds between API calls

# The 34 chapters missing Turkish notes
MISSING = [
    ('MRK', list(range(1, 17))),
    ('PHP', list(range(1, 5))),
    ('JOL', list(range(1, 4))),
    ('1JN', list(range(1, 6))),
    ('2JN', [1]),
    ('3JN', [1]),
    ('1TI', [3, 4, 5, 6]),
]

translator = GoogleTranslator(source='en', target='tr')
_cache: dict[str, str] = {}


def translate(text: str) -> str:
    if not text or not text.strip():
        return text
    t = text.strip()
    if t in _cache:
        return _cache[t]
    # Use HTML tags to preserve bold markers through translation
    html = t.replace('**', '\x00')  # temp marker
    parts = html.split('\x00')
    # Reconstruct with <b> tags: odd-indexed parts are inside bold
    reconstructed = ''
    for i, part in enumerate(parts):
        if i % 2 == 1:  # inside ** **
            reconstructed += f'<b>{part}</b>'
        else:
            reconstructed += part

    for attempt in range(4):
        try:
            result = translator.translate(reconstructed) or reconstructed
            time.sleep(DELAY)
            # Convert <b>...</b> back to **...**
            result = re.sub(r'<b>(.*?)</b>', r'**\1**', result, flags=re.DOTALL)
            _cache[t] = result
            return result
        except Exception as e:
            print(f'    [retry {attempt+1}] {e}')
            time.sleep(2 ** (attempt + 1))

    _cache[t] = t  # fallback: keep original
    return t


def process_chapter(book: str, ch: int) -> None:
    en_path = EN_DIR / book / f'{ch}.json'
    tr_path = TR_DIR / book / f'{ch}.json'

    if not en_path.exists():
        print(f'  ⚠ EN missing: {book}/{ch}')
        return

    en = json.loads(en_path.read_text('utf-8'))
    en_notes = en.get('notes', [])
    en_passages = en.get('passages', [])

    if not en_notes and not en_passages:
        print(f'  ⚠ EN has no notes: {book}/{ch}')
        return

    # Load existing TR to preserve book_intro, chapter_intro
    tr = {}
    if tr_path.exists():
        try:
            tr = json.loads(tr_path.read_text('utf-8'))
        except Exception:
            pass

    tr_notes = []
    for note in en_notes:
        ref  = note.get('ref', '')
        text = note.get('text', '')
        tr_text = translate(text)
        tr_notes.append({'ref': ref, 'text': tr_text})

    tr_passages = []
    for p in en_passages:
        ref   = p.get('ref', '')
        title = translate(p.get('title', ''))
        text  = translate(p.get('text', ''))
        tr_passages.append({'ref': ref, 'title': title, 'text': text})

    merged = {
        'source': 'ESV Global Study Bible (Crossway) — Türkçe çeviri — DeepL translator — Lisans kapsamında kullanılmaktadır',
        **({k: tr[k] for k in ('book_intro', 'chapter_intro') if k in tr and tr[k]}),
        'passages': tr_passages,
        'notes':    tr_notes,
    }

    tr_path.parent.mkdir(parents=True, exist_ok=True)
    tr_path.write_text(json.dumps(merged, ensure_ascii=False, indent=2), 'utf-8')

    print(f'  ✓ {book}/{ch}: {len(tr_notes)} notes, {len(tr_passages)} passages')


def main():
    total_books = sum(len(chs) for _, chs in MISSING)
    done = 0

    for book, chapters in MISSING:
        print(f'\n── {book} ──')
        for ch in chapters:
            process_chapter(book, ch)
            done += 1

    print(f'\n─── Done: {done}/{total_books} chapters translated ───')


if __name__ == '__main__':
    main()
