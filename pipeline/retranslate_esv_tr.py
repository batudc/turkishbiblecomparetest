#!/usr/bin/env python3
"""
Retranslate ESV TR chapters where notes are still in English.
Uses GoogleTranslator with bold-marker preservation.

Usage:
    python3 pipeline/retranslate_esv_tr.py --worker 0 --total 3
    python3 pipeline/retranslate_esv_tr.py --book HEB
"""

import json, re, time, sys
from pathlib import Path
from deep_translator import GoogleTranslator

ROOT   = Path(__file__).parent.parent
EN_DIR = ROOT / 'data' / 'commentary-en'
TR_DIR = ROOT / 'data' / 'commentary-tr-esv'
DELAY  = 0.35

# Books still in English (detected by word-ratio scan)
BAD_BOOKS = [
    'GEN','EXO','LEV','NUM','DEU','JOS','JDG','RUT',
    '1SA','2SA','1KI','2KI','EZR','NEH','EST','JOB',
    'PSA','PRO','ECC','SNG','ISA','JER','LAM','EZK',
    'DAN','HOS','AMO','OBA','JON','MIC','NAH','HAB',
    'ZEP','HAG','ZEC','MAL',
    'LUK','JHN','HEB','1CO',
]

CHAPTERS = {
    'GEN':50,'EXO':40,'LEV':27,'NUM':36,'DEU':34,'JOS':24,'JDG':21,'RUT':4,
    '1SA':31,'2SA':24,'1KI':22,'2KI':25,'EZR':10,'NEH':13,'EST':10,'JOB':42,
    'PSA':150,'PRO':31,'ECC':12,'SNG':8,'ISA':66,'JER':52,'LAM':5,'EZK':48,
    'DAN':12,'HOS':14,'AMO':9,'OBA':1,'JON':4,'MIC':7,'NAH':3,'HAB':3,
    'ZEP':3,'HAG':2,'ZEC':14,'MAL':4,
    'LUK':24,'JHN':21,'HEB':13,'1CO':16,
}

# CLI
args = sys.argv[1:]
def get_arg(name):
    try: return args[args.index(f'--{name}') + 1]
    except: return None

specific_book = get_arg('book')
if specific_book:
    books_to_process = [specific_book.upper()]
else:
    worker = int(get_arg('worker') or '0')
    total  = int(get_arg('total')  or '1')
    books_to_process = [b for i, b in enumerate(BAD_BOOKS) if i % total == worker]

translator = GoogleTranslator(source='en', target='tr')
_cache: dict = {}

EN_WORDS = re.compile(r'\b(the|and|for|this|that|with|from|which|have|been|also|were|not|but|see|his|her|its|they|their|are|was|will|is|in|of|to|a|an|be|by|as|at|on|it|he|she|we|you|do|did|if|or|so|all|can|has|had|him|our|out|who|may|into|when|than|then|thus)\b')
TR_WORDS = re.compile(r'\b(bu|ve|ile|için|bir|olan|ama|daha|çünkü|olarak|onun|ise|da|de|ki|ne|var|tanrı|isa|rab|gibi|kadar|her|o|yani|bkz|göre|ancak|artık|zira|eğer|fakat|bunun|bunlar)\b', re.IGNORECASE)

def is_english(text: str) -> bool:
    en = len(EN_WORDS.findall(text))
    tr = len(TR_WORDS.findall(text))
    return en > tr * 3 and en > 4

def translate(text: str) -> str:
    if not text or not text.strip():
        return text
    t = text.strip()
    if t in _cache:
        return _cache[t]
    # Wrap **bold** → <b>bold</b> so Google preserves them
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
            print(f'    [retry {attempt+1}/{5}, wait {wait}s] {str(e)[:60]}', flush=True)
            time.sleep(wait)

    # Hard fallback — keep original
    _cache[t] = t
    return t

def needs_translation(notes: list) -> bool:
    if not notes:
        return False
    # Also retranslate if any note has empty text
    if any(not n.get('text', '').strip() for n in notes):
        return True
    sample = ' '.join(n.get('text', '') for n in notes[:5])
    return is_english(sample)

def process_chapter(book: str, ch: int) -> bool:
    tr_path = TR_DIR / book / f'{ch}.json'
    en_path = EN_DIR / book / f'{ch}.json'

    if not tr_path.exists() and not en_path.exists():
        return False

    tr = {}
    if tr_path.exists():
        try: tr = json.loads(tr_path.read_text('utf-8'))
        except: pass

    # Check if already translated
    if not needs_translation(tr.get('notes', [])):
        print(f'  [{ch}] ok ', end='', flush=True)
        return False

    # Load EN source
    if not en_path.exists():
        print(f'  [{ch}] no-en ', end='', flush=True)
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

    tr_path.parent.mkdir(parents=True, exist_ok=True)
    tr_path.write_text(json.dumps(merged, ensure_ascii=False, indent=2), 'utf-8')
    print(f'  [{ch}]✓{len(tr_notes)}n ', end='', flush=True)
    return True

# ── Main ─────────────────────────────────────────────────────────────────
total_done = 0
for book in books_to_process:
    max_ch = CHAPTERS.get(book, 0)
    if not max_ch:
        print(f'\nUnknown book: {book}')
        continue
    print(f'\n── {book} ({max_ch} ch) ──', flush=True)
    for ch in range(1, max_ch + 1):
        process_chapter(book, ch)
        total_done += 1
    print()

print(f'\n─── Done: {total_done} chapters processed ───')
