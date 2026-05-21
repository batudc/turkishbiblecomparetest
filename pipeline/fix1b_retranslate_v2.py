#!/usr/bin/env python3
"""
Fix 1b: Retranslate still-English definitions with safer rate limiting.
Reads current def from the tr JSON directly (already English), translates each.
Uses 1.5s delay between calls to avoid rate limiting.
"""
import json, re, time, sys
from pathlib import Path
from deep_translator import GoogleTranslator

ROOT    = Path(__file__).parent.parent
STRONGS = ROOT / 'data/strongs'
DELAY   = 1.5   # seconds between API calls

tr = GoogleTranslator(source='en', target='tr')
cache = {}

NUM_RE = re.compile(r'(\d+)\.\s*')

def split_numbered(text: str) -> list[tuple[str, str]]:
    parts = re.split(r'(\d+\.)', text.replace('\n', ' '))
    result = []
    i = 0
    while i < len(parts):
        if re.match(r'\d+\.', parts[i].strip()):
            num = parts[i].strip()
            body = parts[i+1].strip() if i+1 < len(parts) else ''
            result.append((num, body))
            i += 2
        else:
            if parts[i].strip():
                result.append(('', parts[i].strip()))
            i += 1
    return result if result else [('', text)]


def translate_one(text: str) -> str:
    """Translate one segment with retry. Returns original on failure."""
    if not text or len(text.strip()) < 2:
        return text
    t = text.strip()
    if t in cache:
        return cache[t]
    for attempt in range(3):
        try:
            result = tr.translate(t[:4800])
            time.sleep(DELAY)
            if result:
                cache[t] = result
                return result
        except Exception as e:
            wait = (attempt + 1) * 3
            print(f'    Retry {attempt+1}/3 after {wait}s: {e}', flush=True)
            time.sleep(wait)
    cache[t] = t
    return t


def smart_translate(text: str) -> str:
    if not text:
        return text
    parts = split_numbered(text)
    out = []
    for num, body in parts:
        tr_body = translate_one(body)
        out.append((num + ' ' + tr_body).strip())
    return '\n'.join(out)


def is_turkish(text: str) -> bool:
    if not text:
        return False
    return any(c in 'çşğıöüÇŞĞİÖÜ' for c in text)


def process(tr_path: Path, ids: list, label: str):
    print(f'\n=== {label}: {len(ids)} entries ===', flush=True)
    entries = json.loads(tr_path.read_text('utf-8'))
    entry_map = {e['id']: e for e in entries}

    done = 0
    skipped = 0
    for eid in ids:
        e = entry_map.get(eid)
        if not e:
            continue
        current_def = e.get('def', '')
        if is_turkish(current_def):
            skipped += 1
            continue
        new_def = smart_translate(current_def)
        if new_def and new_def != current_def:
            e['def'] = new_def
            done += 1
        if (done + skipped) % 20 == 0:
            pct = (done + skipped) / len(ids) * 100
            print(f'  {done+skipped}/{len(ids)} ({pct:.0f}%) — {done} translated, {skipped} already done', flush=True)
            tr_path.write_text(json.dumps(entries, ensure_ascii=False, separators=(',', ':')), 'utf-8')

    tr_path.write_text(json.dumps(entries, ensure_ascii=False, separators=(',', ':')), 'utf-8')
    print(f'  Done. {done} translated, {skipped} skipped (already Turkish)', flush=True)


def main():
    ids = json.loads(Path('/tmp/retranslate_ids2.json').read_text())
    process(STRONGS / 'heb_tr.json', ids['heb'], 'Hebrew')
    process(STRONGS / 'grk_tr.json', ids['grk'], 'Greek')
    print('\n=== Fix 1b complete ===', flush=True)


if __name__ == '__main__':
    main()
