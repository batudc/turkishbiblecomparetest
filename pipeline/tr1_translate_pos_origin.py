#!/usr/bin/env python3
"""
Agent 1: Translate pos (part of speech) and origin fields → Sözcük Türü & Köken
Outputs: data/strongs/tr1_pos_heb.json, tr1_pos_grk.json
Format: {"H123": {"pos": "...", "origin": "..."}, ...}
"""
import json, time, re
from pathlib import Path
from deep_translator import GoogleTranslator

ROOT    = Path(__file__).parent.parent
STRONGS = ROOT / 'data/strongs'
TR_DELAY = 0.25

translator = GoogleTranslator(source='en', target='tr')

# Cache repeated values (many words share same pos tags)
_cache = {}

def translate(text: str) -> str:
    if not text: return ''
    t = text.strip()
    if t in _cache: return _cache[t]
    for attempt in range(4):
        try:
            result = translator.translate(t[:4900]) or t
            _cache[t] = result
            time.sleep(TR_DELAY)
            return result
        except Exception as e:
            print(f'    [retry {attempt+1}] {e}')
            time.sleep(2 ** attempt)
    _cache[t] = t
    return t


def process(lang_file: Path, out_file: Path, prefix: str):
    print(f'\n{prefix} — {lang_file.name}')
    source = [e for e in json.loads(lang_file.read_text('utf-8')) if 'error' not in e]
    print(f'  Entries: {len(source)}')

    existing = {}
    if out_file.exists():
        existing = json.loads(out_file.read_text('utf-8'))
        print(f'  Resuming: {len(existing)} done')

    done = 0
    for e in source:
        key = f'{prefix}{e["id"]}'
        if key in existing:
            done += 1
            continue
        entry = {}
        if e.get('pos'):    entry['pos']    = translate(e['pos'])
        if e.get('origin'): entry['origin'] = translate(e['origin'])
        existing[key] = entry
        done += 1
        if done % 100 == 0:
            pct = done / len(source) * 100
            print(f'  {done}/{len(source)} ({pct:.1f}%)')
            out_file.write_text(json.dumps(existing, ensure_ascii=False, separators=(',', ':')), 'utf-8')

    out_file.write_text(json.dumps(existing, ensure_ascii=False, separators=(',', ':')), 'utf-8')
    print(f'  Done. {len(existing)} entries saved.')


def main():
    process(STRONGS / 'hebrew.json', STRONGS / 'tr1_pos_heb.json', 'H')
    process(STRONGS / 'greek.json',  STRONGS / 'tr1_pos_grk.json', 'G')
    print('\n=== Agent 1 complete ===')

if __name__ == '__main__':
    main()
