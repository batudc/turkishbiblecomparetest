#!/usr/bin/env python3
"""
Agent 4: Translate strongs_def field → Strong's Concordance
Outputs: data/strongs/tr4_sc_heb.json, tr4_sc_grk.json
Format: {"H123": "translated concordance text", ...}
"""
import json, time
from pathlib import Path
from deep_translator import GoogleTranslator

ROOT    = Path(__file__).parent.parent
STRONGS = ROOT / 'data/strongs'
TR_DELAY = 0.3  # slightly slower, these texts are longer

translator = GoogleTranslator(source='en', target='tr')
_cache = {}

def translate(text: str) -> str:
    if not text: return ''
    t = text.strip()
    if t in _cache: return _cache[t]
    # Chunk if too long
    if len(t) > 4800:
        # Translate in halves and join
        mid = t.rfind(' ', 0, 2400)
        if mid == -1: mid = 2400
        a = translate(t[:mid])
        b = translate(t[mid:].strip())
        result = (a + ' ' + b).strip()
        _cache[t] = result
        return result
    for attempt in range(4):
        try:
            result = translator.translate(t) or t
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
        text = e.get('strongs_def', '')
        existing[key] = translate(text) if text else ''
        done += 1
        if done % 100 == 0:
            pct = done / len(source) * 100
            print(f'  {done}/{len(source)} ({pct:.1f}%)')
            out_file.write_text(json.dumps(existing, ensure_ascii=False, separators=(',', ':')), 'utf-8')

    out_file.write_text(json.dumps(existing, ensure_ascii=False, separators=(',', ':')), 'utf-8')
    print(f'  Done. {len(existing)} entries saved.')


def main():
    process(STRONGS / 'hebrew.json', STRONGS / 'tr4_sc_heb.json', 'H')
    process(STRONGS / 'greek.json',  STRONGS / 'tr4_sc_grk.json', 'G')
    print('\n=== Agent 4 complete ===')

if __name__ == '__main__':
    main()
