#!/usr/bin/env python3
"""
Translates the UPPER half of Hebrew (H4500–H8674) for all 4 fields, in parallel threads.
Writes to SEPARATE files: tr1_pos_heb_b.json, tr2_def_heb_b.json, etc.
(avoids any race condition with the existing lower-half agents)

tr5_merge_and_dedupe.py will combine _b files automatically.
"""
import json, time, threading
from pathlib import Path
from deep_translator import GoogleTranslator

ROOT    = Path(__file__).parent.parent
STRONGS = ROOT / 'data/strongs'
DELAY   = 0.3
START   = 4500   # well above current agent positions (~1000-1600)

all_source = [e for e in json.loads((STRONGS / 'hebrew.json').read_text('utf-8')) if 'error' not in e]
source = [e for e in all_source if e['id'] >= START]
print(f'Hebrew upper range H{START}+: {len(source)} entries')


def make_translator():
    return GoogleTranslator(source='en', target='tr')


def translate_one(translator, text: str, cache: dict) -> str:
    if not text: return ''
    t = text.strip()[:4900]
    if t in cache: return cache[t]
    for attempt in range(4):
        try:
            r = translator.translate(t) or t
            cache[t] = r
            time.sleep(DELAY)
            return r
        except Exception as e:
            print(f'  [err] {e}')
            time.sleep(2 ** attempt)
    cache[t] = t
    return t


def run_task(name: str, out_file: Path, extract_fn):
    tr    = make_translator()
    cache = {}

    existing = {}
    if out_file.exists():
        try:
            existing = json.loads(out_file.read_text('utf-8'))
            print(f'  [{name}] Resuming: {len(existing)} done')
        except Exception:
            pass

    done = 0
    for e in source:
        key = f'H{e["id"]}'
        if key in existing:
            done += 1
            continue
        text = extract_fn(e)
        if isinstance(text, dict):
            existing[key] = {k: translate_one(tr, v, cache) for k, v in text.items()}
        else:
            existing[key] = translate_one(tr, text, cache)
        done += 1
        if done % 100 == 0:
            pct = done / len(source) * 100
            print(f'  [{name}] {done}/{len(source)} ({pct:.1f}%)')
            out_file.write_text(json.dumps(existing, ensure_ascii=False, separators=(',', ':')), 'utf-8')

    out_file.write_text(json.dumps(existing, ensure_ascii=False, separators=(',', ':')), 'utf-8')
    print(f'  [{name}] Done. {len(existing)} → {out_file.name}')


TASKS = [
    ('pos_origin', STRONGS / 'tr1_pos_heb_b.json',
     lambda e: {'pos': e.get('pos',''), 'origin': e.get('origin','')}),
    ('def',        STRONGS / 'tr2_def_heb_b.json',
     lambda e: e.get('short_def') or e.get('nas_def') or ''),
    ('nas_def',    STRONGS / 'tr3_nasdef_heb_b.json',
     lambda e: e.get('nas_def', '')),
    ('sc',         STRONGS / 'tr4_sc_heb_b.json',
     lambda e: e.get('strongs_def', '')[:4900]),
]


def main():
    threads = []
    for name, out, fn in TASKS:
        t = threading.Thread(target=run_task, args=(name, out, fn), daemon=True)
        t.start()
        threads.append(t)
        time.sleep(0.8)

    for t in threads:
        t.join()

    print('\n=== Hebrew upper range complete ===')


if __name__ == '__main__':
    main()
