#!/usr/bin/env python3
"""
Runs all 4 Greek translation tasks (pos/origin, def, nas_def, sc) in parallel threads.
Writes to tr1_pos_grk.json, tr2_def_grk.json, tr3_nasdef_grk.json, tr4_sc_grk.json.
Safe to run alongside the Hebrew agents (no shared output files).
"""
import json, time, re, threading
from pathlib import Path
from deep_translator import GoogleTranslator

ROOT    = Path(__file__).parent.parent
STRONGS = ROOT / 'data/strongs'
DELAY   = 0.3   # per-thread delay — 4 threads × 0.3s ≈ 1.2 req/s per IP (safe)

source = [e for e in json.loads((STRONGS / 'greek.json').read_text('utf-8')) if 'error' not in e]
print(f'Greek entries: {len(source)}')


def make_translator():
    return GoogleTranslator(source='en', target='tr')


def translate_one(translator, text: str, cache: dict) -> str:
    if not text: return ''
    t = text.strip()
    if t in cache: return cache[t]
    chunk = t[:4900]
    for attempt in range(4):
        try:
            r = translator.translate(chunk) or chunk
            cache[t] = r
            time.sleep(DELAY)
            return r
        except Exception as e:
            print(f'  [err] {e}')
            time.sleep(2 ** attempt)
    cache[t] = t
    return t


def run_task(field_name: str, out_file: Path, extract_fn, prefix='G'):
    tr   = make_translator()
    cache = {}

    existing = {}
    if out_file.exists():
        try:
            existing = json.loads(out_file.read_text('utf-8'))
            print(f'  [{field_name}] Resuming: {len(existing)} done')
        except Exception:
            pass

    done = 0
    for e in source:
        key = f'{prefix}{e["id"]}'
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
            print(f'  [{field_name}] {done}/{len(source)} ({pct:.1f}%)')
            out_file.write_text(json.dumps(existing, ensure_ascii=False, separators=(',', ':')), 'utf-8')

    out_file.write_text(json.dumps(existing, ensure_ascii=False, separators=(',', ':')), 'utf-8')
    print(f'  [{field_name}] Done. {len(existing)} entries saved → {out_file.name}')


TASKS = [
    ('pos_origin', STRONGS / 'tr1_pos_grk.json',
     lambda e: {'pos': e.get('pos',''), 'origin': e.get('origin','')}),
    ('def',        STRONGS / 'tr2_def_grk.json',
     lambda e: e.get('short_def') or e.get('nas_def') or ''),
    ('nas_def',    STRONGS / 'tr3_nasdef_grk.json',
     lambda e: e.get('nas_def', '')),
    ('sc',         STRONGS / 'tr4_sc_grk.json',
     lambda e: (e.get('strongs_def') or '')[:4900]),
]


def main():
    threads = []
    for name, out, fn in TASKS:
        t = threading.Thread(target=run_task, args=(name, out, fn), daemon=True)
        t.start()
        threads.append(t)
        time.sleep(0.5)   # stagger starts slightly

    for t in threads:
        t.join()

    print('\n=== All Greek tasks complete ===')


if __name__ == '__main__':
    main()
