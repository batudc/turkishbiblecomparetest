#!/usr/bin/env python3
"""
Agent 5: Merge tr1–tr4 outputs into heb_tr.json and grk_tr.json.
Also checks for duplicate 'def' values across different Strong's numbers
and re-translates them with added context to produce distinct definitions.

Run AFTER tr1–tr4 complete.

Output format per entry:
{"id": N, "pos": "...", "origin": "...", "def": "...", "nas_def": "...", "sc": "..."}
"""
import json, time, sys
from pathlib import Path
from collections import defaultdict
from deep_translator import GoogleTranslator

ROOT    = Path(__file__).parent.parent
STRONGS = ROOT / 'data/strongs'
TR_DELAY = 0.25

translator = GoogleTranslator(source='en', target='tr')

def translate_with_context(text: str, context: str) -> str:
    if not text: return ''
    full = f'[Bağlam: {context}] {text}'
    for attempt in range(4):
        try:
            result = translator.translate(full[:4900]) or text
            time.sleep(TR_DELAY)
            return result
        except Exception as e:
            print(f'  [retry {attempt+1}] {e}')
            time.sleep(2 ** attempt)
    return text


def load_map(path: Path) -> dict:
    if not path.exists():
        print(f'  WARNING: {path.name} not found — skipping')
        return {}
    return json.loads(path.read_text('utf-8'))


def load_combined(base: Path) -> dict:
    """Load base file and merge optional _b (upper-range) file."""
    data = load_map(base)
    b_path = base.with_name(base.stem + '_b' + base.suffix)
    if b_path.exists():
        b_data = load_map(b_path)
        print(f'    Merging {b_path.name}: {len(b_data)} extra entries')
        data = {**data, **b_data}   # _b entries override if overlapping (shouldn't happen)
    return data


def merge(lang: str, prefix: str, source_file: Path, out_file: Path):
    print(f'\n=== Merging {lang} ===')

    source_entries = {e['id']: e for e in json.loads(source_file.read_text('utf-8')) if 'error' not in e}
    pos_map    = load_combined(STRONGS / f'tr1_pos_{lang}.json')
    def_map    = load_combined(STRONGS / f'tr2_def_{lang}.json')
    nasdef_map = load_combined(STRONGS / f'tr3_nasdef_{lang}.json')
    sc_map     = load_combined(STRONGS / f'tr4_sc_{lang}.json')

    # Build merged list
    result = []
    for num, e in source_entries.items():
        key = f'{prefix}{num}'
        pos_data   = pos_map.get(key, {})
        entry = {
            'id': num,
            'pos':    pos_data.get('pos', '') if isinstance(pos_data, dict) else '',
            'origin': pos_data.get('origin', '') if isinstance(pos_data, dict) else '',
            'def':    def_map.get(key, ''),
            'nas_def': nasdef_map.get(key, ''),
            'sc':     sc_map.get(key, ''),
        }
        result.append(entry)

    # ── Duplicate detection in 'def' ──
    print(f'  Checking for duplicate definitions…')
    def_counts = defaultdict(list)
    for e in result:
        d = e['def'].strip()
        if d:
            def_counts[d].append(e['id'])

    dupe_ids = {eid for ids in def_counts.values() if len(ids) > 1 for eid in ids}
    print(f'  Found {len(dupe_ids)} entries with non-unique definitions')

    if dupe_ids:
        src_lookup = source_entries
        for e in result:
            if e['id'] not in dupe_ids: continue
            orig = src_lookup.get(e['id'], {})
            ctx_parts = []
            if orig.get('title'):   ctx_parts.append(orig['title'])
            if orig.get('word'):    ctx_parts.append(orig['word'])
            if orig.get('translit'):ctx_parts.append(orig['translit'])
            ctx = ', '.join(ctx_parts)
            original_text = orig.get('short_def') or orig.get('nas_def') or ''
            if original_text:
                new_def = translate_with_context(original_text, ctx)
                print(f'  Re-translated {prefix}{e["id"]}: "{e["def"]}" → "{new_def}"')
                e['def'] = new_def

    # Save
    out_file.write_text(json.dumps(result, ensure_ascii=False, separators=(',', ':')), 'utf-8')
    print(f'  Saved {len(result)} entries → {out_file.name}')


def main():
    missing = []
    for f in ['tr1_pos_heb.json', 'tr2_def_heb.json', 'tr3_nasdef_heb.json', 'tr4_sc_heb.json',
              'tr1_pos_grk.json', 'tr2_def_grk.json', 'tr3_nasdef_grk.json', 'tr4_sc_grk.json']:
        if not (STRONGS / f).exists():
            missing.append(f)
    if missing:
        print(f'WAITING: These files not yet ready: {missing}')
        print('Run this script after agents 1–4 have completed.')
        sys.exit(1)

    merge('heb', 'H', STRONGS / 'hebrew.json', STRONGS / 'heb_tr.json')
    merge('grk', 'G', STRONGS / 'greek.json',  STRONGS / 'grk_tr.json')
    print('\n=== Agent 5 complete — heb_tr.json and grk_tr.json ready ===')

if __name__ == '__main__':
    main()
