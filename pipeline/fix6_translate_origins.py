#!/usr/bin/env python3
"""
Fix 6: Translate English origin fields to Turkish.
Uses GoogleTranslator; protects Strong's IDs (G/H numbers) with placeholders.
Greek/Hebrew characters are preserved automatically by the translator.
"""
import json, re, time
from pathlib import Path
from deep_translator import GoogleTranslator

ROOT    = Path(__file__).parent.parent
STRONGS = ROOT / 'data/strongs'

EN_MARKERS = [
    'from ', 'primary ', 'of hebrew', 'of latin', 'of aramaic',
    'apparently', 'meaning ', 'unused root', 'probably ', 'a form of',
    'strengthened', 'prolonged', 'derivative', 'compound', 'particle',
    'intensive', 'reduplication', 'contraction', 'cognate', 'akin',
    'perhaps', 'a root', 'a word', 'denoting',
]

def needs_translation(origin: str) -> bool:
    lo = origin.lower()
    return any(m in lo for m in EN_MARKERS)

def translate_origin(origin: str, translator) -> str:
    if not needs_translation(origin):
        return origin
    # Protect Strong's IDs (they get Turkish suffixes added automatically, which is fine)
    ids = {}
    counter = [0]
    def protect(m):
        k = f'STID{counter[0]}'
        ids[k] = m.group(0)
        counter[0] += 1
        return k
    tmp = re.sub(r'[GH]\d+', protect, origin)
    try:
        translated = translator.translate(tmp)
        if not translated:
            return origin
        for k, v in ids.items():
            translated = translated.replace(k, v)
        return translated
    except Exception:
        return origin

def process(fname: str):
    path = STRONGS / fname
    entries = json.loads(path.read_text('utf-8'))
    translator = GoogleTranslator(source='en', target='tr')

    to_fix = [(i, e) for i, e in enumerate(entries)
              if e.get('origin') and needs_translation(e['origin'])]
    print(f'{fname}: {len(to_fix)} origin fields to translate')

    changed = 0
    for idx, (i, e) in enumerate(to_fix):
        old = e['origin']
        new = translate_origin(old, translator)
        if new != old:
            entries[i]['origin'] = new
            changed += 1
        if (idx + 1) % 100 == 0:
            print(f'  {idx+1}/{len(to_fix)} ({changed} changed)...')
            time.sleep(0.5)

    path.write_text(
        json.dumps(entries, ensure_ascii=False, separators=(',', ':')), 'utf-8')
    print(f'{fname}: done — {changed}/{len(to_fix)} fields updated')

def main():
    process('grk_tr.json')
    process('heb_tr.json')
    print('=== Fix 6 complete ===')

if __name__ == '__main__':
    main()
