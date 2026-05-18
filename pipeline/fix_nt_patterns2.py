#!/usr/bin/env python3
"""Second batch of targeted OCR fixes for HKTN New Testament.

Covers:
  - MAT 4:10  Ardıma / Rabb'e
  - MAT 4:19  takılın
  - MAT 4:21  Sonra
  - MAT 4:24  Suriye'nin
  - MRK/LUK/JHN/ACT  Son ra → Sonra (11 verses)
  - JHN 21:6  sahilin
  - ACT 4:32  malından
  - ACT 4:34  bulunmuyordu / tarla
  - REV 22:17 gelin (bride split as 'ge lin')
  - REV 22:21 Rabbimiz / tümünüzle
  - 2JN 22:17 gelin / susayan gelsin
  - 2JN 22:21 Rabbimiz / tümünüzle

Usage:
    python3 pipeline/fix_nt_patterns2.py            # apply in-place
    python3 pipeline/fix_nt_patterns2.py --dry-run  # preview only
"""

import json, sys
from pathlib import Path

DRY_RUN  = '--dry-run' in sys.argv
HKTN_DIR = Path(__file__).parent.parent / 'data' / 'translations' / 'HKTN'

RSQ = '’'  # U+2019 RIGHT SINGLE QUOTATION MARK

# (book, chapter, verse) -> ordered list of (find, replace) pairs
PATCHES: dict = {
    # ── MAT 4 ─────────────────────────────────────────────────────────────────
    ('MAT',  4, 10): [('Ardı ma', 'Ardıma'),
                      ('Rabb' + RSQ + ' e', 'Rabb' + RSQ + 'e')],
    ('MAT',  4, 19): [('takı lın', 'takılın')],
    ('MAT',  4, 21): [('Son ra', 'Sonra')],
    ('MAT',  4, 24): [('Suriye' + RSQ + '- nin', 'Suriye' + RSQ + 'nin')],
    # ── Son ra → Sonra ────────────────────────────────────────────────────────
    ('MRK',  3, 13): [('Son ra', 'Sonra')],
    ('MRK',  7, 14): [('Son ra', 'Sonra')],
    ('LUK',  6,  9): [('Son ra', 'Sonra')],
    ('LUK',  6, 17): [('Son ra', 'Sonra')],
    ('LUK',  7, 14): [('Son ra', 'Sonra')],
    ('LUK', 12, 54): [('Son ra', 'Sonra')],
    ('LUK', 22, 36): [('Son ra', 'Sonra')],
    ('JHN', 19, 39): [('Son ra', 'Sonra')],
    ('ACT',  7, 14): [('Son ra', 'Sonra')],
    ('ACT', 25, 23): [('Son ra', 'Sonra')],
    ('ACT', 27, 36): [('Son ra', 'Sonra')],
    # ── other specific fixes ──────────────────────────────────────────────────
    ('JHN', 21,  6): [('sahi lin', 'sahilin')],
    ('ACT',  4, 32): [('ma lın dan', 'malından')],
    ('ACT',  4, 34): [('bu lun mu yor du', 'bulunmuyordu'),
                      ('tar la', 'tarla')],
    # REV 22:17 — 'ge lin' is the word 'gelin' (bride), OCR split
    ('REV', 22, 17): [('ge lin', 'gelin')],
    # REV 22:21 — split words in closing doxology
    ('REV', 22, 21): [('Rabbi miz', 'Rabbimiz'),
                      ('t\xfc m\xfc n\xfcz le', 't\xfcm\xfcn\xfczle')],
    # 2JN ch.22 files (Revelation content landed in wrong folder — do not delete)
    ('2JN', 22, 17): [('ge lin', 'gelin'),
                      ('su sayangelsin', 'susayan gelsin')],
    ('2JN', 22, 21): [('Rabbi miz', 'Rabbimiz'),
                      ('t\xfc m\xfc n\xfcz le', 't\xfcm\xfcn\xfczle')],
}


def apply_patches(text: str, patches: list) -> str:
    for old, new in patches:
        text = text.replace(old, new)
    return text


def main():
    total_changed = 0
    log: list = []

    for (book, ch_num, v_num), patches in PATCHES.items():
        ch_file = HKTN_DIR / book / f'{ch_num}.json'
        if not ch_file.exists():
            print(f'  MISSING: {book}/{ch_num}.json')
            continue
        data = json.loads(ch_file.read_text(encoding='utf-8'))
        changed = False

        for item in data.get('content', []):
            if item.get('v') != v_num or 'text' not in item:
                continue
            original = item['text']
            text = apply_patches(original, patches)
            if text != original:
                item['text'] = text
                changed = True
                total_changed += 1
                log.append(
                    f'{book} {ch_num}:{v_num}\n'
                    f'  BEFORE: {original}\n'
                    f'  AFTER:  {text}'
                )

        if changed and not DRY_RUN:
            ch_file.write_text(
                json.dumps(data, ensure_ascii=False, indent=2),
                encoding='utf-8'
            )

    prefix = '[DRY RUN] ' if DRY_RUN else ''
    print(f'{prefix}Fixed {total_changed} verses.\n')
    for entry in log:
        print(entry)
        print()


if __name__ == '__main__':
    main()
