#!/usr/bin/env python3
"""Strip embedded cross-reference annotation blocks from HKTN NT verses,
and apply any remaining word-split fixes found during the full audit.

Annotation blocks are bibliographic/commentary tails that were OCR-captured
from the source book's footnote sections and landed in the verse text field.
The verse text ends at the given sentinel; everything after is stripped.

Usage:
    python3 pipeline/fix_nt_annotations.py            # apply in-place
    python3 pipeline/fix_nt_annotations.py --dry-run  # preview only
"""

import json, sys
from pathlib import Path

DRY_RUN  = '--dry-run' in sys.argv
HKTN_DIR = Path(__file__).parent.parent / 'data' / 'translations' / 'HKTN'

RSQ = '’'  # RIGHT SINGLE QUOTATION MARK

# Verses whose trailing annotation block must be stripped.
# Text is truncated to text[:sentinel_end].
STRIP_AFTER: dict = {
    ('ACT',  2, 47): 'topluyordu.',
    ('LUK', 15, 32): 'dedi.',
    ('2TH',  2, 17): 'kılsın.',
    ('2CO',  7, 16): 'sevinçliyim.',
    ('1PE',  5, 14): 'Âmin.',
    ('REV',  3, 22): 'duysun.',
    ('REV', 22, 21): 'Âmin.',
    ('2JN', 22, 21): 'Âmin.',
}

# Additional word-split / suffix-space patches
PATCHES: dict = {
    # 1PE 5:14 — apostrophe-space suffix split
    ('1PE', 5, 14): [('Mesih' + RSQ + ' de', 'Mesih' + RSQ + 'de')],
}


def strip_after(text: str, sentinel: str) -> str:
    idx = text.find(sentinel)
    if idx == -1:
        return text
    return text[:idx + len(sentinel)]


def apply_patches(text: str, patches: list) -> str:
    for old, new in patches:
        text = text.replace(old, new)
    return text


def process(book: str, ch_num: int, v_num: int, item: dict) -> str:
    text = item['text']
    key = (book, ch_num, v_num)

    if key in PATCHES:
        text = apply_patches(text, PATCHES[key])

    if key in STRIP_AFTER:
        text = strip_after(text, STRIP_AFTER[key])

    return text


def main():
    all_keys = set(STRIP_AFTER) | set(PATCHES)
    total_changed = 0
    log: list = []

    for (book, ch_num, v_num) in all_keys:
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
            text = process(book, ch_num, v_num, item)
            if text != original:
                item['text'] = text
                changed = True
                total_changed += 1
                log.append(
                    f'{book} {ch_num}:{v_num}\n'
                    f'  BEFORE: {original[:120]}{"..." if len(original) > 120 else ""}\n'
                    f'  AFTER:  {text[:120]}{"..." if len(text) > 120 else ""}'
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
