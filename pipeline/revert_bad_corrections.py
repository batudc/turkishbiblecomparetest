#!/usr/bin/env python3
"""
revert_bad_corrections.py — Revert false-positive ?→ğ corrections.

Pattern: word ends with ğ followed immediately by a closing quote/apostrophe
and then NOT followed by a Turkish letter (i.e. it was a sentence-end question
mark + closing quote, not a mid-word ğ).

Run once to fix all affected YYY1987 source files.
"""

import re, json
from pathlib import Path

PROJECT  = Path(__file__).resolve().parent.parent
YYY_DIR  = PROJECT / 'data' / 'translations' / 'YYY1987'

# Pattern: lowercase letter(s) + ğ + closing quote + (space | end | punctuation)
# The ğ was incorrectly substituted from ? in positions like "bilmiyorğ'" or "edeceğizğ'"
_BAD_PATTERN = re.compile(
    r'([a-zçğışöüA-ZÇĞİÖŞÜ]+)'   # word chars before the bad ğ
    r'ğ'                            # the falsely-inserted ğ
    r"([''''‘’‚‛])"  # closing quote/apostrophe
    r'(?![a-zA-ZÇçĞğİıÖöŞşÜü])'   # NOT followed by a Turkish letter
)

def _revert_text(text):
    """Replace bad [word]ğ' → [word]?' in the given text."""
    def _replace(m):
        return m.group(1) + '?' + m.group(2)
    new = _BAD_PATTERN.sub(_replace, text)
    return new, new != text

def _process_file(json_path):
    data = json.loads(json_path.read_text(encoding='utf-8'))
    changed = False
    for verse in data.get('content', []):
        text = verse.get('text', '')
        new_text, was_changed = _revert_text(text)
        if was_changed:
            print(f'  Reverted in {json_path.parent.name} {json_path.stem}:{verse["v"]}')
            print(f'    Before: {text}')
            print(f'    After:  {new_text}')
            verse['text'] = new_text
            changed = True
    if changed:
        json_path.write_text(
            json.dumps(data, ensure_ascii=False, indent=2),
            encoding='utf-8',
        )
    return changed

total = 0
for book_dir in sorted(YYY_DIR.iterdir()):
    if not book_dir.is_dir():
        continue
    for json_file in sorted(book_dir.glob('*.json')):
        if _process_file(json_file):
            total += 1

print(f'\nDone. {total} file(s) had bad ğ\' reversions applied.')
