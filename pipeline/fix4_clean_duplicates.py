#!/usr/bin/env python3
"""
Fix 4: Remove duplicate words in comma-separated lists within definition fields.
  "Kurtarıcı, Kurtarıcı"       → "Kurtarıcı"
  "Meshedilmiş Olan, Mesih, Mesih" → "Meshedilmiş Olan, Mesih"
Works line-by-line so numbered items stay intact.
"""
import json, re
from pathlib import Path

ROOT    = Path(__file__).parent.parent
STRONGS = ROOT / 'data/strongs'

def _bare(text: str) -> str:
    """Strip leading number marker + parenthetical qualifiers, then normalize.
    '4. (işlev itibariyle) (akraba) Kurtarıcı' → 'kurtarıcı'
    """
    t = re.sub(r'^\d+\.\s*', '', text.strip())   # remove "4. " prefix
    t = re.sub(r'\([^)]*\)\s*', '', t)            # remove (qualifier) groups
    return re.sub(r'[^\w]', '', t, flags=re.UNICODE).lower()

def dedup_comma_segment(segment: str) -> str:
    """Remove duplicate tokens in one comma-separated segment.
    Compares using the bare (stripped) form so 'N. (qual) Word, Word' deduplicates.
    """
    if ',' not in segment:
        return segment
    parts = [p.strip() for p in segment.split(',')]
    seen, unique = set(), []
    for p in parts:
        k = _bare(p)
        if k and k not in seen:
            seen.add(k)
            unique.append(p)
        elif not k:
            unique.append(p)   # keep punctuation-only pieces as-is
    return ', '.join(unique)

def clean_field(text: str) -> str:
    if not text:
        return text
    # Process line by line to preserve newline structure
    lines = text.split('\n')
    return '\n'.join(dedup_comma_segment(line) for line in lines)

def process(fname: str):
    path = STRONGS / fname
    entries = json.loads(path.read_text('utf-8'))
    changed = 0
    for e in entries:
        for field in ('def', 'nas_def', 'sc'):
            old = e.get(field, '')
            new = clean_field(old)
            if new != old:
                e[field] = new
                changed += 1
    path.write_text(
        json.dumps(entries, ensure_ascii=False, separators=(',', ':')), 'utf-8')
    print(f'{fname}: {changed} fields cleaned')

def main():
    process('heb_tr.json')
    process('grk_tr.json')
    print('=== Fix 4 complete ===')

if __name__ == '__main__':
    main()
