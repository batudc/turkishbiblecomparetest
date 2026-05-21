#!/usr/bin/env python3
"""
Fix 2: Apply parenthetical qualifier replacement map to ALL existing translations,
and apply numbered-list newline normalization to all fields.
Also post-processes sc (concordance) and nas_def fields.
Run after fix1.
"""
import json, re
from pathlib import Path

ROOT    = Path(__file__).parent.parent
STRONGS = ROOT / 'data/strongs'

QUAL_MAP = {
    r'\(figuratively\)':       '(mecazi olarak)',
    r'\(literally\)':          '(kelimenin tam anlamıyla)',
    r'\(abstractly\)':         '(soyut olarak)',
    r'\(concretely\)':         '(somut olarak)',
    r'\(by implication\)':     '(dolaylı olarak)',
    r'\(by analogy\)':         '(analoji yoluyla)',
    r'\(by extension\)':       '(geniş anlamda)',
    r'\(by Hebraism\)':        '(İbranice deyim)',
    r'\(by Hebraicism\)':      '(İbranice deyim)',
    r'\(specially\)':          '(özellikle)',
    r'\(especially\)':         '(bilhassa)',
    r'\(properly\)':           '(asıl anlamıyla)',
    r'\(technically\)':        '(teknik olarak)',
    r'\(genitive\)':           '(iyelik)',
    r'\(plural\)':             '(çoğul)',
    r'\(singular\)':           '(tekil)',
    r'\(causative\)':          '(sebep bildiren)',
    r'\(reflexive\)':          '(dönüşlü)',
    r'\(passive\)':            '(edilgen)',
    r'\(active\)':             '(etken)',
    r'\(in a good sense\)':    '(olumlu anlamda)',
    r'\(in a bad sense\)':     '(olumsuz anlamda)',
    r'\(euphemistically\)':    '(örtmeceli olarak)',
    r'\(by euphemism\)':       '(örtmece)',
    r'\(intensive\)':          '(pekiştirmeli)',
    r'\(causally\)':           '(nedensel olarak)',
    r'\(denominative\)':       '(isimden türetilmiş)',
    r'\(transitively\)':       '(geçişli)',
    r'\(intransitively\)':     '(geçişsiz)',
    r'\(relatively\)':         '(göreli olarak)',
    r'\(specifically\)':       '(özgül olarak)',
    r'\(collectively\)':       '(toplu olarak)',
    r'\(comparatively\)':      '(karşılaştırmalı)',
    r'\(i\.e\.\)':             '(yani)',
    r'\(e\.g\.\)':             '(örneğin)',
}

# Format numbered list: "1. foo2. bar" → "1. foo\n2. bar"
NUM_RE = re.compile(r'(?<!\d)(\d+)\.\s*(?=[A-ZÇĞIİÖŞÜa-züöşçğıiI(])')


def apply_fixes(text: str) -> str:
    if not text: return text
    for pat, rep in QUAL_MAP.items():
        text = re.sub(pat, rep, text, flags=re.IGNORECASE)
    # Add line breaks between numbered items
    text = NUM_RE.sub(r'\n\1. ', text).strip()
    return text


def process(fname: str):
    path = STRONGS / fname
    entries = json.loads(path.read_text('utf-8'))
    changed = 0
    for e in entries:
        for field in ['def', 'nas_def', 'sc', 'origin']:
            old = e.get(field, '')
            new = apply_fixes(old)
            if new != old:
                e[field] = new
                changed += 1
    path.write_text(json.dumps(entries, ensure_ascii=False, separators=(',', ':')), 'utf-8')
    print(f'{fname}: {changed} fields updated')


def main():
    process('heb_tr.json')
    process('grk_tr.json')
    print('=== Fix 2 complete ===')

if __name__ == '__main__':
    main()
