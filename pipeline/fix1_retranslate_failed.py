#!/usr/bin/env python3
"""
Fix 1: Re-translate definitions that Google Translate left in English.
Strategy: strip numbered list markers, translate each sentence/item separately,
reassemble. Also apply a parenthetical qualifier replacement map.
"""
import json, re, time
from pathlib import Path
from deep_translator import GoogleTranslator

ROOT    = Path(__file__).parent.parent
STRONGS = ROOT / 'data/strongs'
DELAY   = 0.2

tr = GoogleTranslator(source='en', target='tr')
cache = {}

# Parenthetical qualifier replacement map (applied AFTER translation as cleanup)
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
    r'\(by implication of\)':  '(ima yoluyla)',
    r'\(i\.e\.\)':             '(yani)',
    r'\(e\.g\.\)':             '(örneğin)',
}

def apply_qual_map(text: str) -> str:
    for pat, rep in QUAL_MAP.items():
        text = re.sub(pat, rep, text, flags=re.IGNORECASE)
    return text


def split_numbered(text: str) -> list[tuple[str, str]]:
    """Split '1. foo2. bar3. baz' into [('1.','foo'),('2.','bar'),('3.','baz')]"""
    parts = re.split(r'(\d+\.)', text)
    result = []
    i = 0
    while i < len(parts):
        if re.match(r'\d+\.', parts[i].strip()):
            num = parts[i].strip()
            body = parts[i+1].strip() if i+1 < len(parts) else ''
            result.append((num, body))
            i += 2
        else:
            if parts[i].strip():
                result.append(('', parts[i].strip()))
            i += 1
    return result if result else [('', text)]


def translate_text(text: str) -> str:
    if not text or len(text) < 4: return text
    t = text.strip()
    if t in cache: return cache[t]
    # Try translation
    for attempt in range(3):
        try:
            r = tr.translate(t[:4800]) or t
            time.sleep(DELAY)
            result = apply_qual_map(r)
            cache[t] = result
            return result
        except Exception as e:
            time.sleep(2 ** attempt)
    cache[t] = t
    return t


def smart_translate(text: str) -> str:
    """Translate numbered definition preserving structure."""
    if not text: return text
    parts = split_numbered(text)
    translated_parts = []
    for num, body in parts:
        tr_body = translate_text(body)
        translated_parts.append((num + ' ' + tr_body).strip())
    return ' '.join(translated_parts)


def process(tr_file: Path, src_file: Path, fail_ids: list, prefix: str):
    print(f'\n=== Re-translating {prefix} ({len(fail_ids)} entries) ===')
    entries = json.loads(tr_file.read_text('utf-8'))
    src_map = {e['id']: e for e in json.loads(src_file.read_text('utf-8')) if 'error' not in e}
    id_set  = set(fail_ids)
    entry_map = {e['id']: e for e in entries}

    done = 0
    for eid in fail_ids:
        e   = entry_map.get(eid)
        src = src_map.get(eid, {})
        if not e: continue

        # Get best source text (prefer short_def, fall back to nas_def)
        src_def = src.get('short_def') or src.get('nas_def') or ''
        if src_def:
            new_def = smart_translate(src_def)
            e['def'] = new_def

        done += 1
        if done % 50 == 0:
            print(f'  {done}/{len(fail_ids)} ({done/len(fail_ids)*100:.1f}%)')
            tr_file.write_text(json.dumps(entries, ensure_ascii=False, separators=(',', ':')), 'utf-8')

    tr_file.write_text(json.dumps(entries, ensure_ascii=False, separators=(',', ':')), 'utf-8')
    print(f'  Done. {done} re-translated.')


def main():
    ids = json.loads(Path('/tmp/retranslate_ids.json').read_text())
    process(STRONGS / 'heb_tr.json', STRONGS / 'hebrew.json', ids['heb'], 'Hebrew')
    process(STRONGS / 'grk_tr.json', STRONGS / 'greek.json',  ids['grk'], 'Greek')
    print('\n=== Fix 1 complete ===')

if __name__ == '__main__':
    main()
