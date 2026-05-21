#!/usr/bin/env python3
"""
Fix 7: Build per-word verse index for TCL02 and KMEYA usage chips.
For each (Strong's ID, translation word) pair, lists verse refs where:
  1. The Strong's ID appears in the interlinear
  2. The translation word (or its stem) appears in the Turkish verse text
Output:
  data/strongs/tcl02_verse_index.json
  data/strongs/kmeya_verse_index.json
Format: {"G2316": {"Tanrı": ["MAT.1.1", "MAT.3.9", ...], "ilah": [...]}, ...}
"""
import json, re, unicodedata
from pathlib import Path
from collections import defaultdict

ROOT    = Path(__file__).parent.parent
IL_DIR  = ROOT / 'data/interlinear'
TR_DIR  = ROOT / 'data/translations'
STRONGS = ROOT / 'data/strongs'

# Use Unicode word chars to handle Ottoman diacritics (â, î, û) in KMEYA text
TR_TOKEN_RE = re.compile(r"[^\W\d_]+(?:'[^\W\d_]+)*", re.UNICODE)
MAX_REFS = 50   # max verse refs per (Strong's ID, word) pair

def tr_low(s: str) -> str:
    s = unicodedata.normalize('NFC', s)
    return s.replace('İ','i').replace('I','ı').lower()

def word_in_text(word: str, text: str) -> bool:
    if ' ' in word:
        # Bigram: both parts must appear as adjacent tokens (with prefix matching)
        parts = [tr_low(p) for p in word.split()]
        tokens = [tr_low(m.group(0)) for m in TR_TOKEN_RE.finditer(text)]
        for i in range(len(tokens) - len(parts) + 1):
            if all(
                tokens[i+j] == parts[j] or
                (tokens[i+j].startswith(parts[j]) and 1 <= len(tokens[i+j]) - len(parts[j]) <= 7)
                for j, _ in enumerate(parts)
            ):
                return True
        return False
    wl = tr_low(word)
    for m in TR_TOKEN_RE.finditer(text):
        tl = tr_low(m.group(0))
        if tl == wl:
            return True
        diff = len(tl) - len(wl)
        if 1 <= diff <= 7 and tl.startswith(wl):
            return True
    return False

def build_strong_to_verses() -> dict:
    """Returns {st_id: [ref, ...]} with refs as 'BOOK.ch.v' strings."""
    index: dict[str, list[str]] = defaultdict(list)
    for book_dir in sorted(IL_DIR.iterdir()):
        if not book_dir.is_dir():
            continue
        book = book_dir.name
        for ch_file in sorted(book_dir.glob('*.json'), key=lambda p: int(p.stem)):
            try:
                data = json.loads(ch_file.read_text('utf-8'))
                ch = data['c']
                for verse in data.get('verses', []):
                    v = verse['v']
                    ref = f'{book}.{ch}.{v}'
                    seen: set[str] = set()
                    for word in verse.get('words', []):
                        st = word.get('st')
                        if st and st not in seen:
                            index[st].append(ref)
                            seen.add(st)
            except Exception:
                pass
    return dict(index)

def load_verse_map(tr_dir: Path) -> dict:
    vm: dict[str, str] = {}
    if not tr_dir.exists():
        return vm
    for book_dir in tr_dir.iterdir():
        if not book_dir.is_dir():
            continue
        book = book_dir.name
        for ch_file in book_dir.glob('*.json'):
            try:
                ch = int(ch_file.stem)
                data = json.loads(ch_file.read_text('utf-8'))
                for item in data.get('content', []):
                    if 'v' in item and 'text' in item:
                        vm[f'{book}.{ch}.{item["v"]}'] = item['text']
            except Exception:
                pass
    return vm

def build_verse_index(
    strong_to_verses: dict,
    verse_map: dict,
    usage_json: dict,
) -> dict:
    result: dict[str, dict[str, list[str]]] = {}
    total = len(usage_json)
    for n, (st_id, words_str) in enumerate(usage_json.items()):
        words = [w.strip() for w in words_str.split(',') if w.strip()]
        refs = list(dict.fromkeys(strong_to_verses.get(st_id, [])))

        entry: dict[str, list[str]] = {}
        for word in words:
            matches = []
            for ref in refs:
                text = verse_map.get(ref)
                if text and word_in_text(word, text):
                    matches.append(ref)
                    if len(matches) >= MAX_REFS:
                        break
            if matches:
                entry[word] = matches

        if entry:
            result[st_id] = entry

        if (n + 1) % 500 == 0:
            print(f'  {n+1}/{total}…')

    return result

def main():
    print('Loading interlinear data…')
    strong_to_verses = build_strong_to_verses()
    print(f'  {len(strong_to_verses)} Strong\'s IDs found')

    print('Loading translation verse maps…')
    tcl02_vm = load_verse_map(TR_DIR / 'TCL02')
    kmeya_vm = load_verse_map(TR_DIR / 'KMEYA')
    print(f'  TCL02: {len(tcl02_vm)} verses  KMEYA: {len(kmeya_vm)} verses')

    tcl02_usage = json.loads((STRONGS / 'tcl02_usage.json').read_text('utf-8'))
    kmeya_usage = json.loads((STRONGS / 'kmeya_usage.json').read_text('utf-8'))

    print(f'Building TCL02 verse index ({len(tcl02_usage)} entries)…')
    tcl02_idx = build_verse_index(strong_to_verses, tcl02_vm, tcl02_usage)
    out = STRONGS / 'tcl02_verse_index.json'
    out.write_text(json.dumps(tcl02_idx, ensure_ascii=False, separators=(',', ':')), 'utf-8')
    print(f'  → {len(tcl02_idx)} entries  ({out.stat().st_size//1024} KB)')

    print(f'Building KMEYA verse index ({len(kmeya_usage)} entries)…')
    kmeya_idx = build_verse_index(strong_to_verses, kmeya_vm, kmeya_usage)
    out = STRONGS / 'kmeya_verse_index.json'
    out.write_text(json.dumps(kmeya_idx, ensure_ascii=False, separators=(',', ':')), 'utf-8')
    print(f'  → {len(kmeya_idx)} entries  ({out.stat().st_size//1024} KB)')

    print('=== Fix 7 complete ===')

if __name__ == '__main__':
    main()
