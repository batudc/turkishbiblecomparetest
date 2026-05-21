#!/usr/bin/env python3
"""
Fix 3: Rebuild TCL02 and KMEYA usage indexes with stricter filtering.
- Only keep words appearing in ≥3 distinct verses (raises signal/noise ratio)
- Exclude Turkish function words more aggressively
- Exclude words < 4 chars (removes most noise)
- Cap output at 6 words per entry (most meaningful)
"""
import json, re, math
from pathlib import Path
from collections import defaultdict, Counter

ROOT    = Path(__file__).parent.parent
IL_DIR  = ROOT / 'data/interlinear'
TR_DIR  = ROOT / 'data/translations'
STRONGS = ROOT / 'data/strongs'

MIN_VERSE_COUNT = 3   # word must appear in this many distinct verses
MAX_WORDS       = 6   # max output words per Strong's number

TR_STOP = {
    've','ile','bir','bu','o','da','de','ki','ama','için','gibi','olan','oldu','olur',
    'var','yok','değil','her','hiç','ne','hem','şimdi','ise','ben','sen','biz','siz',
    'onlar','den','dan','nin','nın','nun','nün','bana','sana','ona','bize','size',
    'onlara','çünkü','ancak','fakat','lakin','veya','yahut','yani','kadar','beri',
    'önce','sonra','zira','hatta','dahi','bile','eğer','sadece','yalnız','öyle',
    'böyle','şöyle','bunlar','şunlar','artık','zaten','pek','çok','daha','her',
    'tüm','bütün','hepsi','bazı','ayrıca','henüz','belki','özellikle','gerçi',
    'aslında','üstelik','doğru','yüzünden','bakımından','göre','karşı','rağmen',
    'dedi','diyor','der','etti','eder','olarak','olan','olduğu','olup','etmek',
    'kişi','şey','adam','zaman','yer','gün','yıl','gibi','kadar','diye','diye',
}

_punct = re.compile(r"[^\w\s\-']", re.UNICODE)

def tokenize(text: str) -> list[str]:
    text = _punct.sub(' ', text).lower()
    return [w for w in text.split() if len(w) >= 4 and w not in TR_STOP and not w.isdigit()]


def load_verse_map(tr_dir: Path) -> dict:
    vm = {}
    if not tr_dir.exists(): return vm
    for book_dir in tr_dir.iterdir():
        if not book_dir.is_dir(): continue
        book = book_dir.name
        for ch_file in book_dir.glob('*.json'):
            try:
                ch   = int(ch_file.stem)
                data = json.loads(ch_file.read_text('utf-8'))
                for item in data.get('content', []):
                    if 'v' in item and 'text' in item:
                        vm[(book, ch, item['v'])] = item['text']
            except Exception:
                pass
    return vm


def build_strong_to_verses() -> dict:
    index = defaultdict(list)
    for book_dir in IL_DIR.iterdir():
        if not book_dir.is_dir(): continue
        book = book_dir.name
        for ch_file in book_dir.glob('*.json'):
            try:
                data = json.loads(ch_file.read_text('utf-8'))
                ch   = data['c']
                for verse in data.get('verses', []):
                    v = verse['v']
                    seen_in_verse = set()
                    for word in verse.get('words', []):
                        st = word.get('st')
                        if st and st not in seen_in_verse:
                            index[st].append((book, ch, v))
                            seen_in_verse.add(st)
            except Exception:
                pass
    return dict(index)


def compute_usage(strong_to_verses: dict, verse_map: dict, label: str) -> dict:
    print(f'  Computing {label}…')

    corpus_counts: Counter = Counter()
    verse_total = 0
    for text in verse_map.values():
        corpus_counts.update(tokenize(text))
        verse_total += 1
    corpus_total = sum(corpus_counts.values())

    usage = {}
    for st_id, verse_refs in strong_to_verses.items():
        # Deduplicate verse refs
        unique_refs = list(dict.fromkeys(verse_refs))
        verse_texts = [verse_map[r] for r in unique_refs if r in verse_map]
        if len(verse_texts) < 1: continue

        # Count words + track per-verse occurrence
        local_counts: Counter = Counter()
        word_verse_count: Counter = Counter()   # how many distinct verses each word appears in
        for text in verse_texts:
            tokens = set(tokenize(text))
            for w in tokens:
                word_verse_count[w] += 1
            local_counts.update(tokenize(text))

        if not local_counts: continue

        local_total = sum(local_counts.values())
        scored = []
        for word, cnt in local_counts.items():
            # Must appear in at least MIN_VERSE_COUNT distinct verses
            if word_verse_count[word] < MIN_VERSE_COUNT: continue
            local_rate  = cnt / max(local_total, 1)
            corpus_rate = corpus_counts.get(word, 0) / max(corpus_total, 1)
            if corpus_rate == 0: corpus_rate = 1 / corpus_total
            score = (local_rate / corpus_rate) * math.log1p(word_verse_count[word])
            scored.append((score, word))

        scored.sort(reverse=True)
        top = [w for _, w in scored[:MAX_WORDS]]
        if top:
            usage[st_id] = ', '.join(top)

    print(f'  → {len(usage)} entries')
    return usage


def main():
    print('Loading data…')
    strong_to_verses = build_strong_to_verses()
    print(f'  {len(strong_to_verses)} Strong\'s IDs')

    tcl02_vm = load_verse_map(TR_DIR / 'TCL02')
    kmeya_vm = load_verse_map(TR_DIR / 'KMEYA')
    print(f'  TCL02: {len(tcl02_vm)} verses, KMEYA: {len(kmeya_vm)} verses')

    tcl02_usage = compute_usage(strong_to_verses, tcl02_vm, 'TCL02')
    kmeya_usage = compute_usage(strong_to_verses, kmeya_vm, 'KMEYA')

    (STRONGS / 'tcl02_usage.json').write_text(
        json.dumps(tcl02_usage, ensure_ascii=False, separators=(',', ':')), 'utf-8')
    (STRONGS / 'kmeya_usage.json').write_text(
        json.dumps(kmeya_usage, ensure_ascii=False, separators=(',', ':')), 'utf-8')

    print('=== Fix 3 complete ===')

if __name__ == '__main__':
    main()
