#!/usr/bin/env python3
"""
Agent 6: Build TCL02 and KMEYA usage indexes for all Strong's numbers.

For each Strong's number, find all verses where it appears (from interlinear data),
fetch those verse texts from TCL02 and KMEYA, then compute which Turkish words are
distinctively used to translate that word (TF-IDF style relative frequency).

Outputs:
  data/strongs/tcl02_usage.json  {"H430": "Tanrı, Allah, tanrılar", "G2316": "Tanrı, tanrı", ...}
  data/strongs/kmeya_usage.json  same format

Also builds a verse reference index (internal, not saved):
  strong_num → [(book, chapter, verse), ...]

Usage:
    python3 pipeline/tr6_build_usage_index.py
"""
import json, re, math
from pathlib import Path
from collections import defaultdict, Counter

ROOT    = Path(__file__).parent.parent
IL_DIR  = ROOT / 'data/interlinear'   # interlinear chapters
TR_DIR  = ROOT / 'data/translations'
STRONGS = ROOT / 'data/strongs'

TCL02_DIR = TR_DIR / 'TCL02'
KMEYA_DIR = TR_DIR / 'KMEYA'

# Turkish stop words (common function words that appear in nearly every verse)
TR_STOP = {
    've', 'ile', 'bir', 'bu', 'o', 'da', 'de', 'ki', 'ama', 'için', 'gibi',
    'olan', 'olan', 'diye', 'oldu', 'olur', 'var', 'yok', 'değil', 'ya',
    'her', 'hiç', 'ne', 'hem', 'şimdi', 'ise', 'onun', 'benim', 'senin',
    'bizim', 'sizin', 'onların', 'bana', 'sana', 'ona', 'bize', 'size',
    'onlara', 'ben', 'sen', 'biz', 'siz', 'onlar', 'den', 'dan', 'ten',
    'tan', 'nin', 'nın', 'nun', 'nün', 'in', 'ın', 'un', 'ün', 'e', 'a',
    'i', 'ı', 'u', 'ü', 'te', 'ta', 'de', 'da', 'le', 'la', 'den', 'dan',
    'ki', 'mi', 'mı', 'mu', 'mü', 'di', 'dı', 'du', 'dü', 'ti', 'tı',
    'ondan', 'benden', 'senden', 'bizden', 'sizden', 'onlardan', 'ben\'im',
    'sen\'in', 'o\'nun', 'biz\'im', 'siz\'in', 'tanrı', 'rab',  # exclude too-common theological terms for now
    'dedi', 'dedi', 'söyledi', 'etti', 'etmek', 'olan', 'şey', 'kişi',
    'çünkü', 'ancak', 'fakat', 'lakin', 'veya', 'yahut', 'yani', 'ile',
    'kadar', 'beri', 'önce', 'sonra', 'zira', 'hatta', 'dahi', 'bile',
    'ise', 'eğer', 'de', 'da', 'ki', 'hem', 'ne', 'ya', 'ya', 'gerek',
    'yoksa', 'belki', 'sadece', 'yalnız', 'öyle', 'böyle', 'şöyle',
    'bu', 'şu', 'o', 'bunlar', 'şunlar', 'onlar', 'bunu', 'şunu', 'onu',
    'bunun', 'şunun', 'onun', 'bunda', 'şunda', 'onda', 'bundan', 'şundan',
    'bunla', 'şunla', 'onla', 'artık', 'zaten', 'bile', 'hep', 'hiç',
    'pek', 'çok', 'az', 'daha', 'en', 'epey', 'biraz', 'tam', 'tam',
    'yeni', 'eski', 'büyük', 'küçük', 'uzun', 'kısa', 'iyi', 'kötü',
    'ilk', 'son', 'başka', 'diğer', 'tüm', 'bütün', 'hepsi', 'bazı',
}

_punct = re.compile(r"[^\w\s'-]", re.UNICODE)

def tokenize(text: str) -> list[str]:
    """Tokenize Turkish text into words, removing punctuation."""
    text = _punct.sub(' ', text).lower()
    return [w for w in text.split() if len(w) > 2 and w not in TR_STOP]


def load_verse_map(tr_dir: Path) -> dict[tuple, str]:
    """Load all verse texts from a translation directory.
    Returns {(book, chapter, verse): text}"""
    vm = {}
    if not tr_dir.exists():
        print(f'  WARNING: {tr_dir} not found')
        return vm
    for book_dir in tr_dir.iterdir():
        if not book_dir.is_dir(): continue
        book = book_dir.name
        for ch_file in book_dir.glob('*.json'):
            try:
                ch = int(ch_file.stem)
                data = json.loads(ch_file.read_text('utf-8'))
                for item in data.get('content', []):
                    if 'v' in item and 'text' in item:
                        vm[(book, ch, item['v'])] = item['text']
            except Exception:
                pass
    return vm


def build_strong_to_verses() -> dict[str, list[tuple]]:
    """Scan all interlinear files → {strong_id: [(book, ch, verse), ...]}"""
    index = defaultdict(list)
    for book_dir in IL_DIR.iterdir():
        if not book_dir.is_dir(): continue
        book = book_dir.name
        for ch_file in sorted(book_dir.glob('*.json'), key=lambda p: int(p.stem)):
            try:
                data = json.loads(ch_file.read_text('utf-8'))
                ch   = data['c']
                for verse in data.get('verses', []):
                    v = verse['v']
                    for word in verse.get('words', []):
                        st = word.get('st')
                        if st:
                            index[st].append((book, ch, v))
            except Exception:
                pass
    print(f'  Strong\'s index: {len(index)} unique IDs, '
          f'{sum(len(v) for v in index.values())} total verse refs')
    return dict(index)


def compute_usage(strong_to_verses: dict, verse_map: dict, label: str) -> dict[str, str]:
    """For each Strong's number, find distinctive Turkish words in its verse set."""
    print(f'\n  Computing {label} usage…')

    # Step 1: Count each word across ALL verses (corpus frequency)
    corpus_word_counts: Counter = Counter()
    total_verses = 0
    for text in verse_map.values():
        tokens = tokenize(text)
        corpus_word_counts.update(tokens)
        total_verses += 1

    corpus_total = sum(corpus_word_counts.values())
    print(f'  Corpus: {total_verses} verses, {len(corpus_word_counts)} unique words')

    # Step 2: For each Strong's number, compute TF-IDF-style score
    usage = {}
    for st_id, verse_refs in strong_to_verses.items():
        # Collect verse texts
        verse_texts = []
        seen = set()
        for ref in verse_refs:
            if ref not in seen and ref in verse_map:
                verse_texts.append(verse_map[ref])
                seen.add(ref)

        if not verse_texts:
            continue

        # Word frequency in this word's verses
        local_counts: Counter = Counter()
        for text in verse_texts:
            local_counts.update(tokenize(text))

        if not local_counts:
            continue

        n_verses = len(verse_texts)

        # Score: relative enrichment = local_rate / corpus_rate
        # local_rate  = count_in_verse_set / n_verse_set_words
        # corpus_rate = count_in_corpus / corpus_total
        local_total = sum(local_counts.values())
        scored = []
        for word, cnt in local_counts.items():
            if cnt < 1: continue
            local_rate  = cnt / max(local_total, 1)
            corpus_rate = corpus_word_counts.get(word, 0) / max(corpus_total, 1)
            if corpus_rate == 0: corpus_rate = 1 / corpus_total
            score = local_rate / corpus_rate
            # Boost by raw count (prefer common translations)
            scored.append((score * math.log1p(cnt), word, cnt))

        scored.sort(reverse=True)
        # Take top 8 most distinctive words
        top = [w for _, w, _ in scored[:8]]
        if top:
            usage[st_id] = ', '.join(top)

    print(f'  Generated usage for {len(usage)} Strong\'s numbers')
    return usage


def main():
    STRONGS.mkdir(parents=True, exist_ok=True)

    print('Building Strong\'s → verse reference index…')
    strong_to_verses = build_strong_to_verses()

    print('\nLoading TCL02 verse texts…')
    tcl02_vm = load_verse_map(TCL02_DIR)
    print(f'  TCL02: {len(tcl02_vm)} verses loaded')

    print('Loading KMEYA verse texts…')
    kmeya_vm = load_verse_map(KMEYA_DIR)
    print(f'  KMEYA: {len(kmeya_vm)} verses loaded')

    tcl02_usage = compute_usage(strong_to_verses, tcl02_vm, 'TCL02')
    kmeya_usage = compute_usage(strong_to_verses, kmeya_vm, 'KMEYA')

    out_tcl = STRONGS / 'tcl02_usage.json'
    out_kme = STRONGS / 'kmeya_usage.json'
    out_tcl.write_text(json.dumps(tcl02_usage, ensure_ascii=False, separators=(',', ':')), 'utf-8')
    out_kme.write_text(json.dumps(kmeya_usage, ensure_ascii=False, separators=(',', ':')), 'utf-8')

    print(f'\nSaved:')
    print(f'  {out_tcl} ({len(tcl02_usage)} entries)')
    print(f'  {out_kme} ({len(kmeya_usage)} entries)')
    print('\n=== Agent 6 complete ===')


if __name__ == '__main__':
    main()
