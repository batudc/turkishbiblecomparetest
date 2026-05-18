#!/usr/bin/env python3
"""Fix OCR word-split artifacts in HKTN.

Two-oracle strategy:
  1. External refs (TCL02, KMEYA, BGT) — verse-level word sets.
  2. HKTN self-corpus — all tokens ≥ MIN_CORPUS chars from the already-extracted text,
     giving us HKTN-specific vocabulary (proper nouns, theological terms, inflected forms).

Join rule:  only tokens NOT in the oracle are candidates for joining.
            (tokens already in the oracle are kept — no "next-is-fragment" heuristic
             because that causes cascade false-joins.)

Accept a join if the combined form is:
  (a) in the oracle (direct match), OR
  (b) a suffix-inflection of a corpus word: some prefix of the candidate
      of length ≥ MIN_PREFIX is in the corpus AND the remaining suffix
      has ≤ MAX_SUFFIX chars.  This handles e.g.:
        "sürgünlüğünden" = corpus "sürgünlüğü" (10) + suffix "nden" (4)
        "Zerubbabel'in"  = corpus "zerubbabel"  (10) + suffix "in"   (2)
      but rejects cascades like "sürgünlüğündensonra" (suffix 9 > MAX_SUFFIX).

Among all valid join lengths, the LONGEST is preferred (greedy) so that
multi-fragment splits resolve to the full word rather than an intermediate stem.

Usage:
    python3 pipeline/fix_hktn_splits.py            # apply fixes in-place
    python3 pipeline/fix_hktn_splits.py --dry-run  # print changes only
"""

import json, re, sys
from pathlib import Path

BASE        = Path(__file__).parent.parent
HKTN_DIR    = BASE / 'data' / 'translations' / 'HKTN'
TRANS_DIR   = BASE / 'data' / 'translations'
REFS        = ['TCL02', 'KMEYA', 'BGT']

MAX_JOIN    = 6    # max extra tokens to try joining
MIN_CORPUS  = 7    # min normalised length to add to HKTN corpus
                   # (5-6 char tokens may still be OCR fragments in the source)
MIN_PREFIX  = 7    # min corpus-word length for prefix-extension matching
MAX_SUFFIX  = 5    # max suffix chars allowed beyond a corpus prefix
MAX_VERSE   = 400  # skip very long "verses" (appendix/bibliography OCR)

DRY_RUN = '--dry-run' in sys.argv

# Common Turkish function words guaranteed to be standalone (all below MIN_CORPUS
# length, so they won't appear in the self-corpus).  Including them in all_words
# prevents the algorithm from treating them as fragments or consuming them during
# prefix-extension joins.
TURKISH_STOPWORDS: set = {
    # conjunctions / discourse
    've', 'da', 'de', 'ki', 'ya', 'ne', 'hem', 'ama', 'fakat', 'lakin',
    'ancak', 'oysa', 'yoksa', 'üstelik', 'zira', 'hatta', 'nasıl', 'veya',
    'çünkü', 'eğer', 'ama', 'zaten', 'ise', 'sanki',
    # common verbs / verb stems often missed by corpus
    'tövbe', 'dedi', 'oldu', 'etti', 'eder',
    # postpositions
    'için', 'ile', 'göre', 'dek', 'beri', 'kadar', 'gibi', 'doğru', 'karşı',
    # negation
    'değil', 'hiç', 'yok', 'var',
    # question particles (normalised — no ü/ı distinction needed after norm())
    'mi', 'mu', 'mı', 'mü',
    # determiners / quantifiers
    'bir', 'bu', 'şu', 'her', 'tüm', 'bazı', 'bütün',
    # short pronouns / cases
    'ben', 'sen', 'biz', 'siz', 'onu', 'ona', 'beni', 'seni',
    'bana', 'sana', 'bize', 'bunu', 'buna', 'onun',
    # other very common short words
    'ise', 'de', 'da', 'mi', 'bile', 'daha', 'çok', 'tam', 'hep',
    'artık', 'hiçbir', 'başka', 'diğer', 'kendi',
    # short theological / sacred names (all below MIN_CORPUS=7 so not in corpus,
    # but must never be consumed as suffix fragments in prefix-extension)
    'rab', 'rabb', 'ruh', 'isa', 'allah', 'tanrı', 'mesih', 'yahve',
    'kuzu', 'baba', 'amin', 'haşa', 'melekler', 'melek',
}


# ── normalisation ──────────────────────────────────────────────────────────────

_STRIP = re.compile(r"[^a-zğüşıöçıàáâãäåæèéêëìíîïðòóôõöùúûüý]")

def norm(word: str) -> str:
    word = word.replace('İ', 'i').replace('İ', 'i')
    word = word.lower()
    return _STRIP.sub('', word)


# ── data loading ───────────────────────────────────────────────────────────────

def load_refs() -> dict:
    refs: dict = {}
    for trans in REFS:
        td = TRANS_DIR / trans
        if not td.exists():
            print(f'  WARNING: {trans} not found, skipping')
            continue
        refs[trans] = {}
        for book_dir in td.iterdir():
            if not book_dir.is_dir():
                continue
            book = book_dir.name
            refs[trans][book] = {}
            for ch_file in book_dir.glob('*.json'):
                try:
                    ch   = int(ch_file.stem)
                    data = json.loads(ch_file.read_text(encoding='utf-8'))
                    refs[trans][book][ch] = {
                        item['v']: item['text']
                        for item in data.get('content', []) if 'v' in item
                    }
                except Exception:
                    pass
    return refs


def build_hktn_corpus() -> set:
    """Collect normalised tokens ≥ MIN_CORPUS chars from all current HKTN verse files."""
    words: set = set()
    for book_dir in HKTN_DIR.iterdir():
        if not book_dir.is_dir():
            continue
        for ch_file in book_dir.glob('*.json'):
            try:
                data = json.loads(ch_file.read_text(encoding='utf-8'))
                for item in data.get('content', []):
                    if 'text' not in item or len(item['text']) > MAX_VERSE:
                        continue
                    for token in item['text'].split():
                        # Skip tokens with interior uppercase — they are OCR merge
                        # artifacts (e.g. "ÇünküO'nun", "yetenRabb") not real words.
                        if any(c.isupper() for c in token[1:]):
                            continue
                        n = norm(token)
                        if len(n) >= MIN_CORPUS:
                            words.add(n)
            except Exception:
                pass
    return words


def ref_words_for(refs, book: str, chapter: int, verse: int) -> set:
    words: set = set()
    for trans_data in refs.values():
        text = trans_data.get(book, {}).get(chapter, {}).get(verse)
        if text:
            for token in text.split():
                n = norm(token)
                if n:
                    words.add(n)
    return words


# ── join validation ────────────────────────────────────────────────────────────

def accept_join(cn: str, all_words: set, hktn_corpus: set) -> bool:
    """Return True if cn is a valid word according to the oracle."""
    if cn in all_words:
        return True
    # Prefix-extension: cn is an inflected form of a long corpus word.
    # e.g. "sürgünlüğünden" = "sürgünlüğü" (corpus, 10 chars) + "nden" (4 chars suffix)
    if len(cn) >= MIN_PREFIX:
        for prefix_end in range(MIN_PREFIX, len(cn)):
            if (len(cn) - prefix_end) <= MAX_SUFFIX and cn[:prefix_end] in hktn_corpus:
                return True
    return False


# ── fix algorithm ──────────────────────────────────────────────────────────────

def fix_splits(text: str, ref_words: set, hktn_corpus: set) -> str:
    # oracle_words: only trusted sources (external refs + hand-curated stopwords).
    # Used for direct-match joins.  The HKTN self-corpus is intentionally excluded
    # here because it may contain merged OCR artifacts (e.g. "yetenrabb") that
    # would be accepted as spurious direct matches and create new false joins.
    oracle_words = ref_words | TURKISH_STOPWORDS
    # all_words: also includes corpus — used for the fragment-detection step
    # (deciding whether the *current* token is already a known word).
    all_words = oracle_words | hktn_corpus

    tokens = text.split()
    result: list = []
    i = 0

    while i < len(tokens):
        token = tokens[i]
        n     = norm(token)

        if n in all_words:
            result.append(token)
            i += 1
            continue

        # Tokens with interior uppercase (e.g. "ÇünküO'nun", "'Tanrı'nınE") are
        # pre-existing OCR merge artifacts, not simple split fragments.  Leave them.
        if any(c.isupper() for c in token[1:]):
            result.append(token)
            i += 1
            continue

        # Token not in oracle → it's an OCR fragment; try joining with next tokens.
        # Iterate longest-first so multi-fragment splits resolve to the full word.
        joined = None
        jcount = 0
        for j in range(MAX_JOIN, 0, -1):
            if i + j >= len(tokens):
                continue
            candidate = ''.join(tokens[i : i + j + 1])
            cn = norm(candidate)

            # Direct oracle match: always accept (handles proper nouns, full inflected
            # forms present in the reference translations).
            if cn in oracle_words:
                joined = candidate
                jcount = j
                break

            # Prefix-extension: only consume tokens that look like suffix fragments:
            #   (a) not in oracle_words (refs + stopwords) — real words are never
            #       consumed, even if they aren't in the corpus.
            #   (b) their original form starts lowercase — uppercase tokens are
            #       proper nouns or sentence-initial words, not suffix material.
            # Crucially we check oracle_words here (not all_words/corpus) so that
            # OCR-artifact forms in the self-corpus don't shield fragments from
            # being consumed (e.g. "lüğünden" may be in corpus as a split artifact
            # but still needs to be joinable into "sürgünlüğünden").
            def _is_fragment(tok):
                n_ = norm(tok)
                if n_ in oracle_words:
                    return False  # confirmed real word
                if tok and tok[0].isupper():
                    return False  # proper noun / sentence-initial
                return True       # lowercase, not in oracle → potential suffix

            if not all(_is_fragment(tokens[i + k]) for k in range(1, j + 1)):
                continue

            if len(cn) >= MIN_PREFIX:
                for prefix_end in range(MIN_PREFIX, len(cn)):
                    if (len(cn) - prefix_end) <= MAX_SUFFIX and cn[:prefix_end] in hktn_corpus:
                        joined = candidate
                        jcount = j
                        break
            if joined:
                break

        if joined:
            result.append(joined)
            i += jcount + 1
        else:
            result.append(token)
            i += 1

    return ' '.join(result)


# ── main ───────────────────────────────────────────────────────────────────────

def main():
    print('Building HKTN self-corpus...')
    hktn_corpus = build_hktn_corpus()
    print(f'  {len(hktn_corpus):,} unique words (length >= {MIN_CORPUS})')

    print('Loading reference translations...')
    refs = load_refs()
    print(f'  Loaded: {", ".join(refs)}')

    total_verses  = 0
    total_changed = 0
    change_log: list = []

    for book_dir in sorted(HKTN_DIR.iterdir()):
        if not book_dir.is_dir():
            continue
        book = book_dir.name

        for ch_file in sorted(book_dir.glob('*.json'), key=lambda p: int(p.stem)):
            chapter = int(ch_file.stem)
            data    = json.loads(ch_file.read_text(encoding='utf-8'))
            content = data.get('content', [])

            changed = False
            for item in content:
                if 'v' not in item:
                    continue

                vnum     = item['v']
                original = item['text']
                total_verses += 1

                if len(original) > MAX_VERSE:
                    continue

                rw    = ref_words_for(refs, book, chapter, vnum)
                fixed = fix_splits(original, rw, hktn_corpus)

                if fixed != original:
                    item['text'] = fixed
                    changed       = True
                    total_changed += 1
                    change_log.append(
                        f'{book} {chapter}:{vnum}\n'
                        f'  BEFORE: {original}\n'
                        f'  AFTER:  {fixed}'
                    )

            if changed and not DRY_RUN:
                ch_file.write_text(
                    json.dumps(data, ensure_ascii=False, indent=2),
                    encoding='utf-8'
                )

    print(f'\n{"[DRY RUN] " if DRY_RUN else ""}Fixed {total_changed} / {total_verses} verses.\n')
    for entry in change_log:
        print(entry)


if __name__ == '__main__':
    main()
