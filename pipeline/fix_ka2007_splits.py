#!/usr/bin/env python3
"""Fix OCR word-split (and bad-merge) artifacts in KA2007.

Strategy:
  - Four clean reference translations (TCL02, YTC, BGT, NWT2025) build:
      HIGH_FREQ  : words appearing ≥10 times across all verses
      IN_DICT    : words appearing ≥3 times
      verse_db   : per-verse token sets for verse-aligned matching
  - KA2007 self-corpus builds KA_INTERNAL (≥2 occurrences, ≥5 chars) and
    KA_INTERNAL_ANY (≥1 occurrence, ≥5 chars) for translation-specific vocab.
  - should_merge() 5-level heuristic decides whether to join adjacent tokens.
  - try_split_bad_merge() splits long tokens at HIGH_FREQ+HIGH_FREQ boundaries.
  - try_split_token() greedy IN_DICT decomposition for opening-quote tokens.

Usage:
    python3 pipeline/fix_ka2007_splits.py            # apply in-place
    python3 pipeline/fix_ka2007_splits.py --dry-run  # print changes only
"""

import json, re, glob, os, sys
from pathlib import Path

BASE     = Path(__file__).parent.parent
KA_DIR   = BASE / 'data' / 'translations' / 'KA2007'
TRANS_DIR = BASE / 'data' / 'translations'
CLEAN_TRANSLATIONS = ['TCL02', 'YTC', 'BGT', 'NWT2025']

DRY_RUN = '--dry-run' in sys.argv

apos = '’'
QUOTE_CHARS = (apos, '‘', "'")


# ── vocabulary tables ────────────────────────────────────────────────────────

def build_vocab():
    global_vocab = {}
    verse_db = {}

    for trans in CLEAN_TRANSLATIONS:
        trans_dir = TRANS_DIR / trans
        if not trans_dir.exists():
            continue
        for fpath in trans_dir.rglob('*.json'):
            parts = fpath.parts
            book = parts[-2]
            try:
                chap = int(parts[-1].replace('.json', ''))
            except ValueError:
                continue
            with open(fpath, encoding='utf-8') as f:
                data = json.load(f)
            for item in data.get('content', []):
                v, text = item.get('v'), item.get('text', '')
                if not v or not text:
                    continue
                key = (book, chap, v)
                if key not in verse_db:
                    verse_db[key] = set()
                for tok in text.split():
                    core = word_core_clean(tok)
                    if core:
                        verse_db[key].add(core)
                        global_vocab[core] = global_vocab.get(core, 0) + 1

    HIGH_FREQ = {w for w, c in global_vocab.items() if c >= 10}
    IN_DICT   = {w for w, c in global_vocab.items() if c >= 3}
    return global_vocab, HIGH_FREQ, IN_DICT, verse_db


def build_ka_internal():
    ka_freq = {}
    for fpath in KA_DIR.rglob('*.json'):
        try:
            with open(fpath, encoding='utf-8') as f:
                data = json.load(f)
        except Exception:
            continue
        for item in data.get('content', []):
            for tok in item.get('text', '').split():
                core = word_core_clean(tok)
                if len(core) >= 5:
                    ka_freq[core] = ka_freq.get(core, 0) + 1
    KA_INTERNAL     = {w for w, c in ka_freq.items() if c >= 2}
    KA_INTERNAL_ANY = set(ka_freq.keys())
    return KA_INTERNAL, KA_INTERNAL_ANY


# ── token utilities ──────────────────────────────────────────────────────────

def word_core_clean(w):
    """Strip punctuation and apostrophe suffixes, return lowercase core."""
    w2 = w.strip('.,;:!?()[]{}"“”\'-')
    w2 = w2.lstrip(apos + '‘\'')
    m = re.search(r'[’‘\']', w2)
    if m:
        w2 = w2[:m.start()]
    return w2.lower()


def has_leading_apos(w):
    return bool(w) and w[0] in QUOTE_CHARS


# ── merge decision ────────────────────────────────────────────────────────────

def should_merge(w1, w2, clean_toks, global_vocab, HIGH_FREQ, IN_DICT,
                 KA_INTERNAL, KA_INTERNAL_ANY):
    c1, c2 = word_core_clean(w1), word_core_clean(w2)
    if not c1 or not c2 or len(c1) < 2 or len(c2) < 2:
        return False
    if has_leading_apos(w1) and len(c1) >= 4:
        return False
    # Don't merge across punctuation boundary (e.g. "düz," + "ey" → "düzey")
    if w1 and w1[-1] in '.,;:!?)':
        return False
    # Don't merge tokens that already carry a Turkish apostrophe suffix
    # (e.g. "Allah'ın" + "Evi" → "Allah'ınEvi" is wrong)
    if apos in w1[1:]:
        return False
    combined = c1 + c2
    if len(combined) < 4:
        return False

    nnih = sum(1 for w in [c1, c2] if w not in HIGH_FREQ)

    # Level 1: verse-aligned (highest confidence)
    # Guard: skip if BOTH parts are HIGH_FREQ AND both appear standalone in this
    # verse's clean tokens — that signals two separate words, not an OCR split
    # (e.g. "biz de" where "bizde" is in oracle but they're two distinct words here)
    if combined in clean_toks:
        if not (nnih == 0 and c1 in clean_toks and c2 in clean_toks):
            return True

    # Level 2: global IN_DICT + at least one non-standalone part
    if nnih >= 1 and combined in IN_DICT:
        return True

    c2_global = global_vocab.get(c2, 0)

    # Level 3: c2 never standalone + combined in IN_DICT
    if c2_global == 0 and len(c2) <= 10:
        if combined in IN_DICT:
            return True

    # Level 4: c2 never standalone + combined in KA2007 vocab
    if c2_global == 0 and len(c2) <= 10:
        if combined in KA_INTERNAL and nnih >= 1:
            return True
        # KA_INTERNAL_ANY: guard against number/word compound false merges
        # (e.g. "bin"+"yediyüz" → "binyediyüz") by requiring c2 doesn't start
        # with a HIGH_FREQ word of length ≥4 (distinguishes "tireyim" from "yediyüz")
        c2_starts_hf4 = any(c2.startswith(hf) for hf in HIGH_FREQ if len(hf) >= 4)
        if combined in KA_INTERNAL_ANY and nnih >= 1 and not c2_starts_hf4:
            return True

    # Level 5: both parts HIGH_FREQ + combined form common enough
    if nnih == 0 and combined in IN_DICT:
        comb_freq = global_vocab.get(combined, 0)
        if comb_freq >= 5:                              # was >= 10
            return True

    return False


# ── split heuristics ──────────────────────────────────────────────────────────

def try_split_bad_merge(token, IN_DICT, HIGH_FREQ, KA_INTERNAL, global_vocab):
    """Split at HIGH_FREQ+HIGH_FREQ boundary for tokens not in dictionaries."""
    core = token.lower().lstrip(''.join(QUOTE_CHARS))
    if core in IN_DICT or core in KA_INTERNAL:
        return None
    # Protect words that appear in any clean translation (global_vocab ≥ 1)
    if global_vocab.get(core, 0) >= 1:
        return None
    if len(core) < 6:
        return None
    # Require both parts ≥ 4 chars to avoid splitting valid words at short suffixes
    # e.g. "bayramlardan" would split "bayramlar"+"dan" (right=3) — prevented here
    for i in range(4, len(core) - 3):
        left  = core[:i]
        right = core[i:]
        if left in HIGH_FREQ and right in HIGH_FREQ:
            offset = len(token) - len(core)
            return token[:i + offset] + ' ' + token[i + offset:]
    return None


def try_split_token(token, IN_DICT, HIGH_FREQ, KA_INTERNAL, is_opening_quote=False):
    """Greedy left-to-right IN_DICT decomposition for long unrecognised tokens."""
    prefix = ''
    if is_opening_quote and token and token[0] in QUOTE_CHARS:
        prefix = token[0]
        core   = token[1:]
    else:
        core = token

    core_lower = core.lower()
    if core_lower in IN_DICT or core_lower in KA_INTERNAL:
        return None
    if len(core) < 6:
        return None

    parts     = []
    remaining = core
    while remaining and len(remaining) >= 2:
        best_len = 0
        for i in range(min(len(remaining), 20), 1, -1):
            candidate = remaining[:i].lower()
            if candidate in IN_DICT:
                rest = remaining[i:].lower()
                if len(rest) == 0 or rest in IN_DICT or rest in HIGH_FREQ:
                    best_len = i
                    break
                elif len(rest) >= 2 and any(
                        rest[:j] in IN_DICT for j in range(2, len(rest) + 1)):
                    best_len = i
                    break
        if best_len == 0:
            for i in range(min(len(remaining), 15), 2, -1):
                if remaining[:i].lower() in HIGH_FREQ:
                    best_len = i
                    break
        if best_len == 0:
            return None
        parts.append(remaining[:best_len])
        remaining = remaining[best_len:]

    if remaining or len(parts) <= 1:
        return None

    return prefix + ' '.join(parts)


# ── main fix loop ─────────────────────────────────────────────────────────────

def fix_text(text, clean_toks, global_vocab, HIGH_FREQ, IN_DICT,
             KA_INTERNAL, KA_INTERNAL_ANY):
    """Multi-pass merge+split until stable (max 15 passes)."""
    # Pre-normalize: split mid-word '(' so "(kala" becomes a separate token.
    # e.g. "ordusunu(kala balığını)" → "ordusunu (kala balığını)"
    text = re.sub(r'(\w)\(', r'\1 (', text)

    for _ in range(15):
        words    = text.split()
        new_words = []
        i        = 0
        changed  = False

        while i < len(words):
            tok       = words[i]
            split_res = None
            tok_lower = tok.lower().lstrip(''.join(QUOTE_CHARS))

            if len(tok) >= 6 and tok_lower not in IN_DICT and tok_lower not in KA_INTERNAL:
                if tok and tok[0] in QUOTE_CHARS:
                    split_res = try_split_token(
                        tok, IN_DICT, HIGH_FREQ, KA_INTERNAL, is_opening_quote=True)
                if not split_res:
                    split_res = try_split_bad_merge(tok, IN_DICT, HIGH_FREQ, KA_INTERNAL, global_vocab)

            if split_res and split_res != tok:
                new_words.extend(split_res.split())
                changed = True
                i += 1
                continue

            if i + 1 < len(words) and should_merge(
                    words[i], words[i + 1], clean_toks,
                    global_vocab, HIGH_FREQ, IN_DICT, KA_INTERNAL, KA_INTERNAL_ANY):
                new_words.append(words[i] + words[i + 1])
                i += 2
                changed = True
            else:
                new_words.append(words[i])
                i += 1

        text = ' '.join(new_words)
        if not changed:
            break

    return text


# ── apply to all files ────────────────────────────────────────────────────────

def main():
    print('Building vocabulary from clean translations...')
    global_vocab, HIGH_FREQ, IN_DICT, verse_db = build_vocab()
    print(f'  HIGH_FREQ: {len(HIGH_FREQ):,}  IN_DICT: {len(IN_DICT):,}  '
          f'verse_db: {len(verse_db):,} keys')

    print('Building KA2007 self-corpus...')
    KA_INTERNAL, KA_INTERNAL_ANY = build_ka_internal()
    print(f'  KA_INTERNAL: {len(KA_INTERNAL):,}  KA_INTERNAL_ANY: {len(KA_INTERNAL_ANY):,}')

    total_verses  = 0
    total_changed = 0
    change_log    = []

    for book_dir in sorted(KA_DIR.iterdir()):
        if not book_dir.is_dir():
            continue
        book = book_dir.name

        for ch_file in sorted(book_dir.glob('*.json'), key=lambda p: int(p.stem)):
            chapter = int(ch_file.stem)
            try:
                data = json.loads(ch_file.read_text(encoding='utf-8'))
            except Exception:
                continue
            content = data.get('content', [])

            changed = False
            for item in content:
                if 'text' not in item:
                    continue
                v        = item.get('v')
                original = item['text']
                total_verses += 1

                clean_toks = verse_db.get((book, chapter, v), set())
                fixed = fix_text(original, clean_toks,
                                 global_vocab, HIGH_FREQ, IN_DICT,
                                 KA_INTERNAL, KA_INTERNAL_ANY)

                if fixed != original:
                    item['text'] = fixed
                    changed       = True
                    total_changed += 1
                    change_log.append(
                        f'{book} {chapter}:{v}\n'
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
