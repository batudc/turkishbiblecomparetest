"""
fixer.py — Word-level OCR correction engine for the YYY1987 pipeline.

Public API:
    fix_book(book_path, freq) -> (corrected_chapters, log)

Corruption pattern corrected:
    İ → Y  |  ı → y  |  ş → b  |  ğ → ö   (PDF rendering artefact)
    9 → ş                                   (digit OCR artefact)

Algorithm:
  1. For each word token in a verse:
     a. Apply 9→ş substitution first (high-confidence digit fix).
     b. Enumerate all subsets of corruptible character positions.
     c. For each candidate correction, look up its frequency in the
        reference-translation vocabulary.
     d. Compute a confidence score that balances candidate frequency
        against how well-known the ORIGINAL form already is.
     e. Apply only if confidence ≥ CONF_APPLY (0.55).
     f. Otherwise, flag the token (MED: 0.28–0.55 | LOW: <0.28) for
        manual review without modifying the text.

Confidence model:
  - If the original word has frequency 0 and the corrected form has
    frequency ≥ 5, confidence rises quickly with corrected frequency.
  - If the original word is already common (freq ≥ 500), a large
    "protection penalty" prevents it from being changed (e.g. 'bey', 'bir').
"""

import re
import copy
from pathlib import Path
from itertools import combinations
from collections import Counter
from typing import Dict, List, Optional, Tuple

from utils import (
    load_book_data, tokenise, turkish_lower, extract_root
)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Corruption map: character in corrupted text → intended character
CORRUPT_MAP: Dict[str, str] = {
    'Y': 'İ',
    'y': 'ı',
    'b': 'ş',
    'ö': 'ğ',
}

CONF_APPLY = 0.55   # minimum confidence to apply a correction
CONF_FLAG  = 0.28   # below this → flagged_low (not applied, lower priority)

# Max corruptible-character combinations to try
# 2^8 = 256 — reasonable upper bound for words with many corruptible chars
_MAX_COMBOS = 256


# ---------------------------------------------------------------------------
# Compiled regexes
# ---------------------------------------------------------------------------

# 9 → ş: digit 9 sandwiched between Turkish letters (mid-word)
_NINE_MID = re.compile(
    r"(?<=[a-zA-ZÇçĞğİıÖöŞşÜü])9(?=[a-zçğışöü'''])"
)
# 9 → ş: digit 9 at word start (preceded by non-letter)
_NINE_START = re.compile(
    r"(?<![a-zA-Z0-9ÇçĞğİıÖöŞşÜü])9(?=[a-zçğışöü])"
)

# Split a token into: non-alpha prefix | alpha core | non-alpha suffix
_ALPHA_SPLIT = re.compile(
    r"^([^a-zA-ZÇçĞğİıÖöŞşÜü]*)"
    r"([a-zA-ZÇçĞğİıÖöŞşÜü''']*)"
    r"(.*)$",
    re.UNICODE,
)

# Identifies word tokens (as opposed to punctuation/whitespace)
_WORD_TOKEN = re.compile(r"[\w'''-]+", re.UNICODE)


# ---------------------------------------------------------------------------
# Frequency helpers
# ---------------------------------------------------------------------------

def _gf(word: str, freq: Counter) -> int:
    """Get combined frequency for *word* (original + lowercase forms)."""
    return freq.get(word, 0) + freq.get(turkish_lower(word), 0)


def _raw_conf(orig_f: int, cand_f: int) -> float:
    """
    Raw confidence that *cand_f* words proves the correction is right,
    given *orig_f* frequency for the original form.

    When orig_f == 0 (unknown word): confidence scales with candidate frequency.
    When orig_f > 0 (known word): need a large frequency ratio to justify change.
    """
    if orig_f == 0:
        if cand_f == 0:    return 0.05
        if cand_f >= 2000: return 0.96
        if cand_f >= 500:  return 0.92
        if cand_f >= 100:  return 0.87
        if cand_f >= 20:   return 0.78
        if cand_f >= 5:    return 0.65
        return 0.52
    # Original exists — need a much higher frequency to justify changing it
    ratio = cand_f / (orig_f + 1)
    if ratio >= 100: return 0.90
    if ratio >= 30:  return 0.80
    if ratio >= 10:  return 0.68
    if ratio >=  5:  return 0.54
    if ratio >=  3:  return 0.38
    return 0.12


def _protection_penalty(orig_f: int) -> float:
    """
    Penalty applied when the original word already exists in the vocabulary.
    Prevents common words (bir, bey, barış, …) from being incorrectly changed.
    """
    if orig_f >= 500: return 0.72
    if orig_f >= 100: return 0.52
    if orig_f >= 20:  return 0.32
    if orig_f >= 5:   return 0.14
    if orig_f >= 1:   return 0.05
    return 0.00


# ---------------------------------------------------------------------------
# Core correction logic
# ---------------------------------------------------------------------------

def _split_tok(word: str) -> Tuple[str, str, str]:
    """Split *word* into (non-alpha prefix, alpha core, non-alpha suffix)."""
    m = _ALPHA_SPLIT.match(word)
    return (m.group(1), m.group(2), m.group(3)) if m else ('', word, '')


def _best_correction(core: str, freq: Counter) -> Tuple[str, float, int]:
    """
    Try every subset of corruptible positions in *core* and return the
    (best_correction, confidence, n_substitutions) triple.

    Returns the original core (unchanged) with conf=1.0, n=0 when no
    corruptible characters are present or no better candidate is found.
    """
    orig_f   = _gf(core, freq)
    corrupt  = [(i, c) for i, c in enumerate(core) if c in CORRUPT_MAP]

    if not corrupt:
        return core, 1.0, 0

    penalty = _protection_penalty(orig_f)
    best_w, best_c, best_n = core, 0.0, 0
    chars = list(core)
    n_combos = 0

    for n in range(1, len(corrupt) + 1):
        for combo in combinations(corrupt, n):
            if n_combos >= _MAX_COMBOS:
                break
            n_combos += 1

            cand = chars[:]
            for idx, ch in combo:
                cand[idx] = CORRUPT_MAP[ch]
            w  = ''.join(cand)
            cf = _gf(w, freq)
            c  = max(0.0, _raw_conf(orig_f, cf) - penalty - (n - 1) * 0.05)

            if c > best_c:
                best_w, best_c, best_n = w, c, n

        if n_combos >= _MAX_COMBOS:
            break

    return best_w, best_c, best_n


def correct_token(word: str, freq: Counter) -> Tuple[str, Optional[dict]]:
    """
    Attempt to correct a single token.

    Returns
    -------
    (output_word, change_record)
        output_word  : corrected if action == 'applied', else original
        change_record: dict describing the decision, or None if no change
    """
    if len(word) < 2:
        return word, None

    prefix, core, suffix = _split_tok(word)

    # ── Pass 1: digit 9 → ş ───────────────────────────────────────────────
    c9 = _NINE_MID.sub('ş', core)
    c9 = _NINE_START.sub('ş', c9)
    if c9 != core:
        fw = prefix + c9 + suffix
        return fw, {
            'orig': word, 'fixed': fw,
            'confidence': 0.95, 'action': 'applied', 'type': '9_ş',
            'orig_freq': _gf(word, freq), 'fixed_freq': _gf(fw, freq),
        }

    # ── Pass 2: vocabulary-guided character substitution ─────────────────
    fixed, conf, n = _best_correction(core, freq)

    if n == 0 or fixed == core:
        return word, None

    fw = prefix + fixed + suffix

    if conf >= CONF_APPLY:
        action, result = 'applied', fw
    elif conf >= CONF_FLAG:
        action, result = 'flagged_med', word   # flag only, do NOT change
    else:
        action, result = 'flagged_low', word   # flag only, do NOT change

    return result, {
        'orig': word, 'fixed': fw,
        'confidence': round(conf, 3), 'action': action,
        'type': 'char_sub', 'n_subs': n,
        'orig_freq': _gf(core, freq), 'fixed_freq': _gf(fixed, freq),
    }


# ---------------------------------------------------------------------------
# Verse and book-level correction
# ---------------------------------------------------------------------------

def _fix_verse(
    text: str,
    freq: Counter,
) -> Tuple[str, List[dict]]:
    """
    Correct a single verse.

    Returns (fixed_text, list_of_change_records).
    Change records have action in {'applied', 'flagged_med', 'flagged_low'}.
    Only 'applied' records actually modify the text.
    """
    tokens  = tokenise(text)
    out     = []
    changes = []

    for tok in tokens:
        if _WORD_TOKEN.fullmatch(tok) and len(tok) >= 2:
            fixed_tok, ch = correct_token(tok, freq)
            if ch:
                changes.append(ch)
            out.append(fixed_tok)
        else:
            out.append(tok)

    return ''.join(out), changes


def fix_book(
    book_path: Path,
    freq: Counter,
) -> Tuple[List[Tuple[int, dict]], dict]:
    """
    Correct every verse in a book using the provided vocabulary *freq*.

    Parameters
    ----------
    book_path : Path
        Directory containing the book's per-chapter JSON files.
    freq      : Counter
        Word-frequency Counter built from reference translations.
        Build once with utils.build_dictionary() and reuse across books.

    Returns
    -------
    corrected_chapters : list of (chapter_number, corrected_data_dict)
        Deep-copied chapter dicts with verse texts replaced.
        Original files on disk are NOT modified.

    log : dict
        {
          'book'         : str,
          'stats'        : {'total', 'applied', 'flagged'},
          'verse_changes': {
              '1:1': {
                  'before'  : original_text,
                  'after'   : corrected_text,
                  'changes' : [applied_records],
                  'flags'   : [flagged_records],
              }, ...
          },
          'all_changes'  : [every_change_record],
        }
    """
    book_path = Path(book_path)
    raw_chapters = load_book_data(book_path)

    corrected: List[Tuple[int, dict]] = []
    verse_changes: dict = {}
    all_changes:   List[dict] = []
    stats = {'total': 0, 'applied': 0, 'flagged': 0}

    for ch_num, data, _ in raw_chapters:
        # Deep-copy so we never mutate the loaded data
        ch_data = copy.deepcopy(data)

        for verse in ch_data.get('content', []):
            orig_text = verse['text']
            fixed_text, changes = _fix_verse(orig_text, freq)
            verse['text'] = fixed_text

            stats['total'] += len([c for c in changes if c['type'] != ''])
            for ch in changes:
                if ch['action'] == 'applied':
                    stats['applied'] += 1
                else:
                    stats['flagged'] += 1

            if changes:
                ref = f'{ch_num}:{verse["v"]}'
                applied = [c for c in changes if c['action'] == 'applied']
                flagged = [c for c in changes if c['action'] != 'applied']
                verse_changes[ref] = {
                    'before':  orig_text,
                    'after':   fixed_text,
                    'changes': applied,
                    'flags':   flagged,
                }
                all_changes.extend(changes)

        corrected.append((ch_num, ch_data))

    log = {
        'book':          book_path.name,
        'stats':         stats,
        'verse_changes': verse_changes,
        'all_changes':   all_changes,
    }

    return corrected, log
