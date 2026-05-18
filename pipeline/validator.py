"""
validator.py — Post-correction quality validation for the YYY1987 pipeline.

Public API:
    validate_book(book_path, corrected_chapters) -> dict

Checks:
  1. Character frequency ranges   — do corrected frequencies look like clean Turkish?
  2. Verse sequence integrity     — no gaps, duplicates, or out-of-order verses.
  3. Remaining OCR artefacts      — digits embedded in words; broken token patterns.
  4. Semantic-risk flags          — corrected word AND original are both valid
                                     high-frequency words (potential false positive).
  5. Short-verse outliers         — verses suspiciously short (possible parse errors).
"""

import re
from pathlib import Path
from typing import List, Dict, Tuple, Optional

from utils import (
    load_book_data, get_all_text, char_frequencies, extract_words,
    turkish_lower, REFERENCE_FREQ
)


# ---------------------------------------------------------------------------
# Validation thresholds
# ---------------------------------------------------------------------------

# Acceptable per-1 000-chars ranges for corrected Turkish NT text.
# Tighter than the detector thresholds to catch incomplete correction.
CLEAN_FREQ_RANGES: Dict[str, Tuple[float, float]] = {
    'ı': (25.0, 50.0),
    'ş': ( 6.0, 18.0),
    'y': (16.0, 36.0),
    'b': ( 9.0, 26.0),
    'ğ': ( 4.0, 15.0),
}

# Minimum meaningful verse length (characters) — below this is suspicious
_MIN_VERSE_CHARS = 8

# Regex: digit(s) sandwiched between Turkish letters
_DIGIT_ARTIFACT_RE = re.compile(
    r'[a-zA-ZÇçĞğİıÖöŞşÜü]\d+[a-zA-ZÇçĞğİıÖöŞşÜü]'
)

# Regex: tokens that look like uncorrected corruption (e.g. 'yabam', 'kibi')
_UNCORRECTED_PATTERN_RE = re.compile(
    r'(?:'
    r'[yY]{2,}[a-zçğışöü]'   # double-y prefix (ı→y artefact)
    r'|[a-zçğışöü]{2,}[bB][yY]'  # -by ending (ş→b + ı→y)
    r'|[a-zçğışöü][yY][bB]'      # yb cluster inside word
    r')'
)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def validate_book(
    book_path: Path,
    corrected_chapters: Optional[List[Tuple[int, dict]]] = None,
) -> dict:
    """
    Validate a book after correction.

    Parameters
    ----------
    book_path          : Path
        Original book directory (used for the book name and, if
        *corrected_chapters* is None, for loading data from disk).
    corrected_chapters : list of (ch_num, data_dict), optional
        In-memory corrected data produced by fixer.fix_book().
        If None, data is loaded from *book_path* on disk.

    Returns
    -------
    dict with keys:
        book              : str
        passed            : bool   True if no critical issues found
        issues            : list   of issue strings
        freq_before       : dict   (only when corrected_chapters provided)
        freq_after        : dict   character frequencies of corrected text
        verse_count       : int
        chapter_count     : int
        remaining_artefacts : list of {chapter, verse, artefact, context}
        short_verses      : list of {chapter, verse, length, text}
        sequence_errors   : list of strings
        semantic_risks    : list (populated by cross-check in orchestrator)
    """
    book_path = Path(book_path)
    report: dict = {
        'book':               book_path.name,
        'passed':             True,
        'issues':             [],
        'freq_after':         {},
        'verse_count':        0,
        'chapter_count':      0,
        'remaining_artefacts': [],
        'short_verses':       [],
        'sequence_errors':    [],
        'uncorrected_tokens': [],
        'semantic_risks':     [],
    }

    # ── Resolve data source ────────────────────────────────────────────────
    if corrected_chapters is not None:
        chapters: List[Tuple[int, dict]] = corrected_chapters
    else:
        raw = load_book_data(book_path)
        chapters = [(ch, data) for ch, data, _ in raw]

    if not chapters:
        report['issues'].append('NO_CHAPTERS')
        report['passed'] = False
        return report

    report['chapter_count'] = len(chapters)

    # ── Gather all text and verse metadata ────────────────────────────────
    all_text_parts: List[str] = []
    seen_uncorrected: set = set()

    for ch_num, data in chapters:
        verses = data.get('content', [])
        report['verse_count'] += len(verses)
        v_nums = [v['v'] for v in verses]

        # Verse sequence check
        _check_sequence(ch_num, v_nums, report)

        for verse in verses:
            text = verse.get('text', '')
            v_num = verse['v']
            all_text_parts.append(text)

            # Remaining digit artefacts
            for m in _DIGIT_ARTIFACT_RE.finditer(text):
                ctx_start = max(0, m.start() - 8)
                ctx_end   = min(len(text), m.end() + 8)
                report['remaining_artefacts'].append({
                    'chapter': ch_num,
                    'verse':   v_num,
                    'artefact': m.group(),
                    'context': text[ctx_start:ctx_end],
                })

            # Short verse outliers
            if 0 < len(text) < _MIN_VERSE_CHARS:
                report['short_verses'].append({
                    'chapter': ch_num,
                    'verse':   v_num,
                    'length':  len(text),
                    'text':    text,
                })

            # Uncorrected suspicious token patterns
            for word in extract_words(text):
                if word not in seen_uncorrected and _UNCORRECTED_PATTERN_RE.search(word.lower()):
                    report['uncorrected_tokens'].append({
                        'chapter': ch_num,
                        'verse':   v_num,
                        'token':   word,
                    })
                    seen_uncorrected.add(word)

    all_text = ' '.join(all_text_parts)

    # ── Post-correction character frequency check ─────────────────────────
    freq_after = char_frequencies(all_text)
    report['freq_after'] = freq_after

    for char, (low, high) in CLEAN_FREQ_RANGES.items():
        val = freq_after.get(char, 0.0)
        ref = REFERENCE_FREQ.get(char, 0.0)

        if val < low:
            issue = (
                f'STILL_LOW_{char}: {val:.1f}‰ '
                f'(target ≥{low:.1f}‰, ref={ref:.1f}‰) — '
                f'possible incomplete correction'
            )
            report['issues'].append(issue)
        elif val > high:
            issue = (
                f'STILL_HIGH_{char}: {val:.1f}‰ '
                f'(target ≤{high:.1f}‰, ref={ref:.1f}‰) — '
                f'possible over-correction'
            )
            report['issues'].append(issue)

    # ── Remaining artefacts flag ───────────────────────────────────────────
    if report['remaining_artefacts']:
        report['issues'].append(
            f'DIGIT_ARTEFACTS: {len(report["remaining_artefacts"])} remaining'
        )

    if report['uncorrected_tokens']:
        report['issues'].append(
            f'UNCORRECTED_TOKENS: {len(report["uncorrected_tokens"])} suspicious'
        )

    report['passed'] = len(report['issues']) == 0
    return report


def check_semantic_risks(
    verse_changes: dict,
    freq: object,
) -> List[dict]:
    """
    Identify cases where BOTH the original and corrected forms are valid,
    high-frequency words — a potential false positive correction.

    Parameters
    ----------
    verse_changes : dict  verse_changes section of the fixer log
    freq          : Counter  reference-translation vocabulary

    Returns list of risk dicts: {ref, orig, fixed, orig_freq, fixed_freq}
    """
    risks = []
    _gf = lambda w: freq.get(w, 0) + freq.get(turkish_lower(w), 0)

    for ref, entry in verse_changes.items():
        for ch in entry.get('changes', []):
            of = _gf(ch['orig'])
            ff = _gf(ch['fixed'])
            # Both are known words and neither dominates — flag as risk
            if of >= 50 and ff >= 50 and ff / (of + 1) < 10:
                risks.append({
                    'ref':        ref,
                    'orig':       ch['orig'],
                    'fixed':      ch['fixed'],
                    'orig_freq':  of,
                    'fixed_freq': ff,
                    'confidence': ch.get('confidence', 0),
                })

    return risks


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _check_sequence(ch_num: int, v_nums: List[int], report: dict) -> None:
    """Detect missing gaps, duplicates, and out-of-order verse numbers."""
    if not v_nums:
        return

    # Duplicates
    seen = set()
    for n in v_nums:
        if n in seen:
            msg = f'Ch {ch_num}: duplicate verse {n}'
            report['sequence_errors'].append(msg)
            report['issues'].append(msg)
        seen.add(n)

    # Out-of-order
    if v_nums != sorted(v_nums):
        msg = f'Ch {ch_num}: verses out of order'
        report['sequence_errors'].append(msg)
        report['issues'].append(msg)

    # Gaps (only flag if > 3 consecutive missing)
    sorted_nums = sorted(set(v_nums))
    if sorted_nums:
        expected = list(range(sorted_nums[0], sorted_nums[-1] + 1))
        missing  = [n for n in expected if n not in seen]
        if len(missing) > 3:
            msg = f'Ch {ch_num}: {len(missing)} missing verses ({missing[:5]}…)'
            report['sequence_errors'].append(msg)
            report['issues'].append(msg)
