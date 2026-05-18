"""
scorer.py — Verse and book-level quality scoring for the YYY1987 pipeline.

Public API:
    score_book(corrected_chapters, verse_changes) -> dict

Score model (0–100):
  - Base score: 100
  - Per applied correction: penalty scaled by (1 - confidence)
  - Many corrections per verse → higher cumulative penalty
  - Flagged (not applied) tokens: smaller penalty each
  - Character frequency conformance bonus/penalty applied at book level
  - Score is clamped to [0, 100]
"""

from typing import List, Tuple, Dict, Optional
from collections import defaultdict


# ---------------------------------------------------------------------------
# Scoring parameters
# ---------------------------------------------------------------------------

# Per-correction penalty: (1 - conf) * CORRECTION_PENALTY_SCALE
# A correction with conf=0.95 → 0.05 * 10 = 0.5 point penalty
# A correction with conf=0.55 → 0.45 * 10 = 4.5 point penalty
_CORRECTION_PENALTY_SCALE = 10.0

# Fixed penalty per flagged (unapplied) token
_FLAG_PENALTY = 1.5

# Verse-level floor: a verse is never scored below this
_VERSE_FLOOR = 0

# For the average score, weight verses with more words more heavily
_USE_WEIGHTED_AVERAGE = True


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def score_book(
    corrected_chapters: List[Tuple[int, dict]],
    verse_changes: dict,
) -> dict:
    """
    Compute quality scores for every verse in *corrected_chapters*.

    Parameters
    ----------
    corrected_chapters : list of (ch_num, data_dict)
        Output of fixer.fix_book() — corrected verse data.
    verse_changes : dict
        verse_changes section of the fixer log.
        Keys: '1:1' style strings.
        Values: {'before', 'after', 'changes': [...], 'flags': [...]}.

    Returns
    -------
    dict:
        verse_scores    : {ref_str: int}   score per verse (0–100)
        average_score   : float            weighted mean across all verses
        total_verses    : int
        changed_verses  : int              number of verses that had any correction
        flagged_verses  : int              number of verses with unapplied flags
        score_histogram : dict             {bucket_label: count}
    """
    verse_scores: Dict[str, int] = {}
    word_counts: Dict[str, int] = {}
    changed_verses = 0
    flagged_verses = 0

    # ── Score each verse ───────────────────────────────────────────────────
    for ch_num, data in corrected_chapters:
        for verse in data.get('content', []):
            ref = f'{ch_num}:{verse["v"]}'
            text = verse.get('text', '')
            n_words = max(len(text.split()), 1)
            word_counts[ref] = n_words

            entry = verse_changes.get(ref)
            if entry is None:
                verse_scores[ref] = 100
                continue

            applied = entry.get('changes', [])
            flagged  = entry.get('flags',   [])

            if applied:
                changed_verses += 1
            if flagged:
                flagged_verses += 1

            penalty = _verse_penalty(applied, flagged)
            score = max(_VERSE_FLOOR, round(100.0 - penalty))
            verse_scores[ref] = min(100, score)

    # ── Weighted average ───────────────────────────────────────────────────
    if _USE_WEIGHTED_AVERAGE and verse_scores:
        total_weight = sum(word_counts.get(r, 1) for r in verse_scores)
        weighted_sum = sum(
            verse_scores[r] * word_counts.get(r, 1) for r in verse_scores
        )
        avg = weighted_sum / max(total_weight, 1)
    elif verse_scores:
        avg = sum(verse_scores.values()) / len(verse_scores)
    else:
        avg = 100.0

    # ── Histogram ─────────────────────────────────────────────────────────
    histogram = _build_histogram(verse_scores)

    return {
        'verse_scores':    verse_scores,
        'average_score':   round(avg, 2),
        'total_verses':    len(verse_scores),
        'changed_verses':  changed_verses,
        'flagged_verses':  flagged_verses,
        'score_histogram': histogram,
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _verse_penalty(applied: list, flagged: list) -> float:
    """
    Compute total penalty for a verse given its applied corrections and flags.

    Applied correction penalty: (1 - confidence) * scale
      → high-confidence corrections are nearly free
      → low-confidence corrections near the apply threshold cost more

    Flagged penalty: flat per flag
      → unapplied flags mean the verse may still have residual issues
    """
    penalty = 0.0

    for ch in applied:
        conf = ch.get('confidence', 0.55)
        penalty += (1.0 - min(conf, 1.0)) * _CORRECTION_PENALTY_SCALE

    penalty += len(flagged) * _FLAG_PENALTY

    return penalty


def _build_histogram(verse_scores: Dict[str, int]) -> Dict[str, int]:
    """Group verse scores into labelled buckets for reporting."""
    buckets = {
        '100':    0,
        '90-99':  0,
        '80-89':  0,
        '70-79':  0,
        '60-69':  0,
        '<60':    0,
    }
    for s in verse_scores.values():
        if s == 100:
            buckets['100'] += 1
        elif s >= 90:
            buckets['90-99'] += 1
        elif s >= 80:
            buckets['80-89'] += 1
        elif s >= 70:
            buckets['70-79'] += 1
        elif s >= 60:
            buckets['60-69'] += 1
        else:
            buckets['<60'] += 1
    return buckets
