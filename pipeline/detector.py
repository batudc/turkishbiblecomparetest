"""
detector.py — Corruption-detection module for the YYY1987 OCR pipeline.

Public API:
    detect_book_corruption(book_path) -> dict

The detector analyses character-frequency distributions and OCR-specific
patterns to decide whether a book requires correction, and how severely it
is corrupted.

Known corruption pattern (PDF rendering artefact):
    İ → Y    ı → y    ş → b    ğ → ö
Plus digit OCR artefacts where digits appear inside Turkish words (9 → ş).
"""

import re
from pathlib import Path
from typing import Dict, List, Any

from utils import load_book_data, get_all_text, char_frequencies, extract_words


# ---------------------------------------------------------------------------
# Thresholds
# ---------------------------------------------------------------------------

# Acceptable frequency ranges (per 1 000 chars) for clean Turkish NT text.
# Values outside these bounds indicate corruption.
FREQ_THRESHOLDS: Dict[str, Dict[str, float]] = {
    'ı': {'low': 22.0,  'high': 52.0},   # dotless-i — HIGH if missing (→y)
    'ş': {'low':  5.5,  'high': 20.0},   # s-cedilla  — HIGH if missing (→b)
    'y': {'low': 16.0,  'high': 40.0},   # y          — HIGH if ı leaked into it
    'b': {'low':  9.0,  'high': 27.0},   # b          — HIGH if ş leaked into it
    'ğ': {'low':  3.5,  'high': 16.0},   # g-breve    — LOW if missing (→ö)
}

# Corruption-issue names and the weight each contributes to the overall score
_ISSUE_WEIGHTS: Dict[str, float] = {
    'LOW_ı':         0.30,
    'LOW_ş':         0.25,
    'HIGH_y':        0.25,
    'HIGH_b':        0.12,
    'LOW_ğ':         0.05,
    'DIGIT_IN_WORD': 0.03,
}

# A book is considered corrupted if the weighted score reaches this threshold
_CORRUPTION_THRESHOLD = 0.28

# Regex: digit(s) sandwiched between Turkish letters — classic OCR artefact
_DIGIT_IN_WORD_RE = re.compile(
    r'[a-zA-ZÇçĞğİıÖöŞşÜü]\d+[a-zA-ZÇçĞğİıÖöŞşÜü]'
)

# Regex: suspicious token patterns that suggest remaining corruption
_SUSPICIOUS_TOKEN_RE = re.compile(
    r'^[a-zA-ZÇçĞğİıÖöŞşÜü]*'
    r'(?:'
    r'[yY]{2,}'          # multiple consecutive y — ı→y artifact
    r'|[bB][yY]'         # by cluster — ş→b + ı→y
    r'|[yY][bB]'         # yb cluster — same artifact
    r'|9[a-zçğışöü]'     # 9→ş OCR
    r')'
    r'[a-zA-ZÇçĞğİıÖöŞşÜü]*$'
)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def detect_book_corruption(book_path: Path) -> dict:
    """
    Analyse a book directory and return a corruption assessment.

    Parameters
    ----------
    book_path : Path
        Directory containing the book's per-chapter JSON files.

    Returns
    -------
    dict with keys:
        is_corrupted : bool
        score        : float  (0.0 = clean, 1.0 = maximally corrupt)
        issues       : list   of issue dicts
        freq         : dict   character frequencies per 1 000 chars
        book         : str    book code (directory name)
        chapters     : int    number of chapters loaded
        total_chars  : int    total text length analysed
        suspicious_tokens : list  sample of suspicious word forms found
    """
    book_path = Path(book_path)

    if not book_path.exists():
        return {
            'is_corrupted': False,
            'score': 0.0,
            'issues': [{'type': 'BOOK_NOT_FOUND', 'detail': str(book_path)}],
            'freq': {},
            'book': book_path.name,
            'chapters': 0,
            'total_chars': 0,
            'suspicious_tokens': [],
        }

    chapters = load_book_data(book_path)
    if not chapters:
        return {
            'is_corrupted': False,
            'score': 0.0,
            'issues': [{'type': 'NO_CHAPTERS'}],
            'freq': {},
            'book': book_path.name,
            'chapters': 0,
            'total_chars': 0,
            'suspicious_tokens': [],
        }

    all_text = get_all_text(chapters)
    if not all_text.strip():
        return {
            'is_corrupted': False,
            'score': 0.0,
            'issues': [{'type': 'NO_TEXT'}],
            'freq': {},
            'book': book_path.name,
            'chapters': len(chapters),
            'total_chars': 0,
            'suspicious_tokens': [],
        }

    freq = char_frequencies(all_text)
    issues: List[Dict[str, Any]] = []

    # ── Frequency anomaly checks ───────────────────────────────────────────
    for char, bounds in FREQ_THRESHOLDS.items():
        val = freq.get(char, 0.0)
        if val < bounds['low']:
            issues.append({
                'type': f'LOW_{char}',
                'value': val,
                'expected_min': bounds['low'],
                'severity': _deviation_severity(val, bounds['low'], direction='below'),
            })
        elif val > bounds['high']:
            issues.append({
                'type': f'HIGH_{char}',
                'value': val,
                'expected_max': bounds['high'],
                'severity': _deviation_severity(val, bounds['high'], direction='above'),
            })

    # ── Digit-in-word OCR artefact check ──────────────────────────────────
    digit_matches = _DIGIT_IN_WORD_RE.findall(all_text)
    if digit_matches:
        issues.append({
            'type': 'DIGIT_IN_WORD',
            'count': len(digit_matches),
            'samples': digit_matches[:10],
        })

    # ── Suspicious token scan (sample of up to 30) ────────────────────────
    suspicious: List[str] = []
    seen: set = set()
    for word in extract_words(all_text):
        if word not in seen and _SUSPICIOUS_TOKEN_RE.match(word) and len(suspicious) < 30:
            suspicious.append(word)
            seen.add(word)

    # ── Weighted corruption score ──────────────────────────────────────────
    issue_types = {i['type'] for i in issues}
    score = sum(w for t, w in _ISSUE_WEIGHTS.items() if t in issue_types)
    score = min(1.0, score)

    # A combination of LOW_ı + HIGH_y is the strongest signal
    is_corrupted = (
        score >= _CORRUPTION_THRESHOLD
        or ('LOW_ı' in issue_types and 'HIGH_y' in issue_types)
        or ('LOW_ş' in issue_types and 'HIGH_b' in issue_types)
    )

    return {
        'is_corrupted': is_corrupted,
        'score': round(score, 3),
        'issues': issues,
        'freq': freq,
        'book': book_path.name,
        'chapters': len(chapters),
        'total_chars': len(all_text),
        'suspicious_tokens': suspicious,
    }


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _deviation_severity(value: float, threshold: float, direction: str) -> str:
    """Classify how far the value deviates from the threshold."""
    if direction == 'below':
        ratio = (threshold - value) / max(threshold, 1)
    else:
        ratio = (value - threshold) / max(threshold, 1)

    if ratio >= 0.5:
        return 'CRITICAL'
    elif ratio >= 0.25:
        return 'HIGH'
    elif ratio >= 0.10:
        return 'MEDIUM'
    return 'LOW'
