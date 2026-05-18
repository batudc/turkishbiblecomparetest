"""
utils.py — Core utilities for the YYY1987 OCR correction pipeline.

Provides:
  - JSON I/O helpers
  - Turkish-aware tokenisation and case-folding
  - Root/stem extraction via suffix stripping
  - Vocabulary builder (frequency Counter) from reference translations
  - Book-data loader
  - Character frequency calculator
"""

import json
import re
from pathlib import Path
from collections import Counter
from typing import Dict, List, Tuple


# ---------------------------------------------------------------------------
# Tokeniser
# ---------------------------------------------------------------------------
# Handles ASCII apostrophe U+0027, right-single-quote U+2019, left U+2018
_TOKEN_RE = re.compile(r"([\w‘’'-]+|[^\w‘’'-]+)", re.UNICODE)
_WORD_RE  = re.compile(r"[\w‘’'-]+", re.UNICODE)


# ---------------------------------------------------------------------------
# Turkish suffix list for root/stem extraction (longest-first for greedy strip)
# ---------------------------------------------------------------------------
_SUFFIXES: List[str] = sorted([
    # long / specific
    'acağınız', 'eceğiniz', 'lacağınız', 'landırdığı', 'leştirilmiş',
    'leşmekten', 'laştığından', 'laştığı', 'larından', 'larınızı',
    'larımızı', 'larımızın', 'layıcısı', 'maktadırlar', 'maktadır',
    'lılığından', 'ılığından', 'tıracağını', 'lılığını',
    # medium
    'leşmek', 'malıdır', 'meliyiz', 'larında', 'larının', 'larını',
    'ından', 'ınkine', 'üğünü', 'ığından', 'acağı', 'eceği',
    'tirilmiş', 'dırdığı', 'ırdığı', 'dığını', 'yıcı', 'ıcı',
    'ların', 'lerin', 'andan', 'enden',
    # short
    'madan', 'meden', 'lara', 'lere', 'dan', 'den', 'tan', 'ten',
    'nın', 'nin', 'nün', 'nun', 'mak', 'mek', 'miş', 'mış',
    'müş', 'muş', 'ler', 'lar', 'yım', 'yiz', 'dı', 'di', 'du', 'dü',
    'ın', 'in', 'un', 'ün', 'da', 'de', 'ta', 'te',
    'na', 'ne', 'ya', 'ye', 'yı', 'yi', 'yü', 'yu',
    'ım', 'im', 'um', 'üm', 'nı', 'ni', 'nu', 'nü',
], key=len, reverse=True)


# ---------------------------------------------------------------------------
# JSON I/O
# ---------------------------------------------------------------------------

def load_json(path: Path) -> dict:
    """Load and parse a UTF-8 JSON file."""
    with open(path, encoding='utf-8') as fh:
        return json.load(fh)


def save_json(path: Path, data: dict, pretty: bool = False) -> None:
    """
    Write *data* as JSON to *path*, creating parent directories as needed.
    Use pretty=True for human-readable output (logs, reports).
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'w', encoding='utf-8') as fh:
        if pretty:
            json.dump(data, fh, ensure_ascii=False, indent=2)
        else:
            json.dump(data, fh, ensure_ascii=False, separators=(',', ':'))


# ---------------------------------------------------------------------------
# Text utilities
# ---------------------------------------------------------------------------

def extract_words(text: str) -> List[str]:
    """
    Tokenise *text* into word tokens (apostrophes kept inside tokens).
    Only tokens of length ≥ 2 are returned.
    """
    return [t for t in _TOKEN_RE.findall(text)
            if _WORD_RE.fullmatch(t) and len(t) >= 2]


def tokenise(text: str) -> List[str]:
    """
    Return every token (word and non-word) in order.
    Useful for round-trip correction that must preserve spacing/punctuation.
    """
    return _TOKEN_RE.findall(text)


def turkish_lower(text: str) -> str:
    """
    Case-fold text with correct Turkish dotted/dotless-I handling:
      İ  →  i   (not the ASCII lowercase i)
      I  →  ı   (dotless)
    """
    return text.replace('İ', 'i').replace('I', 'ı').lower()


def extract_root(word: str) -> str:
    """
    Strip common Turkish nominal/verbal suffixes to obtain an approximate root.
    Used for cross-translation vocabulary lookups (we search for the root
    as a substring of candidate words in reference translations).

    Falls back to the first max(4, len-2) characters when no suffix matches.
    """
    w = turkish_lower(word)
    for suffix in _SUFFIXES:
        if w.endswith(suffix) and len(w) - len(suffix) >= 3:
            return w[: len(w) - len(suffix)]
    return w[:max(4, len(w) - 2)]


# ---------------------------------------------------------------------------
# Vocabulary builder
# ---------------------------------------------------------------------------

def build_dictionary(trans_dir: Path, translations: List[str]) -> Counter:
    """
    Build a word-frequency Counter by scanning every verse in *translations*.

    Both the original surface form and its turkish_lower variant are counted
    so that case-insensitive lookups work correctly without re-normalising
    every query.

    Returns: Counter  {word: total_occurrences, ...}
    """
    trans_dir = Path(trans_dir)
    freq: Counter = Counter()

    for trans in translations:
        tdir = trans_dir / trans
        if not tdir.exists():
            continue
        for jf in tdir.rglob('*.json'):
            try:
                data = load_json(jf)
                for verse in data.get('content', []):
                    for w in extract_words(verse.get('text', '')):
                        freq[w] += 1
                        freq[turkish_lower(w)] += 1
            except Exception:
                pass  # malformed file — skip silently

    return freq


# ---------------------------------------------------------------------------
# Book-data helpers
# ---------------------------------------------------------------------------

def load_book_data(book_path: Path) -> List[Tuple[int, dict, Path]]:
    """
    Load all chapter JSON files for a book directory.

    Returns a list of (chapter_number, chapter_dict, file_path) tuples
    sorted by chapter number (ascending).
    """
    book_path = Path(book_path)
    chapters: List[Tuple[int, dict, Path]] = []
    for jf in sorted(book_path.glob('*.json'), key=lambda p: int(p.stem)):
        try:
            data = load_json(jf)
            chapters.append((int(jf.stem), data, jf))
        except Exception:
            pass
    return chapters


def get_all_text(chapters) -> str:
    """
    Concatenate all verse text from a chapters list.
    Accepts either (ch_num, data) or (ch_num, data, path) tuples —
    the data dict is always the second element.
    """
    parts: List[str] = []
    for entry in chapters:
        data = entry[1]
        for verse in data.get('content', []):
            t = verse.get('text', '')
            if t:
                parts.append(t)
    return ' '.join(parts)


# ---------------------------------------------------------------------------
# Character frequency analysis
# ---------------------------------------------------------------------------

# Reference frequencies (per 1 000 chars) from clean Turkish NT translations
REFERENCE_FREQ: Dict[str, float] = {
    'ı': 37.5,
    'ş': 11.5,
    'y': 26.1,
    'b': 17.6,
    'ğ':  8.5,
    'i': 34.8,
    'ç':  8.2,
    'ö':  4.9,
    'ü':  9.1,
}


def char_frequencies(text: str) -> Dict[str, float]:
    """
    Return per-1 000-chars occurrence rates for key Turkish characters.
    Keys: ı i ş y b ğ ç ö ü Y İ
    """
    total = max(len(text), 1)
    chars = 'ıışybğçöüYİ'
    return {c: round(text.count(c) * 1000 / total, 2) for c in chars}
