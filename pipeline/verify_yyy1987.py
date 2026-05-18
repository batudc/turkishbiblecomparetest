#!/usr/bin/env python3
"""
verify_yyy1987.py — Strict post-correction verification of the YYY1987 dataset.

Six focus areas:
  1. Rare word anomalies  (digit artifacts, non-Turkish tokens, low-frequency tokens)
  2. Cross-translation diff  (length outliers, vocabulary mismatches vs TCL02/KMEYA/NWT2025)
  3. Verse structure validation  (missing, duplicate, truncated verses)
  4. Character edge cases  (remaining ?, digits, mixed Latin, wrong diacritics)
  5. Potential uncorrected corruption  (valid words that are likely false negatives)
  6. Contextual mismatch  (words present in YYY but absent from ALL references in same verse)

Output: output/final_verification_report.txt
"""

import re, json, sys, math
from pathlib import Path
from collections import defaultdict, Counter
from itertools import combinations
from typing import Dict, List, Optional, Tuple

PIPELINE = Path(__file__).resolve().parent
PROJECT  = PIPELINE.parent
sys.path.insert(0, str(PIPELINE))

from utils import load_book_data, build_dictionary, tokenise, turkish_lower, extract_root
from fixer import _gf, _split_tok, _WORD_TOKEN, _raw_conf, _protection_penalty, CORRUPT_MAP

YYY_DIR   = PROJECT / 'data' / 'translations' / 'YYY1987'
TRANS_DIR = PROJECT / 'data' / 'translations'
OUTPUT    = PROJECT / 'output'
REFS      = ['TCL02', 'KMEYA', 'NWT2025']

NT_BOOKS = [
    'MAT','MRK','LUK','JHN','ACT','ROM','1CO','2CO','GAL','EPH',
    'PHP','COL','1TH','2TH','1TI','2TI','TIT','PHM','HEB','JAS',
    '1PE','2PE','1JN','2JN','3JN','JUD','REV',
]

CANONICAL = {
    'MAT': {1:25,2:23,3:17,4:25,5:48,6:34,7:29,8:34,9:38,10:42,11:30,12:50,
            13:58,14:36,15:39,16:28,17:27,18:35,19:30,20:34,21:46,22:46,
            23:39,24:51,25:46,26:75,27:66,28:20},
    'MRK': {1:45,2:28,3:35,4:41,5:43,6:56,7:37,8:38,9:50,10:52,11:33,
            12:44,13:37,14:72,15:47,16:8},
    'LUK': {1:80,2:52,3:38,4:44,5:39,6:49,7:50,8:56,9:62,10:42,11:54,
            12:59,13:35,14:35,15:32,16:31,17:37,18:43,19:48,20:47,21:38,
            22:71,23:56,24:53},
    'JHN': {1:51,2:25,3:36,4:54,5:47,6:71,7:53,8:59,9:41,10:42,11:57,
            12:50,13:38,14:31,15:27,16:33,17:26,18:40,19:42,20:31,21:25},
    'ACT': {1:26,2:47,3:26,4:37,5:42,6:15,7:60,8:40,9:43,10:48,11:30,
            12:25,13:52,14:28,15:41,16:40,17:34,18:28,19:41,20:38,21:40,
            22:30,23:35,24:27,25:27,26:32,27:44,28:31},
    'ROM': {1:32,2:29,3:31,4:25,5:21,6:23,7:25,8:39,9:33,10:21,11:36,
            12:21,13:14,14:23,15:33,16:27},
    '1CO': {1:31,2:16,3:23,4:21,5:13,6:20,7:40,8:13,9:27,10:33,11:34,
            12:31,13:13,14:40,15:58,16:24},
    '2CO': {1:24,2:17,3:18,4:18,5:21,6:18,7:16,8:24,9:15,10:18,11:33,
            12:21,13:14},
    'GAL': {1:24,2:21,3:29,4:31,5:26,6:18},
    'EPH': {1:23,2:22,3:21,4:32,5:33,6:24},
    'PHP': {1:30,2:30,3:21,4:23},
    'COL': {1:29,2:23,3:25,4:18},
    '1TH': {1:10,2:20,3:13,4:18,5:28},
    '2TH': {1:12,2:17,3:18},
    '1TI': {1:20,2:15,3:16,4:16,5:25,6:21},
    '2TI': {1:18,2:26,3:17,4:22},
    'TIT': {1:16,2:15,3:15},
    'PHM': {1:25},
    'HEB': {1:14,2:18,3:19,4:16,5:14,6:20,7:28,8:13,9:28,10:39,11:40,
            12:29,13:25},
    'JAS': {1:27,2:26,3:18,4:17,5:20},
    '1PE': {1:25,2:25,3:22,4:19,5:14},
    '2PE': {1:21,2:22,3:18},
    '1JN': {1:10,2:29,3:24,4:21,5:21},
    '2JN': {1:13},
    '3JN': {1:15},
    'JUD': {1:25},
    'REV': {1:20,2:29,3:22,4:11,5:14,6:17,7:17,8:13,9:21,10:11,11:19,
            12:17,13:18,14:20,15:8,16:21,17:18,18:24,19:21,20:15,21:27,22:21},
}

# ---------------------------------------------------------------------------
# Regex patterns for character-level checks
# ---------------------------------------------------------------------------

_DIGIT_IN_WORD  = re.compile(r'[a-zA-ZÇçĞğİıÖöŞşÜü]\d|\d[a-zA-ZÇçĞğİıÖöŞşÜü]')
_QUEST_ANYWHERE = re.compile(r'\?')
_QUOTE_MID_WORD = re.compile(
    r'[a-zA-ZÇçĞğİıÖöŞşÜü]["""„][a-zA-ZÇçĞğİıÖöŞşÜü]'
)
_FOOTNOTE_LEAK  = re.compile(
    r'(?:Dipnot|dipnot|footnot|\bBkz\b|\bbkz\b|\bGrekçede\b|Asıl metinde)',
    re.IGNORECASE,
)
_PURE_LATIN_BLOCK = re.compile(
    r'\b[A-Za-z]{4,}\b'  # 4+ chars that look purely ASCII in Turkish text
)
# Characters that should not appear raw in clean Turkish Bible text
_STRAY_CONTROL  = re.compile(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]')

# Corruption chars that the pipeline's CORRUPT_MAP targets
_CORRUPT_CHARS  = frozenset('YybböÖ')


# ---------------------------------------------------------------------------
# Findings store
# ---------------------------------------------------------------------------

findings: List[dict] = []


def flag(book, ch, v, text_snippet, reason, severity, detail=''):
    findings.append({
        'book': book, 'ch': ch, 'v': v,
        'snippet': text_snippet[:300],
        'reason': reason,
        'severity': severity,  # CRITICAL / HIGH / MEDIUM / LOW
        'detail': detail,
    })


# ---------------------------------------------------------------------------
# Data loaders
# ---------------------------------------------------------------------------

def load_yyy_book(book: str) -> Dict[int, Dict[int, str]]:
    """Return {ch: {v: text}} for a YYY1987 book."""
    book_path = YYY_DIR / book
    if not book_path.exists():
        return {}
    result: Dict[int, Dict[int, str]] = {}
    try:
        raw = load_book_data(book_path)
    except Exception:
        return {}
    for ch_num, data, _ in raw:
        result[ch_num] = {v['v']: v['text'] for v in data.get('content', [])}
    return result


def load_ref_book(ref: str, book: str) -> Dict[int, Dict[int, str]]:
    """Return {ch: {v: text}} for a reference translation book."""
    book_path = TRANS_DIR / ref / book
    if not book_path.exists():
        return {}
    result: Dict[int, Dict[int, str]] = {}
    try:
        raw = load_book_data(book_path)
    except Exception:
        return {}
    for ch_num, data, _ in raw:
        result[ch_num] = {v['v']: v['text'] for v in data.get('content', [])}
    return result


def _word_tokens(text: str) -> List[str]:
    """Extract lowercase Turkish word tokens from text."""
    return [turkish_lower(w) for w in re.findall(r"[a-zA-ZÇçĞğİıÖöŞşÜü''']+", text)
            if len(w) >= 2]


# ---------------------------------------------------------------------------
# Check 1: Structural validation (missing/extra/duplicate verses)
# ---------------------------------------------------------------------------

def check_structure(book: str, yyy_chapters: Dict[int, Dict[int, str]]):
    canon = CANONICAL.get(book, {})
    if not canon:
        return

    yyy_ch_set = set(yyy_chapters.keys())
    canon_ch_set = set(canon.keys())

    missing_chs = sorted(canon_ch_set - yyy_ch_set)
    if missing_chs:
        flag(book, None, None,
             f'Missing chapters: {missing_chs}',
             'MISSING_CHAPTER',
             'CRITICAL',
             f'Canonical chapter count = {len(canon_ch_set)}, found = {len(yyy_ch_set)}')

    for ch, verses in sorted(yyy_chapters.items()):
        v_nums = sorted(verses.keys())
        canon_max = canon.get(ch, 0)

        # Duplicates (shouldn't happen after JSON load, but check)
        seen: set = set()
        for vn in v_nums:
            if vn in seen:
                flag(book, ch, vn, '', 'DUPLICATE_VERSE', 'CRITICAL')
            seen.add(vn)

        # Gaps > 2 consecutive missing verses
        for i in range(len(v_nums) - 1):
            gap = v_nums[i + 1] - v_nums[i]
            if gap > 2:
                missing = list(range(v_nums[i] + 1, v_nums[i + 1]))
                flag(book, ch, v_nums[i],
                     f'Gap after verse {v_nums[i]}',
                     'VERSE_GAP',
                     'CRITICAL',
                     f'Missing verses: {missing}')

        # Chapter too short vs canonical
        if canon_max and v_nums:
            actual_max = max(v_nums)
            if actual_max < canon_max - 3:
                flag(book, ch, actual_max,
                     f'Last verse={actual_max}, canonical max={canon_max}',
                     'TRUNCATED_CHAPTER',
                     'HIGH',
                     f'Missing {canon_max - actual_max} verses at end of chapter')


# ---------------------------------------------------------------------------
# Check 2: Character edge cases per verse
# ---------------------------------------------------------------------------

def check_characters(book: str, ch: int, v: int, text: str):
    # Remaining ? marks
    if _QUEST_ANYWHERE.search(text):
        # Distinguish real question marks (sentence-end) from embedded ones
        for m in _QUEST_ANYWHERE.finditer(text):
            pos = m.start()
            after = text[pos + 1:pos + 4].strip()
            before_char = text[pos - 1] if pos > 0 else ''
            # Embedded ? (not at sentence end) = HIGH issue
            if before_char.isalpha() or (after and after[0].isalpha()):
                flag(book, ch, v,
                     text[max(0, pos - 15):pos + 15],
                     'EMBEDDED_QUESTION_MARK',
                     'HIGH',
                     f'Possible uncorrected OCR artefact at position {pos}')

    # Digit artifacts inside words
    for m in _DIGIT_IN_WORD.finditer(text):
        ctx = text[max(0, m.start() - 10):m.end() + 10]
        flag(book, ch, v, ctx, 'DIGIT_IN_WORD', 'HIGH',
             f'Token: {m.group()!r}')

    # Quote chars mid-word
    for m in _QUOTE_MID_WORD.finditer(text):
        flag(book, ch, v, m.group(), 'QUOTE_CHAR_MID_WORD', 'HIGH',
             'Possible uncorrected OCR quote artifact')

    # Footnote text leaked into verse
    if _FOOTNOTE_LEAK.search(text):
        flag(book, ch, v,
             text[:120],
             'FOOTNOTE_LEAK',
             'HIGH',
             'Footnote keywords detected inside verse text')

    # Control characters
    if _STRAY_CONTROL.search(text):
        flag(book, ch, v, repr(text[:60]), 'CONTROL_CHAR', 'HIGH',
             'Non-printable control character in verse text')

    # Very short verse (< 4 words) — possible truncation
    words = text.split()
    if 0 < len(words) < 5:
        flag(book, ch, v, text, 'VERY_SHORT_VERSE', 'MEDIUM',
             f'Only {len(words)} word(s) — possible truncation or merge')

    # Extremely long verse (> 500 chars) — possible footnote merge
    if len(text) > 500:
        flag(book, ch, v, text[:120] + '…',
             'VERY_LONG_VERSE', 'MEDIUM',
             f'{len(text)} chars — possible footnote contamination')


# ---------------------------------------------------------------------------
# Check 3: Rare word anomalies
# ---------------------------------------------------------------------------

# Tokens that look like OCR garbage
_GARBAGE_TOKEN_RE = re.compile(
    r'(?:'
    r'[a-zA-ZÇçĞğİıÖöŞşÜü][0-9]{2,}'  # letter + 2+ digits
    r'|[0-9]{2,}[a-zA-ZÇçĞğİıÖöŞşÜü]'  # 2+ digits + letter
    r'|[A-Z]{5,}'                         # 5+ consecutive uppercase (likely an artefact)
    r'|[a-zçğışöü]{2,}[A-ZÇĞIŞÖÜ][a-zçğışöü]{2,}'  # CamelCase mid-word
    r')'
)

def check_rare_words(book: str, ch: int, v: int, text: str,
                     freq: Counter, ref_vocab: Counter):
    """Flag words that look anomalous based on vocabulary evidence."""
    tokens = re.findall(r"[a-zA-ZÇçĞğİıÖöŞşÜü''']{3,}", text)
    for tok in tokens:
        lc = turkish_lower(tok)

        # OCR garbage pattern
        if _GARBAGE_TOKEN_RE.search(tok):
            flag(book, ch, v, tok, 'OCR_GARBAGE_TOKEN', 'HIGH',
                 f'Token matches known garbage pattern')

        # Word not in reference vocab OR pipeline frequency at all
        if freq.get(lc, 0) + freq.get(tok, 0) == 0:
            if ref_vocab.get(lc, 0) + ref_vocab.get(tok, 0) == 0:
                # Check root
                root = extract_root(lc)
                if not root or (freq.get(root, 0) + ref_vocab.get(root, 0) < 3):
                    flag(book, ch, v, tok, 'UNKNOWN_TOKEN', 'LOW',
                         'Not found in pipeline freq or ref_vocab (including root)')


# ---------------------------------------------------------------------------
# Check 4: Cross-translation length comparison
# ---------------------------------------------------------------------------

def check_length_vs_refs(book: str, ch: int, v: int, yyy_text: str,
                          ref_texts: List[str]):
    """Flag when YYY verse length deviates sharply from all references."""
    if not ref_texts:
        return
    yyy_wc = len(yyy_text.split())
    ref_wcs = [len(t.split()) for t in ref_texts if t.strip()]
    if not ref_wcs:
        return
    ref_median = sorted(ref_wcs)[len(ref_wcs) // 2]
    if ref_median < 3:
        return  # too short to compare meaningfully

    ratio = yyy_wc / ref_median
    if ratio < 0.35:
        flag(book, ch, v,
             yyy_text[:120],
             'VERSE_TOO_SHORT_VS_REFS',
             'HIGH',
             f'YYY={yyy_wc}w, refs_median={ref_median}w (ratio={ratio:.2f}). '
             f'Ref samples: {ref_texts[0][:80]}…')
    elif ratio > 3.5:
        flag(book, ch, v,
             yyy_text[:120],
             'VERSE_TOO_LONG_VS_REFS',
             'MEDIUM',
             f'YYY={yyy_wc}w, refs_median={ref_median}w (ratio={ratio:.2f}). '
             'Possible footnote merge')


# ---------------------------------------------------------------------------
# Check 5: Potential uncorrected corruption (false negatives)
# ---------------------------------------------------------------------------

# These pairs map (corrupt_char → correct_char) for mid-word positions
_CORRUPTION_PAIRS = [
    ('b', 'ş'),
    ('y', 'ı'),
    ('Y', 'İ'),
    ('ö', 'ğ'),
]

def _substitute_one(word: str, pos: int, old: str, new: str) -> str:
    return word[:pos] + new + word[pos + 1:]


def check_uncorrected_corruption(book: str, ch: int, v: int, text: str,
                                  freq: Counter):
    """
    For each word in the verse that contains a potentially-corrupt character,
    check whether substituting that character gives a much higher-frequency word.
    Only flag when the substitution is compelling (large freq ratio AND original
    is low-freq).
    """
    tokens = re.findall(r"[a-zA-ZÇçĞğİıÖöŞşÜü''']{3,}", text)
    for tok in tokens:
        prefix, core, suffix = _split_tok(tok)
        orig_f = _gf(core, freq)
        if orig_f >= 200:
            # Very common word — almost certainly correct
            continue

        for old, new in _CORRUPTION_PAIRS:
            positions = [i for i, c in enumerate(core) if c == old]
            if not positions:
                continue
            for pos in positions:
                candidate_core = _substitute_one(core, pos, old, new)
                cand_f = _gf(candidate_core, freq)
                if cand_f < 20:
                    continue  # candidate not well-known either
                # Ratio-based check: candidate should dominate strongly
                if orig_f == 0 and cand_f >= 50:
                    ratio_label = f'orig_f=0 → cand_f={cand_f}'
                    flag(book, ch, v,
                         tok,
                         'UNCORRECTED_CORRUPTION_CANDIDATE',
                         'MEDIUM',
                         f'"{tok}" → "{prefix+candidate_core+suffix}" '
                         f'({old}→{new} at pos {pos}); {ratio_label}. '
                         f'May have been protected by fixer (orig was valid).')
                    break  # one flag per token
                elif orig_f > 0 and cand_f / (orig_f + 1) >= 20:
                    flag(book, ch, v,
                         tok,
                         'UNCORRECTED_CORRUPTION_CANDIDATE',
                         'MEDIUM',
                         f'"{tok}" → "{prefix+candidate_core+suffix}" '
                         f'({old}→{new} at pos {pos}); '
                         f'orig_f={orig_f}, cand_f={cand_f}, '
                         f'ratio={cand_f/(orig_f+1):.1f}')
                    break


# ---------------------------------------------------------------------------
# Check 6: Vocabulary cross-reference (words absent from ALL refs in same verse)
# ---------------------------------------------------------------------------

def check_vocab_vs_refs(book: str, ch: int, v: int, yyy_text: str,
                         ref_texts: List[str], freq: Counter):
    """
    For each word in YYY that contains a corruption-prone character,
    check if it appears in none of the reference verses but its 'corrected'
    form does.
    """
    if not ref_texts:
        return

    # Build combined ref word set for this verse
    ref_words: Counter = Counter()
    for rt in ref_texts:
        for w in _word_tokens(rt):
            ref_words[w] += 1

    yyy_words = _word_tokens(yyy_text)

    for word in yyy_words:
        # Only check words with corruption-prone characters
        if not any(c in word for c in 'yböY'):
            continue
        if freq.get(word, 0) >= 500:
            continue  # very common — skip
        if ref_words.get(word, 0) > 0:
            continue  # present in at least one reference

        # Try all single-char substitutions
        for old, new in _CORRUPTION_PAIRS:
            positions = [i for i, c in enumerate(word) if c == old]
            for pos in positions:
                cand = _substitute_one(word, pos, old, new)
                if ref_words.get(cand, 0) >= 2:  # appears in 2+ ref verses
                    flag(book, ch, v,
                         word,
                         'WORD_IN_YYY_NOT_IN_REFS',
                         'HIGH',
                         f'"{word}" not in any ref verse; '
                         f'substituted form "{cand}" ({old}→{new}) appears '
                         f'{ref_words[cand]}× in refs. Possible false-negative correction.')
                    break


# ---------------------------------------------------------------------------
# Main scan
# ---------------------------------------------------------------------------

def main():
    OUTPUT.mkdir(exist_ok=True)
    report_path = OUTPUT / 'final_verification_report.txt'

    print('Building pipeline vocabulary...')
    freq = build_dictionary(TRANS_DIR, ['TCL02', 'KMEYA', 'NWT2025'])
    print(f'  {len(freq):,} word forms')

    # Also build a flat "all reference words" vocabulary for rare-word checks
    print('Building flat reference vocabulary...')
    ref_vocab: Counter = Counter()
    for ref in REFS:
        ref_dir = TRANS_DIR / ref
        if not ref_dir.exists():
            continue
        for book in NT_BOOKS:
            book_path = ref_dir / book
            if not book_path.exists():
                continue
            try:
                raw = load_book_data(book_path)
            except Exception:
                continue
            for _, data, _ in raw:
                for verse in data.get('content', []):
                    for w in _word_tokens(verse.get('text', '')):
                        ref_vocab[w] += 1
    print(f'  {len(ref_vocab):,} unique words in reference translations')

    print(f'\nScanning {len(NT_BOOKS)} books...')

    total_verses = 0
    for book in NT_BOOKS:
        yyy_chapters = load_yyy_book(book)
        if not yyy_chapters:
            flag(book, None, None, '', 'BOOK_MISSING', 'CRITICAL',
                 'Book directory not found in YYY1987')
            continue

        # Load reference data for this book
        ref_books = {ref: load_ref_book(ref, book) for ref in REFS}

        # Structure check (chapter/verse level)
        check_structure(book, yyy_chapters)

        for ch in sorted(yyy_chapters.keys()):
            verses = yyy_chapters[ch]
            for v in sorted(verses.keys()):
                text = verses[v]
                total_verses += 1

                # Gather reference texts for this verse
                ref_texts = []
                for ref in REFS:
                    rt = ref_books.get(ref, {}).get(ch, {}).get(v, '')
                    if rt:
                        ref_texts.append(rt)

                # Run all checks
                check_characters(book, ch, v, text)
                check_rare_words(book, ch, v, text, freq, ref_vocab)
                check_length_vs_refs(book, ch, v, text, ref_texts)
                check_uncorrected_corruption(book, ch, v, text, freq)
                check_vocab_vs_refs(book, ch, v, text, ref_texts, freq)

        print(f'  {book}: {sum(len(v) for v in yyy_chapters.values())} verses')

    # ---------------------------------------------------------------------------
    # Report
    # ---------------------------------------------------------------------------
    sev_order = {'CRITICAL': 0, 'HIGH': 1, 'MEDIUM': 2, 'LOW': 3}
    findings.sort(key=lambda f: (
        sev_order.get(f['severity'], 9),
        f['book'],
        f['ch'] or 0,
        f['v'] or 0,
    ))

    counts = Counter(f['severity'] for f in findings)
    by_reason = Counter(f['reason'] for f in findings)

    lines = []
    lines.append('=' * 70)
    lines.append('YYY1987 FINAL VERIFICATION REPORT')
    lines.append('=' * 70)
    lines.append(f'Total verses scanned : {total_verses:,}')
    lines.append(f'Total issues found   : {len(findings):,}')
    lines.append('')
    lines.append('By severity:')
    for sev in ['CRITICAL', 'HIGH', 'MEDIUM', 'LOW']:
        lines.append(f'  {sev:10}: {counts.get(sev, 0):,}')
    lines.append('')
    lines.append('By reason (top 30):')
    for reason, cnt in by_reason.most_common(30):
        lines.append(f'  {reason:45}: {cnt:,}')
    lines.append('')
    lines.append('=' * 70)
    lines.append('DETAILED FINDINGS')
    lines.append('=' * 70)

    current_sev = None
    for f in findings:
        if f['severity'] != current_sev:
            current_sev = f['severity']
            lines.append('')
            lines.append(f'── {current_sev} ──────────────────────────────────────')

        loc = f'{f["book"]}'
        if f['ch'] is not None:
            loc += f' {f["ch"]}:{f["v"]}'
        lines.append(f'\n[{f["severity"]}] {loc}')
        lines.append(f'  Reason  : {f["reason"]}')
        lines.append(f'  Snippet : {f["snippet"]}')
        if f['detail']:
            lines.append(f'  Detail  : {f["detail"]}')

    report = '\n'.join(lines)
    report_path.write_text(report, encoding='utf-8')
    print(f'\n{"=" * 60}')
    print(f'  Verses scanned  : {total_verses:,}')
    print(f'  Issues found    : {len(findings):,}')
    print(f'  CRITICAL        : {counts.get("CRITICAL", 0):,}')
    print(f'  HIGH            : {counts.get("HIGH", 0):,}')
    print(f'  MEDIUM          : {counts.get("MEDIUM", 0):,}')
    print(f'  LOW             : {counts.get("LOW", 0):,}')
    print(f'{"=" * 60}')
    print(f'Report → {report_path}')


if __name__ == '__main__':
    main()
