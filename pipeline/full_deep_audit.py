#!/usr/bin/env python3
"""
full_deep_audit.py — Deep validation and correction for all 27 YYY1987 NT books.

Correction passes per verse (in order):
  1. Footnote contamination removal
  2. Prefix artifact cleaning  (>, -, ", '' at verse start)
  3. Inline reference marker removal  (e.g. 7S4, 1S4 embedded in text)
  4. ? replacement  (vocab-validated: ğ or ş or ı or i)
  5. " mid-word replacement  (vocab-validated: ş or ğ)
  6. Digit-in-word correction  (vocab-validated, also tries extended map on top)
  7. Extended vocabulary-guided correction  (fixer + ı→i with extra penalty)

Writes corrections directly to YYY1987 source JSON files.

Outputs:
    output/full_audit_log.txt      — per-verse change log (TSV)
    output/structure_issues.json   — structural problems per book
"""

import re, json, sys, copy
from pathlib import Path
from collections import defaultdict
from itertools import combinations, product as iprod

PIPELINE = Path(__file__).resolve().parent
PROJECT  = PIPELINE.parent
sys.path.insert(0, str(PIPELINE))

from utils import (
    load_book_data, save_json, build_dictionary, tokenise, turkish_lower, extract_root,
)
from fixer import (
    _split_tok, _gf, CONF_APPLY, CONF_FLAG,
    _WORD_TOKEN, _raw_conf, _protection_penalty,
)

TRANS_DIR = PROJECT / 'data' / 'translations'
YYY_DIR   = TRANS_DIR / 'YYY1987'
OUTPUT    = PROJECT / 'output'
REF_TRANS = ['TCL02', 'KMEYA', 'NWT2025']

# ---------------------------------------------------------------------------
# Regex patterns
# ---------------------------------------------------------------------------

# Footnote contamination at end of verse text
_FOOTNOTE_RE = re.compile(
    r'\s{0,3}[A-Z0-9İI]{1,4}\s+Dipnotlar[ıiy]?[ıiy]?:?.*$',
    re.DOTALL | re.IGNORECASE,
)
_FOOTNOTE2_RE = re.compile(
    r'\s+[0-9]\s+[A-Z]{1,3}\s+Dipnot.*$',
    re.DOTALL | re.IGNORECASE,
)

# Inline reference markers like " 7S4Ben" → " Ben"
_INLINE_REF_RE = re.compile(
    r'\s+[0-9]{1,2}[A-Z]{1,3}[0-9]{1,2}(?=[A-ZÇĞİÖŞÜa-zçğışöü])'
)

# Prefix artifacts at start of verse text
_GT_PREFIX_RE   = re.compile(r'^>\s*')
_DASH_CAP_RE    = re.compile(r'^-(?=[A-ZÇĞİÖŞÜ])')
_QUOTE_CAP_RE   = re.compile(r'^[""„]{1,2}(?=[A-ZÇĞİÖŞÜ])')
_APOS_CAP_RE    = re.compile(r"^[''']{1,2}(?=[A-ZÇĞİÖŞÜ])")

# Turkish letter set (for context extraction)
_TR_SET = frozenset(
    "abcdefghijklmnopqrstuvwxyz"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "ÇçĞğİıÖöŞşÜü"
    "'''"
)

# Mid-verse stray markers: "> " or "< " surrounded by spaces
_MID_GT_RE = re.compile(r'\s+[<>]\s+')
# Orphan ? at very start of text (no valid word substitution found — just remove it)
_ORPHAN_QUEST_RE = re.compile(r'^\?(?=[A-ZÇĞİÖŞÜ])')

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_MAX_COMBOS = 512

# Extended corruption map (adds ı→i with extra penalty)
EXTENDED_MAP = {'Y': 'İ', 'y': 'ı', 'b': 'ş', 'ö': 'ğ', 'ı': 'i'}

# Digit → possible character candidates
DIGIT_CANDS = {
    '0': ['o', 'ö', 'ğ'],
    '1': ['i', 'ı', 'l'],
    '2': ['z', 'ş'],
    '3': ['ş', 'ğ'],
    '4': ['ş'],
    '5': ['ş', 's'],
    '6': ['ğ', 'b', 'ö'],
    '7': ['t'],
    '8': ['ş', 'ğ'],
}

# ? → possible character candidates (lowercase position)
QUEST_LOWER_CANDS = ['ğ', 'ş', 'ı', 'i']
# ? → possible character candidates (start-of-word position)
QUEST_START_CANDS = ['ş', 'ğ', 'İ', 'Ş', 'Ğ']

# " mid-word → possible character candidates
QUOTE_MID_CANDS = ['ş', 'ğ', 'i', 'ı']

# ---------------------------------------------------------------------------
# Canonical NT verse counts (for structure validation)
# ---------------------------------------------------------------------------

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
# Extended best-correction (adds ı→i with higher penalty)
# ---------------------------------------------------------------------------

def _extended_best_correction(core, freq):
    """
    Like fixer._best_correction but uses EXTENDED_MAP (includes ı→i).
    The ı→i substitution carries an extra 0.15 penalty to reduce false corrections.
    """
    orig_f  = _gf(core, freq)
    corrupt = [(i, c) for i, c in enumerate(core) if c in EXTENDED_MAP]

    if not corrupt:
        return core, 1.0, 0

    penalty  = _protection_penalty(orig_f)
    best_w, best_c, best_n = core, 0.0, 0
    chars    = list(core)
    n_combos = 0

    for n in range(1, len(corrupt) + 1):
        for combo in combinations(corrupt, n):
            if n_combos >= _MAX_COMBOS:
                break
            n_combos += 1
            cand         = chars[:]
            extra_penalty = sum(0.15 for _, ch in combo if ch == 'ı')
            for idx, ch in combo:
                cand[idx] = EXTENDED_MAP[ch]
            w  = ''.join(cand)
            cf = _gf(w, freq)
            c  = max(0.0, _raw_conf(orig_f, cf) - penalty
                     - (n - 1) * 0.05 - extra_penalty)
            if c > best_c:
                best_w, best_c, best_n = w, c, n
        if n_combos >= _MAX_COMBOS:
            break

    return best_w, best_c, best_n


# ---------------------------------------------------------------------------
# Helper: word plausibility score (full word OR root frequency)
# ---------------------------------------------------------------------------

def _word_score(word, freq):
    """
    Return the best available frequency evidence for *word*:
      1. Direct full-word frequency
      2. Root frequency (handles rare inflected forms like dinleyeceğinden)
    """
    f = _gf(word, freq)
    if f >= 5:
        return f
    root = extract_root(turkish_lower(word))
    rf   = _gf(root, freq)
    # Root evidence is weaker — only use for very high-frequency roots
    return rf // 4 if rf >= 80 else f


def _fully_correct(word, freq):
    """Apply extended correction to *word* and return best corrected form."""
    prefix, core, suffix = _split_tok(word)
    corr_core, conf, _   = _extended_best_correction(core, freq)
    if conf >= CONF_APPLY:
        return prefix + corr_core + suffix
    return word


# ---------------------------------------------------------------------------
# Pass 1 & 2: Artifact cleaning
# ---------------------------------------------------------------------------

def _clean_artifacts(text):
    """Remove footnote contamination, prefix artifacts, inline reference markers."""
    changes = []

    # Footnote contamination at end
    for pat in (_FOOTNOTE_RE, _FOOTNOTE2_RE):
        new = pat.sub('', text).rstrip()
        if new != text:
            stripped = text[len(new):].strip()[:60]
            changes.append({'type': 'FOOTNOTE_STRIPPED', 'action': 'applied',
                            'stripped': stripped})
            text = new

    # Inline reference markers (7S4Ben → Ben)
    new = _INLINE_REF_RE.sub(' ', text)
    if new != text:
        changes.append({'type': 'INLINE_REF_REMOVED', 'action': 'applied'})
        text = new

    # Mid-verse stray > or < surrounded by spaces
    new = _MID_GT_RE.sub(' ', text)
    if new != text:
        changes.append({'type': 'MID_GT_REMOVED', 'action': 'applied'})
        text = new

    # Prefix artifacts (>, -, ", '')
    for pat, name in [
        (_GT_PREFIX_RE,  'GT_PREFIX'),
        (_DASH_CAP_RE,   'DASH_PREFIX'),
        (_QUOTE_CAP_RE,  'QUOTE_PREFIX'),
        (_APOS_CAP_RE,   'APOS_PREFIX'),
    ]:
        new = pat.sub('', text)
        if new != text:
            changes.append({'type': name + '_REMOVED', 'action': 'applied'})
            text = new

    # Orphan ? at text start before capital (no word substitution possible)
    new = _ORPHAN_QUEST_RE.sub('', text)
    if new != text:
        changes.append({'type': 'ORPHAN_QUEST_REMOVED', 'action': 'applied'})
        text = new

    return text.strip(), changes


# ---------------------------------------------------------------------------
# Pass 3: ? replacement (vocab-validated)
# ---------------------------------------------------------------------------

def _fix_question_marks(text, freq):
    """
    Replace each ? with the best vocab-validated character.
    For each candidate (ğ, ş, ı, i …) we also apply extended correction on top,
    so double-corrupted forms like kayna?y → kaynağı are handled in one pass.
    """
    if '?' not in text:
        return text, []

    changes   = []
    txt_list  = list(text)
    positions = [i for i, c in enumerate(txt_list) if c == '?']

    for pos in reversed(positions):
        # Expand to full word boundaries
        start = pos - 1
        while start >= 0 and txt_list[start] in _TR_SET:
            start -= 1
        start += 1

        end = pos + 1
        while end < len(txt_list) and txt_list[end] in _TR_SET:
            end += 1

        left  = ''.join(txt_list[start:pos])
        right = ''.join(txt_list[pos + 1:end])

        # Skip if right side has no real Turkish letter (e.g. ?'' at sentence end)
        if not right or not any(c.isalpha() and c not in "'''''‘’“”" for c in right):
            continue

        cands = QUEST_START_CANDS if not left else QUEST_LOWER_CANDS
        best_sub, best_score, best_final = None, 4, None

        for sub in cands:
            word = left + sub + right
            # Direct frequency
            s = _word_score(word, freq)
            if s > best_score:
                best_sub, best_score, best_final = sub, s, word
            # Also try with extended correction applied on top
            word2 = _fully_correct(word, freq)
            if word2 != word:
                s2 = _word_score(word2, freq)
                if s2 > best_score:
                    best_sub, best_score, best_final = sub, s2, word2

        if best_sub is not None:
            orig_word = left + '?' + right
            changes.append({
                'type': 'QUEST_SUB', 'action': 'applied',
                'orig': orig_word, 'fixed': best_final,
                'fixed_freq': best_score,
            })
            # Splice the corrected word back into txt_list
            txt_list[start:end] = list(best_final)
            # Adjust positions of remaining ? entries (all to the left — reversed)

    return ''.join(txt_list), changes


# ---------------------------------------------------------------------------
# Pass 4: " mid-word replacement (vocab-validated)
# ---------------------------------------------------------------------------

_QUOTE_CHARS = frozenset('"“”„')

def _fix_quote_chars(text, freq):
    """Replace " characters mid-word with ş or ğ (vocab-validated, + extended correction)."""
    if not any(c in _QUOTE_CHARS for c in text):
        return text, []

    changes   = []
    txt_list  = list(text)
    positions = [i for i, c in enumerate(txt_list) if c in _QUOTE_CHARS]

    for pos in reversed(positions):
        start = pos - 1
        while start >= 0 and txt_list[start] in _TR_SET:
            start -= 1
        start += 1

        end = pos + 1
        while end < len(txt_list) and txt_list[end] in _TR_SET:
            end += 1

        left  = ''.join(txt_list[start:pos])
        right = ''.join(txt_list[pos + 1:end])

        # Only fix mid-word occurrences where both sides have real Turkish letters
        if not (left and right):
            continue
        if not any(c.isalpha() and c not in "'''''''\"\"\"„" for c in right):
            continue

        best_sub, best_score, best_final = None, 4, None
        for sub in QUOTE_MID_CANDS:
            word  = left + sub + right
            s     = _word_score(word, freq)
            word2 = _fully_correct(word, freq)
            s2    = _word_score(word2, freq) if word2 != word else 0
            if max(s, s2) > best_score:
                best_score = max(s, s2)
                best_sub   = sub
                best_final = word2 if s2 >= s else word

        if best_sub is not None:
            orig_q = left + txt_list[pos] + right
            changes.append({
                'type': 'QUOTE_SUB', 'action': 'applied',
                'orig': orig_q, 'fixed': best_final,
                'fixed_freq': best_score,
            })
            txt_list[start:end] = list(best_final)

    return ''.join(txt_list), changes


# ---------------------------------------------------------------------------
# Pass 5: Digit-in-word correction (vocab-validated)
# ---------------------------------------------------------------------------

_DIGIT_WORD_RE = re.compile(
    r'[a-zA-ZÇçĞğİıÖöŞşÜü][a-zA-ZÇçĞğİıÖöŞşÜü0-9]*[0-9][a-zA-ZÇçĞğİıÖöŞşÜü0-9]*'
    r'|[0-9]+[a-zA-ZÇçĞğİıÖöŞşÜü][a-zA-ZÇçĞğİıÖöŞşÜü0-9]*',
    re.UNICODE,
)


def _fix_digit_word(word, freq):
    """Try all digit substitutions (+ extended map on top) and return best."""
    digits = [(i, c) for i, c in enumerate(word) if c.isdigit() and c != '9']
    if not digits:
        return word, None

    orig_f = _gf(word, freq)
    if orig_f >= 50:        # already a recognised word → leave it
        return word, None

    chars      = list(word)
    best_w     = word
    best_score = max(orig_f, 4)
    n_combos   = 0

    for n in range(1, min(len(digits), 4) + 1):
        for combo in combinations(digits, n):
            cands = [DIGIT_CANDS.get(c, []) for _, c in combo]
            if not all(cands):
                continue
            for subs in iprod(*cands):
                if n_combos >= _MAX_COMBOS:
                    break
                n_combos += 1
                cand = chars[:]
                for (idx, _), sub in zip(combo, subs):
                    cand[idx] = sub
                w = ''.join(cand)

                # Check digit-only substituted form
                f = _gf(w, freq)
                if f > best_score:
                    best_w, best_score = w, f

                # Also apply extended correction on top (handles double corruption)
                _, core, _ = _split_tok(w)
                corr_core, conf, _ = _extended_best_correction(core, freq)
                if conf >= CONF_APPLY:
                    pre, _, suf = _split_tok(w)
                    w2 = pre + corr_core + suf
                    f2 = _gf(w2, freq)
                    if f2 > best_score:
                        best_w, best_score = w2, f2

            if n_combos >= _MAX_COMBOS:
                break
        if n_combos >= _MAX_COMBOS:
            break

    if best_w != word and best_score >= 5:
        return best_w, {
            'type': 'DIGIT_SUB', 'action': 'applied',
            'orig': word, 'fixed': best_w,
            'orig_freq': orig_f, 'fixed_freq': best_score,
        }
    return word, None


def _fix_digits_in_text(text, freq):
    """Apply digit-in-word correction across an entire verse text."""
    if not any(c.isdigit() and c != '9' for c in text):
        return text, []

    changes = []

    def _replace(m):
        word = m.group()
        fixed, ch = _fix_digit_word(word, freq)
        if ch:
            changes.append(ch)
        return fixed

    return _DIGIT_WORD_RE.sub(_replace, text), changes


# ---------------------------------------------------------------------------
# Pass 6: Extended vocabulary-guided correction
# ---------------------------------------------------------------------------

def _fix_verse_extended(text, freq):
    """
    Apply word-level correction using EXTENDED_MAP (fixer + ı→i).
    Only tokens whose core contains at least one corruptible character are checked.
    """
    tokens  = tokenise(text)
    out     = []
    changes = []

    for tok in tokens:
        if not (_WORD_TOKEN.fullmatch(tok) and len(tok) >= 2):
            out.append(tok)
            continue

        prefix, core, suffix = _split_tok(tok)
        fixed_core, conf, n = _extended_best_correction(core, freq)

        if n == 0 or fixed_core == core:
            out.append(tok)
            continue

        fw = prefix + fixed_core + suffix

        if conf >= CONF_APPLY:
            action, result = 'applied', fw
        elif conf >= CONF_FLAG:
            action, result = 'flagged_med', tok
        else:
            action, result = 'flagged_low', tok

        out.append(result)
        changes.append({
            'orig': tok, 'fixed': fw,
            'confidence': round(conf, 3), 'action': action,
            'type': 'char_sub', 'n_subs': n,
            'orig_freq': _gf(core, freq), 'fixed_freq': _gf(fixed_core, freq),
        })

    return ''.join(out), changes


# ---------------------------------------------------------------------------
# Structure validation
# ---------------------------------------------------------------------------

def _check_structure(book, chapters):
    """Check chapter and verse structure against canonical NT counts."""
    issues = []
    canonical = CANONICAL.get(book, {})
    ch_dict   = {ch: data for ch, data in chapters}

    if canonical:
        expected = set(canonical.keys())
        actual   = set(ch_dict.keys())
        missing  = sorted(expected - actual)
        extra    = sorted(actual   - expected)
        if missing:
            issues.append({'type': 'MISSING_CHAPTERS', 'chapters': missing})
        if extra:
            issues.append({'type': 'EXTRA_CHAPTERS', 'chapters': extra})

    for ch, data in sorted(chapters, key=lambda x: x[0]):
        verses = data.get('content', [])
        v_nums = [v['v'] for v in verses]
        if not v_nums:
            issues.append({'type': 'EMPTY_CHAPTER', 'ch': ch})
            continue

        seen = set()
        for n in v_nums:
            if n in seen:
                issues.append({'type': 'DUPLICATE_VERSE', 'ch': ch, 'v': n})
            seen.add(n)

        if v_nums != sorted(v_nums):
            issues.append({'type': 'OUT_OF_ORDER', 'ch': ch})

        sorted_v = sorted(seen)
        for i in range(len(sorted_v) - 1):
            gap = sorted_v[i + 1] - sorted_v[i]
            if gap > 1:
                missing_vs = list(range(sorted_v[i] + 1, sorted_v[i + 1]))
                issues.append({'type': 'VERSE_GAP', 'ch': ch,
                               'missing': missing_vs})

        if canonical and ch in canonical:
            canon_max  = canonical[ch]
            actual_max = max(v_nums)
            if actual_max < canon_max - 2:
                issues.append({'type': 'CHAPTER_TOO_SHORT', 'ch': ch,
                               'expected': canon_max, 'actual': actual_max})
            elif actual_max > canon_max + 2:
                issues.append({'type': 'CHAPTER_TOO_LONG', 'ch': ch,
                               'expected': canon_max, 'actual': actual_max})

    return issues


# ---------------------------------------------------------------------------
# Per-book audit
# ---------------------------------------------------------------------------

def audit_book(book_path, freq, log_lines, struct_reports):
    book    = book_path.name
    raw     = load_book_data(book_path)
    if not raw:
        return {'book': book, 'error': 'NO_DATA'}

    chapters      = [(ch, data) for ch, data, _ in raw]
    struct_issues = _check_structure(book, chapters)
    if struct_issues:
        struct_reports[book] = struct_issues

    total_v = applied = flagged = 0

    for ch_num, data, jf in raw:
        ch_data    = copy.deepcopy(data)
        ch_changed = False

        for verse in ch_data.get('content', []):
            total_v  += 1
            orig_text = verse['text']
            text      = orig_text
            all_ch    = []

            # Pass 1+2: artifact cleaning
            text, c = _clean_artifacts(text)
            all_ch += c

            # Pass 3: ? substitution
            text, c = _fix_question_marks(text, freq)
            all_ch += c

            # Pass 4: " mid-word substitution
            text, c = _fix_quote_chars(text, freq)
            all_ch += c

            # Pass 5: digit-in-word correction
            text, c = _fix_digits_in_text(text, freq)
            all_ch += c

            # Pass 6: extended vocab correction
            text, c = _fix_verse_extended(text, freq)
            all_ch += c

            if text == orig_text:
                continue

            verse['text'] = text
            ch_changed    = True
            ref           = f'{book} {ch_num}:{verse["v"]}'

            for ch in all_ch:
                act = ch.get('action', '')
                if act == 'applied' or ch.get('type', '').endswith(
                        ('STRIPPED', 'REMOVED', 'QUEST_SUB', 'QUOTE_SUB', 'DIGIT_SUB')):
                    applied += 1
                    reason   = ch.get('type', '?')
                    orig_w   = ch.get('orig', orig_text[:40])
                    fixed_w  = ch.get('fixed', text[:40])
                    log_lines.append(
                        f'{ref}\t{orig_w[:50]}\t{fixed_w[:50]}\t{reason}'
                    )
                elif act.startswith('flagged'):
                    flagged += 1
                    log_lines.append(
                        f'{ref}\t{ch.get("orig","")[:50]}\t[FLAG:{act}]\t'
                        f'conf={ch.get("confidence",0):.2f}'
                    )

        if ch_changed:
            jf.write_text(
                json.dumps(ch_data, ensure_ascii=False, separators=(',', ':')),
                encoding='utf-8',
            )

    return {
        'book':          book,
        'total_verses':  total_v,
        'applied':       applied,
        'flagged':       flagged,
        'struct_issues': len(struct_issues),
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    OUTPUT.mkdir(parents=True, exist_ok=True)

    print('Building vocabulary from reference translations...')
    from collections import Counter
    freq = build_dictionary(TRANS_DIR, REF_TRANS)
    print(f'  {len(freq):,} word forms\n')

    books = sorted(p.name for p in YYY_DIR.iterdir() if p.is_dir())
    print(f'Deep-auditing {len(books)} books...')
    print(f'  {"Book":6}  {"Applied":>8}  {"Flagged":>8}  {"Struct":>6}')
    print('  ' + '─' * 36)

    log_lines    = ['REF\tBEFORE\tAFTER\tREASON']
    struct_rpts  = {}
    summaries    = []

    for book in books:
        result = audit_book(YYY_DIR / book, freq, log_lines, struct_rpts)
        summaries.append(result)
        a  = result.get('applied', 0)
        fl = result.get('flagged', 0)
        st = result.get('struct_issues', 0)
        line = f'  {book:6}  {a:>8}  {fl:>8}  {st:>6}'
        if st > 0 or a > 0:
            line += '  ←'
        print(line)

    # Write audit log
    log_path = OUTPUT / 'full_audit_log.txt'
    log_path.write_text('\n'.join(log_lines), encoding='utf-8')

    # Write structure report
    save_json(OUTPUT / 'structure_issues.json', struct_rpts, pretty=True)

    # Summary
    total_v  = sum(s.get('total_verses', 0) for s in summaries)
    total_a  = sum(s.get('applied',      0) for s in summaries)
    total_fl = sum(s.get('flagged',      0) for s in summaries)
    total_st = sum(s.get('struct_issues',0) for s in summaries)

    print(f'\n{"═"*50}')
    print(f'  Verses checked        : {total_v:,}')
    print(f'  Corrections applied   : {total_a:,}')
    print(f'  Items flagged         : {total_fl:,}')
    print(f'  Structural issues     : {total_st:,}')
    print(f'  Log  →  {log_path}')
    print(f'  Struct → {OUTPUT/"structure_issues.json"}')
    print(f'{"═"*50}')

    if struct_rpts:
        print('\nStructural issues summary:')
        for book, issues in struct_rpts.items():
            types = ', '.join(set(i['type'] for i in issues))
            print(f'  {book}: {types}')


if __name__ == '__main__':
    main()
