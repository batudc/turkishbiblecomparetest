#!/usr/bin/env python3
"""
Reconstruct YYY1987 Bible translation into individual verses
matching TCL02 verse structure.
"""

import json
import re
import os
import unicodedata

BASE = "/Users/batuhandemircan/website building/data/translations"
SRC  = os.path.join(BASE, "YYY1987")
REF  = os.path.join(BASE, "TCL02")
OUT  = os.path.join(BASE, "YYY1987_REBUILT")

# ─── garbage stripping ───────────────────────────────────────────────────────

GARBAGE_PATTERNS = [
    # cross-references like 3:16, 5:1-3, 12:1-5
    r'\b\d{1,2}:\d{1,2}(?:-\d{1,2})?\b',
    # abbreviated book refs like Kol.3:4, Mat.5:1-3, Rom.8:28
    r'\b[A-ZÇĞİÖŞÜa-zçğıöşü]{2,4}\.\d{1,2}:\d{1,2}(?:-\d{1,2})?\b',
    # "bkz." followed by optional text until punctuation
    r'bkz\.\s*\S+',
    # section headers / footnote markers
    r'Dipnotları\b',
    r'Kaynak ayetler\b',
    r'\bELÇ\b',
    # lone uppercase abbreviations at word boundary (e.g. ELÇ, YSA)
    r'\b[A-Z]{3,5}\b',
    # Roman numerals standing alone (I-XII common)
    r'\b(?:X{0,3}(?:IX|IV|V?I{0,3}))\b',
    # stray lone digits
    r'(?<!\w)\d(?!\w)',
    # Copyright / publisher symbols that crept in
    r'[©®™]',
    # decorative section headers embedded as text (title-case lines mid-text)
    # e.g. "Meryem, Elizabet'i ziyaret ediyor" — we keep these as they are
    # verse boundary markers from OCR: odd single chars like © ! at start
]

_GARBAGE_RE = re.compile('|'.join(GARBAGE_PATTERNS), re.UNICODE)


def strip_garbage(text: str) -> str:
    """Remove cross-refs, footnote markers, stray digits, etc."""
    text = _GARBAGE_RE.sub(' ', text)
    # collapse multiple spaces
    text = re.sub(r'  +', ' ', text)
    return text.strip()


# ─── text similarity helpers ─────────────────────────────────────────────────

def normalize(s: str) -> str:
    """Lower-case, strip diacritics/punctuation for fuzzy matching."""
    s = s.lower()
    # NFC normalisation keeps Turkish chars; NFKD would strip them
    s = re.sub(r'[^\w\s]', ' ', s, flags=re.UNICODE)
    return re.sub(r'\s+', ' ', s).strip()


def word_overlap(a: str, b: str) -> float:
    wa = set(normalize(a).split())
    wb = set(normalize(b).split())
    if not wa or not wb:
        return 0.0
    return len(wa & wb) / max(len(wa), len(wb))


def starts_overlap(candidate: str, anchor: str, n: int = 6) -> float:
    """How many of first n words of anchor appear (in order) in candidate."""
    anchor_words = normalize(anchor).split()[:n]
    cand_norm    = normalize(candidate)
    hits = sum(1 for w in anchor_words if w in cand_norm)
    return hits / max(len(anchor_words), 1)


# ─── core reconstruction ─────────────────────────────────────────────────────

def flatten_yyy(content: list) -> str:
    """Join all YYY verse texts into one string."""
    parts = []
    for entry in content:
        t = entry.get("text", "").strip()
        if t:
            parts.append(t)
    return " ".join(parts)


def split_into_sentences(text: str) -> list:
    """Split text into sentence-level chunks."""
    # Split on sentence-ending punctuation followed by whitespace + capital
    parts = re.split(r'(?<=[.!?])\s+(?=[A-ZÇĞİÖŞÜ])', text)
    # Also split on newlines
    result = []
    for p in parts:
        result.extend(p.split('\n'))
    return [r.strip() for r in result if r.strip()]


def split_segment_by_anchors(seg_text: str, tcl_verses_slice: list) -> list:
    """
    Split a single YYY text segment into sub-segments matching
    the TCL verse slice. Uses anchor matching first, then proportional fallback.
    Returns list of text strings (one per TCL verse in slice).
    """
    n = len(tcl_verses_slice)
    if n == 0:
        return []
    if n == 1:
        return [seg_text.strip()]

    seg_text = seg_text.strip()
    if not seg_text:
        return [""] * n

    words = seg_text.split()
    nw = len(words)

    if nw <= n:
        # Too few words: assign one word per verse, rest empty
        result = []
        for i in range(n):
            result.append(words[i] if i < nw else "")
        return result

    # Build anchor fingerprints for each TCL verse in slice
    TR_STOP = {'ve', 'de', 'da', 'ki', 'bir', 'bu', 'o', 'ile', 'ama',
               'ise', 'çünkü', 'onun', 'ona', 'için', 'olan', 'olarak',
               'ben', 'sen', 'biz', 'siz', 'onlar', 'dedi', 'diye',
               'şöyle', 'böyle', 'gibi', 'kadar', 'ne', 'ya', 'mı', 'mi',
               'mu', 'mü', 'dır', 'dir', 'dur', 'dür', 'tır', 'tir'}

    def anchor_keys(tcl_text):
        words_n = normalize(tcl_text).split()[:12]
        return [w for w in words_n if w not in TR_STOP and len(w) > 2][:5]

    # Find breakpoints greedily
    breakpoints = [0]
    search_start = 0

    for i in range(1, n):
        tcl_text = tcl_verses_slice[i]["text"]
        keys = anchor_keys(tcl_text)

        best_pos = search_start + max(1, (nw - search_start) // (n - i + 1))
        best_score = -1.0

        lo = search_start + 1
        # Search up to 3x proportional distance
        prop_step = max(3, nw // n)
        hi = min(nw - (n - i), lo + prop_step * 4)

        for pos in range(lo, hi):
            if not keys:
                break
            window = ' '.join(words[pos:pos + 10])
            w_norm = normalize(window)
            hits = sum(1 for k in keys if k in w_norm)
            score = hits / len(keys)
            if score > best_score:
                best_score = score
                best_pos = pos
            if score >= 0.8:
                break

        # If no good match, fall back to proportional
        if best_score < 0.25:
            remaining = n - i
            remaining_words = nw - breakpoints[-1]
            step = max(1, remaining_words // (remaining + 1))
            best_pos = breakpoints[-1] + step

        best_pos = max(best_pos, breakpoints[-1] + 1)
        best_pos = min(best_pos, nw - (n - i))
        breakpoints.append(best_pos)
        search_start = best_pos

    breakpoints.append(nw)

    result = []
    for i in range(n):
        chunk = ' '.join(words[breakpoints[i]:breakpoints[i+1]]).strip()
        result.append(chunk)
    return result


def reconstruct_chapter(yyy_content: list, tcl_content: list) -> list:
    """
    Segment the YYY text into exactly len(tcl_content) verses,
    using both YYY verse numbers and TCL02 verse texts as anchors.
    Returns list of {"v": N, "text": "..."} dicts.

    Strategy:
    - Build a map from verse_num -> YYY text for each YYY entry
    - For each TCL verse, find the covering YYY entry by verse number
    - If a YYY entry covers multiple consecutive TCL verses, split its
      text into sub-segments using TCL anchor text matching
    - YYY verse v=X is the starting verse; it covers up to (but not
      including) the next YYY verse number
    """
    N = len(tcl_content)
    if N == 0:
        return []

    if not yyy_content:
        return [{"v": v["v"], "text": ""} for v in tcl_content]

    # Build ordered list of (yyy_verse_num, cleaned_text)
    yyy_entries = []
    for entry in yyy_content:
        v = entry.get("v", 0)
        t = strip_garbage(entry.get("text", "").strip())
        yyy_entries.append((v, t))

    # Build yyy verse number set for quick lookup
    yyy_verse_nums = [e[0] for e in yyy_entries]

    # For each TCL verse, determine which YYY entry "covers" it
    # A YYY entry with verse_num=X covers TCL verses from X up to
    # (but not including) the next YYY verse number.

    # Group TCL verses by which YYY entry covers them
    # tcl_groups[i] = list of TCL verse indices covered by yyy_entries[i]
    tcl_groups = [[] for _ in range(len(yyy_entries))]

    for tcl_idx, tcl_v in enumerate(tcl_content):
        tcl_vnum = tcl_v["v"]

        # Find the YYY entry that covers this TCL verse
        # = the last YYY entry with verse_num <= tcl_vnum
        covering_yyy_idx = 0
        for yi, yvnum in enumerate(yyy_verse_nums):
            if yvnum <= tcl_vnum:
                covering_yyy_idx = yi
            else:
                break

        tcl_groups[covering_yyy_idx].append(tcl_idx)

    # Now build result
    result = [None] * N

    for yi, (yvnum, ytext) in enumerate(yyy_entries):
        group = tcl_groups[yi]
        if not group:
            continue

        if len(group) == 1:
            # One TCL verse per YYY entry: direct assignment
            result[group[0]] = {"v": tcl_content[group[0]]["v"], "text": ytext}
        else:
            # Multiple TCL verses: split ytext
            tcl_slice = [tcl_content[idx] for idx in group]
            sub_texts = split_segment_by_anchors(ytext, tcl_slice)
            for k, idx in enumerate(group):
                t = sub_texts[k] if k < len(sub_texts) else ""
                result[idx] = {"v": tcl_content[idx]["v"], "text": t}

    # Fill any None slots (shouldn't happen, but safety)
    for i in range(N):
        if result[i] is None:
            result[i] = {"v": tcl_content[i]["v"], "text": ""}

    return result


# ─── file I/O ─────────────────────────────────────────────────────────────────

def load_json(path: str):
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def save_json(path: str, data: dict):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, separators=(',', ':'))


# ─── main ────────────────────────────────────────────────────────────────────

BOOKS = [
    ("1CO", 16),
    ("HEB", 13),
    ("2CO", 13),
    ("EPH", 6),
]

summary = []

for book, num_chapters in BOOKS:
    for chap in range(1, num_chapters + 1):
        yyy_path = os.path.join(SRC, book, f"{chap}.json")
        tcl_path = os.path.join(REF, book, f"{chap}.json")
        out_path = os.path.join(OUT, book, f"{chap}.json")

        # Load source
        try:
            yyy_data = load_json(yyy_path)
        except FileNotFoundError:
            print(f"  MISSING SRC: {yyy_path}")
            summary.append((book, chap, 0, "MISSING_SRC"))
            continue

        # Load reference
        try:
            tcl_data = load_json(tcl_path)
        except FileNotFoundError:
            print(f"  MISSING REF: {tcl_path}")
            summary.append((book, chap, 0, "MISSING_REF"))
            continue

        yyy_content = yyy_data.get("content", [])
        tcl_content = tcl_data.get("content", [])
        N = len(tcl_content)

        rebuilt = reconstruct_chapter(yyy_content, tcl_content)

        out_data = {
            "t": "YYY1987",
            "b": book,
            "c": chap,
            "content": rebuilt,
        }

        save_json(out_path, out_data)
        print(f"  {book} {chap:2d}: {N} verses -> {out_path.split('YYY1987_REBUILT/')[-1]}")
        summary.append((book, chap, N, "OK"))

print()
print("=" * 60)
print("SUMMARY")
print("=" * 60)
for book, chap, n, status in summary:
    print(f"  {book} {chap:2d}  {n:3d} verses  [{status}]")

total_verses = sum(n for _, _, n, s in summary if s == "OK")
total_chaps  = sum(1 for _, _, _, s in summary if s == "OK")
print()
print(f"Total chapters processed: {total_chaps}")
print(f"Total verses written:     {total_verses}")
