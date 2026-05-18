#!/usr/bin/env python3
"""
Rebuild YYY1987 MAT chapters using TCL02 canonical verse numbers.

Strategy:
- Map YYY verse numbers → their texts
- TCL gives canonical verse number list
- For each TCL verse: if YYY has same verse number, use it directly
- Where a single YYY verse covers multiple TCL verses (merged), spread its
  text across those TCL verses using TCL anchor hints
- Where YYY has no matching verse at all, use ""
"""
import json
import re
import os

BASE = "/Users/batuhandemircan/website building/data/translations"
YYY_DIR = os.path.join(BASE, "YYY1987", "MAT")
TCL_DIR = os.path.join(BASE, "TCL02", "MAT")
OUT_DIR = os.path.join(BASE, "YYY1987_REBUILT", "MAT")
os.makedirs(OUT_DIR, exist_ok=True)

# ──────────────────────────────────────────────────
# Garbage-stripping patterns
# ──────────────────────────────────────────────────
GARBAGE_PATTERNS = [
    # Cross-reference numbers like "3:16" or "5:1-3" or "12:1–4"
    r'\b\d{1,3}:\d{1,3}(?:[–\-]\d{1,3})?\b',
    # "bkz." followed by anything up to whitespace or comma
    r'bkz\.\s*\S+',
    # Section headers / footnote markers
    r'Dipnotlar[ıi]',
    r'Kaynak\s+ayetler',
    r'\bELÇ\b',
    # Abbreviated book refs like "Kol.3:4", "Rom.8:1", "İbr.1:2" etc.
    r'\b[A-ZÇĞİÖŞÜa-zçğışöüñ]{2,5}\.\d{1,3}:\d{1,3}(?:[–\-]\d{1,3})?\b',
    # Lone digits (verse markers left in text) — standalone single/double digit
    # only when surrounded by spaces or start/end
    r'(?:^|\s)\d{1,2}(?=\s|$)',
    # Roman numerals standalone (II, III, IV, VI, VII, VIII, IX, X, XI, XII, ...)
    r'(?<!\w)(M{0,4}(?:CM|CD|D?C{0,3})(?:XC|XL|L?X{0,3})(?:IX|IV|V?I{0,3}))(?!\w)',
]

GARBAGE_RE = re.compile('|'.join(GARBAGE_PATTERNS))

def clean_text(raw: str) -> str:
    text = GARBAGE_RE.sub(' ', raw)
    # Collapse multiple spaces
    text = re.sub(r'  +', ' ', text)
    return text.strip()

# ──────────────────────────────────────────────────
# Segmentation helpers (for splitting one YYY verse into multiple TCL verses)
# ──────────────────────────────────────────────────

def first_words(s: str, n: int = 8) -> list:
    return re.findall(r'\S+', s)[:n]

def stem_tr(word: str) -> str:
    """Very rough Turkish stemmer: strip common suffixes for fuzzy matching."""
    word = word.lower()
    for suffix in ['nın', 'nin', 'nun', 'nün', 'ının', 'inin', 'unun', 'ünün',
                   'lar', 'ler', 'da', 'de', 'ta', 'te', 'dan', 'den', 'tan', 'ten',
                   'a', 'e', 'ı', 'i', 'u', 'ü', 'lar', 'ler']:
        if word.endswith(suffix) and len(word) - len(suffix) >= 3:
            return word[:-len(suffix)]
    return word

def find_anchor_pos(text: str, anchor_words: list, start: int) -> int:
    if not anchor_words:
        return start

    text_lower = text.lower()

    # Strategy 1: exact anchor word search — try each anchor word in order
    best_pos = -1
    best_score = -1
    for anchor_word in anchor_words:
        first_word = anchor_word.lower()
        pattern = re.compile(re.escape(first_word), re.IGNORECASE)
        for m in pattern.finditer(text, start):
            snippet = text_lower[m.start(): m.start() + 200]
            score = sum(1 for w in anchor_words if w.lower() in snippet)
            if score > best_score:
                best_score = score
                best_pos = m.start()
        if best_score >= 3:
            break

    if best_pos >= start and best_score >= 2:
        return best_pos

    # Strategy 2: stemmed fuzzy match — use stem of anchor words
    stemmed_anchors = [stem_tr(w) for w in anchor_words if len(w) >= 4]
    if stemmed_anchors:
        for anchor_stem in stemmed_anchors[:3]:
            if len(anchor_stem) < 3:
                continue
            pattern = re.compile(re.escape(anchor_stem), re.IGNORECASE)
            for m in pattern.finditer(text, start):
                snippet = text_lower[m.start(): m.start() + 200]
                score = sum(1 for s in stemmed_anchors if s in snippet)
                if score > best_score:
                    best_score = score
                    best_pos = m.start()
            if best_score >= 2:
                break

    return best_pos if best_pos >= start else -1

def split_text_for_tcl_group(yyy_text: str, tcl_group: list) -> list:
    """
    Split yyy_text into len(tcl_group) segments guided by TCL anchor hints.
    tcl_group is a list of TCL verse dicts (each has 'text').
    Returns list of strings, same length as tcl_group.
    """
    n = len(tcl_group)
    if n == 0:
        return []
    if n == 1:
        return [yyy_text.strip()]

    text = yyy_text
    anchors = [first_words(v["text"], 6) for v in tcl_group]

    # Build cut points
    cut_points = [0]
    search_start = 0
    for i in range(1, n):
        pos = find_anchor_pos(text, anchors[i], search_start)
        if pos < 0 or pos <= cut_points[-1]:
            # Fallback: divide proportionally
            remaining = len(text) - cut_points[-1]
            steps_left = n - i
            pos = cut_points[-1] + max(1, remaining // (steps_left + 1))
        pos = min(pos, len(text))
        # Try to break at sentence/clause boundary near pos
        window_start = max(cut_points[-1], pos - 40)
        window_end = min(len(text), pos + 40)
        window = text[window_start:window_end]
        # Look for sentence boundary
        for pat in [r'(?<=[.!?»])\s+', r'(?<=\s)']:
            boundaries = [m.start() + window_start for m in re.finditer(pat, window)]
            if boundaries:
                closest = min(boundaries, key=lambda x: abs(x - pos))
                if cut_points[-1] < closest <= len(text):
                    pos = closest
                    break
        cut_points.append(pos)
        search_start = pos + 1

    cut_points.append(len(text))

    segments = []
    for i in range(n):
        seg = text[cut_points[i]:cut_points[i+1]].strip()
        segments.append(seg)
    return segments

# ──────────────────────────────────────────────────
# Main logic: map YYY → TCL using verse number matching
# ──────────────────────────────────────────────────

def process_chapter(chap: int):
    yyy_path = os.path.join(YYY_DIR, f"{chap}.json")
    tcl_path = os.path.join(TCL_DIR, f"{chap}.json")
    out_path = os.path.join(OUT_DIR, f"{chap}.json")

    with open(yyy_path, encoding="utf-8") as f:
        yyy_data = json.load(f)
    with open(tcl_path, encoding="utf-8") as f:
        tcl_data = json.load(f)

    tcl_verses = tcl_data["content"]
    yyy_verses = yyy_data["content"]

    # Build a map: YYY verse_number → cleaned text
    yyy_map = {v["v"]: clean_text(v.get("text", "")) for v in yyy_verses}
    yyy_nums = sorted(yyy_map.keys())

    # Build the output content using TCL verse numbers
    # Strategy:
    #   Group consecutive TCL verses that share the same "YYY coverage":
    #   - A TCL verse is "covered" by a YYY verse if:
    #     (a) YYY has exact same verse number, OR
    #     (b) YYY has a verse number just before the TCL verse number (merged coverage)
    #
    # More precisely:
    #   For each TCL verse v_t:
    #     Find the YYY verse that "owns" it:
    #       = the largest YYY verse number <= v_t
    #     Group TCL verses by which YYY verse owns them.
    #     For each group, split that YYY verse's text into len(group) segments.

    def yyy_owner(tcl_v):
        """Return the YYY verse number that owns TCL verse tcl_v."""
        owner = None
        for yn in yyy_nums:
            if yn <= tcl_v:
                owner = yn
            else:
                break
        return owner

    # Group TCL verses by their YYY owner
    groups = {}  # yyy_v_num -> list of tcl verse dicts
    for tv in tcl_verses:
        owner = yyy_owner(tv["v"])
        if owner is None:
            owner = "__none__"
        groups.setdefault(owner, []).append(tv)

    # Build output
    result_map = {}  # tcl_v_num -> text

    for owner, tcl_group in groups.items():
        if owner == "__none__":
            for tv in tcl_group:
                result_map[tv["v"]] = ""
            continue

        yyy_text = yyy_map.get(owner, "")
        if not yyy_text.strip():
            for tv in tcl_group:
                result_map[tv["v"]] = ""
            continue

        # Split YYY text into segments
        segs = split_text_for_tcl_group(yyy_text, tcl_group)
        for i, tv in enumerate(tcl_group):
            result_map[tv["v"]] = segs[i] if i < len(segs) else ""

    # Build output content in TCL canonical order
    content = []
    for tv in tcl_verses:
        content.append({"v": tv["v"], "text": result_map.get(tv["v"], "")})

    out_data = {
        "t": "YYY1987",
        "b": "MAT",
        "c": chap,
        "content": content
    }

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out_data, f, ensure_ascii=False, separators=(",", ":"))

    verse_nums = [v["v"] for v in content]
    empty_count = sum(1 for v in content if not v["text"].strip())
    empty_vs = [v["v"] for v in content if not v["text"].strip()]
    status = f"({empty_count} empty: {empty_vs})" if empty_count else ""
    print(f"MAT {chap}: {len(content)} verses, numbers={verse_nums} {status}✓")
    return chap, len(content), verse_nums

if __name__ == "__main__":
    for ch in range(1, 29):
        process_chapter(ch)
    print("\nAll 28 chapters of MAT processed successfully.")
