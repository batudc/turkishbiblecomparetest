#!/usr/bin/env python3
"""
ACT Bible verse reconstruction: YYY1987 -> YYY1987_REBUILT
Segments cleaned YYY text into N verses matched to TCL02 verse boundaries.
"""

import json
import os
import re

BASE = "/Users/batuhandemircan/website building/data/translations"
SRC = os.path.join(BASE, "YYY1987", "ACT")
REF = os.path.join(BASE, "TCL02", "ACT")
OUT = os.path.join(BASE, "YYY1987_REBUILT", "ACT")

os.makedirs(OUT, exist_ok=True)


def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def clean_text(raw: str) -> str:
    """Strip garbage patterns from raw YYY text."""
    text = raw

    # Remove standalone section headings like ">..." or ">>..."
    text = re.sub(r'>{1,3}[^<\n]*', ' ', text)

    # Remove abbreviated cross-references like Kol.3:4, Rom.1:2-3, Flp.2:1
    text = re.sub(r'\b[A-ZÇĞİÖŞÜa-zçğışöşü]{2,4}\.\d+:\d+(?:-\d+)?\b', ' ', text)

    # Remove cross-ref patterns like 3:16 or 5:1-3 (standalone, not part of words)
    text = re.sub(r'(?<!\w)\d{1,3}:\d{1,3}(?:-\d{1,3})?(?!\w)', ' ', text)

    # Remove "bkz." followed by a word
    text = re.sub(r'\bbkz\.\s*\S+', ' ', text, flags=re.IGNORECASE)

    # Remove "Dipnotları" and everything after it on the same "sentence"
    text = re.sub(r'Dipnotlar[ıi]\b.*?(?=[A-ZÇĞIÖŞÜ]|\Z)', ' ', text, flags=re.DOTALL)

    # Remove "Kaynak ayetler" section
    text = re.sub(r'Kaynak ayetler\b.*?(?=[A-ZÇĞIÖŞÜ]|\Z)', ' ', text, flags=re.DOTALL)

    # Remove "ELÇ" standalone abbreviation
    text = re.sub(r'\bELÇ\b', ' ', text)

    # Remove standalone lone digits (not part of words) e.g. "50 " "212 " "X>" etc.
    text = re.sub(r'(?<!\w)\d{1,3}(?!\w)', ' ', text)

    # Remove standalone roman numerals (I, II, III, IV, V, VI, VII, VIII, IX, X, XI, XII)
    text = re.sub(r'(?<!\w)(X{0,3}(?:IX|IV|V?I{0,3}))(?!\w)', ' ', text)

    # Remove section title markers like "Matiya, Yahuda'nın yerine seçiliyor" (title-like phrases)
    # These are typically preceded/followed by > or are header-style
    # Already handled by > removal above

    # Clean up exclamation marks used as footnote markers: word! -> word
    text = re.sub(r'(\w)!', r'\1', text)

    # Remove § and similar stray markers
    text = re.sub(r'[§!]{1,3}', ' ', text)

    # Remove "s" prefix markers like "sBunun" -> "Bunun" (single lowercase letter stuck to uppercase)
    text = re.sub(r'(?<!\w)[a-z](?=[A-ZÇĞİÖŞÜ])', '', text)

    # Remove orphan "X" characters
    text = re.sub(r'(?<!\w)X(?!\w)', ' ', text)

    # Normalize whitespace
    text = re.sub(r'\s+', ' ', text).strip()

    return text


def flatten_yyy(content: list) -> str:
    """Join all verse texts into one big string."""
    parts = []
    for item in content:
        t = item.get("text", "")
        if t:
            parts.append(t)
    return " ".join(parts)


def build_anchor_words(tcl_text: str, n: int = 6) -> list:
    """Extract first n significant words from a TCL02 verse text as anchor."""
    # Remove punctuation at start
    t = re.sub(r'^[«»""\'"\s]+', '', tcl_text).strip()
    words = t.split()
    # Return first n words, stripped of quotes/punctuation
    anchor = []
    for w in words[:n]:
        w_clean = re.sub(r'[«»""\'",.:;!?()\[\]]+', '', w).strip()
        if w_clean:
            anchor.append(w_clean)
    return anchor


def find_anchor_position(text: str, anchor_words: list, start: int = 0) -> int:
    """
    Find the position in text where anchor_words sequence begins.
    Tries to match first 3 words as a phrase, falls back to first 2, then first word.
    """
    if not anchor_words:
        return -1

    # Try matching decreasing number of words
    for n_words in [min(4, len(anchor_words)), min(3, len(anchor_words)), min(2, len(anchor_words)), 1]:
        if n_words < 1:
            continue
        phrase = " ".join(anchor_words[:n_words])
        pos = text.find(phrase, start)
        if pos != -1:
            return pos

    return -1


def segment_text_by_anchors(flat_text: str, tcl_content: list) -> list:
    """
    Segment flat_text into N verses, where N = len(tcl_content).
    Uses TCL02 verse texts as anchor hints for boundaries.
    Returns list of (verse_num, text) tuples.
    """
    n = len(tcl_content)
    if n == 0:
        return []

    if n == 1:
        return [(tcl_content[0]["v"], flat_text.strip())]

    # Build anchors for each TCL02 verse
    anchors = []
    for item in tcl_content:
        anchor_words = build_anchor_words(item.get("text", ""))
        anchors.append(anchor_words)

    # Find boundary positions in flat_text for each verse
    positions = []
    search_from = 0

    for i, anchor_words in enumerate(anchors):
        pos = find_anchor_position(flat_text, anchor_words, search_from)
        if pos != -1 and pos >= search_from:
            positions.append(pos)
            # Allow next search to start just after this position
            search_from = pos + 1
        else:
            # Try searching from a bit earlier if we can
            if i > 0 and positions:
                fallback_start = positions[-1] + 1
            else:
                fallback_start = search_from
            pos2 = find_anchor_position(flat_text, anchor_words, fallback_start)
            if pos2 != -1:
                positions.append(pos2)
                search_from = pos2 + 1
            else:
                # Couldn't find anchor — mark as -1 (will fill later)
                positions.append(-1)

    # Fill in -1 positions with interpolated values
    # First pass: fill leading -1s
    # Find first valid position
    first_valid = 0
    for i, p in enumerate(positions):
        if p != -1:
            first_valid = i
            break

    # Fill positions before first valid with 0
    for i in range(first_valid):
        positions[i] = 0

    # Fill in remaining -1s by linear interpolation between neighbors
    for i in range(len(positions)):
        if positions[i] == -1:
            # Find next valid
            next_valid = None
            for j in range(i + 1, len(positions)):
                if positions[j] != -1:
                    next_valid = j
                    break
            if next_valid is not None:
                prev_pos = positions[i - 1] if i > 0 else 0
                next_pos = positions[next_valid]
                gap = next_valid - i + 1
                step = (next_pos - prev_pos) // gap
                positions[i] = prev_pos + step
            else:
                # No valid position found after; use end of text
                positions[i] = len(flat_text)

    # Ensure positions are monotonically increasing
    for i in range(1, len(positions)):
        if positions[i] <= positions[i - 1]:
            positions[i] = positions[i - 1] + 1

    # Now slice text into verse segments
    result = []
    for i, item in enumerate(tcl_content):
        start = positions[i]
        end = positions[i + 1] if i + 1 < len(positions) else len(flat_text)
        # Don't let end exceed text length
        end = min(end, len(flat_text))
        segment = flat_text[start:end].strip()
        result.append((item["v"], segment))

    return result


def process_chapter(chap: int) -> int:
    """Process one chapter. Returns number of verses written."""
    src_path = os.path.join(SRC, f"{chap}.json")
    ref_path = os.path.join(REF, f"{chap}.json")
    out_path = os.path.join(OUT, f"{chap}.json")

    src_data = load_json(src_path)
    ref_data = load_json(ref_path)

    tcl_content = ref_data["content"]
    n = len(tcl_content)

    # Flatten and clean YYY text
    raw_flat = flatten_yyy(src_data["content"])
    clean = clean_text(raw_flat)

    # Segment
    segments = segment_text_by_anchors(clean, tcl_content)

    # Build output content
    out_content = []
    for v_num, text in segments:
        out_content.append({"v": v_num, "text": text})

    out_data = {
        "t": "YYY1987",
        "b": "ACT",
        "c": chap,
        "content": out_content
    }

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out_data, f, ensure_ascii=False, separators=(",", ":"))

    return len(out_content)


def main():
    print("Processing ACT chapters 1–28...\n")
    results = []
    for chap in range(1, 29):
        try:
            n = process_chapter(chap)
            results.append((chap, n, True, None))
        except Exception as e:
            results.append((chap, 0, False, str(e)))

    print("Summary:")
    for chap, n, ok, err in results:
        if ok:
            print(f"  ACT {chap}: {n} verses ✓")
        else:
            print(f"  ACT {chap}: ERROR — {err}")


if __name__ == "__main__":
    main()
