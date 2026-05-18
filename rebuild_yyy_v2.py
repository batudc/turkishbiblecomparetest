#!/usr/bin/env python3
"""
YYY1987 Bible Verse Reconstruction v2 - JHN and LUK
Strategy:
  - Use YYY1987 verse boundaries as primary structure
  - Each YYY verse covers a range of TCL02 verses
  - Within each YYY verse's text, use TCL02 anchor search to sub-segment
  - Fall back to proportional splitting when anchors fail
"""

import json
import os
import re

BASE = "/Users/batuhandemircan/website building/data/translations"
YYY_DIR = os.path.join(BASE, "YYY1987")
TCL_DIR = os.path.join(BASE, "TCL02")
OUT_DIR = os.path.join(BASE, "YYY1987_REBUILT")

BOOKS = [
    ("JHN", 21),
    ("LUK", 24),
]


def clean_text(raw: str) -> str:
    """Strip garbage patterns from raw YYY text."""
    t = raw
    # Remove cross-references like Kol.3:4, Mat.5:1, 3:16, 5:1-3
    t = re.sub(r'\b[1-3]?[A-ZÇĞİÖŞÜa-zçğışöüé]{1,4}\.\s*\d+:\d+(?:-\d+)?\b', '', t)
    t = re.sub(r'(?<!\w)\d{1,3}:\d{1,3}(?:-\d{1,3})?(?!\w)', '', t)
    # Remove "bkz." + following word(s)
    t = re.sub(r'bkz\.\s*\S+', '', t, flags=re.IGNORECASE)
    # Remove section labels
    t = re.sub(r'Dipnotlar[ıi]\b', '', t, flags=re.IGNORECASE)
    t = re.sub(r'Kaynak\s+ayetler\b', '', t, flags=re.IGNORECASE)
    t = re.sub(r'\bELÇ\b', '', t)
    # Remove lone roman numerals
    t = re.sub(r'(?<!\w)(X{0,3})(IX|IV|V?I{0,3})(?!\w)', '', t)
    # Remove stray single digits
    t = re.sub(r'(?<!\w)\d(?!\w)', '', t)
    # Normalize whitespace
    t = re.sub(r'\s{2,}', ' ', t)
    return t.strip()


def normalize_for_search(text: str) -> str:
    """Lowercase, unify quotes, strip punctuation, collapse whitespace."""
    text = text.lower()
    for ch in ['«', '»', '“', '”', '‘', '’',
               '„', '‟', '‹', '›',
               '"', '"', '‘', '’']:
        text = text.replace(ch, ' ')
    for ch in ['’', 'ʼ', '`', '\xb4', 'ʻ']:
        text = text.replace(ch, "'")
    text = re.sub(r"[^\w\s']", ' ', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def search_anchor(flat_norm: str, anchor_text: str, start: int) -> int:
    """
    Search for anchor text in flat_norm starting from start.
    Returns position or -1 if not found.
    Tries progressively fewer words.
    """
    hint_norm = normalize_for_search(anchor_text[:80])
    if not hint_norm:
        return -1
    words = hint_norm.split()
    if not words:
        return -1

    for num_words in range(min(5, len(words)), 0, -1):
        key = " ".join(words[:num_words])
        min_len = 5 if num_words == 1 else 3
        if len(key) < min_len:
            continue
        idx = flat_norm.find(key, start)
        if idx != -1:
            return idx
    return -1


def segment_block(text: str, tcl_sub: list) -> list:
    """
    Segment a block of YYY text into len(tcl_sub) parts
    guided by TCL02 anchors. Returns list of strings.
    """
    n = len(tcl_sub)
    if n == 0:
        return []
    if n == 1:
        return [text.strip()]

    flat_len = len(text)
    norm = normalize_for_search(text)
    norm_len = len(norm)

    if norm_len == 0:
        return [""] * n

    def norm_to_orig(np):
        if norm_len == 0:
            return 0
        return min(int(np * flat_len / norm_len), flat_len)

    # Find split points for verses 1..n-1
    known = {0: 0, n: norm_len}
    search_start = 0

    for i in range(1, n):
        anchor = tcl_sub[i].get("text", "")
        pos = search_anchor(norm, anchor, search_start)
        if pos != -1:
            known[i] = pos
            search_start = pos + 1

    # Interpolate unknown positions
    sorted_keys = sorted(known.keys())
    positions = []
    for i in range(n):
        if i in known:
            positions.append(known[i])
        else:
            prev_k = max(k for k in sorted_keys if k <= i)
            next_k = min(k for k in sorted_keys if k > i)
            frac = (i - prev_k) / (next_k - prev_k)
            positions.append(int(known[prev_k] + frac * (known[next_k] - known[prev_k])))

    positions.append(norm_len)

    # Ensure monotonic
    for i in range(1, len(positions)):
        if positions[i] <= positions[i - 1]:
            positions[i] = positions[i - 1] + 1

    # Convert to original positions
    orig = [norm_to_orig(p) for p in positions]
    orig[0] = 0
    orig[-1] = flat_len
    for i in range(1, len(orig)):
        if orig[i] <= orig[i - 1]:
            orig[i] = orig[i - 1] + 1

    # Extract segments
    result = []
    for i in range(n):
        start = min(orig[i], flat_len)
        end = min(orig[i + 1], flat_len)
        result.append(text[start:end].strip())
    return result


def rebuild_chapter(book: str, chap: int) -> dict:
    """Rebuild one YYY1987 chapter using TCL02 verse structure."""
    yyy_path = os.path.join(YYY_DIR, book, f"{chap}.json")
    tcl_path = os.path.join(TCL_DIR, book, f"{chap}.json")

    with open(tcl_path, "r", encoding="utf-8") as f:
        tcl_data = json.load(f)
    tcl_verses = tcl_data.get("content", [])
    n_tcl = len(tcl_verses)

    if n_tcl == 0:
        return {"t": "YYY1987", "b": book, "c": chap, "content": []}

    try:
        with open(yyy_path, "r", encoding="utf-8") as f:
            yyy_data = json.load(f)
        yyy_content = yyy_data.get("content", [])
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"  WARNING: {yyy_path}: {e}")
        return {
            "t": "YYY1987", "b": book, "c": chap,
            "content": [{"v": v["v"], "text": ""} for v in tcl_verses]
        }

    # Build a map: canonical verse number -> cleaned text from YYY
    yyy_map = {}
    for item in yyy_content:
        vnum = item["v"]
        text = clean_text(item.get("text", "").strip())
        yyy_map[vnum] = text

    # TCL verse numbers
    tcl_vnums = [v["v"] for v in tcl_verses]
    yyy_vnums = sorted(yyy_map.keys())

    # --- Assign each TCL verse to a YYY source verse ---
    # YYY verse V covers TCL verses from V up to (next YYY verse - 1)
    # We map each TCL verse to the best matching YYY verse

    def find_yyy_owner(tcl_v):
        """Find which YYY verse owns this TCL verse number."""
        owner = None
        for yv in yyy_vnums:
            if yv <= tcl_v:
                owner = yv
            else:
                break
        return owner

    # Group TCL verses by their YYY owner
    groups = {}  # yyy_v -> [list of (tcl_index, tcl_verse)]
    for i, tv in enumerate(tcl_verses):
        owner = find_yyy_owner(tv["v"])
        if owner is None:
            # TCL verse comes before first YYY verse - assign to first YYY
            owner = yyy_vnums[0] if yyy_vnums else None
        if owner is not None:
            if owner not in groups:
                groups[owner] = []
            groups[owner].append((i, tv))

    # For each YYY verse, segment its text into the TCL sub-verses
    output = [None] * n_tcl

    for yyy_v, tcl_group in groups.items():
        yyy_text = yyy_map.get(yyy_v, "")
        tcl_sub = [item[1] for item in tcl_group]
        indices = [item[0] for item in tcl_group]

        if not yyy_text.strip():
            for idx in indices:
                output[idx] = ""
            continue

        segments = segment_block(yyy_text, tcl_sub)

        for j, idx in enumerate(indices):
            output[idx] = segments[j] if j < len(segments) else ""

    # Fill any None slots (TCL verses with no YYY owner)
    for i in range(n_tcl):
        if output[i] is None:
            output[i] = ""

    content = [
        {"v": tcl_verses[i]["v"], "text": output[i]}
        for i in range(n_tcl)
    ]

    return {"t": "YYY1987", "b": book, "c": chap, "content": content}


def main():
    summary = []

    for book, num_chapters in BOOKS:
        out_book_dir = os.path.join(OUT_DIR, book)
        os.makedirs(out_book_dir, exist_ok=True)
        print(f"\n=== {book} ({num_chapters} chapters) ===")

        for chap in range(1, num_chapters + 1):
            result = rebuild_chapter(book, chap)

            out_path = os.path.join(out_book_dir, f"{chap}.json")
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(result, f, ensure_ascii=False, separators=(",", ":"))

            verse_count = len(result.get("content", []))
            empty_count = sum(1 for v in result["content"] if not v["text"].strip())
            summary.append((book, chap, verse_count, empty_count))
            flag = f" [!{empty_count} empty]" if empty_count else ""
            print(f"  {book} {chap:2d}: {verse_count} verses{flag}")

    print("\n=== SUMMARY ===")
    print(f"{'Book':<6} {'Chap':>5} {'Verses':>7} {'Empty':>6}")
    print("-" * 28)
    total_v = 0
    total_e = 0
    for book, chap, count, empty in summary:
        flag = " *" if empty else ""
        print(f"{book:<6} {chap:>5} {count:>7} {empty:>6}{flag}")
        total_v += count
        total_e += empty
    print("-" * 28)
    print(f"{'TOTAL':<6} {'':>5} {total_v:>7} {total_e:>6}")


if __name__ == "__main__":
    main()
