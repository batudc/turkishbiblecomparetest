#!/usr/bin/env python3
"""
YYY1987 Bible verse reconstruction script — robust segmentation for 25 problem chapters.
Rebuilds YYY1987 chapters to match TCL02 verse structure.
Uses anchor matching with proportional fallback to guarantee zero empty verses.
"""

import json
import os
import re
import unicodedata

BASE = "/Users/batuhandemircan/website building/data/translations"
YYY_DIR = os.path.join(BASE, "YYY1987")
TCL_DIR = os.path.join(BASE, "TCL02")
OUT_DIR = os.path.join(BASE, "YYY1987_REBUILT")

# The 25 problem chapters to fix
TARGET_CHAPTERS = [
    ("GAL", [2, 3, 5, 6]),
    ("JAS", [5]),
    ("MAT", [7, 15, 27]),
    ("MRK", [3, 14]),
    ("REV", [4, 5, 12, 13, 19]),
    ("ROM", [3, 4, 5, 7, 9, 10, 11, 13, 15, 16]),
]

BOOKS = [
    ("2TI", 4),
    ("COL", 4),
    ("PHP", 4),
    ("2PE", 3),
    ("2TH", 3),
    ("TIT", 3),
    ("2JN", 1),
    ("3JN", 1),
    ("JUD", 1),
    ("PHM", 1),
]


# ============================================================
# ROBUST SEGMENTATION HELPERS
# ============================================================

def normalize_anchor(text: str) -> str:
    """Lowercase, strip diacritics/punctuation for fuzzy anchor matching."""
    text = text.lower()
    # NFKD decompose to separate base chars from combining marks
    nfkd = unicodedata.normalize("NFKD", text)
    # Keep only ASCII letters, Turkish letters and spaces
    result = re.sub(r"[^\w\s]", " ", nfkd)
    result = re.sub(r"\s+", " ", result).strip()
    return result


def make_anchor(verse_text: str, max_chars: int = 55) -> str:
    """Build a short anchor from the first words of a verse (normalized)."""
    norm = normalize_anchor(verse_text)
    words = norm.split()
    anchor = ""
    for w in words:
        if len(anchor) + len(w) + 1 > max_chars:
            break
        anchor += (" " if anchor else "") + w
    return anchor


def search_anchor(anchor: str, flat_norm: str, from_pos: int) -> int:
    """Try progressively shorter prefixes; return position or -1."""
    words = anchor.split()
    for length in range(len(words), 0, -1):
        prefix = " ".join(words[:length])
        if len(prefix) < 4 and length <= 2:
            continue
        pos = flat_norm.find(prefix, from_pos)
        if pos != -1:
            return pos
    return -1


def proportional_positions(known: dict, total: int) -> list:
    """
    Given a dict of {verse_idx: char_pos} with sentinel {n: total_len},
    interpolate positions for all missing verse indices 0..n-1.
    """
    n = max(known.keys())  # n is the sentinel (= number of verses)
    sorted_keys = sorted(known.keys())
    positions = [None] * n

    for i in range(n):
        if i in known:
            positions[i] = known[i]

    # Fill gaps by linear interpolation between known neighbours
    for i in range(n):
        if positions[i] is not None:
            continue
        # Find prev known
        prev_k = max((k for k in known if k <= i), default=0)
        next_k = min((k for k in known if k > i), default=n)
        p0 = known.get(prev_k, 0)
        p1 = known.get(next_k, total)
        span = next_k - prev_k
        if span == 0:
            positions[i] = p0
        else:
            frac = (i - prev_k) / span
            positions[i] = int(p0 + frac * (p1 - p0))

    return positions


def build_word_starts(flat: str) -> list:
    """Return sorted list of char positions where words start in flat."""
    return [m.start() for m in re.finditer(r'\S+', flat)]


def snap_to_word(pos: int, word_starts: list, flat_len: int, min_pos: int = 0) -> int:
    """Snap pos to the nearest word-start >= min_pos."""
    if not word_starts:
        return max(pos, min_pos)
    # Find first word start >= min_pos that is also >= pos
    # If pos falls mid-word, advance to the next word start
    candidates = [w for w in word_starts if w >= max(pos, min_pos)]
    if candidates:
        return candidates[0]
    # Nothing found ahead — use last word start >= min_pos
    candidates2 = [w for w in word_starts if w >= min_pos]
    return candidates2[-1] if candidates2 else min_pos


def segment_flat_text(flat: str, tcl_verses: list) -> list:
    """
    Segment flat YYY text into exactly len(tcl_verses) parts.
    - Builds word-level index of the original flat text.
    - Anchor match works on normalize_anchor(flat) with norm->orig word mapping.
    - Proportional fallback for unmatched anchors.
    - All split points are snapped to word boundaries in original flat.
    Returns list of str, one per TCL verse.
    """
    n = len(tcl_verses)
    if n == 0:
        return []
    if n == 1:
        return [flat.strip()]

    flat_len = len(flat)

    # Build parallel structures: original word positions and normalized word list
    # word_map[i] = (orig_start, orig_end, norm_word)
    word_map = []
    for m in re.finditer(r'\S+', flat):
        norm_w = normalize_anchor(m.group()).replace(" ", "")  # single word, no spaces
        if norm_w:
            word_map.append((m.start(), m.end(), norm_w))

    n_words = len(word_map)
    if n_words == 0:
        return [""] * n

    # Build normalized flat string and keep a word_starts_in_norm array
    # norm_flat = space-joined normalized words
    norm_words = [w[2] for w in word_map]
    norm_flat = " ".join(norm_words)
    norm_flat_len = len(norm_flat)

    # Precompute: norm_flat word i starts at char position norm_word_starts[i]
    norm_word_starts = []
    pos = 0
    for w in norm_words:
        norm_word_starts.append(pos)
        pos += len(w) + 1  # +1 for space

    def norm_pos_to_orig_word_idx(np: int) -> int:
        """Convert a char position in norm_flat to the nearest word index."""
        # Binary search for largest norm_word_starts[i] <= np
        lo, hi = 0, n_words - 1
        while lo < hi:
            mid = (lo + hi + 1) // 2
            if norm_word_starts[mid] <= np:
                lo = mid
            else:
                hi = mid - 1
        return lo

    def orig_start_of_word(widx: int) -> int:
        return word_map[widx][0] if widx < n_words else flat_len

    # Phrase search in norm_flat: find anchor phrase starting at norm_flat position >= from_norm
    def find_phrase_in_norm(anchor: str, from_norm: int) -> int:
        words = anchor.split()
        for length in range(len(words), 0, -1):
            prefix = " ".join(words[:length])
            if len(prefix) < 4 and length <= 2:
                continue
            pos = norm_flat.find(prefix, from_norm)
            if pos != -1:
                return pos
        return -1

    # Step 1: Find anchors for each verse boundary (verse 1..n-1)
    # known_widx[i] = word index in word_map where verse i starts
    known_widx = {0: 0, n: n_words}
    last_norm_pos = 0

    for i in range(1, n):
        verse_text = tcl_verses[i].get("text", "")
        if not verse_text.strip():
            continue
        anchor = make_anchor(verse_text)
        norm_pos = find_phrase_in_norm(anchor, last_norm_pos)
        if norm_pos != -1 and norm_pos > last_norm_pos:
            widx = norm_pos_to_orig_word_idx(norm_pos)
            known_widx[i] = widx
            last_norm_pos = norm_pos

    # Step 2: Interpolate unknown positions in word-index space
    positions_widx = proportional_positions(known_widx, n_words)
    positions_widx.append(n_words)  # sentinel

    # Step 3: Enforce strictly increasing word indices
    for i in range(1, len(positions_widx)):
        if positions_widx[i] <= positions_widx[i - 1]:
            positions_widx[i] = positions_widx[i - 1] + 1
    positions_widx = [min(w, n_words) for w in positions_widx]

    # Step 4: Convert word indices to orig char positions and slice
    segments = []
    for i in range(n):
        w_start = positions_widx[i]
        w_end = positions_widx[i + 1]
        if w_start >= n_words:
            segments.append("")
            continue
        char_start = word_map[w_start][0]
        if w_end >= n_words:
            char_end = flat_len
        else:
            char_end = word_map[w_end][0]
        seg = flat[char_start:char_end].strip()
        segments.append(seg)

    # Emergency: fix empty/near-empty segments by redistributing words
    _fix_empty_segments(segments, tcl_verses)

    return segments


def _fix_empty_segments(segs: list, tcl_verses: list, min_words: int = 2):
    """
    In-place: find runs of near-empty segments and redistribute words from
    adjacent over-stuffed segments using TCL verse lengths as weights.
    Works at the word level to guarantee no empty verses.
    """
    n = len(segs)
    if n == 0:
        return

    # Convert segments to word lists
    word_lists = [s.split() for s in segs]
    tcl_lens = [len(v.get("text", "")) for v in tcl_verses]

    changed = True
    passes = 0
    while changed and passes < 20:
        changed = False
        passes += 1

        # Find any segment with fewer than min_words
        for i in range(n):
            if len(word_lists[i]) >= min_words:
                continue

            # Find the largest adjacent segment to borrow from
            # Look wider: scan all segments, prefer closest
            best_donor = -1
            best_count = 0
            for j in range(n):
                if j == i:
                    continue
                if len(word_lists[j]) > best_count:
                    best_count = len(word_lists[j])
                    best_donor = j

            if best_donor == -1 or best_count < min_words + 1:
                continue

            j = best_donor
            words_j = word_lists[j]

            # How many words does verse i need?
            need = max(min_words, len(word_lists[i]))
            # Use TCL length ratio as guide
            total_len = tcl_lens[i] + tcl_lens[j] if (tcl_lens[i] + tcl_lens[j]) > 0 else 2
            frac_i = tcl_lens[i] / total_len
            total_words = len(words_j) + len(word_lists[i])
            target_i = max(min_words, int(total_words * frac_i))
            target_j = total_words - target_i

            if target_j < min_words:
                target_j = min_words
                target_i = total_words - target_j

            # Merge and split
            if j < i:
                combined = words_j + word_lists[i]
                word_lists[j] = combined[:target_j]
                word_lists[i] = combined[target_j:]
            else:
                combined = word_lists[i] + words_j
                word_lists[i] = combined[:target_i]
                word_lists[j] = combined[target_i:]

            changed = True

    # Write back
    for i in range(n):
        segs[i] = " ".join(word_lists[i])


def rebuild_target_chapter(book: str, chap: int) -> tuple:
    """Rebuild one of the 25 problem chapters. Returns (verse_count, empty_count)."""
    yyy_path = os.path.join(YYY_DIR, book, f"{chap}.json")
    tcl_path = os.path.join(TCL_DIR, book, f"{chap}.json")
    out_path = os.path.join(OUT_DIR, book, f"{chap}.json")

    with open(yyy_path, "r", encoding="utf-8") as f:
        yyy_data = json.load(f)
    with open(tcl_path, "r", encoding="utf-8") as f:
        tcl_data = json.load(f)

    tcl_verses = tcl_data.get("content", [])
    n = len(tcl_verses)

    # Join all YYY verse texts into flat string and clean
    raw_parts = [v.get("text", "").strip() for v in yyy_data.get("content", []) if v.get("text", "").strip()]
    flat_raw = " ".join(raw_parts)

    # Strip reference markers etc.
    flat = _strip_markers(flat_raw)

    if not flat.strip() or n == 0:
        content = [{"v": v["v"], "text": ""} for v in tcl_verses]
        _write_json(out_path, {"t": "YYY1987", "b": book, "c": chap, "content": content})
        return n, n

    segments = segment_flat_text(flat, tcl_verses)

    content = []
    for i, v in enumerate(tcl_verses):
        text = segments[i].strip() if i < len(segments) else ""
        content.append({"v": v["v"], "text": text})

    _write_json(out_path, {"t": "YYY1987", "b": book, "c": chap, "content": content})

    empty = sum(1 for c in content if not c["text"].strip())
    return n, empty


def _strip_markers(text: str) -> str:
    """Remove footnote artifacts, cross-reference markers, lone digits."""
    t = text
    t = re.sub(r"\b[A-Z][a-z]{0,3}\.?\s*\d+:\d+(?:-\d+)?\b", " ", t)
    t = re.sub(r"(?<!\w)\d{1,3}:\d{1,3}(?:-\d{1,3})?(?!\w)", " ", t)
    t = re.sub(r"\bbkz\.\s*\S+", " ", t, flags=re.IGNORECASE)
    t = re.sub(r"\b(Dipnotlar[ıi]|Kaynak\s+ayetler|ELÇ)\b", " ", t, flags=re.IGNORECASE)
    t = re.sub(r"(?<!\w)\d{1,3}(?!\w)", " ", t)
    t = re.sub(r"\b(xvi{0,3}|xi{0,3}|vi{0,3}|iv|ix|x{1,3}|v{1}|i{1,3})\b", " ", t, flags=re.IGNORECASE)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def _write_json(path: str, data: dict):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, separators=(",", ":"))


# ============================================================
# END ROBUST SEGMENTATION HELPERS
# ============================================================


def clean_text(raw: str) -> str:
    """Strip garbage patterns from raw YYY text."""
    t = raw

    # Remove cross-references like Kol.3:4, Mat.5:1, Rom.1:16-17, 3:16, 5:1-3
    # First: abbreviated book refs like Kol.3:4 or 1Ko.3:4
    t = re.sub(r'\b[1-3]?[A-ZÇĞİÖŞÜa-zçğışöüé]{1,4}\.\s*\d+:\d+(?:-\d+)?\b', '', t)
    # Plain chapter:verse like 3:16 or 5:1-3 (must be preceded by space/start)
    t = re.sub(r'(?<!\w)\d{1,3}:\d{1,3}(?:-\d{1,3})?(?!\w)', '', t)

    # Remove "bkz." + following word(s)
    t = re.sub(r'bkz\.\s*\S+', '', t, flags=re.IGNORECASE)

    # Remove "Dipnotları" and "Kaynak ayetler"
    t = re.sub(r'Dipnotlar[ıi]\b', '', t, flags=re.IGNORECASE)
    t = re.sub(r'Kaynak\s+ayetler\b', '', t, flags=re.IGNORECASE)

    # Remove "ELÇ" standalone
    t = re.sub(r'\bELÇ\b', '', t)

    # Remove lone roman numerals (e.g. " I ", " II ", " IV ")
    t = re.sub(r'(?<!\w)(X{0,3})(IX|IV|V?I{0,3})(?!\w)', '', t)

    # Remove section headers that are all-caps Turkish (e.g. "İKefernahum'a girdiler")
    # These look like "İK..." — catch them by removing leading non-sentence fragments
    t = re.sub(r'[İ][A-ZÇĞÖŞÜ][A-ZÇĞÖŞÜa-zçğışöü]+\'[a-zçğışöü]+ [a-zçğışöü]+', '', t)

    # Remove stray single digits (verse numbers scattered in text)
    t = re.sub(r'(?<!\w)\d(?!\w)', '', t)

    # Remove stray punctuation artifacts from removals
    t = re.sub(r'\s{2,}', ' ', t)
    t = re.sub(r'^\s+', '', t)
    t = t.strip()

    return t


def flatten_yyy(content: list) -> str:
    """Join all YYY verse texts into one string."""
    parts = []
    for item in content:
        text = item.get("text", "").strip()
        if text:
            cleaned = clean_text(text)
            if cleaned:
                parts.append(cleaned)
    return " ".join(parts)


def normalize_for_search(text: str) -> str:
    """
    Normalize text for fuzzy search:
    - lowercase
    - unify all quote/apostrophe characters to plain ascii
    - collapse whitespace
    """
    # Unify various quote characters
    text = text.lower()
    # Smart quotes and guillemets -> plain quotes or space
    for ch in ['«', '»', '“', '”', '‘', '’',
               '„', '‟', '‹', '›', '「', '」',
               '"', '"', ''', ''', '«', '»', '„']:
        text = text.replace(ch, ' ')
    # Various apostrophes -> plain apostrophe
    for ch in ['’', 'ʼ', '`', '´', 'ʻ', '⯀']:
        text = text.replace(ch, "'")
    # Remove punctuation that would interfere with matching (keep letters, apostrophe, space)
    text = re.sub(r"[^\w\s']", ' ', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def extract_words(norm_text: str) -> list:
    """Extract meaningful word tokens (skip short words as first tokens)."""
    words = norm_text.split()
    # Filter out very short words that could cause false matches
    return words


def build_word_index(flat: str):
    """
    Build a list of (word_start_char, word_end_char, normalized_word) for every
    word in flat. Returns the list and a parallel flat_norm string of space-joined
    normalized words.
    """
    # Split preserving positions via regex
    words = []
    for m in re.finditer(r'\S+', flat):
        norm = normalize_for_search(m.group())
        if norm:
            words.append((m.start(), m.end(), norm))
    norm_joined = " ".join(w[2] for w in words)
    return words, norm_joined


def find_in_word_index(word_list, norm_joined, query_words, start_word_idx):
    """
    Find the first occurrence of query_words (list of normalized strings) in
    word_list at or after start_word_idx.
    Returns the char start of the matching word in the original flat string,
    and the word index, or (-1, -1) if not found.
    """
    n_q = len(query_words)
    if n_q == 0:
        return -1, -1
    n_w = len(word_list)
    for i in range(start_word_idx, n_w - n_q + 1):
        match = True
        for j in range(n_q):
            if word_list[i + j][2] != query_words[j]:
                match = False
                break
        if match:
            return word_list[i][0], i
    return -1, -1


def find_verse_boundaries(flat: str, tcl_verses: list) -> list:
    """
    Segment flat YYY text into N verses guided by TCL02 anchor hints.
    Works entirely in original-string word positions to avoid mid-word cuts.

    Strategy:
    1. Build word index (original char positions + normalized forms).
    2. For each verse boundary i (from verse 1 onward), search forward for
       the TCL02 anchor words using decreasing prefix lengths.
    3. Record found boundaries as char positions in flat.
    4. Interpolate unknown boundaries proportionally between known ones.
    5. Snap interpolated positions to the nearest word boundary.
    6. Extract segments.
    """
    n = len(tcl_verses)
    if n == 0:
        return []
    if n == 1:
        return [flat]

    flat_len = len(flat)
    word_list, norm_joined = build_word_index(flat)
    n_words = len(word_list)

    # known_char[i] = char position where verse i begins in flat
    known_char = {0: 0}
    search_word_idx = 0  # search starts from this word index

    for i in range(1, n):
        hint_text = tcl_verses[i].get("text", "")[:80]
        hint_norm = normalize_for_search(hint_text)
        if not hint_norm:
            continue

        words = hint_norm.split()
        found_char = -1
        found_widx = -1

        # Try decreasing prefix lengths: 5, 4, 3, 2; min word key length 5 chars
        for num_words in range(min(6, len(words)), 0, -1):
            key_words = words[:num_words]
            key_str = " ".join(key_words)
            if len(key_str) < 5 and num_words <= 2:
                continue
            char_pos, widx = find_in_word_index(word_list, norm_joined, key_words, search_word_idx)
            if char_pos != -1:
                found_char = char_pos
                found_widx = widx
                break

        if found_char != -1:
            known_char[i] = found_char
            search_word_idx = found_widx + 1

    # Add sentinel
    known_char[n] = flat_len

    # Build list of char positions, interpolating unknowns
    sorted_known = sorted(known_char.keys())
    positions = []

    for i in range(n):
        if i in known_char:
            positions.append(known_char[i])
        else:
            # Find surrounding known indices
            prev_ki = max(k for k in sorted_known if k <= i)
            next_ki = min(k for k in sorted_known if k > i)
            prev_p = known_char[prev_ki]
            next_p = known_char[next_ki]
            frac = (i - prev_ki) / (next_ki - prev_ki)
            interp = int(prev_p + frac * (next_p - prev_p))
            # Snap to nearest word boundary (start of a word)
            # Find word whose start is closest to interp
            best_pos = interp
            if word_list:
                dists = [(abs(w[0] - interp), w[0]) for w in word_list if w[0] >= prev_p]
                if dists:
                    best_pos = min(dists, key=lambda x: x[0])[1]
            positions.append(best_pos)

    positions.append(flat_len)

    # Ensure strictly non-decreasing, snapping to word starts
    for i in range(1, len(positions) - 1):
        if positions[i] <= positions[i - 1]:
            # Advance to next word after positions[i-1]
            later_words = [w[0] for w in word_list if w[0] > positions[i - 1]]
            positions[i] = later_words[0] if later_words else positions[i - 1] + 1

    # Extract verse texts
    result = []
    for i in range(n):
        start = min(positions[i], flat_len)
        end = min(positions[i + 1], flat_len)
        segment = flat[start:end].strip()
        result.append(segment)

    return result


def rebuild_chapter(book: str, chap: int) -> dict:
    """Rebuild one chapter of YYY1987."""
    yyy_path = os.path.join(YYY_DIR, book, f"{chap}.json")
    tcl_path = os.path.join(TCL_DIR, book, f"{chap}.json")

    # Load source YYY
    try:
        with open(yyy_path, "r", encoding="utf-8") as f:
            yyy_data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"  WARNING: Cannot read {yyy_path}: {e}")
        # Return empty structure
        try:
            with open(tcl_path, "r", encoding="utf-8") as f:
                tcl_data = json.load(f)
            content = [{"v": item["v"], "text": ""} for item in tcl_data.get("content", [])]
        except Exception:
            content = []
        return {"t": "YYY1987", "b": book, "c": chap, "content": content}

    # Load reference TCL02
    with open(tcl_path, "r", encoding="utf-8") as f:
        tcl_data = json.load(f)

    tcl_verses = tcl_data.get("content", [])
    n = len(tcl_verses)

    if n == 0:
        return {"t": "YYY1987", "b": book, "c": chap, "content": []}

    yyy_content = yyy_data.get("content", [])

    # Flatten YYY text
    flat = flatten_yyy(yyy_content)

    if not flat.strip():
        # No YYY text available
        content = [{"v": item["v"], "text": ""} for item in tcl_verses]
        return {"t": "YYY1987", "b": book, "c": chap, "content": content}

    # Segment into N verses
    segments = find_verse_boundaries(flat, tcl_verses)

    # Build output content with TCL02 verse numbers
    content = []
    for i, tcl_item in enumerate(tcl_verses):
        text = segments[i] if i < len(segments) else ""
        content.append({"v": tcl_item["v"], "text": text})

    return {"t": "YYY1987", "b": book, "c": chap, "content": content}


def main():
    import sys
    mode = sys.argv[1] if len(sys.argv) > 1 else "target"

    if mode == "target":
        # Fix the 25 problem chapters
        print(f"{'Book':<6} {'Chap':<5} {'Verses':<8} {'Empty':<6} Status")
        print("-" * 45)
        total_verses = 0
        total_empty = 0
        for book, chapters in TARGET_CHAPTERS:
            for chap in chapters:
                try:
                    vc, ec = rebuild_target_chapter(book, chap)
                    status = "OK" if ec == 0 else f"WARN: {ec} empty"
                    print(f"{book:<6} {chap:<5} {vc:<8} {ec:<6} {status}")
                    total_verses += vc
                    total_empty += ec
                except Exception as e:
                    print(f"{book:<6} {chap:<5} {'ERR':<8} {'?':<6} {e}")
        print("-" * 45)
        print(f"{'TOTAL':<6} {'':<5} {total_verses:<8} {total_empty:<6}")

    else:
        # Legacy mode: rebuild original BOOKS list
        summary = []
        for book, num_chapters in BOOKS:
            out_book_dir = os.path.join(OUT_DIR, book)
            os.makedirs(out_book_dir, exist_ok=True)
            for chap in range(1, num_chapters + 1):
                result = rebuild_chapter(book, chap)
                out_path = os.path.join(out_book_dir, f"{chap}.json")
                with open(out_path, "w", encoding="utf-8") as f:
                    json.dump(result, f, ensure_ascii=False, separators=(",", ":"))
                verse_count = len(result.get("content", []))
                summary.append((book, chap, verse_count))
                print(f"  {book} {chap:2d}: {verse_count} verses -> {out_path}")

        print("\n=== SUMMARY ===")
        print(f"{'Book':<6} {'Chap':>5} {'Verses':>7}")
        print("-" * 22)
        total = 0
        for book, chap, count in summary:
            print(f"{book:<6} {chap:>5} {count:>7}")
            total += count
        print("-" * 22)
        print(f"{'TOTAL':<6} {'':>5} {total:>7}")


if __name__ == "__main__":
    main()
