#!/usr/bin/env python3
"""
Rebuild YYY1987 ACT chapters using TCL02 canonical verse numbers.
"""
import json
import re
import os

BASE = "/Users/batuhandemircan/website building"
YYY_DIR = os.path.join(BASE, "data/translations/YYY1987/ACT")
TCL_DIR = os.path.join(BASE, "data/translations/TCL02/ACT")
OUT_DIR = os.path.join(BASE, "data/translations/YYY1987_REBUILT/ACT")

os.makedirs(OUT_DIR, exist_ok=True)


def clean_yyy_text(text):
    """Strip cross-refs, footnote markers, section headers, lone digits/romans.

    Section headers in YYY appear as '>Text here' — we strip the '>' marker
    but KEEP the following text (it's part of the narrative).
    """
    # Remove cross-references like 3:16 or 10:20-25
    text = re.sub(r'\b\d{1,3}:\d{1,3}(?:-\d{1,3})?\b', '', text)
    # Remove "bkz." followed by word
    text = re.sub(r'\bbkz\.\s*\S+', '', text, flags=re.IGNORECASE)
    # Remove "bkz:" followed by word
    text = re.sub(r'\bbkz:\s*\S+', '', text, flags=re.IGNORECASE)
    # Strip ">" section markers but keep text after them (replace '>' with space)
    text = text.replace('>', ' ')
    # Remove "Dipnotları" keyword and following content on same token
    text = re.sub(r'\bDipnotlar[ıi]\b.*', '', text)
    # Remove "Kaynak ayetler" and following
    text = re.sub(r'\bKaynak ayetler\b.*', '', text)
    # Remove lone "ELÇ" abbreviation
    text = re.sub(r'\bELÇ\b', '', text)
    # Remove lone roman numerals (uppercase, standalone)
    text = re.sub(r'(?<!\w)(XIV|XIII|XII|XI|IX|VIII|VII|VI|IV|III|II|XI|X|V|I)(?!\w)', '', text)
    # Remove lone digits (standalone numbers not part of words)
    text = re.sub(r'(?<!\w)\d+(?!\w)', '', text)
    # Remove exclamation marks used as footnote markers (e.g., "kilometre!")
    text = re.sub(r'(\w)!', r'\1', text)
    # Collapse multiple spaces
    text = re.sub(r' {2,}', ' ', text)
    text = text.strip()
    return text


def normalize(text):
    """Normalize for comparison: remove quotes, lowercase, collapse whitespace."""
    text = text.replace('“', '').replace('”', '')  # " "
    text = text.replace('‘', '').replace('’', '')  # ' '
    text = text.replace('«', '').replace('»', '')  # « »
    text = text.replace('"', '').replace("'", '')
    text = re.sub(r'\s+', ' ', text)
    return text.strip().lower()


def flatten_yyy(yyy_data):
    """Join all verse texts into one big string."""
    parts = []
    for entry in yyy_data["content"]:
        t = entry["text"].strip()
        if t:
            parts.append(t)
    return " ".join(parts)


def find_split_pos_in_norm(flat_norm, anchor_norm, search_start=0):
    """
    Find position in normalized flat_norm where anchor begins.
    Try progressively shorter matches. Returns index or -1.
    """
    for length in [50, 35, 25, 18, 12, 8]:
        probe = anchor_norm[:length].strip()
        if len(probe) < 5:
            continue
        idx = flat_norm.find(probe, search_start)
        if idx != -1:
            return idx
    return -1


def build_norm_to_raw_map(flat_raw, flat_norm):
    """
    Build a mapping from normalized position -> raw position.
    Both flat_raw and flat_norm are derived from same source, but
    flat_norm has had quotes/punctuation removed and lowercased.

    We build this by aligning characters.
    """
    # Simple approach: track position correspondence
    norm_to_raw = []
    raw_pos = 0
    for norm_ch in flat_norm:
        # Advance in raw until we find a char that would normalize to norm_ch
        while raw_pos < len(flat_raw):
            raw_ch_norm = normalize(flat_raw[raw_pos])
            if raw_ch_norm == norm_ch:
                norm_to_raw.append(raw_pos)
                raw_pos += 1
                break
            else:
                raw_pos += 1
        else:
            norm_to_raw.append(raw_pos)
    return norm_to_raw


def segment_text(flat_raw, tcl_data):
    """
    Segment flat_raw into N parts aligned with TCL02 verses.
    Uses TCL02 verse text (normalized) as boundary anchors.
    """
    flat_norm = normalize(flat_raw)
    n = len(tcl_data["content"])

    if n == 0:
        return []

    # Find split positions in NORMALIZED space
    norm_split_positions = []
    search_start = 0

    for i, entry in enumerate(tcl_data["content"]):
        anchor_norm = normalize(entry["text"])
        v = entry["v"]

        if i == 0:
            pos = find_split_pos_in_norm(flat_norm, anchor_norm, 0)
            norm_split_positions.append(max(0, pos) if pos != -1 else 0)
            search_start = norm_split_positions[0]
        else:
            pos = find_split_pos_in_norm(flat_norm, anchor_norm, search_start)
            if pos != -1 and pos > search_start - 5:  # allow slight overlap
                norm_split_positions.append(pos)
                search_start = pos + 1
            else:
                norm_split_positions.append(-1)  # mark as missing

    # Fill in missing positions by interpolation
    for i in range(len(norm_split_positions)):
        if norm_split_positions[i] == -1:
            prev_pos = 0
            for j in range(i - 1, -1, -1):
                if norm_split_positions[j] != -1:
                    prev_pos = norm_split_positions[j]
                    break

            # Find next valid
            next_pos = len(flat_norm)
            count_missing = 0
            k = i
            while k < len(norm_split_positions) and norm_split_positions[k] == -1:
                count_missing += 1
                k += 1
            if k < len(norm_split_positions):
                next_pos = norm_split_positions[k]

            # Distribute evenly among missing positions
            gap = next_pos - prev_pos
            idx_in_run = 0
            for m in range(i, i + count_missing):
                idx_in_run += 1
                norm_split_positions[m] = prev_pos + gap * idx_in_run // (count_missing + 1)
            # Skip ahead
            i += count_missing - 1

    # Build norm_to_raw map
    norm_to_raw = build_norm_to_raw_map(flat_raw, flat_norm)

    def norm_pos_to_raw(norm_pos):
        if norm_pos >= len(norm_to_raw):
            return len(flat_raw)
        return norm_to_raw[norm_pos]

    # Extract segments using raw positions
    segments = []
    for i, entry in enumerate(tcl_data["content"]):
        v = entry["v"]
        norm_start = norm_split_positions[i]
        norm_end = norm_split_positions[i + 1] if i + 1 < n else len(flat_norm)

        raw_start = norm_pos_to_raw(norm_start)
        raw_end = norm_pos_to_raw(norm_end)

        seg = flat_raw[raw_start:raw_end].strip()
        seg = re.sub(r'\s+', ' ', seg).strip()
        segments.append((v, seg))

    return segments


def process_chapter(chap_num):
    yyy_path = os.path.join(YYY_DIR, f"{chap_num}.json")
    tcl_path = os.path.join(TCL_DIR, f"{chap_num}.json")
    out_path = os.path.join(OUT_DIR, f"{chap_num}.json")

    with open(yyy_path, "r", encoding="utf-8") as f:
        yyy_data = json.load(f)
    with open(tcl_path, "r", encoding="utf-8") as f:
        tcl_data = json.load(f)

    # Step 1: Flatten YYY
    flat_raw = flatten_yyy(yyy_data)

    # Step 2: Clean
    flat_clean = clean_yyy_text(flat_raw)

    # Step 3: Segment using normalized anchors
    segments = segment_text(flat_clean, tcl_data)

    # Step 4: Build output
    content = []
    for (v, text) in segments:
        content.append({"v": v, "text": text})

    output = {
        "t": "YYY1987",
        "b": "ACT",
        "c": chap_num,
        "content": content
    }

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    ok = len(segments) == len(tcl_data["content"])
    status = "OK" if ok else "MISMATCH"
    print(f"Chapter {chap_num}: {len(segments)} segments (TCL02={len(tcl_data['content'])} verses) [{status}]")
    return ok


if __name__ == "__main__":
    all_ok = True
    for chap in range(1, 29):
        ok = process_chapter(chap)
        if not ok:
            all_ok = False
    if all_ok:
        print("\nAll chapters processed successfully!")
    else:
        print("\nSome chapters had issues — check warnings above.")
