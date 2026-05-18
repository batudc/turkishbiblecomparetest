#!/usr/bin/env python3
"""
Reconstruct YYY1987 verses to match TCL02 canonical verse structure.

For each chapter:
1. Read YYY1987 and TCL02 JSON files
2. For each TCL02 verse, find the best matching text from YYY1987
3. Where YYY combines multiple verses, split text using TCL02 anchors
4. Missing content -> ""
5. Write output to YYY1987_REBUILT

Key logic:
- YYY entry with verse v that matches TCL02 verse v: use YYY text directly
- YYY entry with verse v that covers multiple TCL02 verses (e.g., YYY v4 contains
  content for TCL02 v4,v5,v6,v7): split using TCL02 boundary anchors
- TCL02 verses with no corresponding YYY content: use ""
- Strip intro/cross-ref text from verse entries
"""

import json
import os
import re

TRANS_ROOT = "/Users/batuhandemircan/website building/data/translations"
YYY_DIR = os.path.join(TRANS_ROOT, "YYY1987")
TCL_DIR = os.path.join(TRANS_ROOT, "TCL02")
OUT_DIR = os.path.join(TRANS_ROOT, "YYY1987_REBUILT")

BOOKS = {
    "PHP": 4,
    "COL": 4,
    "2TI": 4,
    "2PE": 3,
    "TIT": 3,
    "2TH": 3,
    "2JN": 1,
    "3JN": 1,
    "JUD": 1,
    "PHM": 1,
}

# Patterns to strip from verse text
STRIP_PATTERNS = [
    # Cross references like "3:16", "Mat.7:15", "Elç.12:12"
    r'\b[A-ZÇa-z][a-z]*\.\d+:\d+[-\d,]*',
    # Standalone verse refs like "3:16"
    r'\b\d+:\d+[-\d,;]*\b',
    # bkz. + word
    r'bkz\.\s*\S+',
    # Section headers like "Dipnotları", "Kaynak ayetler"
    r'Dipnotları.*?(?=\n|$)',
    r'Kaynak ayetler.*?(?=\n|$)',
    # "ELÇ" abbreviation
    r'\bELÇ\b',
    # Footnote numbers (lone digits/roman numerals at start of text fragments)
    # Small superscript markers like '" !' etc.
    r'[\'\"!]{1,2}(?=\s)',
    r'\s[\'\"!]{1,2}$',
    # Trailing cross-ref blocks (sequences of refs at end)
    r'\s+\d+:\d+(?:[,;]\s*\d+(?::\d+)?)*\s*$',
]

INTRO_PATTERNS = [
    r'Genel bakış:',
    r'Ana hatlar:',
    r'Mektubun içeriği:',
    r'\[verse text missing',
]


def is_intro_text(text):
    """Check if text looks like a book introduction rather than a verse."""
    for pat in INTRO_PATTERNS:
        if re.search(pat, text):
            return True
    # Very long text (>500 chars) for verse 1 is likely intro
    if len(text) > 500:
        return True
    return False


def clean_text(text):
    """Strip cross-refs and other noise from verse text."""
    if not text:
        return ""

    # Strip trailing cross-ref blocks (series of ref numbers at end)
    # e.g. "1:1-4; 4:29 1:3; 3:22-25:4:14-15;5:5.9-12,18,20"
    text = re.sub(r'\s+[\d:;.,\- ]{10,}$', '', text).strip()

    # Remove footnote/cross-ref markers: lone digits like " 3 " or at end
    text = re.sub(r'\s+\d+\s*$', '', text).strip()

    # Remove leading/trailing punctuation artifacts
    text = text.strip(' \'"!,')

    return text


def find_split_point(combined_text, anchor_text, min_pos=0):
    """
    Find where anchor_text (from TCL02 next verse) might begin in combined_text.
    Returns position in combined_text, or -1 if not found.

    Uses first 60 chars of anchor as search key.
    """
    if not anchor_text:
        return -1

    # Use first meaningful words of anchor (skip short words)
    words = anchor_text.split()
    # Try with first 5 significant words
    search_key = ' '.join(words[:5]) if len(words) >= 5 else anchor_text[:40]

    # Remove punctuation for fuzzy match
    def normalize(s):
        s = re.sub(r'[.,;:!?\'"«»–—]', '', s)
        s = re.sub(r'\s+', ' ', s)
        return s.lower().strip()

    norm_combined = normalize(combined_text)
    norm_key = normalize(search_key)

    if not norm_key:
        return -1

    pos = norm_combined.find(norm_key, min_pos)
    if pos < 0:
        # Try with first 3 words
        search_key2 = ' '.join(words[:3]) if len(words) >= 3 else anchor_text[:25]
        norm_key2 = normalize(search_key2)
        pos = norm_combined.find(norm_key2, min_pos)

    if pos < 0:
        return -1

    # Map normalized position back to original text position
    # Count characters in original that correspond to normalized chars up to pos
    orig_pos = 0
    norm_pos = 0
    orig_text_normalized = normalize(combined_text)

    # Simple heuristic: ratio of positions
    if len(orig_text_normalized) > 0:
        ratio = len(combined_text) / len(orig_text_normalized)
        approx_orig = int(pos * ratio)
        # Search backwards from approx_orig for a sentence boundary or space
        search_start = max(0, approx_orig - 20)
        search_end = min(len(combined_text), approx_orig + 20)
        # Find space near the approx position
        chunk = combined_text[search_start:search_end]
        space_pos = chunk.rfind(' ')
        if space_pos >= 0:
            return search_start + space_pos + 1
        return approx_orig

    return -1


def split_combined_verses(yyy_text, tcl_verses_for_range):
    """
    Given a combined YYY text that covers multiple TCL02 verses,
    split it to match each TCL02 verse.

    tcl_verses_for_range: list of (verse_num, tcl_text) for the verses covered
    Returns: dict of verse_num -> extracted_text
    """
    result = {}
    n = len(tcl_verses_for_range)

    if n == 0:
        return result

    if n == 1:
        v_num, _ = tcl_verses_for_range[0]
        result[v_num] = clean_text(yyy_text)
        return result

    # Try to split the combined text using TCL02 anchors
    remaining_text = yyy_text
    splits = []

    for i in range(n - 1):
        v_num, tcl_text = tcl_verses_for_range[i]
        next_v_num, next_tcl_text = tcl_verses_for_range[i + 1]

        # Find where next verse begins in remaining_text
        split_pos = find_split_point(remaining_text, next_tcl_text)

        if split_pos > 0 and split_pos < len(remaining_text) - 5:
            splits.append((v_num, remaining_text[:split_pos].strip()))
            remaining_text = remaining_text[split_pos:].strip()
        else:
            # Can't split cleanly - assign all remaining to first, empty to rest
            splits.append((v_num, remaining_text.strip()))
            remaining_text = ""

    # Last verse gets whatever remains
    last_v_num = tcl_verses_for_range[-1][0]
    splits.append((last_v_num, remaining_text.strip()))

    for v_num, text in splits:
        result[v_num] = clean_text(text)

    return result


def rebuild_chapter(book, chap_num):
    """Rebuild a single chapter of YYY1987 to match TCL02 verse structure."""

    yyy_path = os.path.join(YYY_DIR, book, f"{chap_num}.json")
    tcl_path = os.path.join(TCL_DIR, book, f"{chap_num}.json")

    with open(yyy_path, 'r', encoding='utf-8') as f:
        yyy_data = json.load(f)

    with open(tcl_path, 'r', encoding='utf-8') as f:
        tcl_data = json.load(f)

    # Build lookup: YYY verse_num -> text
    yyy_by_verse = {}
    for entry in yyy_data['content']:
        v = entry['v']
        text = entry.get('text', '')
        yyy_by_verse[v] = text

    # Get TCL02 verse list in order
    tcl_verses = [(e['v'], e['text']) for e in tcl_data['content']]
    tcl_verse_nums = [v for v, _ in tcl_verses]
    tcl_verse_set = set(tcl_verse_nums)

    # Get YYY verse numbers in order
    yyy_verse_nums_ordered = [e['v'] for e in yyy_data['content']]

    # For each YYY entry, determine which TCL02 verses it covers
    # A YYY entry with verse v covers all TCL02 verses from v up to (but not including)
    # the next YYY verse number

    # Build: yyy_verse -> list of TCL02 verses it should cover
    yyy_verse_to_tcl = {}

    for i, yyy_v in enumerate(yyy_verse_nums_ordered):
        # Find the next YYY verse number
        if i + 1 < len(yyy_verse_nums_ordered):
            next_yyy_v = yyy_verse_nums_ordered[i + 1]
        else:
            next_yyy_v = 999999

        # Find TCL02 verses in range [yyy_v, next_yyy_v)
        covered = [(v, t) for v, t in tcl_verses if yyy_v <= v < next_yyy_v]
        yyy_verse_to_tcl[yyy_v] = covered

    # Now build the output
    # For each TCL02 verse, find its text from YYY
    tcl_to_text = {}

    for yyy_v in yyy_verse_nums_ordered:
        yyy_text = yyy_by_verse[yyy_v]
        covered_tcl = yyy_verse_to_tcl[yyy_v]

        if not covered_tcl:
            continue

        # Check if this is intro text for verse 1
        if yyy_v == 1 and is_intro_text(yyy_text):
            # Try to find verse 1 text at the end of the intro
            # Often the intro ends with the actual verse 1 text
            # Split at last sentence that looks like a verse start
            # For now, mark as empty
            # But check if there's actual verse text at the end
            # Look for patterns like "...selam!" which is typical for v1

            # Find the last occurrence of actual verse content
            # Try to find where the real verse 1 begins (after intro markers)
            # Common pattern: intro ends, then actual verse text begins

            # Look for "selam!" or similar greeting
            verse_start = -1
            for marker in ['selam!', 'çağrılmışlara selam', 'esenlik olsun']:
                idx = yyy_text.rfind(marker)
                if idx > 0:
                    # Find start of the sentence containing this marker
                    sent_start = yyy_text.rfind('.', 0, idx)
                    if sent_start < 0:
                        sent_start = 0
                    else:
                        sent_start += 1
                    verse_start = sent_start
                    break

            if verse_start > 0 and len(yyy_text) - verse_start < 400:
                actual_v1_text = yyy_text[verse_start:].strip()
                # Assign to first covered TCL verse
                first_v = covered_tcl[0][0]
                tcl_to_text[first_v] = clean_text(actual_v1_text)
                # Rest are empty
                for v, _ in covered_tcl[1:]:
                    if v not in tcl_to_text:
                        tcl_to_text[v] = ""
            else:
                # Mark all as empty
                for v, _ in covered_tcl:
                    if v not in tcl_to_text:
                        tcl_to_text[v] = ""
            continue

        # Check for "[verse text missing" marker
        if '[verse text missing' in yyy_text:
            for v, _ in covered_tcl:
                if v not in tcl_to_text:
                    tcl_to_text[v] = ""
            continue

        if len(covered_tcl) == 1:
            # Direct mapping
            v_num = covered_tcl[0][0]
            tcl_to_text[v_num] = clean_text(yyy_text)
        else:
            # Need to split
            split_result = split_combined_verses(yyy_text, covered_tcl)
            for v_num, text in split_result.items():
                tcl_to_text[v_num] = text

    # Build final content list following TCL02 verse order
    output_content = []
    for v_num, tcl_text in tcl_verses:
        text = tcl_to_text.get(v_num, "")
        output_content.append({"v": v_num, "text": text})

    return {
        "t": "YYY1987",
        "b": book,
        "c": chap_num,
        "content": output_content
    }


def main():
    for book, num_chapters in BOOKS.items():
        out_book_dir = os.path.join(OUT_DIR, book)
        os.makedirs(out_book_dir, exist_ok=True)

        for chap in range(1, num_chapters + 1):
            print(f"Processing {book} chapter {chap}...")

            try:
                result = rebuild_chapter(book, chap)

                out_path = os.path.join(out_book_dir, f"{chap}.json")
                with open(out_path, 'w', encoding='utf-8') as f:
                    json.dump(result, f, ensure_ascii=False, indent=2)

                # Print summary
                total = len(result['content'])
                empty = sum(1 for e in result['content'] if not e['text'])
                print(f"  -> {total} verses, {empty} empty")

            except Exception as e:
                print(f"  ERROR: {e}")
                import traceback
                traceback.print_exc()


if __name__ == '__main__':
    main()
