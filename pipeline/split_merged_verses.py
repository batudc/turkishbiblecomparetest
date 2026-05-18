#!/usr/bin/env python3
"""
split_merged_verses.py — Split merged YYY1987 verses at OCR'd verse-number markers.

In the source PDF, verse numbers appear as superscripts. OCR often misread these as
"?" characters, producing text like:
    "...end of v7 text. ?Start of v9 text..."
where the "?" is the OCR'd digit for verse 9.

This script:
  1. Detects   [sentence-end][whitespace]?[CapitalLetter]   patterns in verse texts
  2. Splits the verse at that boundary (removing the stray "?")
  3. Matches the second segment against TCL02 to assign the correct canonical number
  4. Re-sorts and deduplicates each chapter
  5. Re-runs the full alignment pass so all verse numbers stay consistent

Run ONCE after rebuild_verse_numbers.py has already assigned initial verse numbers.
"""

import json, re, difflib
from pathlib import Path

PROJECT  = Path(__file__).resolve().parent.parent
YYY_DIR  = PROJECT / 'data' / 'translations' / 'YYY1987'
TCL_DIR  = PROJECT / 'data' / 'translations' / 'TCL02'
REPORT   = PROJECT / 'output' / 'split_merged_report.txt'

# A "?" that appears AFTER sentence-ending punctuation and BEFORE a capital letter
# is an OCR'd verse number, not a real question mark.
SPLIT_PAT = re.compile(r'([.!»\'])\s+\?([A-ZÇĞIÖŞÜ])')

SEARCH_WINDOW  = 15
MIN_CONFIDENCE = 0.20   # lower threshold for split segments (shorter text)


# ---------------------------------------------------------------------------
# Similarity helpers (same as rebuild_verse_numbers.py)
# ---------------------------------------------------------------------------
def _norm(text: str, n: int) -> str:
    t = text.lower()
    t = re.sub(r'[^\w\s]', '', t, flags=re.UNICODE)
    return t[:n].strip()

def char_sim(a: str, b: str) -> float:
    na, nb = _norm(a, 80), _norm(b, 80)
    if not na or not nb:
        return 0.0
    return difflib.SequenceMatcher(None, na, nb).ratio()

def word_jaccard(a: str, b: str) -> float:
    na, nb = _norm(a, 200), _norm(b, 200)
    wa, wb = set(na.split()), set(nb.split())
    if not wa or not wb:
        return 0.0
    return len(wa & wb) / len(wa | wb)

def score(a: str, b: str) -> float:
    return 0.6 * char_sim(a, b) + 0.4 * word_jaccard(a, b)


# ---------------------------------------------------------------------------
# Split pass
# ---------------------------------------------------------------------------
def split_chapter(yyy_path: Path, tcl_path: Path, report_lines: list) -> int:
    yyy_data = json.loads(yyy_path.read_text(encoding='utf-8'))
    tcl_data = json.loads(tcl_path.read_text(encoding='utf-8'))
    tcl_by_v  = {v['v']: v['text'] for v in tcl_data['content']}
    tcl_v_list = sorted(tcl_by_v.keys())

    book = yyy_data.get('b', yyy_path.parent.name)
    chap = yyy_data.get('c', int(yyy_path.stem))

    new_content = []
    splits_made = 0

    for verse in yyy_data['content']:
        text   = verse['text']
        cur_v  = verse['v']

        m = SPLIT_PAT.search(text)
        if not m:
            new_content.append(verse)
            continue

        # --- Split the verse ---
        # Part 1: everything up to and including the sentence-ending char
        split_end_of_p1 = m.start() + 1      # position after [.!»'] char
        part1 = text[:split_end_of_p1].strip()

        # Part 2: everything after the "?" (skip whitespace and the "?" itself)
        after_marker = text[split_end_of_p1:].lstrip()
        assert after_marker[0] == '?', f'Expected ? at split point, got: {repr(after_marker[:5])}'
        part2 = after_marker[1:].lstrip()     # strip the "?" and any trailing space

        # Find the correct verse number for part 2 by TCL02 matching
        floor_idx = len(tcl_v_list)
        for i, vn in enumerate(tcl_v_list):
            if vn >= cur_v + 1:
                floor_idx = i
                break
        window = tcl_v_list[floor_idx : floor_idx + SEARCH_WINDOW]

        best_v2    = cur_v + 1   # fallback
        best_score = -1.0
        for tcl_v in window:
            s = score(part2, tcl_by_v[tcl_v])
            if s > best_score:
                best_score = s
                best_v2 = tcl_v

        if best_score < MIN_CONFIDENCE:
            best_v2 = cur_v + 1   # fallback: just increment

        splits_made += 1
        report_lines.append(
            f'  {book} {chap}:{cur_v} → {cur_v}+{best_v2}  (score={best_score:.2f})'
        )
        report_lines.append(f'    p1: {part1[:70]}')
        report_lines.append(f'    p2: {part2[:70]}')

        new_content.append({'v': cur_v,  'text': part1})
        new_content.append({'v': best_v2,'text': part2})

    if splits_made == 0:
        return 0

    # Merge into full data, sort by verse number, dedup (keep first of each v)
    yyy_data['content'] = new_content
    yyy_data['content'].sort(key=lambda v: v['v'])

    seen: set[int] = set()
    deduped = []
    for v in yyy_data['content']:
        if v['v'] not in seen:
            seen.add(v['v'])
            deduped.append(v)
        else:
            report_lines.append(f'  DEDUP: dropped duplicate v{v["v"]} in {book} {chap}')
    yyy_data['content'] = deduped

    yyy_path.write_text(
        json.dumps(yyy_data, ensure_ascii=False, indent=2),
        encoding='utf-8',
    )
    return splits_made


# ---------------------------------------------------------------------------
# Alignment pass (re-run after splitting)
# ---------------------------------------------------------------------------
def realign_chapter(yyy_path: Path, tcl_path: Path) -> int:
    """Light re-alignment: ensure verse numbers are still the best TCL match
    and are strictly increasing.  Returns count of reassignments."""
    yyy_data = json.loads(yyy_path.read_text(encoding='utf-8'))
    tcl_data = json.loads(tcl_path.read_text(encoding='utf-8'))
    tcl_by_v  = {v['v']: v['text'] for v in tcl_data['content']}
    tcl_v_list = sorted(tcl_by_v.keys())

    if not tcl_v_list:
        return 0

    changed      = 0
    mono_floor   = 0

    for verse in yyy_data['content']:
        orig_v = verse['v']
        text   = verse['text']

        floor_idx = len(tcl_v_list)
        for i, vn in enumerate(tcl_v_list):
            if vn >= mono_floor:
                floor_idx = i
                break
        window = tcl_v_list[floor_idx : floor_idx + SEARCH_WINDOW]

        best_v    = None
        best_score = -1.0
        for tcl_v in window:
            s = score(text, tcl_by_v[tcl_v])
            if s > best_score:
                best_score = s
                best_v = tcl_v

        if best_score >= 0.20 and best_v is not None:
            new_v = best_v
        else:
            new_v = max(orig_v, mono_floor)

        mono_floor = new_v + 1

        if new_v != orig_v:
            verse['v'] = new_v
            changed += 1

    if changed:
        yyy_data['content'].sort(key=lambda v: v['v'])
        seen: set[int] = set()
        deduped = []
        for v in yyy_data['content']:
            if v['v'] not in seen:
                seen.add(v['v'])
                deduped.append(v)
        yyy_data['content'] = deduped
        yyy_path.write_text(
            json.dumps(yyy_data, ensure_ascii=False, indent=2),
            encoding='utf-8',
        )

    return changed


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    REPORT.parent.mkdir(exist_ok=True)
    report_lines = ['YYY1987 SPLIT MERGED VERSES REPORT', '=' * 60]

    total_splits    = 0
    total_realigned = 0

    for book_dir in sorted(YYY_DIR.iterdir()):
        if not book_dir.is_dir():
            continue
        for yyy_file in sorted(book_dir.glob('*.json'), key=lambda p: int(p.stem)):
            chap     = int(yyy_file.stem)
            tcl_file = TCL_DIR / book_dir.name / f'{chap}.json'
            if not tcl_file.exists():
                continue

            n_splits = split_chapter(yyy_file, tcl_file, report_lines)
            total_splits += n_splits

    report_lines.append(f'\n--- Re-alignment pass after splits ---')

    for book_dir in sorted(YYY_DIR.iterdir()):
        if not book_dir.is_dir():
            continue
        for yyy_file in sorted(book_dir.glob('*.json'), key=lambda p: int(p.stem)):
            chap     = int(yyy_file.stem)
            tcl_file = TCL_DIR / book_dir.name / f'{chap}.json'
            if not tcl_file.exists():
                continue
            n = realign_chapter(yyy_file, tcl_file)
            total_realigned += n

    report_lines.append(f'\n{"="*60}')
    report_lines.append(f'Total verses split   : {total_splits}')
    report_lines.append(f'Re-aligned after split: {total_realigned}')

    REPORT.write_text('\n'.join(report_lines) + '\n', encoding='utf-8')
    print('\n'.join(report_lines[-20:]))
    print(f'\nFull report: {REPORT}')


if __name__ == '__main__':
    main()
