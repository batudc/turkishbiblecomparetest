#!/usr/bin/env python3
"""
rebuild_verse_numbers.py — Rebuild YYY1987 verse numbers via semantic alignment
against TCL02 as the canonical reference.

YYY1987 is a compressed study Bible: N canonical verses are merged into one YYY
verse. The verse label should be the FIRST canonical verse in the merged block.
Since PDF extraction lost track of that label, most chapters have sequential
v1,v2,v3... labels instead of the correct v1,v2,v5,v8... canonical labels.

Algorithm per chapter:
  1. Strip leading ' / '' / " OCR artifacts from verse texts.
  2. Load TCL02 reference verses for the same book/chapter.
  3. For each YYY verse (in order), find the best-matching TCL02 verse using
     a combined similarity score within a forward search window.
  4. Apply monotonic constraint: assigned numbers never decrease.
  5. Write corrected JSON + append to alignment report.
"""

import json, re, difflib
from pathlib import Path

PROJECT  = Path(__file__).resolve().parent.parent
YYY_DIR  = PROJECT / 'data' / 'translations' / 'YYY1987'
TCL_DIR  = PROJECT / 'data' / 'translations' / 'TCL02'
REPORT   = PROJECT / 'output' / 'verse_numbering_report.txt'

# Leading-quote artifact patterns to strip from verse text only (not the number)
_LEAD_QUOTE = re.compile(r'^[\'"""‘’‚‛“”]+\s*')

SEARCH_WINDOW   = 15   # how many TCL verses ahead we look
MIN_CONFIDENCE  = 0.25 # below this score, keep original YYY number


# ---------------------------------------------------------------------------
# Similarity helpers
# ---------------------------------------------------------------------------
def _normalise(text: str, n: int) -> str:
    """Lowercase + strip punctuation, take first n chars."""
    text = text.lower()
    text = re.sub(r'[^\w\s]', '', text, flags=re.UNICODE)
    return text[:n].strip()


def char_sim(a: str, b: str) -> float:
    """SequenceMatcher ratio on first 80 chars (normalised)."""
    na, nb = _normalise(a, 80), _normalise(b, 80)
    if not na or not nb:
        return 0.0
    return difflib.SequenceMatcher(None, na, nb).ratio()


def word_jaccard(a: str, b: str) -> float:
    """Jaccard overlap on word sets of first 200 chars (normalised)."""
    na, nb = _normalise(a, 200), _normalise(b, 200)
    wa = set(na.split())
    wb = set(nb.split())
    if not wa or not wb:
        return 0.0
    return len(wa & wb) / len(wa | wb)


def score(a: str, b: str) -> float:
    return 0.6 * char_sim(a, b) + 0.4 * word_jaccard(a, b)


# ---------------------------------------------------------------------------
# Per-chapter processing
# ---------------------------------------------------------------------------
def process_chapter(yyy_path: Path, tcl_path: Path, report_lines: list) -> int:
    """
    Align verse numbers for one chapter.
    Returns count of verses whose number was changed.
    """
    yyy_data = json.loads(yyy_path.read_text(encoding='utf-8'))
    tcl_data = json.loads(tcl_path.read_text(encoding='utf-8'))

    yyy_verses = yyy_data['content']
    tcl_verses = tcl_data['content']

    if not tcl_verses:
        return 0

    book = yyy_data.get('b', yyy_path.parent.name)
    chap = yyy_data.get('c', int(yyy_path.stem))

    # Build TCL lookup: verse_number → text
    tcl_by_v: dict[int, str] = {v['v']: v['text'] for v in tcl_verses}
    tcl_v_list = sorted(tcl_by_v.keys())   # sorted canonical verse numbers

    changed = 0
    monotone_floor = 0   # assigned numbers must be >= this

    chapter_log = []

    for yyy_verse in yyy_verses:
        original_v = yyy_verse['v']
        raw_text    = yyy_verse['text']

        # Step 1: strip leading quote artifacts
        clean_text = _LEAD_QUOTE.sub('', raw_text)
        if clean_text != raw_text:
            yyy_verse['text'] = clean_text
            raw_text = clean_text

        # Step 2: determine search range
        # Start from the current monotone floor; look up to SEARCH_WINDOW ahead
        floor_idx = len(tcl_v_list)  # default: past end → empty window
        for i, vn in enumerate(tcl_v_list):
            if vn >= monotone_floor:
                floor_idx = i
                break

        window = tcl_v_list[floor_idx : floor_idx + SEARCH_WINDOW]

        # Step 3: score each candidate
        best_v    = None
        best_score = -1.0
        for tcl_v in window:
            s = score(raw_text, tcl_by_v[tcl_v])
            if s > best_score:
                best_score = s
                best_v = tcl_v

        # Step 4: apply assignment
        if best_score >= MIN_CONFIDENCE and best_v is not None:
            new_v = best_v
        else:
            # Low confidence: keep original, but still enforce monotone
            new_v = max(original_v, monotone_floor)

        # Monotone floor update: strictly increasing — next verse must be > this
        monotone_floor = new_v + 1

        if new_v != original_v:
            changed += 1
            chapter_log.append(
                f'  v{original_v:>3} → v{new_v:>3}  score={best_score:.2f}  '
                f'{raw_text[:55]}'
            )
            yyy_verse['v'] = new_v
        else:
            chapter_log.append(
                f'  v{original_v:>3} → v{new_v:>3}  score={best_score:.2f}  '
                f'[unchanged]  {raw_text[:40]}'
            )

    if changed > 0:
        report_lines.append(f'\n{book} {chap}  ({changed} renumbered)')
        report_lines.extend(chapter_log)

        # Re-sort by new verse number (should already be monotone but be safe)
        yyy_data['content'].sort(key=lambda v: v['v'])

        # Check for duplicate verse numbers after alignment (safety net)
        seen: set[int] = set()
        for v in yyy_data['content']:
            vn = v['v']
            if vn in seen:
                report_lines.append(
                    f'  WARNING: duplicate v{vn} after alignment — dropping later occurrence'
                )
                v['_dup'] = True
            else:
                seen.add(vn)
        yyy_data['content'] = [v for v in yyy_data['content'] if not v.get('_dup')]

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
    report_lines = ['YYY1987 VERSE NUMBERING REBUILD REPORT', '=' * 60]

    total_chapters  = 0
    total_changed_v = 0
    skipped_no_tcl  = []

    for book_dir in sorted(YYY_DIR.iterdir()):
        if not book_dir.is_dir():
            continue
        book = book_dir.name
        tcl_book_dir = TCL_DIR / book

        for yyy_file in sorted(book_dir.glob('*.json'), key=lambda p: int(p.stem)):
            chap = int(yyy_file.stem)
            tcl_file = tcl_book_dir / f'{chap}.json'

            if not tcl_file.exists():
                skipped_no_tcl.append(f'{book}/{chap}')
                continue

            total_chapters += 1
            n = process_chapter(yyy_file, tcl_file, report_lines)
            total_changed_v += n

    report_lines.append('\n' + '=' * 60)
    report_lines.append(f'Chapters processed : {total_chapters}')
    report_lines.append(f'Verses renumbered  : {total_changed_v}')
    if skipped_no_tcl:
        report_lines.append(f'Skipped (no TCL02) : {len(skipped_no_tcl)}')
        for s in skipped_no_tcl:
            report_lines.append(f'  {s}')

    report_text = '\n'.join(report_lines) + '\n'
    REPORT.write_text(report_text, encoding='utf-8')
    print(report_text[:3000])
    if len(report_text) > 3000:
        print(f'... (truncated; full report at {REPORT})')
    print(f'\nDone. {total_changed_v} verse numbers updated across {total_chapters} chapters.')


if __name__ == '__main__':
    main()
