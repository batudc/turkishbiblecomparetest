#!/usr/bin/env python3
"""
realign_global.py — Full verse-number rebuild using global DP alignment.

Problems with the previous window-based approach:
  - Fragment verses (one canonical verse split into two YYY entries by OCR)
    advance the monotone floor too early, corrupting all subsequent labels.
  - A search window of ±15 cannot recover from offsets that are >15 away.

This script fixes both by:
  1. Pre-processing: merge fragment verses (a verse whose text ends without
     sentence-closing punctuation is almost certainly a line-break OCR artifact —
     its following entry is a fragment continuation and should be joined).
  2. Global DP alignment: build the full N×M similarity matrix, then find the
     best strictly-increasing assignment of TCL verse numbers to YYY verses via
     dynamic programming. No window constraint; finds the globally optimal match.
  3. Re-applies "?" split pass at the end for residual verse-boundary markers.

Run this script to replace the earlier rebuild_verse_numbers + split_merged passes.
"""

import json, re, difflib
from pathlib import Path

PROJECT  = Path(__file__).resolve().parent.parent
YYY_DIR  = PROJECT / 'data' / 'translations' / 'YYY1987'
TCL_DIR  = PROJECT / 'data' / 'translations' / 'TCL02'
REPORT   = PROJECT / 'output' / 'realign_global_report.txt'

SEARCH_WINDOW  = 15    # kept for the split pass only
MIN_CONFIDENCE = 0.20  # below this score, keep original number


# ---------------------------------------------------------------------------
# Similarity helpers
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

def sim(a: str, b: str) -> float:
    return 0.6 * char_sim(a, b) + 0.4 * word_jaccard(a, b)


# ---------------------------------------------------------------------------
# 1. Fragment merging
# ---------------------------------------------------------------------------
# A verse ending with NO punctuation at all is an OCR line-break fragment.
# Verses ending with comma/semicolon/colon are legitimate clauses that simply
# continue into the next verse at a natural boundary — do NOT merge those.
_SENT_END = re.compile(r'[.!?»""\'\")\]…›,;:]+\s*$', re.UNICODE)

def _ends_sentence(text: str) -> bool:
    return bool(_SENT_END.search(text))

def merge_fragments(verses: list[dict]) -> tuple[list[dict], int]:
    """
    Merge continuation fragments into the preceding verse.
    Returns (new_verses, n_merged).
    """
    if not verses:
        return verses, 0

    result   = []
    merged_n = 0

    buf_v    = verses[0]['v']
    buf_text = verses[0]['text']

    for verse in verses[1:]:
        text = verse['text']

        if not _ends_sentence(buf_text):
            # Previous verse is incomplete → current is a continuation fragment
            buf_text  = buf_text.rstrip() + ' ' + text.lstrip()
            merged_n += 1
        else:
            result.append({'v': buf_v, 'text': buf_text})
            buf_v    = verse['v']
            buf_text = text

    result.append({'v': buf_v, 'text': buf_text})
    return result, merged_n


# ---------------------------------------------------------------------------
# 2. Global DP alignment
# ---------------------------------------------------------------------------
NEG_INF = float('-inf')

def dp_align(yyy_verses: list[dict], tcl_verses: list[dict]) -> list[int]:
    """
    Return list of TCL verse numbers to assign to each YYY verse.
    Uses global DP (longest increasing subsequence with maximum score).
    """
    N = len(yyy_verses)
    M = len(tcl_verses)

    if N == 0:
        return []
    if M == 0:
        return [v['v'] for v in yyy_verses]

    # When YYY has more verses than TCL, a strictly-increasing DP assignment
    # is impossible for all N verses.  Keep original numbers; the caller's
    # fallback (max(orig, mono_floor)) handles ordering.
    if N > M:
        mono = 0
        result = []
        for v in yyy_verses:
            nv = max(v['v'], mono + 1) if mono else v['v']
            mono = nv
            result.append(nv)
        return result

    yyy_texts = [v['text'] for v in yyy_verses]
    tcl_texts = [v['text'] for v in tcl_verses]
    tcl_vnums = [v['v']    for v in tcl_verses]

    # Build score matrix (N × M) — precomputed once
    S = [[sim(yyy_texts[i], tcl_texts[j]) for j in range(M)] for i in range(N)]

    # dp[i][j] = best total score when yyy[i] is matched to tcl[j],
    # given all previous yyy verses were matched to tcl indices < j.
    dp    = [[NEG_INF] * M for _ in range(N)]
    prev  = [[-1]      * M for _ in range(N)]   # traceback

    # First YYY verse: can match any TCL verse
    for j in range(M):
        dp[0][j] = S[0][j]

    # Remaining YYY verses
    for i in range(1, N):
        running_best_val = NEG_INF
        running_best_j   = -1

        for j in range(M):
            # running_best = max(dp[i-1][k]) for k < j
            if j > 0 and dp[i-1][j-1] > running_best_val:
                running_best_val = dp[i-1][j-1]
                running_best_j   = j - 1

            if running_best_val > NEG_INF:
                dp[i][j] = S[i][j] + running_best_val
                prev[i][j] = running_best_j

    # Backtrack: find j* that maximises dp[N-1][j]
    j_star = max(range(M), key=lambda j: dp[N-1][j])
    assign = [0] * N
    assign[N-1] = j_star

    for i in range(N - 2, -1, -1):
        j_next    = assign[i + 1]
        # find k < j_next maximising dp[i][k]
        best_k    = max((k for k in range(j_next)), key=lambda k: dp[i][k], default=0)
        assign[i] = best_k

    # Apply MIN_CONFIDENCE: if score is too low, keep original
    result = []
    for i, j in enumerate(assign):
        if S[i][j] >= MIN_CONFIDENCE:
            result.append(tcl_vnums[j])
        else:
            # Low confidence: keep original, but clamp to be >= previous
            orig = yyy_verses[i]['v']
            prev_assigned = result[-1] if result else 0
            result.append(max(orig, prev_assigned + 1))

    return result


# ---------------------------------------------------------------------------
# 3. "?" verse-boundary split pass (re-applied after DP alignment)
# ---------------------------------------------------------------------------
SPLIT_PAT = re.compile(r'([.!»\'])\s+\?([A-ZÇĞIÖŞÜ])')

def split_question_markers(
    verses: list[dict],
    tcl_by_v: dict[int, str],
    tcl_v_list: list[int],
) -> tuple[list[dict], int]:
    """
    Split any verse that contains a ?[Capital] OCR verse-number marker.
    The text "...end. ?Start..." becomes two entries; the second one is
    matched against TCL to find its correct verse number.
    """
    new_verses = []
    splits_made = 0

    for verse in verses:
        text  = verse['text']
        cur_v = verse['v']
        m = SPLIT_PAT.search(text)
        if not m:
            new_verses.append(verse)
            continue

        split_pos = m.start() + 1
        part1 = text[:split_pos].strip()
        after = text[split_pos:].lstrip()
        part2 = after[1:].lstrip() if after.startswith('?') else after

        # Find best TCL verse for part2 (after cur_v)
        floor_idx = len(tcl_v_list)
        for i, vn in enumerate(tcl_v_list):
            if vn >= cur_v + 1:
                floor_idx = i
                break
        window = tcl_v_list[floor_idx : floor_idx + SEARCH_WINDOW]

        best_v2 = cur_v + 1
        best_s  = -1.0
        for tv in window:
            s = sim(part2, tcl_by_v[tv])
            if s > best_s:
                best_s, best_v2 = s, tv

        if best_s < MIN_CONFIDENCE:
            best_v2 = cur_v + 1

        new_verses.append({'v': cur_v,   'text': part1})
        new_verses.append({'v': best_v2, 'text': part2})
        splits_made += 1

    return new_verses, splits_made


# ---------------------------------------------------------------------------
# Per-chapter processing
# ---------------------------------------------------------------------------
def process_chapter(
    yyy_path: Path,
    tcl_path: Path,
    report_lines: list,
) -> dict:
    """
    Returns dict with keys: fragments_merged, verses_reassigned, splits_made.
    """
    yyy_data = json.loads(yyy_path.read_text(encoding='utf-8'))
    tcl_data = json.loads(tcl_path.read_text(encoding='utf-8'))

    verses_orig = yyy_data['content']
    tcl_verses  = tcl_data['content']
    tcl_by_v    = {v['v']: v['text'] for v in tcl_verses}
    tcl_v_list  = sorted(tcl_by_v.keys())

    book = yyy_data.get('b', yyy_path.parent.name)
    chap = yyy_data.get('c', int(yyy_path.stem))

    # Step 1: strip leading quote artifacts from verse text
    for v in verses_orig:
        v['text'] = re.sub(r'^[\'"""\'\'‚‛""]+\s*', '', v['text'])

    # Step 2: merge fragment verses
    verses_merged, n_frag = merge_fragments(verses_orig)

    # Step 3: global DP alignment
    new_vnums = dp_align(verses_merged, tcl_verses)
    reassigned = 0
    for verse, new_v in zip(verses_merged, new_vnums):
        if verse['v'] != new_v:
            reassigned += 1
            verse['v'] = new_v

    # Step 4: "?" split pass
    verses_split, n_splits = split_question_markers(verses_merged, tcl_by_v, tcl_v_list)

    # Step 5: sort, dedup
    verses_split.sort(key=lambda v: v['v'])
    seen: set[int] = set()
    final_verses = []
    for v in verses_split:
        if v['v'] not in seen:
            seen.add(v['v'])
            final_verses.append(v)

    changed = (
        n_frag > 0
        or reassigned > 0
        or n_splits > 0
        or len(final_verses) != len(verses_orig)
    )

    if changed:
        yyy_data['content'] = final_verses
        yyy_path.write_text(
            json.dumps(yyy_data, ensure_ascii=False, indent=2),
            encoding='utf-8',
        )
        if n_frag > 0 or n_splits > 0:
            report_lines.append(
                f'{book} {chap}: merged={n_frag} frags, split={n_splits} markers, '
                f'reassigned={reassigned} verse numbers'
            )

    return {'fragments_merged': n_frag, 'verses_reassigned': reassigned, 'splits_made': n_splits}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    REPORT.parent.mkdir(exist_ok=True)
    report_lines = ['YYY1987 GLOBAL RE-ALIGNMENT REPORT', '=' * 60]

    total_frag = total_reassign = total_splits = 0

    for book_dir in sorted(YYY_DIR.iterdir()):
        if not book_dir.is_dir():
            continue
        for yyy_file in sorted(book_dir.glob('*.json'), key=lambda p: int(p.stem)):
            chap     = int(yyy_file.stem)
            tcl_file = TCL_DIR / book_dir.name / f'{chap}.json'
            if not tcl_file.exists():
                continue

            stats = process_chapter(yyy_file, tcl_file, report_lines)
            total_frag     += stats['fragments_merged']
            total_reassign += stats['verses_reassigned']
            total_splits   += stats['splits_made']

    # Integrity check
    dup_count = order_count = 0
    for book_dir in sorted(YYY_DIR.iterdir()):
        if not book_dir.is_dir():
            continue
        for f in sorted(book_dir.glob('*.json')):
            d  = json.loads(f.read_text())
            vs = [v['v'] for v in d.get('content', [])]
            seen2: set[int] = set()
            for i, vn in enumerate(vs):
                if vn in seen2:
                    dup_count += 1
                seen2.add(vn)
                if i > 0 and vs[i] <= vs[i - 1]:
                    order_count += 1

    report_lines += [
        '',
        '=' * 60,
        f'Fragments merged    : {total_frag}',
        f'Verse numbers changed: {total_reassign}',
        f'? Splits applied    : {total_splits}',
        '',
        f'Post-run integrity:',
        f'  Duplicate verse numbers : {dup_count}',
        f'  Ordering violations     : {order_count}',
    ]

    REPORT.write_text('\n'.join(report_lines) + '\n', encoding='utf-8')

    # Print summary + any notable lines
    notable = [l for l in report_lines if 'merged=' in l and 'merged=0' not in l]
    for l in notable[:30]:
        print(l)
    print()
    print(report_lines[-9])
    print(report_lines[-8])
    print(report_lines[-7])
    print(report_lines[-5])
    print(report_lines[-4])
    print(report_lines[-3])
    print(f'\nFull report: {REPORT}')


if __name__ == '__main__':
    main()
