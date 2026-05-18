"""
orchestrator.py — Main runner for the YYY1987 OCR correction pipeline.

Usage:
    python orchestrator.py [--books BOOK1 BOOK2 ...] [--force] [--dry-run]

Flags:
    --books     Process only these book codes (default: all books in YYY1987/)
    --force     Re-process books even if output already exists
    --dry-run   Detect + report only; do not write any output files

Pipeline per book:
    1. detect   — is the book corrupted?
    2. fix      — apply vocabulary-guided corrections (in-memory only)
    3. validate — post-correction quality checks
    4. score    — verse-level quality scores

Output (NEVER touches original data):
    output/cleaned/{BOOK}/    — per-chapter JSON files, corrected text
    output/logs/{BOOK}.log.json
    output/flags/{BOOK}.flags.json
    output/scores.json        — all book scores merged into one file
"""

import sys
import json
import argparse
from pathlib import Path
from collections import Counter

# ── Path setup ────────────────────────────────────────────────────────────────

_PIPELINE_DIR = Path(__file__).resolve().parent
_PROJECT_ROOT = _PIPELINE_DIR.parent

DATA_DIR   = _PROJECT_ROOT / 'data' / 'translations'
YYY_DIR    = DATA_DIR / 'YYY1987'
OUTPUT_DIR = _PROJECT_ROOT / 'output'

CLEANED_DIR = OUTPUT_DIR / 'cleaned'
LOGS_DIR    = OUTPUT_DIR / 'logs'
FLAGS_DIR   = OUTPUT_DIR / 'flags'
SCORES_FILE = OUTPUT_DIR / 'scores.json'

# Reference translations used to build the correction vocabulary
REFERENCE_TRANSLATIONS = ['TCL02', 'KMEYA', 'NWT2025']

# ── Pipeline imports ──────────────────────────────────────────────────────────

from utils     import build_dictionary, load_book_data, save_json
from detector  import detect_book_corruption
from fixer     import fix_book
from validator import validate_book, check_semantic_risks
from scorer    import score_book


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ensure_output_dirs() -> None:
    for d in (CLEANED_DIR, LOGS_DIR, FLAGS_DIR):
        d.mkdir(parents=True, exist_ok=True)


def _list_books() -> list:
    """Return sorted list of book codes present in YYY1987/."""
    if not YYY_DIR.exists():
        return []
    return sorted(p.name for p in YYY_DIR.iterdir() if p.is_dir())


def _save_cleaned(book: str, corrected_chapters: list) -> None:
    """Write corrected chapter data to output/cleaned/{book}/."""
    book_out = CLEANED_DIR / book
    book_out.mkdir(parents=True, exist_ok=True)
    for ch_num, data in corrected_chapters:
        save_json(book_out / f'{ch_num}.json', data)


def _build_flags(
    book: str,
    validate_report: dict,
    fix_log: dict,
) -> dict:
    """Collect all flag-worthy items into a single flags document."""
    flagged_tokens = []
    for ch in fix_log.get('all_changes', []):
        if ch.get('action', '') != 'applied':
            flagged_tokens.append(ch)

    return {
        'book':              book,
        'remaining_artefacts': validate_report.get('remaining_artefacts', []),
        'short_verses':      validate_report.get('short_verses', []),
        'sequence_errors':   validate_report.get('sequence_errors', []),
        'uncorrected_tokens':validate_report.get('uncorrected_tokens', []),
        'semantic_risks':    validate_report.get('semantic_risks', []),
        'flagged_corrections': flagged_tokens,
    }


def _print_book_summary(
    book: str,
    detect: dict,
    fix_log: dict,
    vreport: dict,
    scores: dict,
) -> None:
    stats  = fix_log.get('stats', {})
    passed = '✓ PASS' if vreport.get('passed') else '✗ FAIL'
    print(
        f'  {book:8}  '
        f'score={detect["score"]:.2f}  '
        f'applied={stats.get("applied", 0):4}  '
        f'flagged={stats.get("flagged", 0):4}  '
        f'avg_quality={scores["average_score"]:5.1f}  '
        f'validate={passed}'
    )
    if not vreport.get('passed'):
        for issue in vreport.get('issues', []):
            print(f'           ↳ {issue}')


# ---------------------------------------------------------------------------
# Per-book processing
# ---------------------------------------------------------------------------

def process_book(
    book: str,
    freq: Counter,
    force: bool = False,
    dry_run: bool = False,
) -> dict:
    """
    Run the full detect→fix→validate→score pipeline for one book.

    Returns a summary dict suitable for inclusion in scores.json.
    """
    book_path = YYY_DIR / book
    out_log   = LOGS_DIR  / f'{book}.log.json'

    # ── Skip if already processed ─────────────────────────────────────────
    if not force and not dry_run and out_log.exists():
        print(f'  {book:8}  [skipped — output exists; use --force to reprocess]')
        try:
            return json.loads(out_log.read_text(encoding='utf-8')).get('summary', {})
        except Exception:
            pass
        return {}

    # ── 1. Detect ─────────────────────────────────────────────────────────
    detect = detect_book_corruption(book_path)

    if not detect['is_corrupted']:
        print(f'  {book:8}  [clean — skipping correction]  score={detect["score"]:.2f}')
        return {
            'book':      book,
            'corrupted': False,
            'score':     detect['score'],
        }

    # ── 2. Fix ────────────────────────────────────────────────────────────
    corrected_chapters, fix_log = fix_book(book_path, freq)

    # ── 3. Validate ───────────────────────────────────────────────────────
    vreport = validate_book(book_path, corrected_chapters)

    # Cross-check semantic risks using fix log + reference vocabulary
    sem_risks = check_semantic_risks(fix_log.get('verse_changes', {}), freq)
    vreport['semantic_risks'] = sem_risks

    # ── 4. Score ──────────────────────────────────────────────────────────
    scores = score_book(corrected_chapters, fix_log.get('verse_changes', {}))

    # ── 5. Save outputs ───────────────────────────────────────────────────
    if not dry_run:
        _save_cleaned(book, corrected_chapters)

        log_doc = {
            'summary': {
                'book':          book,
                'corrupted':     True,
                'detect_score':  detect['score'],
                'stats':         fix_log.get('stats', {}),
                'validate':      {
                    'passed':  vreport['passed'],
                    'issues':  vreport['issues'],
                },
                'average_quality': scores['average_score'],
                'score_histogram': scores['score_histogram'],
            },
            'detection':    detect,
            'fix_stats':    fix_log.get('stats', {}),
            'verse_changes': fix_log.get('verse_changes', {}),
            'validation':   vreport,
            'scores':       scores,
        }
        save_json(out_log, log_doc, pretty=True)

        flags_doc = _build_flags(book, vreport, fix_log)
        save_json(FLAGS_DIR / f'{book}.flags.json', flags_doc, pretty=True)

    _print_book_summary(book, detect, fix_log, vreport, scores)

    return {
        'book':             book,
        'corrupted':        True,
        'detect_score':     detect['score'],
        'stats':            fix_log.get('stats', {}),
        'validate_passed':  vreport['passed'],
        'validate_issues':  vreport['issues'],
        'average_quality':  scores['average_score'],
        'score_histogram':  scores['score_histogram'],
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(argv=None) -> None:
    parser = argparse.ArgumentParser(
        description='YYY1987 OCR correction pipeline orchestrator'
    )
    parser.add_argument(
        '--books', nargs='+', metavar='BOOK',
        help='Process only these book codes (e.g. GEN MAT ROM)'
    )
    parser.add_argument(
        '--force', action='store_true',
        help='Re-process even if output already exists'
    )
    parser.add_argument(
        '--dry-run', action='store_true',
        help='Detect and report only — do not write output files'
    )
    args = parser.parse_args(argv)

    if not YYY_DIR.exists():
        print(f'ERROR: YYY1987 directory not found at {YYY_DIR}', file=sys.stderr)
        sys.exit(1)

    books = args.books if args.books else _list_books()
    if not books:
        print('ERROR: No book directories found in YYY1987/', file=sys.stderr)
        sys.exit(1)

    if not args.dry_run:
        _ensure_output_dirs()

    # ── Build vocabulary once ─────────────────────────────────────────────
    print('Building correction vocabulary...')
    freq = build_dictionary(DATA_DIR, REFERENCE_TRANSLATIONS)
    print(f'  Vocabulary: {len(freq):,} word forms\n')

    # ── Process books ─────────────────────────────────────────────────────
    label = '[DRY RUN] ' if args.dry_run else ''
    print(f'{label}Processing {len(books)} book(s):')
    print(f'  {"Book":8}  {"Detect":22}  {"Applied":12}  {"Flagged":12}  {"Quality":12}  {"Validate"}')
    print('  ' + '─' * 80)

    all_summaries = []
    for book in books:
        try:
            summary = process_book(
                book, freq,
                force=args.force,
                dry_run=args.dry_run,
            )
            if summary:
                all_summaries.append(summary)
        except Exception as exc:
            print(f'  {book:8}  ERROR: {exc}')

    # ── Write aggregated scores ───────────────────────────────────────────
    if not args.dry_run and all_summaries:
        save_json(SCORES_FILE, {'books': all_summaries}, pretty=True)
        print(f'\nScores written → {SCORES_FILE}')

    # ── Final summary ─────────────────────────────────────────────────────
    processed  = [s for s in all_summaries if s.get('corrupted')]
    clean      = [s for s in all_summaries if not s.get('corrupted')]
    passed     = [s for s in processed if s.get('validate_passed')]
    failed     = [s for s in processed if not s.get('validate_passed')]

    total_applied = sum(
        s.get('stats', {}).get('applied', 0) for s in processed
    )
    total_flagged = sum(
        s.get('stats', {}).get('flagged', 0) for s in processed
    )

    print(f'\n{"═"*60}')
    print(f'  Books processed : {len(books)}')
    print(f'  Corrupted       : {len(processed)}')
    print(f'  Clean (skipped) : {len(clean)}')
    print(f'  Validate PASS   : {len(passed)}')
    print(f'  Validate FAIL   : {len(failed)}')
    print(f'  Total applied   : {total_applied:,}')
    print(f'  Total flagged   : {total_flagged:,}')
    if processed:
        avg_q = sum(s.get('average_quality', 100) for s in processed) / len(processed)
        print(f'  Avg quality     : {avg_q:.1f}/100')
    print(f'{"═"*60}')

    if failed:
        print('\nBooks with validation issues:')
        for s in failed:
            for issue in s.get('validate_issues', []):
                print(f'  [{s["book"]}] {issue}')


if __name__ == '__main__':
    main()
