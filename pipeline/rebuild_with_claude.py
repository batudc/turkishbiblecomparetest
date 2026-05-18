#!/usr/bin/env python3
"""
rebuild_with_claude.py — Orchestrated verse reconstruction using Claude API.

For each YYY1987 chapter:
  1. Flatten all verse texts into one clean raw string
  2. Strip footnotes, cross-reference indices, and garbage patterns
  3. Call Claude with the raw text + TCL02 reference verse list
  4. Validate response (correct count, valid JSON, no empty texts)
  5. Save to data/translations/YYY1987_REBUILT/

Usage:
    export ANTHROPIC_API_KEY=sk-ant-...
    python3 pipeline/rebuild_with_claude.py

    # Single book test:
    python3 pipeline/rebuild_with_claude.py --book ROM

    # Resume (skip already-done chapters):
    python3 pipeline/rebuild_with_claude.py --resume

Cost estimate: ~7,500 chapters × ~3K tokens = $0.02–$0.05 with Haiku
"""

import json
import os
import re
import time
import argparse
from pathlib import Path

import anthropic

# Load .env from project root if present (so Claude Code can run without shell export)
_ENV_FILE = Path(__file__).resolve().parent.parent / '.env'
if _ENV_FILE.exists():
    for _line in _ENV_FILE.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith('#') and '=' in _line:
            _k, _, _v = _line.partition('=')
            os.environ.setdefault(_k.strip(), _v.strip().strip('"').strip("'"))

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT     = Path(__file__).resolve().parent.parent
YYY_DIR     = PROJECT / 'data' / 'translations' / 'YYY1987'
TCL_DIR     = PROJECT / 'data' / 'translations' / 'TCL02'
OUT_DIR     = PROJECT / 'data' / 'translations' / 'YYY1987_REBUILT'
REPORT_PATH = PROJECT / 'output' / 'rebuild_claude_report.jsonl'
LOG_PATH    = PROJECT / 'output' / 'rebuild_claude.log'

MODEL       = 'claude-haiku-4-5-20251001'
MAX_TOKENS  = 8192    # large chapters can produce 6K+ output tokens
MAX_RETRIES = 3
RATE_DELAY  = 0.15    # seconds between successful API calls

# ---------------------------------------------------------------------------
# Garbage patterns to strip BEFORE sending to Claude
# (footnotes, cross-reference indices, page artefacts)
# ---------------------------------------------------------------------------
_GARBAGE = [
    re.compile(r'\bELÇ\b.*'),                         # "ELÇ Dipnotları ..."
    re.compile(r'\bDipnotlar[ıi]\b.*'),
    re.compile(r'\bKaynak\s+ayetler?\b.*'),
    re.compile(r'(?<!\w)\d{1,3}:\d{1,3}(?:-\d{1,3})?(?:,\s*\d{1,3})*(?!\w)'),  # refs like 3:16, 5:1-3
    re.compile(r'bkz\.\s*\S+'),
    re.compile(r'(?:örn|vb|vs)\.\s*\S+'),
    re.compile(r'[A-Z][a-z]{1,4}\.\d{1,2}:\d{1,2}'),  # abbreviated refs like Kol.3:4
    re.compile(r'^\s*[IVX\d]{1,4}\s*$', re.MULTILINE),  # lone roman numerals / digits
]

def clean_text(raw: str) -> str:
    """Strip known garbage patterns and normalise whitespace."""
    text = raw
    for pat in _GARBAGE:
        text = pat.sub(' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

# ---------------------------------------------------------------------------
# Build VERSE_COUNT from TCL02 (used as expected verse count per chapter)
# ---------------------------------------------------------------------------
def build_verse_counts() -> dict[str, dict[int, int]]:
    counts: dict[str, dict[int, int]] = {}
    for book_dir in sorted(TCL_DIR.iterdir()):
        if not book_dir.is_dir():
            continue
        book = book_dir.name
        counts[book] = {}
        for f in sorted(book_dir.glob('*.json')):
            chap = int(f.stem)
            data = json.loads(f.read_text(encoding='utf-8'))
            counts[book][chap] = len(data['content'])
    return counts

# ---------------------------------------------------------------------------
# Flatten YYY chapter into one raw string
# ---------------------------------------------------------------------------
def flatten(yyy_data: dict) -> str:
    parts = [v['text'].strip() for v in yyy_data.get('content', []) if v.get('text')]
    return ' '.join(parts)

# ---------------------------------------------------------------------------
# Claude API call
# ---------------------------------------------------------------------------
SYSTEM_PROMPT = """\
You are a deterministic verse reconstruction engine.

CRITICAL RULES:
1. DO NOT rewrite, paraphrase, or improve the text in any way.
2. You MUST preserve the original wording EXACTLY as given.
3. You are ONLY allowed to:
   - split text into verses
   - remove clear non-verse artifacts (footnotes, cross-references like "3:16", \
"Dipnotları", "Kaynak ayetler", "bkz." abbreviations, isolated digits/roman numerals)
4. NEVER invent or guess missing content.
5. If content for a verse is missing, return: {"v": X, "text": ""}

VERSE SEGMENTATION RULES:
6. The input contains OCR errors:
   - verse numbers may appear as ? or ??
   - multiple verses may be merged into one block
7. Use sentence boundaries and the provided TCL02 reference anchors to split correctly.
8. Each verse must end at a natural sentence boundary (or group of sentences if required).

STRICT OUTPUT RULES:
9. Output EXACTLY EXPECTED_VERSES items.
10. Verse numbers MUST match the TCL02 canonical verse numbers EXACTLY.
    Use the v numbers from the TCL02 reference list — do NOT use sequential 1,2,3... numbering.
    If TCL02 has verses [1, 3, 4, 7, 8] then your output must use those exact numbers.
11. Output MUST be valid JSON array.
12. No commentary. No markdown fences. No explanations.

FAIL CONDITIONS (these will cause a retry — avoid them):
- Wrong verse count
- Modified wording
- Missing verses
- Extra verses
- Markdown-wrapped output

Output format: [{"v": 1, "text": "..."}, {"v": 2, "text": "..."}, ...]
"""

def call_claude(
    client: anthropic.Anthropic,
    book: str,
    chap: int,
    raw_text: str,
    expected: int,
    tcl_verses: list[dict],
) -> list[dict] | None:
    """
    Call Claude to segment raw_text into exactly `expected` numbered verses.
    Returns list of {"v": int, "text": str} or None if all retries fail.
    """
    # Build concise TCL02 reference — first ~30 chars of each verse as anchors
    ref_lines = '\n'.join(
        f'v{v["v"]}: {v["text"][:80].rstrip()}…'
        for v in tcl_verses
    )

    user_msg = (
        f'BOOK: {book}\n'
        f'CHAPTER: {chap}\n'
        f'EXPECTED_VERSES: {expected}\n'
        f'\n'
        f'CANONICAL REFERENCE (TCL02 — use these as verse-boundary anchors):\n'
        f'{ref_lines}\n'
        f'\n'
        f'SOURCE TEXT (YYY1987, OCR-corrupted, needs segmentation):\n'
        f'<<<\n{raw_text}\n>>>\n'
        f'\n'
        f'Output the {expected} verses as a JSON array. Nothing else.'
    )

    for attempt in range(MAX_RETRIES):
        try:
            resp = client.messages.create(
                model=MODEL,
                max_tokens=MAX_TOKENS,
                system=SYSTEM_PROMPT,
                messages=[{'role': 'user', 'content': user_msg}],
                temperature=0.0 if attempt == 0 else 0.3,
            )

            raw = resp.content[0].text.strip()

            # Extract JSON array (might be wrapped in markdown fences)
            m = re.search(r'\[[\s\S]*\]', raw)
            if not m:
                print(f'    attempt {attempt+1}: no JSON array in response')
                continue

            verses = json.loads(m.group())

            if not isinstance(verses, list) or len(verses) == 0:
                continue

            # Normalise: ensure v is int, text is str
            cleaned = []
            for item in verses:
                v_num = int(item.get('v', 0))
                text  = str(item.get('text', '')).strip()
                if v_num > 0:
                    cleaned.append({'v': v_num, 'text': text})

            if len(cleaned) == 0:
                continue

            # Strict validation: exact count required, no tolerance
            if len(cleaned) != expected:
                diff = len(cleaned) - expected
                print(f'    attempt {attempt+1}: got {len(cleaned)}, expected {expected} (diff {diff:+d})')
                continue

            return cleaned

        except json.JSONDecodeError as e:
            print(f'    attempt {attempt+1}: JSON parse error — {e}')
        except anthropic.RateLimitError:
            wait = 30 * (attempt + 1)
            print(f'    rate limit — sleeping {wait}s')
            time.sleep(wait)
        except anthropic.APIError as e:
            print(f'    API error attempt {attempt+1}: {e}')
            time.sleep(5)

    return None

# ---------------------------------------------------------------------------
# Write output file
# ---------------------------------------------------------------------------
def write_chapter(book: str, chap: int, verses: list[dict]) -> Path:
    out_book = OUT_DIR / book
    out_book.mkdir(parents=True, exist_ok=True)
    data = {
        't': 'YYY1987',
        'b': book,
        'c': chap,
        'content': verses,
    }
    out_path = out_book / f'{chap}.json'
    out_path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding='utf-8',
    )
    return out_path

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--book',   help='Process only this book (e.g. ROM)')
    parser.add_argument('--resume', action='store_true', help='Skip already-rebuilt chapters')
    args = parser.parse_args()

    api_key = os.environ.get('ANTHROPIC_API_KEY', '')
    if not api_key:
        print('ERROR: ANTHROPIC_API_KEY not set.')
        print('Run:  export ANTHROPIC_API_KEY=sk-ant-...')
        return

    client = anthropic.Anthropic(api_key=api_key)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.parent.mkdir(exist_ok=True)
    LOG_PATH.parent.mkdir(exist_ok=True)

    verse_counts = build_verse_counts()

    # Collect work items
    work = []
    for book_dir in sorted(YYY_DIR.iterdir()):
        if not book_dir.is_dir():
            continue
        book = book_dir.name
        if args.book and book != args.book:
            continue
        if book not in verse_counts:
            continue
        for yyy_file in sorted(book_dir.glob('*.json'), key=lambda p: int(p.stem)):
            chap     = int(yyy_file.stem)
            tcl_file = TCL_DIR / book / f'{chap}.json'
            if not tcl_file.exists():
                continue
            if chap not in verse_counts[book]:
                continue
            out_file = OUT_DIR / book / f'{chap}.json'
            if args.resume and out_file.exists():
                continue
            work.append((book, chap, yyy_file, tcl_file, verse_counts[book][chap]))

    total  = len(work)
    done   = 0
    fails  = []

    print(f'Processing {total} chapters  (model: {MODEL})')
    print('─' * 60)

    report_fh = open(REPORT_PATH, 'w', encoding='utf-8')
    log_fh    = open(LOG_PATH,    'w', encoding='utf-8')

    for book, chap, yyy_file, tcl_file, expected in work:
        done += 1
        prefix = f'[{done:3d}/{total}] {book} {chap}'

        yyy_data  = json.loads(yyy_file.read_text(encoding='utf-8'))
        tcl_data  = json.loads(tcl_file.read_text(encoding='utf-8'))
        raw_text  = clean_text(flatten(yyy_data))

        orig_count = len(yyy_data['content'])
        print(f'{prefix}  {orig_count}→{expected} verses … ', end='', flush=True)

        verses = call_claude(client, book, chap, raw_text, expected, tcl_data['content'])

        if verses is None:
            print('FAILED')
            fails.append(f'{book}/{chap}')
            record = {'status': 'FAIL', 'book': book, 'chap': chap}
        else:
            out_path = write_chapter(book, chap, verses)
            got = len(verses)
            marker = '✓' if got == expected else f'~{got}'
            print(f'{marker}')
            log_fh.write(f'{book} {chap}: {orig_count} → {got} (expected {expected})\n')
            record = {'status': 'OK', 'book': book, 'chap': chap,
                      'orig': orig_count, 'got': got, 'expected': expected}

        report_fh.write(json.dumps(record, ensure_ascii=False) + '\n')
        report_fh.flush()

        time.sleep(RATE_DELAY)

    report_fh.close()
    log_fh.close()

    print()
    print('─' * 60)
    print(f'Done: {done - len(fails)}/{total} succeeded')
    if fails:
        print(f'Failed ({len(fails)}): {", ".join(fails)}')
    print(f'Output : {OUT_DIR}')
    print(f'Report : {REPORT_PATH}')
    print(f'Log    : {LOG_PATH}')


if __name__ == '__main__':
    main()
