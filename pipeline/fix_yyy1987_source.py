#!/usr/bin/env python3
"""
fix_yyy1987_source.py — Re-fetch YYY1987 chapters from christiananswers.net,
detect truncated/wrong verse text, and fix in-place.

Problem: Some verses span multiple <p> tags on the site.
The original scraper only captured the first <p>, missing continuations.

Usage:
    python3 pipeline/fix_yyy1987_source.py             # dry run (report only)
    python3 pipeline/fix_yyy1987_source.py --apply     # apply fixes
"""

import json, re, sys, time, urllib.request
from html.parser import HTMLParser
from pathlib import Path

APPLY   = '--apply' in sys.argv
DATA    = Path(__file__).parent.parent / 'data' / 'translations' / 'YYY1987'

BOOK_SLUGS = {
    'MAT': 'mat',   'MRK': 'mark',  'LUK': 'luke',  'JHN': 'john',
    'ACT': 'acts',  'ROM': 'rom',   '1CO': '1cor',  '2CO': '2cor',
    'GAL': 'gal',   'EPH': 'eph',   'PHP': 'phil',  'COL': 'col',
    '1TH': '1th',   '2TH': '2th',   '1TI': '1tim',  '2TI': '2tim',
    'TIT': 'titus', 'PHM': 'phile', 'HEB': 'heb',   'JAS': 'james',
    '1PE': '1pet',  '2PE': '2pet',  '1JN': '1john', '2JN': '2john',
    '3JN': '3john', 'JUD': 'jude',  'REV': 'rev',
}

NT_BOOKS = list(BOOK_SLUGS.keys())


# ── HTML Parser ────────────────────────────────────────────────────────────────

class VerseParser(HTMLParser):
    """
    Parses a christiananswers.net chapter page.

    Structure observed:
      <p><a class="verse" id="N">N</a>[-<a id="M">M</a>] verse text...</p>
      <p>continuation paragraph (no verse anchor)</p>   ← appended to prev verse
      <p class="center"><b>Section Heading</b></p>       ← skipped
      <p class="center">(<a ...>cross-ref</a>)</p>      ← skipped
    """

    def __init__(self):
        super().__init__()
        self.verses: dict[int, str] = {}   # verse_start_num → full text
        self._cur_vstart: int | None = None
        self._cur_vend:   int | None = None
        self._cur_parts:  list[str]  = []
        self._in_p:       bool       = False
        self._p_class:    str        = ''
        self._depth:      int        = 0   # nesting depth within current <p>
        self._skip_p:     bool       = False
        self._in_anchor:  bool       = False
        self._anchor_id:  str        = ''
        self._anchor_cls: str        = ''
        self._is_range_dash: bool    = False
        self._seen_verse_anchor: bool = False  # has this <p> a verse anchor?

    # ── helpers ──
    def _flush(self):
        if self._cur_vstart is None:
            return
        text = ' '.join(' '.join(p.split()) for p in self._cur_parts if p.strip())
        text = re.sub(r'\s+', ' ', text).strip()
        if text:
            self.verses[self._cur_vstart] = text
            # Range verses: map each verse number to same text
            if self._cur_vend and self._cur_vend != self._cur_vstart:
                for vn in range(self._cur_vstart + 1, self._cur_vend + 1):
                    self.verses[vn] = text

    def _new_verse(self, vstart: int, vend: int | None):
        self._flush()
        self._cur_vstart  = vstart
        self._cur_vend    = vend
        self._cur_parts   = []

    # ── HTMLParser callbacks ──
    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        if tag == 'p':
            self._in_p   = True
            self._p_class = a.get('class', '')
            self._depth  = 0
            self._skip_p = False
            self._seen_verse_anchor = False
        elif tag in ('b', 'strong', 'i', 'em', 'span'):
            self._depth += 1
        elif tag == 'a' and self._in_p:
            self._in_anchor  = True
            self._anchor_id  = a.get('id', '')
            self._anchor_cls = a.get('class', '')
        elif tag == 'br' and self._in_p and not self._skip_p:
            if self._cur_parts:
                self._cur_parts.append(' ')

    def handle_endtag(self, tag):
        if tag == 'p':
            if self._in_p and not self._skip_p and self._cur_vstart is not None:
                # collect what we have so far as one paragraph segment
                pass  # parts already accumulated
            self._in_p = False
            self._p_class = ''
            self._seen_verse_anchor = False
        elif tag in ('b', 'strong', 'i', 'em', 'span'):
            self._depth -= 1
        elif tag == 'a':
            self._in_anchor = False

    def handle_data(self, data):
        if not self._in_p:
            return

        if self._in_anchor:
            # Verse anchor: <a class="verse" id="N">N</a>
            if self._anchor_cls == 'verse' and self._anchor_id.isdigit():
                vn = int(self._anchor_id)
                self._new_verse(vn, None)
                self._seen_verse_anchor = True
                return
            # Range end: <a id="M">M</a> immediately after a verse anchor
            if self._anchor_cls == '' and self._anchor_id.isdigit() and self._cur_vstart is not None:
                self._cur_vend = int(self._anchor_id)
                return
            # Otherwise it's a cross-ref link text — skip
            return

        # Skip section-heading / cross-ref paragraphs
        # (class="center" or class="turbiblesubhead" or any non-empty class)
        if self._p_class:
            self._skip_p = True
            return

        if self._skip_p:
            return

        # Continuation paragraph: <p> with no verse anchor yet seen
        # but we have an active verse → append
        # (handled below for both cases)

        stripped = data.strip()
        if stripped == '-':
            # The dash between verse numbers in a range — ignore
            return

        if self._cur_vstart is not None and stripped:
            self._cur_parts.append(data)

    def close(self):
        self._flush()
        super().close()


# ── Fetching ───────────────────────────────────────────────────────────────────

def fetch_html(book: str, chapter: int) -> str | None:
    slug = BOOK_SLUGS[book]
    url  = f'https://christiananswers.net/turkish/bible-tr/tr-{slug}{chapter}.html'
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=20) as r:
            return r.read().decode('utf-8', errors='replace')
    except Exception as e:
        print(f'  ERROR fetching {url}: {e}')
        return None


def parse_chapter(html: str) -> dict[int, str]:
    p = VerseParser()
    p.feed(html)
    p.close()
    return p.verses


# ── Comparison & fix ──────────────────────────────────────────────────────────

def normalise(t: str) -> str:
    return re.sub(r'\s+', ' ', t).strip()


def fix_chapter(book: str, ch: int, src_verses: dict[int, str]) -> list[str]:
    """Compare source verses against JSON, return list of change descriptions."""
    json_path = DATA / book / f'{ch}.json'
    if not json_path.exists():
        return []

    data = json.loads(json_path.read_text(encoding='utf-8'))
    changes = []
    modified = False

    for item in data.get('content', []):
        if 'v' not in item or 'text' not in item:
            continue
        v       = item['v']
        old     = normalise(item['text'])
        src     = normalise(src_verses.get(v, ''))

        if not src:
            continue  # source doesn't have this verse → skip

        if old == src:
            continue  # identical → nothing to do

        # Is the old text a strict prefix of the source? (truncation case)
        if src.startswith(old) and len(src) > len(old):
            missing = src[len(old):].strip()
            changes.append(
                f'  {book} {ch}:{v} TRUNCATED → added: …{missing[:80]}'
            )
            if APPLY:
                item['text'] = src
                modified = True
        else:
            # Different text — log but don't auto-fix
            changes.append(
                f'  {book} {ch}:{v} DIFFER\n'
                f'    JSON: {old[:100]}\n'
                f'    SRC : {src[:100]}'
            )

    if APPLY and modified:
        json_path.write_text(
            json.dumps(data, ensure_ascii=False, indent=2),
            encoding='utf-8'
        )

    return changes


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    mode = 'APPLY' if APPLY else 'DRY RUN'
    print(f'=== fix_yyy1987_source.py  [{mode}] ===\n')

    total_truncated = 0
    total_differ    = 0

    for book in NT_BOOKS:
        book_dir = DATA / book
        if not book_dir.exists():
            print(f'{book}: not found, skipping')
            continue

        chapters = sorted(int(f.stem) for f in book_dir.glob('*.json'))
        print(f'{book} ({len(chapters)} chapters)…')

        for ch in chapters:
            html = fetch_html(book, ch)
            if html is None:
                continue

            src_verses = parse_chapter(html)
            if not src_verses:
                print(f'  {book} {ch}: parser returned no verses — check HTML')
                continue

            changes = fix_chapter(book, ch, src_verses)
            for c in changes:
                print(c)
                if 'TRUNCATED' in c:
                    total_truncated += 1
                elif 'DIFFER' in c:
                    total_differ += 1

            time.sleep(0.4)  # polite crawl delay

    print(f'\n=== DONE ===')
    print(f'Truncated verses fixed : {total_truncated}')
    print(f'Differing verses (log) : {total_differ}')
    if not APPLY:
        print('\nRun with --apply to write fixes.')


if __name__ == '__main__':
    main()
