#!/usr/bin/env python3
"""
Fetch Greek Strong's lexicon entries (G1–G5624) from biblehub.com
and save to data/strongs/greek.json.

Uses ThreadPoolExecutor for concurrent fetching with rate-limiting.
Resumes from any partial progress file automatically.

Usage:
    python3 pipeline/fetch_strongs_greek.py
"""
import json
import re
import time
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from html.parser import HTMLParser
from pathlib import Path

OUT_FILE    = Path(__file__).parent.parent / 'data/strongs/greek.json'
TOTAL       = 5624
WORKERS     = 8          # concurrent fetches
DELAY       = 0.05       # seconds between each worker's requests
RETRY_MAX   = 4
HEADERS     = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
    'Accept': 'text/html,application/xhtml+xml',
    'Accept-Language': 'en-US,en;q=0.9',
}

# ── HTML cleaning ──────────────────────────────────────────────────
_TAG = re.compile(r'<[^>]+>')
_WS  = re.compile(r'\s+')

def strip(s):
    return _WS.sub(' ', _TAG.sub('', s)).strip()

def clean_x(s):
    """Remove Strong's concordance 'X ' notation from comma-separated lists."""
    if not s:
        return s
    parts = s.split(',')
    return ', '.join(re.sub(r'^X\s+', '', p.strip()).strip() for p in parts)

# ── Parse one Strong's page ────────────────────────────────────────
def parse_page(num: int, html: str) -> dict:
    entry = {'id': num}

    lb = re.search(r'<div id="leftbox">(.*?)(?=<div id="rightbox"|<div class="maintable2"|$)',
                   html, re.DOTALL)
    body = lb.group(1) if lb else html

    # title: "alpha: Alpha"
    m = re.search(r'class="toptitle2">([^<]+)<', body)
    if m: entry['title'] = m.group(1).strip()

    # original Greek word
    m = re.search(r'class="greek"[^>]*>([^<]+)<', body)
    if m: entry['word'] = m.group(1).strip()

    # structured fields from "Tophdg" label rows
    for label, key in [
        ('Part of Speech', 'pos'),
        ('Transliteration', 'translit'),
        ('Pronunciation', 'pronunc'),
        ('Phonetic Spelling', 'phonetic'),
        ('Word Origin', 'origin'),
        ('KJV', 'kjv'),
    ]:
        pat = rf'<span class="tophdg">{label}.*?</span>(.*?)(?=<br|<span class="tophdg"|<div)'
        m = re.search(pat, body, re.DOTALL | re.IGNORECASE)
        if m:
            val = clean_x(strip(m.group(1)))
            if val:
                entry[key] = val

    # Short definition — numbered list between last tophdg field and first vheading2
    m = re.search(r'(?:Word Origin.*?<br><br>)(.*?)(?=<div class="vheading2")',
                  body, re.DOTALL | re.IGNORECASE)
    if m:
        short = strip(m.group(1))
        if short:
            entry['short_def'] = short

    # Strong's Exhaustive Concordance section
    m = re.search(r"Strong's Exhaustive Concordance</div>(.*?)(?=<div class=\"vheading2\"|$)",
                  body, re.DOTALL | re.IGNORECASE)
    if m:
        sec = clean_x(strip(m.group(1)))
        sec = re.sub(r'\s*see (GREEK|HEBREW)\s+\S+', '', sec).strip()
        if sec:
            entry['strongs_def'] = sec

    # NAS Definition (public domain concordance data)
    m = re.search(r'NAS Exhaustive Concordance</div>(.*?)(?=<div class="vheading2"|<iframe|$)',
                  body, re.DOTALL | re.IGNORECASE)
    if m:
        nas = m.group(1)
        def_m = re.search(r'<span class="hdg">Definition</span><br>(.*?)(?=<br>|<span|$)', nas, re.DOTALL)
        if def_m:
            entry['nas_def'] = strip(def_m.group(1))
        nasb_m = re.search(r'<span class="hdg">NASB Translation</span><br>(.*?)(?=<[^/]|$)', nas, re.DOTALL)
        if nasb_m:
            entry['nasb'] = strip(nasb_m.group(1))

    return entry


# ── Fetch one page with retries ────────────────────────────────────
def fetch_one(num: int) -> dict | None:
    url = f'https://biblehub.com/greek/{num}.htm'
    for attempt in range(RETRY_MAX):
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=15) as resp:
                html = resp.read().decode('utf-8', errors='replace')
            time.sleep(DELAY)
            return parse_page(num, html)
        except Exception as e:
            if attempt < RETRY_MAX - 1:
                time.sleep(2 ** attempt)
            else:
                print(f'  FAILED G{num}: {e}')
                return {'id': num, 'error': str(e)}
    return None


# ── Main ───────────────────────────────────────────────────────────
def main():
    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    # Load existing progress
    existing = {}
    if OUT_FILE.exists():
        try:
            existing = {e['id']: e for e in json.loads(OUT_FILE.read_text('utf-8'))}
            print(f'Resuming: {len(existing)} entries already fetched')
        except Exception:
            pass

    to_fetch = [n for n in range(1, TOTAL + 1) if n not in existing]
    print(f'Fetching {len(to_fetch)} remaining entries with {WORKERS} workers…')

    results = dict(existing)
    done = len(existing)
    save_every = 200

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {pool.submit(fetch_one, n): n for n in to_fetch}
        for fut in as_completed(futures):
            entry = fut.result()
            if entry:
                results[entry['id']] = entry
            done += 1
            if done % 50 == 0:
                pct = done / TOTAL * 100
                print(f'  {done}/{TOTAL} ({pct:.1f}%)')
            if done % save_every == 0:
                _save(results)

    _save(results)
    print(f'\nDone. {len(results)} entries saved to {OUT_FILE}')


def _save(results: dict):
    # Save as sorted list
    lst = [results[k] for k in sorted(results)]
    OUT_FILE.write_text(json.dumps(lst, ensure_ascii=False, separators=(',', ':')), 'utf-8')
    print(f'  Saved {len(lst)} entries.')


if __name__ == '__main__':
    main()
