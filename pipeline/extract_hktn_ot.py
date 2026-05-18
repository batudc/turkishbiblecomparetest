#!/usr/bin/env python3
"""Extract HKTN OT verse text from OCR JSON.

Source: data needed to be work on/Ekumenik_KKtp_10_Aralik_2024-compressed.json
Output: data/translations/HKTN/{BOOK}/{chapter}.json

OT spans pages index 5–838 (document pages 6–839).
"""

import json, re, os
from pathlib import Path

BASE = Path(__file__).parent.parent
INPUT_FILE = BASE / 'data needed to be work on' / 'Ekumenik_KKtp_10_Aralik_2024-compressed.json'
OUTPUT_DIR = BASE / 'data' / 'translations' / 'HKTN'

OT_START_PAGE_IDX = 5    # page_id 6 (first GEN page)
OT_END_PAGE_IDX   = 840  # exclusive; MAL ch4 ends on page 839, NT starts p840

TRANS_ID = 'HKTN'

# Page header book-name fragments → USFM codes.
# Keys use straight apostrophes (U+0027) because _normalize() converts
# curly U+2018/U+2019 before matching.
HEADER_BOOK_MAP = {
    # Longer / more-specific entries MUST come before any prefix they share.
    # parse_page_header_books sorts by key length (longest first), so ordering
    # here only matters for readability.
    'TEKVİN':                     'GEN',
    'ÇIKIŞ':                      'EXO',
    'LEVİLİLER':                  'LEV',
    'SAYILAR':                    'NUM',
    'TESNİYE':                    'DEU',
    'YEŞU':                       'JOS',
    'HAKİMLER':                   'JDG',
    'RUT':                        'RUT',
    'I. SAMUEL':                  '1SA',
    'II. SAMUEL':                 '2SA',
    'II.SAMUEL':                  '2SA',
    'I. KRALLAR':                 '1KI',
    'II. KRALLAR':                '2KI',
    'I. TARİHLER':                '1CH',
    'I. TARIHLER':                '1CH',   # variant without dotted İ
    'II. TARİHLER':               '2CH',
    'II. TARIHLER':               '2CH',
    'EK ve EZRA':                 'EZR',   # appendix page before Ezra proper
    'II. EZRA NEHEMYA':           'NEH',   # must come before II. EZRA and NEHEMYA
    'II. EZRA':                   '2ES',
    'I. EZRA':                    '1ES',
    'EZRA':                       'EZR',
    'NEHEMYA':                    'NEH',
    'TOBİT':                      'TOB',
    'YUDİT':                      'JDT',
    "ESTER'İN HAK KİTABI":        'GES',   # must come before ESTER
    'ESTER':                      'EST',
    'SUZANNA':                    'SUS',
    'I. MAKABELİLER':             '1MA',
    'I.MAKABELİLER':              '1MA',
    'II. MAKABELİLER':            '2MA',
    'II.MAKABELİLER':             '2MA',
    'III. MAKABELİLER':           '3MA',
    'EYÜP':                       'JOB',
    'MEZMURLAR':                  'PSA',
    "SÜLEYMAN'IN MESELLERİ":      'PRO',
    'VAİZ':                       'ECC',
    'EZGİLER EZGİSİ':             'SNG',
    'BİLGELİK KİTABI':            'WIS',
    'KİLİSENİN KİTABI':           'SIR',   # matches "KİLİSENİN KİTABI / EKLESİYASTİKUS"
    'İŞAYA':                      'ISA',
    "YEREMYA'NIN AĞITLARI":       'LAM',   # must come before YEREMYA
    "YEREMYA'NIN MEKTUBU":        'LJE',   # must come before YEREMYA
    'YEREMYA':                    'JER',
    "MANESSA'NIN DUASI":          'MAN',
    "BARUK'UN KİTABI":            'BAR',   # must come before BARUK
    'BARUK':                      'BAR',
    'HEZEKİYEL':                  'EZK',
    'DANİYEL':                    'DAN',
    'ÜÇ DELİKANLININ EZGİSİ':     'S3Y',
    'BEL VE EJDERHA':             'BEL',
    'HOŞEA':                      'HOS',
    'YOEL':                       'JOE',
    'AMOS':                       'AMO',
    'OBADYA':                     'OBA',
    'YUNUS':                      'JON',
    'MİKA':                       'MIC',
    'NAHUM':                      'NAH',
    'HABAKKUK':                   'HAB',
    'SEFENYA':                    'ZEP',
    'HAGGAY':                     'HAG',
    'ZEKERİYA':                   'ZEC',
    'MALAKİ':                     'MAL',
}

# Single-chapter books (default chapter = 1)
SINGLE_CHAPTER = {'SUS', 'LJE', 'MAN', 'S3Y', 'BEL', 'OBA'}

# Expected last verse count per book's last chapter (used for book-transition detection).
# Approximate: the algorithm triggers when last_verse_num >= 80% of this value.
BOOK_LAST_VERSE = {
    'GEN': 26, 'EXO': 38, 'LEV': 34, 'NUM': 13, 'DEU': 12,
    'JOS': 33, 'JDG': 25, 'RUT': 22,
    '1SA': 13, '2SA': 25, '1KI': 53, '2KI': 30,
    '1CH': 30, '2CH': 23,
    'EZR': 44, '1ES': 55, '2ES': 78, 'NEH': 31,
    'TOB': 15, 'JDT': 25, 'EST': 3, 'GES': 14,
    'SUS': 64, '1MA': 24, '2MA': 39, '3MA': 23,
    'JOB': 17, 'PSA': 6, 'PRO': 31, 'ECC': 14,
    'SNG': 14, 'WIS': 22, 'SIR': 30,
    'ISA': 24, 'JER': 34, 'LAM': 22, 'LJE': 73,
    'MAN': 15, 'BAR': 73, 'EZK': 35, 'DAN': 13,
    'S3Y': 68, 'BEL': 42,
    'HOS': 9, 'JOE': 21, 'AMO': 15, 'OBA': 21,
    'JON': 11, 'MIC': 20, 'NAH': 19, 'HAB': 19,
    'ZEP': 20, 'HAG': 23, 'ZEC': 21, 'MAL': 24,
}

# Turkish ordinal words used as chapter headers instead of digits (e.g. "Yedinci Bölüm:")
TURKISH_ORDINAL_CHAP = {
    'Birinci': 1, 'İkinci': 2, 'Üçüncü': 3, 'Dördüncü': 4,
    'Beşinci': 5, 'Altıncı': 6, 'Yedinci': 7, 'Sekizinci': 8,
    'Dokuzuncu': 9, 'Onuncu': 10,
}
_TR_ORD_PAT = re.compile(r'(' + '|'.join(TURKISH_ORDINAL_CHAP) + r')\s+Bölüm\s*:')

# Pages before which a given book code must NOT be switched to
# (printing anomalies: "I. KRALLAR" headers appear on 1SA pages 227-228;
#  actual 2SA→1KI transition page is index 267, so min must be ≤ 267)
BOOK_MIN_PAGE = {
    '1KI': 267,
}


def _normalize(text):
    """Normalize curly apostrophes to straight for consistent matching."""
    return text.replace('‘', "'").replace('’', "'")


_SORTED_MAP = sorted(HEADER_BOOK_MAP.items(), key=lambda kv: -len(kv[0]))


def parse_page_header_books(text, page_idx):
    """Extract USFM codes from a page header using substring scanning.

    Scans each '/'-separated segment for any known book key (longest-first),
    respecting word boundaries.  Handles OCR patterns like:
      "11,12 ( EZGİLER EZGİSİ - 1,2"   (digits + paren before book name)
      "LEVİLİLER - 27 /SAYILAR - 1"     (slash without space before it)
      "ESKİ AHİT - TEVRAT BİLGELİK KİTABI" (prefix before book name)
    Longer keys are matched first so "ESTER'İN HAK KİTABI" wins over "ESTER".
    """
    text = _normalize(text)
    books = []
    # Split on '/' with optional surrounding spaces
    for segment in re.split(r'\s*/\s*', text):
        consumed: list[tuple[int, int]] = []
        for key, code in _SORTED_MAP:
            idx = 0
            while True:
                pos = segment.find(key, idx)
                if pos == -1:
                    break
                end = pos + len(key)
                # Skip if overlaps an already-consumed (longer) match
                if any(r[0] <= pos < r[1] or r[0] < end <= r[1] for r in consumed):
                    idx = pos + 1
                    continue
                # Word-boundary: char before key must not be alphabetic
                before_ok = pos == 0 or not segment[pos - 1].isalpha()
                # Word-boundary: char after key must not be alphabetic
                after_ok = end >= len(segment) or not segment[end].isalpha()
                if before_ok and after_ok:
                    min_page = BOOK_MIN_PAGE.get(code, 0)
                    if page_idx >= min_page and code not in books:
                        books.append(code)
                    consumed.append((pos, end))
                idx = pos + 1
    return books


def is_footnote(text):
    """True if text looks like a cross-reference footnote line."""
    slash_refs = re.findall(r'\d+/\d+', text)
    return len(slash_refs) >= 2 and not re.search(r'\d+\*', text)


def clean_verse(text):
    """Strip OCR noise from verse text."""
    W  = r'[A-Za-zÀ-ÿğüşıöçĞÜŞİÖÇ]'
    WL = r'[a-zğüşıöç]'

    # 1. Strip stray 3+ digit page/score artifacts
    text = re.sub(fr'({W})-\s*\d{{3,}}\s+({W})', r'\1\2', text)
    text = re.sub(r'\s\d{3,}(?=\s|$)', ' ', text)

    # 2. Remove numeric footnote markers
    text = re.sub(fr'(?<={W})\(\d+\)(?={W})', ' ', text)
    text = re.sub(r'\(\d+\)', '', text)
    text = re.sub(fr'(?<={W})\([*§‡+†]\)(?={W})', ' ', text)
    text = re.sub(r'\([*§‡+†]\)', '', text)

    # 3. Fix hyphen-based word-split OCR artifacts
    text = re.sub(fr'({W})\s+[-–]\s*[,.]?\s*({W})', r'\1\2', text)
    text = re.sub(fr'({W})-\s+({W})', r'\1\2', text)
    text = re.sub(fr'({W})-({WL})', r'\1\2', text)

    # 4. Remove stray reference noise
    text = re.sub(r'\b[A-ZÀ-Ÿa-zğüşıöçĞÜŞİÖÇ]+\.\d+/\d+[,\s]?', '', text)

    return ' '.join(text.split()).strip()


def save_chapter(book, chapter, verses):
    if not verses:
        return
    content = [{'v': v, 'text': verses[v].strip()}
               for v in sorted(verses) if verses[v].strip()]
    if not content:
        return
    book_dir = OUTPUT_DIR / book
    book_dir.mkdir(parents=True, exist_ok=True)
    out = {'t': TRANS_ID, 'b': book, 'c': chapter, 'content': content}
    path = book_dir / f'{chapter}.json'
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print(f'  {book}/{chapter}.json  ({len(content)} verses)')


def main():
    with open(INPUT_FILE, encoding='utf-8') as f:
        data = json.load(f)
    pages = data['pages']

    current_book    = None
    current_chapter = None
    current_verses  = {}
    last_verse_num  = None
    pending_next_books = []
    past_books = set()  # books already processed; prevents re-adding from running headers

    def flush():
        nonlocal current_verses, last_verse_num
        if current_book and current_chapter and current_verses:
            save_chapter(current_book, current_chapter, current_verses)
        current_verses = {}
        last_verse_num = None

    def switch_book(book):
        nonlocal current_book, current_chapter, pending_next_books
        flush()
        if current_book:
            past_books.add(current_book)
        current_book    = book
        current_chapter = None
        if book in SINGLE_CHAPTER:
            current_chapter = 1
        if book in pending_next_books:
            idx2 = pending_next_books.index(book)
            pending_next_books = pending_next_books[idx2 + 1:]
        print(f'>> Book: {book}')

    def switch_chapter(chap):
        nonlocal current_chapter
        if current_chapter == chap:
            return
        if current_chapter is not None and chap == 1 and current_chapter > 1:
            if pending_next_books:
                next_book = pending_next_books[0]
                switch_book(next_book)
                flush()
                current_chapter = 1
                return
        flush()
        current_chapter = chap

    def add_verse(v_num, text):
        nonlocal last_verse_num, current_chapter
        if (v_num == 1 and last_verse_num is not None and
                current_book and
                last_verse_num >= BOOK_LAST_VERSE.get(current_book, 999) * 0.8 and
                pending_next_books):
            next_book = pending_next_books[0]
            switch_book(next_book)
            current_chapter = 1
        text = clean_verse(text)
        if not text or not current_book or not current_chapter:
            return
        if v_num in current_verses:
            current_verses[v_num] += ' ' + text
        else:
            current_verses[v_num] = text
        last_verse_num = v_num

    def append_last(text):
        if last_verse_num is not None and current_book and current_chapter:
            text = clean_verse(text)
            if text:
                current_verses[last_verse_num] += ' ' + text

    for page_idx in range(OT_START_PAGE_IDX, OT_END_PAGE_IDX):
        p = pages[page_idx]

        page_header_books = []
        for item in p['content']:
            pos  = item['position']
            y_min = min(pos[1], pos[3], pos[5], pos[7])
            y_max = max(pos[1], pos[3], pos[5], pos[7])
            if y_max < 30:
                page_header_books.extend(parse_page_header_books(item['text'], page_idx))
            elif y_min < 22:
                page_header_books.extend(parse_page_header_books(item['text'][:150], page_idx))

        # Look-ahead: also scan next page's headers so books that start in the
        # current page's right column (before their own header page) are already
        # in pending_next_books when their content is processed.
        if page_idx + 1 < OT_END_PAGE_IDX:
            next_p = pages[page_idx + 1]
            for item in next_p['content']:
                pos  = item['position']
                y_min = min(pos[1], pos[3], pos[5], pos[7])
                y_max = max(pos[1], pos[3], pos[5], pos[7])
                if y_max < 30:
                    page_header_books.extend(parse_page_header_books(item['text'], page_idx + 1))
                elif y_min < 22:
                    page_header_books.extend(parse_page_header_books(item['text'][:150], page_idx + 1))

        for b in page_header_books:
            if b not in pending_next_books and b != current_book and b not in past_books:
                pending_next_books.append(b)

        # Bootstrap: at OT start (current_book is None), switch to first known book
        if current_book is None and pending_next_books:
            switch_book(pending_next_books[0])

        def _col_sort(it):
            pos = it['position']
            yx  = max(pos[1], pos[3], pos[5], pos[7])
            if yx < 30:
                return (0, 0)
            xm = min(pos[0], pos[2], pos[4], pos[6])
            ym = min(pos[1], pos[3], pos[5], pos[7])
            return (1 if xm < 215 else 2, ym)

        sorted_items = sorted(p['content'], key=_col_sort)

        for item in sorted_items:
            text = item['text']
            pos   = item['position']
            y_min = min(pos[1], pos[3], pos[5], pos[7])
            y_max = max(pos[1], pos[3], pos[5], pos[7])

            is_fullpage = y_min < 22

            if y_max < 30:
                continue
            if re.match(r'^\d{1,4}$', text.strip()):
                continue
            if len(text.strip()) <= 2:
                continue

            # Detect chapter header (pure — no verse markers in same item)
            chap_match = re.search(r'(\d+)\.?\s*(?:Bölüm|[Mm]ezmur)\s*:', text)
            if chap_match and not re.search(r'\d+\*', text):
                switch_chapter(int(chap_match.group(1)))
            elif not chap_match:
                ord_m = _TR_ORD_PAT.search(text)
                if ord_m and not re.search(r'\d+\*', text):
                    switch_chapter(TURKISH_ORDINAL_CHAP[ord_m.group(1)])

            if is_footnote(text):
                continue

            all_markers = []
            for m in re.finditer(r'(\d+)\.?\s*(?:Bölüm|[Mm]ezmur)\s*:', text):
                all_markers.append(('chapter', int(m.group(1)), m.start(), m.end()))
            for m in _TR_ORD_PAT.finditer(text):
                all_markers.append(('chapter', TURKISH_ORDINAL_CHAP[m.group(1)], m.start(), m.end()))
            for m in re.finditer(r'\(?(\d+)\)?\*', text):
                all_markers.append(('verse', int(m.group(1)), m.start(), m.end()))
            all_markers.sort(key=lambda x: x[2])

            if not any(t == 'verse' for t, *_ in all_markers):
                if (not re.search(r'\d+\.?\s*(?:Bölüm|[Mm]ezmur)\s*:', text) and
                        not _TR_ORD_PAT.search(text) and
                        current_book and current_chapter and last_verse_num):
                    slash_count = len(re.findall(r'\d+/\d+', text))
                    if slash_count < 2:
                        append_last(text)
                continue

            prev_end        = 0
            pending_vnum    = None
            pending_rnum    = None
            right_col_buffer = {}
            right_col_next  = None

            for marker_type, marker_val, m_start, m_end in all_markers:
                segment = text[prev_end:m_start]

                if segment.strip():
                    if pending_rnum is not None:
                        rtext = clean_verse(segment)
                        if rtext:
                            if pending_rnum in right_col_buffer:
                                right_col_buffer[pending_rnum] += ' ' + rtext
                            else:
                                right_col_buffer[pending_rnum] = rtext
                    elif pending_vnum is not None:
                        add_verse(pending_vnum, segment)
                    elif not re.search(r'Bölüm\s*:', segment) and not re.search(r'\d+\.?\s*[Mm]ezmur\s*:', segment):
                        slash_count = len(re.findall(r'\d+/\d+', segment))
                        if slash_count < 2:
                            append_last(segment)

                if marker_type == 'chapter':
                    pending_vnum = None
                    pending_rnum = None
                    switch_chapter(marker_val)
                    if right_col_buffer:
                        for rnum, rtext in right_col_buffer.items():
                            rtext = rtext.strip()
                            if rtext and current_book and current_chapter:
                                if rnum in current_verses:
                                    current_verses[rnum] += ' ' + rtext
                                else:
                                    current_verses[rnum] = rtext
                        right_col_buffer = {}
                        right_col_next   = None
                else:  # verse marker
                    if (is_fullpage
                            and last_verse_num is not None and last_verse_num > 10
                            and marker_val < last_verse_num // 3
                            and right_col_next is None):
                        remaining_chaps = [val for typ, val, mstart, _ in all_markers
                                           if typ == 'chapter' and mstart > m_start]
                        if remaining_chaps:
                            right_col_next   = marker_val
                            right_col_buffer = {}

                    if (right_col_next is not None
                            and right_col_next <= marker_val <= right_col_next + 5):
                        pending_rnum   = marker_val
                        pending_vnum   = None
                        right_col_next = marker_val + 1
                    else:
                        pending_vnum = marker_val
                        pending_rnum = None

                prev_end = m_end

            tail = text[prev_end:]
            if tail.strip():
                if pending_rnum is not None:
                    rtext = clean_verse(tail)
                    if rtext:
                        if pending_rnum in right_col_buffer:
                            right_col_buffer[pending_rnum] += ' ' + rtext
                        else:
                            right_col_buffer[pending_rnum] = rtext
                elif pending_vnum is not None:
                    add_verse(pending_vnum, tail)

    flush()
    print('\nDone.')


if __name__ == '__main__':
    main()
