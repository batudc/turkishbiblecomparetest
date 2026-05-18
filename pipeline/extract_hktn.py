#!/usr/bin/env python3
"""Extract HKTN NT verse text from OCR JSON.

Source: data needed to be work on/Ekumenik_KKtp_10_Aralik_2024-compressed.json
Output: data/translations/HKTN/{BOOK}/{chapter}.json
"""

import json, re, os
from pathlib import Path

BASE = Path(__file__).parent.parent
INPUT_FILE = BASE / 'data needed to be work on' / 'Ekumenik_KKtp_10_Aralik_2024-compressed.json'
OUTPUT_DIR = BASE / 'data' / 'translations' / 'HKTN'
NT_START_PAGE_IDX = 839  # page_id 840

TRANS_ID = 'HKTN'

# Map page header book name fragments → USFM codes
HEADER_BOOK_MAP = {
    'MATTA': 'MAT',
    'MARKOS': 'MRK',
    'LUKA': 'LUK',
    'YUHANNA': 'JHN',
    'HAVARİLER TARİHİ': 'ACT',
    'ELÇİLERİN İŞLERİ': 'ACT',
    'ROMALILARA MEKTUP': 'ROM',
    'I . KORİNTOSLULAR': '1CO',
    'I. KORİNTOSLULAR': '1CO',
    'II. KORİNTOSLULAR': '2CO',
    'GALATYALILAR': 'GAL',
    'EFESOSLULARA MEKTUP': 'EPH',
    "FİLİPİLİLER'E MEKTUP": 'PHP',
    'KOLESELİLERE MEKTUP': 'COL',
    'I. SELANİKLİLER': '1TH',
    'II. SELANİKLİLER': '2TH',
    'I. TİMOTEOS': '1TI',
    'II. TİMETEOS': '2TI',
    'II. TİMOTEOS': '2TI',
    'TİTUS': 'TIT',
    "FİLİMON'A MEKTUP": 'PHM',
    'İBRANİLİLER': 'HEB',
    "YAKUP'UN MEKTUBU": 'JAS',
    '1. PETRUS': '1PE',
    '1I. PETRUS': '2PE',
    'I. YUHANNA': '1JN',
    'II. YUHANNA': '2JN',
    'III.YUHANNA': '3JN',
    "YAHUDA'NIN MEKTUBU": 'JUD',
    'HAVARİ YUHANNA': 'REV',  # matches 'HAVARİ YUHANNA'NIN VAHYİ'
    "YUHANNA'NIN VAHYİ": 'REV',
}

# Book title substrings in content items → USFM codes (order matters: longer first)
CONTENT_BOOK_TITLES = [
    ("İNCİL’ MATTA’NIN YAZISINA GÖRE", 'MAT'),
    ("İNCİL’ MARKOS’UN YAZISINA GÖRE", 'MRK'),
    ("İNCİL’ LUKA’NIN YAZISINA GÖRE", 'LUK'),
    ("İNCİL’ YUHANNA’NIN YAZISINA GÖRE", 'JHN'),
    ('HAVARİLER TARİHİ', 'ACT'),
    ('ROMALILARA MEKTUBU', 'ROM'),
    ("KORİNTOSLULAR’A  BİRİNCİ MEKTUBU", '1CO'),
    ("KORİNTOSLULAR’A BİRİNCİ MEKTUBU", '1CO'),
    ("KORİNTOSLULAR’A İKİNCİ MEKTUBU", '2CO'),
    ("GALATYALILAR’A MEKTUBU", 'GAL'),
    ("EFESLİLER’E MEKTUBU", 'EPH'),
    ("FİLİPİLİLER’E MEKTUBU", 'PHP'),
    ("KOLOSELİLER’E MEKTUBU", 'COL'),
    ("SELÂNİKLİLER’E I. MEKTUBU", '1TH'),
    ("SELÂNİKLİLER’E İKİNCİ MEKTUBU", '2TH'),
    ("TİMETEOS’A I. MEKTUBU", '1TI'),
    ("TİMOTEOS’A I. MEKTUBU", '1TI'),
    ("TİMETEOS’A II. MEKTUBU", '2TI'),
    ("TİMOTEOS’A II. MEKTUBU", '2TI'),
    ("TİTUS’A MEKTUBU", 'TIT'),
    ("FİLİMON’A MEKTUBU", 'PHM'),
    ("İBRANİLİLER’E MEKTUBU", 'HEB'),
    ("YAKUP’UN GENEL MEKTUBU", 'JAS'),
    ("PETRUS’UN BİRİNCİ GENEL MEKTUBU", '1PE'),
    ("PETRUS’UN İKİNCİ GENEL MEKTUBU", '2PE'),
    ("HAVARİ YUHANNA’NIN I. GENEL MEKTUBU", '1JN'),
    ("HAVARİ YUHANNA’NIN II. MEKTUBU", '2JN'),
    ("HAVARİ YUHANNA’NIN III. MEKTUBU", '3JN'),
    ("HAVARİ YAHUDA’NIN GENEL MEKTUBU", 'JUD'),
    ("HAVARİ YUHANNA’NIN VAHYİ", 'REV'),
]

# NT book order for next-book lookup
NT_ORDER = [
    'MAT','MRK','LUK','JHN','ACT','ROM','1CO','2CO','GAL','EPH',
    'PHP','COL','1TH','2TH','1TI','2TI','TIT','PHM','HEB','JAS',
    '1PE','2PE','1JN','2JN','3JN','JUD','REV'
]

# Single-chapter books (default chapter = 1)
SINGLE_CHAPTER = {'PHM', '2JN', '3JN', 'JUD'}

# Expected last verse count per book's last chapter (for transition detection)
BOOK_LAST_VERSE = {
    'MAT': 20, 'MRK': 20, 'LUK': 53, 'JHN': 25,
    'ACT': 31, 'ROM': 27, '1CO': 24, '2CO': 14,
    'GAL': 18, 'EPH': 24, 'PHP': 23, 'COL': 18,
    '1TH': 28, '2TH': 18, '1TI': 21, '2TI': 22,
    'TIT': 15, 'PHM': 25, 'HEB': 25, 'JAS': 20,
    '1PE': 14, '2PE': 18, '1JN': 21, '2JN': 13,
    '3JN': 14, 'JUD': 25, 'REV': 21,
}


def _normalize(text):
    """Normalize curly apostrophes to straight ones for consistent matching."""
    return text.replace('’', "'").replace('‘', "'")


def parse_page_header_books(text):
    """Extract list of USFM codes from a page header like 'MATTA - 28 / MARKOS - 1'."""
    text = _normalize(text)
    books = []
    for segment in text.split(' / '):
        book_name = segment.split(' - ')[0].strip()
        for key, code in HEADER_BOOK_MAP.items():
            if book_name == key or book_name.startswith(key):
                if code not in books:
                    books.append(code)
                break
    return books


def detect_book_in_content(text):
    """Return USFM code if text contains a known book title substring."""
    for pattern, code in CONTENT_BOOK_TITLES:
        if pattern in text:
            return code
    return None


def is_footnote(text):
    """True if text looks like a footnote reference line."""
    slash_refs = re.findall(r'\d+/\d+', text)
    return len(slash_refs) >= 2 and not re.search(r'\d+\*', text)


def clean_verse(text):
    """Strip OCR noise from verse text."""
    W = r'[A-Za-zÀ-ÿğüşıöçĞÜŞİÖÇ]'   # word char (Turkish alphabet)
    WL = r'[a-zğüşıöç]'               # word char, lowercase only

    # 1. Strip stray 3+ digit numbers (page/score OCR artifacts like "842", "889")
    #    Case A: word- 842 word  (hyphen + number between word segments)
    text = re.sub(fr'({W})-\s*\d{{3,}}\s+({W})', r'\1\2', text)
    #    Case B: isolated number mid-text or at end of segment (end-of-page artifact)
    text = re.sub(r'\s\d{3,}(?=\s|$)', ' ', text)

    # 2. Remove numeric footnote markers (space between word chars to prevent merging)
    text = re.sub(fr'(?<={W})\(\d+\)(?={W})', ' ', text)
    text = re.sub(r'\(\d+\)', '', text)
    # Remove symbol footnote markers
    text = re.sub(fr'(?<={W})\([*§‡+†]\)(?={W})', ' ', text)
    text = re.sub(r'\([*§‡+†]\)', '', text)

    # 3. Fix hyphen-based word-split OCR artifacts:
    #    a. space + hyphen + optional-punctuation + space between word chars
    #       e.g. "yol - larını" → "yollarını",  "Ya - , kup'un" → "Yakup'un"
    text = re.sub(fr'({W})\s+[-–]\s*[,.]?\s*({W})', r'\1\2', text)
    #    b. hyphen + one-or-more spaces (trailing line-break hyphen)
    #       e.g. "du-  runcaya" → "duruncaya"
    text = re.sub(fr'({W})-\s+({W})', r'\1\2', text)
    #    c. direct hyphen + lowercase continuation (no spaces)
    #       e.g. "vaf-tiz" → "vaftiz", "yara-maz" → "yaramaz"
    text = re.sub(fr'({W})-({WL})', r'\1\2', text)

    # 4. Remove stray reference noise (patterns like "Mat.11/10," within verse text)
    text = re.sub(r'\b[A-ZÀ-Ÿa-zğüşıöçĞÜŞİÖÇ]+\.\d+/\d+[,\s]?', '', text)
    # Normalize whitespace
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

    current_book = None
    current_chapter = None
    current_verses = {}
    last_verse_num = None
    # Queue of books expected from transitional page headers
    pending_next_books = []

    def flush():
        nonlocal current_verses, last_verse_num
        if current_book and current_chapter and current_verses:
            save_chapter(current_book, current_chapter, current_verses)
        current_verses = {}
        last_verse_num = None

    def switch_book(book):
        nonlocal current_book, current_chapter, pending_next_books
        flush()
        current_book = book
        current_chapter = None
        if book in SINGLE_CHAPTER:
            current_chapter = 1
        # Remove this book from pending queue
        if book in pending_next_books:
            idx = pending_next_books.index(book)
            pending_next_books = pending_next_books[idx+1:]
        print(f'>> Book: {book}')

    def switch_chapter(chap):
        nonlocal current_chapter
        if current_chapter == chap:
            return
        # Chapter went back to 1 while we were at >1 → book transition
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
        # Check if verse num is 1 while last was near expected last verse → book transition
        if (v_num == 1 and last_verse_num is not None and
                current_book and last_verse_num >= BOOK_LAST_VERSE.get(current_book, 999) * 0.8 and
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

    for page_idx in range(NT_START_PAGE_IDX, len(pages)):
        p = pages[page_idx]

        # Parse page headers for book info.
        # Also check full-page OCR items (y_min < 22) whose header text is
        # embedded at the start of the item rather than in a separate header item.
        page_header_books = []
        for item in p['content']:
            pos = item['position']
            y_min = min(pos[1], pos[3], pos[5], pos[7])
            y_max = max(pos[1], pos[3], pos[5], pos[7])
            if y_max < 30:
                page_header_books.extend(parse_page_header_books(item['text']))
            elif y_min < 22:
                page_header_books.extend(parse_page_header_books(item['text'][:150]))

        # Update pending_next_books from page headers
        for b in page_header_books:
            if b not in pending_next_books and b != current_book:
                pending_next_books.append(b)

        # Sort items: headers first (y_max<30), then left col (x_min<215) top-to-bottom,
        # then right col (x_min>=215) top-to-bottom. Fixes OCR pages with wrong col order.
        def _col_sort(it):
            pos = it['position']
            yx = max(pos[1], pos[3], pos[5], pos[7])
            if yx < 30:
                return (0, 0)
            xm = min(pos[0], pos[2], pos[4], pos[6])
            ym = min(pos[1], pos[3], pos[5], pos[7])
            return (1 if xm < 215 else 2, ym)
        sorted_items = sorted(p['content'], key=_col_sort)

        # Process content items
        for item in sorted_items:
            text = item['text']
            pos = item['position']
            y_min = min(pos[1], pos[3], pos[5], pos[7])
            y_max = max(pos[1], pos[3], pos[5], pos[7])

            # Full-page OCR merge: header text is embedded at the top of the item
            is_fullpage = y_min < 22

            # Skip page headers
            if y_max < 30:
                continue
            # Skip page numbers
            if re.match(r'^\d{1,4}$', text.strip()):
                continue
            # Skip very short OCR artifacts
            if len(text.strip()) <= 2:
                continue

            # Always detect book title and chapter header (even in footnotes)
            # — chapter headers sometimes appear embedded inside footnote blocks
            book_from_content = detect_book_in_content(text)
            if book_from_content and book_from_content != current_book:
                # Only switch immediately if book title precedes first verse marker.
                # If the title appears after verse text (OCR merged last verses + next
                # book title), defer to the pending_next_books mechanism instead.
                first_verse = re.search(r'\d+\*', text)
                if first_verse is None:
                    switch_book(book_from_content)
                else:
                    for pattern, code in CONTENT_BOOK_TITLES:
                        if code == book_from_content:
                            title_pos = text.find(pattern)
                            if title_pos != -1 and title_pos < first_verse.start():
                                switch_book(book_from_content)
                            break

            # Detect chapter header (may be inside footnote or mixed item)
            # Only use the FIRST one found; sequential processing handles in-item headers
            chap_match = re.search(r'(\d+)\.\s*Bölüm:', text)
            if chap_match and not re.search(r'\d+\*', text):
                # Pure chapter header (no verse markers) — safe to switch now
                switch_chapter(int(chap_match.group(1)))

            # Skip footnote items for verse extraction
            if is_footnote(text):
                continue

            # Process verse markers sequentially, interleaving chapter header detection.
            # Build ordered list of all markers in this item.
            all_markers = []
            for m in re.finditer(r'(\d+)\.\s*Bölüm:', text):
                all_markers.append(('chapter', int(m.group(1)), m.start(), m.end()))
            for m in re.finditer(r'(\d+)\*', text):
                all_markers.append(('verse', int(m.group(1)), m.start(), m.end()))
            all_markers.sort(key=lambda x: x[2])

            if not any(t == 'verse' for t, *_ in all_markers):
                # No verse markers — plain continuation or header-only
                if 'Bölüm:' not in text and current_book and current_chapter and last_verse_num:
                    slash_count = len(re.findall(r'\d+/\d+', text))
                    if slash_count < 2:
                        append_last(text)
                continue

            # Walk through markers in text order.
            # For full-page interleaved items, right-col verses (starting at 1)
            # may appear before the chapter header. We buffer them and flush into
            # the new chapter when the header fires, keeping left-col tail verses
            # in the current chapter.
            prev_end = 0
            pending_vnum = None   # left-col verse being accumulated
            pending_rnum = None   # right-col verse being buffered
            right_col_buffer = {} # verse_num → text for interleaved right-col content
            right_col_next = None # next expected right-col sequence number (None = not active)

            for marker_type, marker_val, m_start, m_end in all_markers:
                segment = text[prev_end:m_start]

                if segment.strip():
                    if pending_rnum is not None:
                        # Text belonging to the right-col verse → buffer it
                        rtext = clean_verse(segment)
                        if rtext:
                            if pending_rnum in right_col_buffer:
                                right_col_buffer[pending_rnum] += ' ' + rtext
                            else:
                                right_col_buffer[pending_rnum] = rtext
                    elif pending_vnum is not None:
                        # Text belonging to the left-col (current chapter) verse
                        add_verse(pending_vnum, segment)
                    elif 'Bölüm:' not in segment:
                        # Prefix / continuation before first verse marker
                        slash_count = len(re.findall(r'\d+/\d+', segment))
                        if slash_count < 2:
                            append_last(segment)

                if marker_type == 'chapter':
                    pending_vnum = None
                    pending_rnum = None
                    switch_chapter(marker_val)
                    # Flush buffered right-col verses into the new chapter
                    if right_col_buffer:
                        for rnum, rtext in right_col_buffer.items():
                            rtext = rtext.strip()
                            if rtext and current_book and current_chapter:
                                if rnum in current_verses:
                                    current_verses[rnum] += ' ' + rtext
                                else:
                                    current_verses[rnum] = rtext
                        right_col_buffer = {}
                        right_col_next = None
                else:  # verse marker
                    # Interleaved-column heuristic for full-page OCR items:
                    # If verse 1 appears after a high verse number AND a chapter
                    # header exists later in this item, start buffering right-col
                    # verses. They'll be flushed into the new chapter when the
                    # header fires, while left-col verses stay in the current chapter.
                    if (is_fullpage and marker_val == 1
                            and last_verse_num is not None and last_verse_num > 10
                            and right_col_next is None):
                        remaining_chaps = [val for typ, val, mstart, _ in all_markers
                                           if typ == 'chapter' and mstart > m_start]
                        if remaining_chaps:
                            right_col_next = 1
                            right_col_buffer = {}

                    if (right_col_next is not None
                            and right_col_next <= marker_val <= right_col_next + 5):
                        # Right-col verse (tolerance ±5 for OCR gaps in sequence):
                        # buffer for the upcoming chapter.
                        pending_rnum = marker_val
                        pending_vnum = None
                        right_col_next = marker_val + 1
                    else:
                        # Left-col verse: assign to current chapter
                        pending_vnum = marker_val
                        pending_rnum = None

                prev_end = m_end

            # Remaining text after last marker
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
