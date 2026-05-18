#!/usr/bin/env python3
"""
integrity_lock_pass.py — Final integrity verification for YYY1987 dataset.

Checks:
  1. Structure: chapter existence, duplicate verses, out-of-order verses
  2. Cross-translation length outliers (extreme ratios vs TCL02/KMEYA/NWT2025)
  3. Text sanity: artifacts, encoding issues, footnote contamination
  4. Special focus: REV 4, TIT 1, ROM 12, JHN 18-19

NO CORRECTIONS are applied. Read-only.
"""

import json, re, sys
from pathlib import Path

PROJECT = Path(__file__).resolve().parent.parent
YYY_DIR = PROJECT / 'data' / 'translations' / 'YYY1987'
TCL_DIR = PROJECT / 'data' / 'translations' / 'TCL02'
KME_DIR = PROJECT / 'data' / 'translations' / 'KMEYA'
NWT_DIR = PROJECT / 'data' / 'translations' / 'NWT2025'

NT_BOOKS = ['MAT','MRK','LUK','JHN','ACT','ROM','1CO','2CO','GAL','EPH','PHP','COL',
            '1TH','2TH','1TI','2TI','TIT','PHM','HEB','JAS','1PE','2PE','1JN','2JN',
            '3JN','JUD','REV']

# Correct NT chapter counts (number of chapters per book)
NT_CHAPTERS = {
    'MAT':27,'MRK':16,'LUK':24,'JHN':21,'ACT':28,'ROM':16,
    '1CO':16,'2CO':13,'GAL':6,'EPH':6,'PHP':4,'COL':4,
    '1TH':5,'2TH':3,'1TI':6,'2TI':4,'TIT':3,'PHM':1,
    'HEB':13,'JAS':5,'1PE':5,'2PE':3,'1JN':5,'2JN':1,'3JN':1,'JUD':1,'REV':22,
}

issues = []

def flag(severity, book, ch, v, problem):
    issues.append((severity, book, ch, v, problem))

def load_yyy(book, ch):
    p = YYY_DIR / book / f'{ch}.json'
    if not p.exists():
        return None
    return json.loads(p.read_text(encoding='utf-8'))

def load_ref(ref_dir, book, ch):
    p = ref_dir / book / f'{ch}.json'
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding='utf-8'))
    except Exception:
        return None

def verse_map(data):
    if data is None:
        return {}
    return {v['v']: v['text'] for v in data.get('content', [])}

# ─── CHECK 1: Structure Integrity ─────────────────────────────────────────────
print('[ CHECK 1 ] Structure integrity...', flush=True)
struct_issues = 0

for book in NT_BOOKS:
    book_dir = YYY_DIR / book
    if not book_dir.exists():
        flag('CRITICAL', book, 0, 0, 'Book directory missing entirely')
        struct_issues += 1
        continue

    max_ch   = NT_CHAPTERS[book]
    ch_files = sorted(book_dir.glob('*.json'), key=lambda p: int(p.stem))
    actual_chs = {int(p.stem) for p in ch_files}

    # Missing chapters
    for ch in range(1, max_ch + 1):
        if ch not in actual_chs:
            flag('CRITICAL', book, ch, 0, f'Chapter {ch} missing')
            struct_issues += 1

    # Extra chapters beyond canonical count
    for ch in sorted(actual_chs):
        if ch > max_ch:
            flag('HIGH', book, ch, 0, f'Chapter {ch} exists but book only has {max_ch} chapters')
            struct_issues += 1

    for ch_file in ch_files:
        ch = int(ch_file.stem)
        if ch > max_ch:
            continue  # already flagged above
        data = json.loads(ch_file.read_text(encoding='utf-8'))
        content = data.get('content', [])

        if not content:
            flag('CRITICAL', book, ch, 0, 'Chapter has zero verses')
            struct_issues += 1
            continue

        v_nums = [v['v'] for v in content]

        # Duplicate verse numbers
        seen = set()
        for vn in v_nums:
            if vn in seen:
                flag('CRITICAL', book, ch, vn, f'Duplicate verse number {vn}')
                struct_issues += 1
            seen.add(vn)

        # Out-of-order verses
        for i in range(len(v_nums) - 1):
            if v_nums[i] >= v_nums[i+1]:
                flag('HIGH', book, ch, v_nums[i],
                     f'Out-of-order: verse {v_nums[i]} followed by {v_nums[i+1]}')
                struct_issues += 1

        # Empty / ultra-short verse texts
        for v in content:
            text = v.get('text', '').strip()
            if not text:
                flag('CRITICAL', book, ch, v['v'], 'Verse has empty text')
                struct_issues += 1
            elif len(text) < 8 and '[verse text missing' not in text:
                flag('HIGH', book, ch, v['v'],
                     f'Verse text suspiciously short ({len(text)} chars): {text!r}')
                struct_issues += 1

print(f'   → {struct_issues} structure issues', flush=True)

# ─── CHECK 2: Extreme Length Outliers ─────────────────────────────────────────
# Note: YYY1987 is a compressed format — many canonical verses are merged.
# Only flag EXTREME cases (ratio <0.1 or >15) to avoid false positives.
print('[ CHECK 2 ] Extreme length outliers...', flush=True)
length_issues = 0

for book in NT_BOOKS:
    book_dir = YYY_DIR / book
    if not book_dir.exists():
        continue
    for ch_file in sorted(book_dir.glob('*.json'), key=lambda p: int(p.stem)):
        ch = int(ch_file.stem)
        yyy_vm = verse_map(json.loads(ch_file.read_text()))
        tcl_vm = verse_map(load_ref(TCL_DIR, book, ch))
        kme_vm = verse_map(load_ref(KME_DIR, book, ch))
        nwt_vm = verse_map(load_ref(NWT_DIR, book, ch))

        ref_vms = [vm for vm in [tcl_vm, kme_vm, nwt_vm] if vm]
        if not ref_vms:
            continue

        for vn, yyy_text in yyy_vm.items():
            ref_texts = [vm[vn] for vm in ref_vms if vn in vm]
            if not ref_texts:
                continue
            ref_avg = sum(len(t) for t in ref_texts) / len(ref_texts)
            if ref_avg < 20:
                continue
            yyy_len = len(yyy_text)
            if yyy_len == 0:
                continue
            ratio = yyy_len / ref_avg
            if ratio < 0.08:
                flag('HIGH', book, ch, vn,
                     f'Verse extremely short vs refs (YYY={yyy_len}ch, refAvg={ref_avg:.0f}ch, ratio={ratio:.2f})')
                length_issues += 1
            elif ratio > 15.0:
                flag('HIGH', book, ch, vn,
                     f'Verse extremely long vs refs (YYY={yyy_len}ch, refAvg={ref_avg:.0f}ch, ratio={ratio:.2f}) — possible accidental duplication')
                length_issues += 1

print(f'   → {length_issues} extreme length outliers', flush=True)

# ─── CHECK 3: Text Sanity ────────────────────────────────────────────────────
print('[ CHECK 3 ] Text sanity...', flush=True)
sanity_issues = 0

# Words starting with ı (dotless) that correctly use capital I:
CORRECT_I_START = re.compile(r'\bI(şık|şığ|şıkl|şıkt|şıktan|şıklar|şın|rmak|rmakl|ssız)')

SANITY_CHECKS = [
    # (pattern, description, severity, skip_if_correct_I)
    (re.compile(r'\bbabkâhin', re.I),             'babkâhin still present (should be başkâhin)', 'CRITICAL', False),
    (re.compile(r'ELÇ Dipnotları'),                'footnote block in verse text',                'CRITICAL', False),
    (re.compile(r'(?:Dipnot|dipnot)\w*\s*:'),      'footnote label in verse text',                'CRITICAL', False),
    (re.compile(r'\[verse text missing'),          'verse marked as missing source text',         'HIGH',     False),
    (re.compile(r'Genel bakış:'),                  'book introduction in verse text',              'HIGH',     False),
    (re.compile(r'(?<!\d)\d{3,}(?=[A-ZÇĞİÖŞÜa-züşğçöı])'),
                                                   'large digit embedded in/before word (page#)', 'HIGH',     False),
    (re.compile(r'\bhaksyzIyk\b|\barkadabiyk\b|adIy\b|yargyçIyk\b'),
                                                   'known Capital-I artifact not corrected',      'HIGH',     False),
    (re.compile(r"dü'ünceler"),                    "ş→' artifact (dü'ünceler)",                   'HIGH',     False),
    (re.compile(r'Permission should be obtained'), 'copyright notice in verse text',              'CRITICAL', False),
    (re.compile(r'\(c\) The Translation Trust'),   'copyright notice in verse text',              'CRITICAL', False),
    (re.compile(r'\bIsa\b'),                       'İsa written as Isa (ASCII I, missing dot)',   'HIGH',     True),
]

for book in NT_BOOKS:
    book_dir = YYY_DIR / book
    if not book_dir.exists():
        continue
    for ch_file in sorted(book_dir.glob('*.json'), key=lambda p: int(p.stem)):
        ch = int(ch_file.stem)
        data = json.loads(ch_file.read_text())
        for v in data.get('content', []):
            vn   = v['v']
            text = v.get('text', '')

            for pat, label, sev, check_correct_I in SANITY_CHECKS:
                if not pat.search(text):
                    continue
                if check_correct_I:
                    # For Isa check: don't flag if it follows İ (already has dot somewhere)
                    # Just flag standalone \bIsa\b
                    hits = pat.findall(text)
                    if not hits:
                        continue
                flag(sev, book, ch, vn, f'{label}')
                sanity_issues += 1

            # Mid-word plain ASCII I (potential l→I artifact)
            for m in re.finditer(r'[a-züşğçöı]I[a-züşğçöı]', text):
                ctx = text[max(0,m.start()-3):m.end()+3]
                flag('HIGH', book, ch, vn, f'mid-word ASCII I (l→I artifact?): …{ctx}…')
                sanity_issues += 1

print(f'   → {sanity_issues} text sanity issues', flush=True)

# ─── CHECK 4: Special Focus Areas ────────────────────────────────────────────
print('[ CHECK 4 ] Special focus areas...', flush=True)
special_issues = 0

# REV chapter 4
rev4 = load_yyy('REV', 4)
if rev4 is None:
    flag('CRITICAL', 'REV', 4, 0, 'REV chapter 4 file does not exist')
    special_issues += 1
else:
    vm4 = verse_map(rev4)
    v_nums = sorted(vm4.keys())
    if v_nums != list(range(1, 12)):
        flag('HIGH', 'REV', 4, 0, f'REV 4 verse numbering wrong: {v_nums} (expected 1-11)')
        special_issues += 1
    else:
        # Content spot checks
        checks = [
            (1,  'kapı',      'opening vision (door in heaven)'),
            (2,  'taht',      'throne vision'),
            (4,  'ihtiyar',   'twenty-four elders'),
            (8,  'kutsal',    'holy, holy, holy acclamation'),
            (11, 'Rabbimiz',  'closing doxology'),
        ]
        for vn, keyword, desc in checks:
            if keyword not in vm4.get(vn, '').lower():
                flag('HIGH', 'REV', 4, vn,
                     f'REV 4:{vn} lacks "{keyword}" ({desc}): {vm4.get(vn,"")[:80]}')
                special_issues += 1

# TIT chapter 1
tit1 = load_yyy('TIT', 1)
if tit1:
    vm_tit = verse_map(tit1)
    required = {
        5:  ('bıraktım',      'left you in Crete'),
        6:  ('İhtiyar',       'elder qualifications'),
        7:  ('Gözetmen',      'overseer/bishop qualification'),
        8:  ('konuksever',    'hospitable'),
        9:  ('sarılmalıdır',  'hold firmly to the word'),
        10: ('boşboğaz',      'rebellious/empty talkers'),
        11: ('ağzını',        'must be silenced'),
        12: ('Giritliler',    'Cretans are liars'),
        13: ('uyar',          'rebuke them sharply'),
        15: ('temiz',         'to the pure all is pure'),
        16: ('inkâr',         'they deny God'),
    }
    for vn, (keyword, desc) in required.items():
        if vn not in vm_tit:
            flag('CRITICAL', 'TIT', 1, vn, f'TIT 1:{vn} missing ({desc})')
            special_issues += 1
        elif keyword not in vm_tit[vn]:
            flag('HIGH', 'TIT', 1, vn,
                 f'TIT 1:{vn} lacks "{keyword}" ({desc}): {vm_tit[vn][:80]}')
            special_issues += 1

# ROM chapter 12
rom12 = load_yyy('ROM', 12)
if rom12:
    vm12 = verse_map(rom12)
    checks = [
        (2,  'değişin',       'be transformed'),
        (3,  'düşüncelerinde','in their thinking'),
        (7,  'düşünerek',     'joyfully hoping'),
        (10, 'arkadaşlık',    'friendship/association'),
        (11, 'Öç',            'vengeance is mine'),
        (12, 'kötülüğü',      'overcome evil with good'),
    ]
    for vn, keyword, desc in checks:
        if vn not in vm12:
            flag('HIGH', 'ROM', 12, vn, f'ROM 12:{vn} missing ({desc})')
            special_issues += 1
        elif keyword not in vm12[vn]:
            flag('HIGH', 'ROM', 12, vn,
                 f'ROM 12:{vn} lacks "{keyword}" ({desc}): {vm12.get(vn,"")[:80]}')
            special_issues += 1
    if 8 in vm12:
        flag('HIGH', 'ROM', 12, 8, 'ROM 12:8 still present — v7/v8 merge appears incomplete')
        special_issues += 1

# JHN 18-19: no babkâhin
for ch in [11, 18, 19, 7, 12]:
    data = load_yyy('JHN', ch)
    if data:
        for v in data['content']:
            if re.search(r'[Bb]ab[Kk]âh', v['text']):
                flag('CRITICAL', 'JHN', ch, v['v'],
                     f'babkâhin still present in JHN {ch}:{v["v"]}')
                special_issues += 1

print(f'   → {special_issues} special focus issues', flush=True)

# ─── FINAL REPORT ─────────────────────────────────────────────────────────────
total_verses = sum(
    len(json.loads(f.read_text()).get('content', []))
    for book in NT_BOOKS
    for f in (YYY_DIR / book).glob('*.json')
    if (YYY_DIR / book).exists()
)

SEV_ORDER = {'CRITICAL': 0, 'HIGH': 1, 'MEDIUM': 2}
issues.sort(key=lambda x: (SEV_ORDER.get(x[0], 9), x[1], x[2], x[3]))

crit = [i for i in issues if i[0] == 'CRITICAL']
high = [i for i in issues if i[0] == 'HIGH']

print()
print('=' * 72)
print('  FINAL INTEGRITY LOCK PASS — REPORT')
print('=' * 72)
print(f'  Total verses checked : {total_verses:,}')
print(f'  Total issues found   : {len(issues)}')
print(f'    CRITICAL : {len(crit)}')
print(f'    HIGH     : {len(high)}')
print('=' * 72)

if crit:
    print('\n▌ CRITICAL ISSUES')
    print('─' * 72)
    for sev, book, ch, vn, prob in crit:
        loc = f'{book} {ch}:{vn}' if vn else f'{book} ch.{ch}'
        print(f'  [{sev}] {loc} — {prob}')

if high:
    print('\n▌ HIGH ISSUES')
    print('─' * 72)
    for sev, book, ch, vn, prob in high:
        loc = f'{book} {ch}:{vn}' if vn else f'{book} ch.{ch}'
        print(f'  [{sev}] {loc} — {prob}')

if not issues:
    print('\n  ✓  ALL CHECKS PASSED — dataset is production-ready')
else:
    total = len(issues)
    real_blocking = len(crit)
    print()
    if real_blocking == 0:
        print('  VERDICT: No CRITICAL issues. HIGH issues are noted above for review.')
    else:
        print(f'  VERDICT: {real_blocking} CRITICAL issue(s) require resolution before production lock.')
print('=' * 72)
