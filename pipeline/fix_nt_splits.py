#!/usr/bin/env python3
"""Fix OCR word-split artifacts found in the NT-wide scan.
Covers: -acak/-ecek future splits, -lar/-ler suffix splits, and merge artifacts.

Usage:
    python3 pipeline/fix_nt_splits.py            # apply in-place
    python3 pipeline/fix_nt_splits.py --dry-run  # preview only
"""

import json, sys
from pathlib import Path

DRY_RUN = '--dry-run' in sys.argv
HKTN = Path(__file__).parent.parent / 'data' / 'translations' / 'HKTN'

RSQ = '’'  # RIGHT SINGLE QUOTATION MARK

# (book, chapter, verse) -> [(find, replace), ...]
PATCHES = {
    # ── MAT ──────────────────────────────────────────────────────────────────
    ('MAT',  2,  5): [('doğ acak', 'doğacak')],
    ('MAT',  2, 16): [('\xe7ocuk lar\xf6ld\xfcrtt\xfc', '\xe7ocukları \xf6ld\xfcrtt\xfc')],
    ('MAT',  9, 35): [('Sinagog larında', 'Sinagoglarında')],
    ('MAT', 11, 25): [('bilge lerden', 'bilgelerden')],
    ('MAT', 12, 48): [('kim lerdir', 'kimlerdir')],
    ('MAT', 15, 25): [('eği lerek', 'eğilerek')],
    ('MAT', 16, 19): [('anahtar larını', 'anahtarlarını')],
    ('MAT', 17, 25): [('Başka larından', 'Başkalarından')],
    ('MAT', 22, 34): [('işittik lerinde', 'işittiklerinde')],

    # ── LUK ──────────────────────────────────────────────────────────────────
    ('LUK',  1, 12): [('sıkı larak', 'sıkılarak')],
    ('LUK',  2, 44): [('Yolcu ların', 'Yolcuların'),
                      ('yakınla rının', 'yakınlarının')],
    ('LUK',  4, 44): [('Sinagog larında', 'Sinagoglarında')],
    ('LUK',  6, 23): [('yapmış lardı', 'yapmışlardı')],
    ('LUK',  7, 17): [('\xe7evre lerinde', '\xe7evrelerinde')],
    ('LUK',  8,  5): [('Ektik lerinden', 'Ektiklerinden')],
    ('LUK',  9, 46): [('kendi lerinden', 'kendilerinden')],
    ('LUK', 13, 22): [('kent lerle', 'kentlerle')],
    ('LUK', 21, 24): [('d\xfcş ecekler', 'd\xfcşecekler')],
    ('LUK', 22, 28): [('sıkıntı larımda', 'sıkıntılarımda')],
    ('LUK', 22, 55): [('oturduk larında', 'oturdukklarında'),
                      ('oturdukklarında', 'oturduklarında')],

    # ── MRK ──────────────────────────────────────────────────────────────────
    ('MRK',  1, 37): [('bulduk larında', 'bulduklarında')],
    ('MRK',  1, 39): [('Sinagog larında', 'Sinagoglarında')],
    ('MRK',  4, 23): [('kulak larvarsa', 'kulakları varsa')],
    ('MRK',  7,  5): [('gele nek lerine', 'geleneklerine')],
    ('MRK',  8, 14): [('unutmuş lardı', 'unutmuşlardı')],
    ('MRK',  8, 27): [('Kayseri ye si', 'Kayseriyesi'),
                      ('\xf6ğrenci lerine', '\xf6ğrencilerine'),
                      ('İn san ların', 'İnsanların')],
    ('MRK', 12, 38): [('selam larla', 'selamlarla')],
    ('MRK', 13,  7): [('Savaş larla', 'Savaşlarla')],

    # ── JHN ──────────────────────────────────────────────────────────────────
    ('JHN',  1, 45): [('peygamber lerin', 'peygamberlerin'),
                      ('Nasıra' + RSQ + ' dan', 'Nasıra' + RSQ + 'dan')],
    ('JHN',  7, 42): [('Kitap larda', 'Kitaplarda')],
    ('JHN', 10, 32): [('G\xf6z lerinizin', 'G\xf6zlerinizin')],
    ('JHN', 11,  7): [('\xf6ğrenci lerine', '\xf6ğrencilerine')],
    ('JHN', 12, 38): [('vaaz larımıza', 'vaazlarımıza')],
    ('JHN', 18, 18): [('yakmış lardı', 'yakmışlardı')],
    ('JHN', 21, 16): [('Koyun larıma', 'Koyunlarıma')],
    ('JHN', 21, 20): [('arka larından', 'arkalarından')],

    # ── ACT ──────────────────────────────────────────────────────────────────
    ('ACT',  2, 44): [('onla rın', 'onların'),
                      ('eşyalarıara larında', 'eşyaları aralarında')],
    ('ACT',  2, 46): [('yiyor lardı', 'yiyorlardı')],
    ('ACT',  4, 26): [('başkan larla', 'başkanlarla')],
    ('ACT',  5, 41): [('saydık larından', 'saydıklarından')],
    ('ACT', 14,  7): [('bildiri yor lardı', 'bildiriyorlardı')],
    ('ACT', 16,  4): [('Kent lerden', 'Kentlerden')],
    ('ACT', 20, 30): [('arka larından', 'arkalarından')],
    ('ACT', 20, 34): [('gereksinim lerimi', 'gereksinimlerimi')],
    ('ACT', 22,  4): [('Erkek lerle', 'Erkeklerle')],

    # ── ROM ──────────────────────────────────────────────────────────────────
    ('ROM',  1, 21): [('d\xfcş\xfcnce leriyle', 'd\xfcş\xfcnceleriyle')],
    ('ROM',  3, 21): [('peygamber lerden', 'peygamberlerden'),
                      ('yapı larak', 'yapılarak')],

    # ── 1CO ──────────────────────────────────────────────────────────────────
    ('1CO', 11, 19): [('grup ların', 'grupların')],
    ('1CO', 15, 41): [('yıldız ların', 'yıldızların')],

    # ── 2CO ──────────────────────────────────────────────────────────────────
    ('2CO', 10, 15): [('Başka larının', 'Başkalarının')],
    ('2CO', 11, 26): [('yolculuk sı rasında', 'yolculuk sırasında'),
                      ('tehlike le rinde', 'tehlikelerinde'),
                      ('tehlike lerin de', 'tehlikelerinde')],

    # ── PHP ──────────────────────────────────────────────────────────────────
    ('PHP',  1, 10): [('hamd lerle', 'hamdlerle')],
    ('PHP',  3, 19): [('ayıp larında', 'ayıplarında')],
    ('PHP',  4, 16): [('gereksinim lerimin', 'gereksinimlerimin')],

    # ── COL ──────────────────────────────────────────────────────────────────
    ('COL',  4,  7): [('d\xfcş\xfcnd\xfck lerimi', 'd\xfcş\xfcnd\xfcklerimi')],

    # ── 1TI ──────────────────────────────────────────────────────────────────
    ('1TI',  1, 11): [('kural larına', 'kurallarına')],
    ('1TI',  5, 17): [('G\xf6 rev leriyle', 'G\xf6revleriyle')],

    # ── 2TI ──────────────────────────────────────────────────────────────────
    ('2TI',  2, 14): [('yapmama larını', 'yapmamalarını')],
    ('2TI',  3,  6): [('g\xfcnah larla', 'g\xfcnahlarla')],

    # ── 1PE ──────────────────────────────────────────────────────────────────
    ('1PE',  2, 13): [('kurum larına', 'kurumlarına')],

    # ── 2PE ──────────────────────────────────────────────────────────────────
    ('2PE',  2, 13): [('karşılıyan lardır', 'karşılıyanlardır')],

    # ── JUD ──────────────────────────────────────────────────────────────────
    ('JUD',  1, 13): [('dalga larıdırlar', 'dalgalarıdırlar')],

    # ── REV ──────────────────────────────────────────────────────────────────
    ('REV',  5,  6): [('ihtiyar ların', 'ihtiyarların')],
    ('REV', 13,  8): [('tapınacak lardır', 'tapınacaklardır')],
    ('REV', 17,  4): [('takı larla', 'takılarla')],
    ('REV', 17, 14): [('Rabb lerin', 'Rabblerin'),
                      ('se\xe7 kin', 'se\xe7kin'),
                      ('sadık lardır', 'sadıklardır')],
}


def main():
    total_changed = 0
    log = []

    for (book, ch, v), patches in PATCHES.items():
        ch_file = HKTN / book / f'{ch}.json'
        if not ch_file.exists():
            print(f'WARNING: {ch_file} not found')
            continue
        data = json.loads(ch_file.read_text(encoding='utf-8'))
        changed_file = False

        for item in data.get('content', []):
            if item.get('v') != v:
                continue
            original = item.get('text', '')
            text = original
            for find, replace in patches:
                text = text.replace(find, replace)
            if text != original:
                item['text'] = text
                changed_file = True
                total_changed += 1
                log.append(f'{book} {ch}:{v}\n  BEFORE: {original}\n  AFTER:  {text}')

        if changed_file and not DRY_RUN:
            ch_file.write_text(
                json.dumps(data, ensure_ascii=False, indent=2),
                encoding='utf-8'
            )

    prefix = '[DRY RUN] ' if DRY_RUN else ''
    print(f'{prefix}Fixed {total_changed} verses.\n')
    for entry in log:
        print(entry)
        print()


if __name__ == '__main__':
    main()
