#!/usr/bin/env python3
"""Targeted OCR-fix patches for HKTN Matthew (MAT).

Each entry in PATCHES is (chapter, verse) -> list of (old, new) text
replacements applied in order.  Special keys handle annotation stripping.

Usage:
    python3 pipeline/fix_matthew.py            # apply in-place
    python3 pipeline/fix_matthew.py --dry-run  # preview only
"""

import json, sys
from pathlib import Path

DRY_RUN = '--dry-run' in sys.argv
MAT_DIR = Path(__file__).parent.parent / 'data' / 'translations' / 'HKTN' / 'MAT'

RSQ = '’'  # RIGHT SINGLE QUOTATION MARK (apostrophe / suffix sep)
LSQ = '‘'  # LEFT SINGLE QUOTATION MARK (opening quote)

# (chapter, verse) -> ordered list of (find, replace) pairs
PATCHES: dict = {
    (1,  3):  [('babasoldu', 'babası oldu')],
    (1,  4):  [('ı Ram', 'Ram')],
    (1,  5):  [('Ra hab', 'Rahab')],
    (1, 14):  [('Ahim' + RSQ + ' in', 'Ahim' + RSQ + 'in')],
    (1, 18):  [('birleşmele rinden', 'birleşmelerinden'),
               ('Kutsal Ruh' + RSQ + ' tan', 'Kutsal Ruh' + RSQ + 'tan')],
    (1, 19):  [('oldu ğundan', 'olduğundan')],
    (2, 17):  [('ı O zaman', 'O zaman')],
    (2, 19):  [('\xf6l\xfc m\xfcn den', '\xf6l\xfcm\xfcnden')],
    (3, 14):  [('vazge\xe7ir meye', 'vazge\xe7irmeye')],
    (4,  6):  [('aşağ ıya', 'aşağıya')],
    (4, 13):  [('Na sı ra’yı', 'Nasıra’yı')],
    (4, 16):  [('\xf6l\xfcm \xfcl ke siyle', '\xf6l\xfcm \xfclkesiyle')],
    (5,  7):  [('onlara ınacak', 'onlara acınacak')],
    (5,  8):  [('acNe mutlu', 'Ne mutlu')],
    (5,  9):  [('ba rış\xe7ıl', 'barış\xe7ıl')],
    (5, 16):  [('babanıı', 'babanızı')],
    (5, 17):  [('zBe ni', 'Beni')],
    (5, 37):  [('ha yırla', 'hayırla'), ('k\xf6t\xfcden dir', 'k\xf6t\xfcdendir')],
    (5, 40):  [('tar tışarak', 'tartışarak'), ('almak almak', 'almak')],
    (6, 10):  [('g\xf6k lerde', 'g\xf6klerde')],
    (6, 14):  [('su\xe7larınızıbağış ladığınız',
                'su\xe7larınızı bağışladığınız')],
    (6, 15):  [('ba ğışlamadığınız', 'bağışlamadığınız')],
    (6, 21):  [('ner deyse', 'nerdeyse')],
    (6, 24):  [('al\xe7al tır', 'al\xe7altır')],
    (7, 29):  [('Ule malar', 'Ulemalar')],
    (8,  4):  [('haha ma(kahine)', 'hahama(kahine)')],
    (9, 26):  [('her ye rine', 'her yerine')],
    (10,  7): [('yaklaş mıştır', 'yaklaşmıştır')],
    (10, 11): [('so ra rak', 'sorarak')],
    (10, 12): [('girdiği niz de', 'girdiğinizde')],
    (10, 13): [('değ mez se', 'değmezse')],
    (10, 15): [('di yorum', 'diyorum')],
    (10, 17): [('ı Ama', 'Ama'), ('e de rek', 'ederek')],
    (10, 18): [('si niz', 'siniz')],
    (10, 19): [('ne di yelim', 'ne diyelim'), ('s\xf6yleyeceğ i niz', 's\xf6yleyeceğiniz')],
    (10, 21): [('\xf6ld\xfcrecek ler dir', '\xf6ld\xfcreceklerdir')],
    (10, 34): [('mYer y\xfcz\xfcne e sen lik', 'Yery\xfcz\xfcne esenlik')],
    (11, 19): [('hem yi yor', 'hem yiyor'),
               ('tarafındanonay landı', 'tarafından onaylan dı'),
               ('tarafından onaylan dı', 'tarafından onaylandı')],
    (11, 20): [('ha rikalar', 'harikalar')],
    (12,  2): [('he l\xe2l', 'hel\xe2l')],
    (12, 16): [('bil dirme melerini', 'bildirmemelerini')],
    (12, 23): [('şaşa rak', 'şaşarak'), ('Da vut oğlu', 'Davut oğlu'),
               ('Di yordu', 'Diyordu')],
    (12, 27): [('\xe7ıka rıyor', '\xe7ıkarıyor')],
    (12, 39): [('işareti n den', 'işaretinden')],
    (13,  5): [('ı Bazısı', 'Bazısı')],
    (13, 17): [('di yorum', 'diyorum'), ('g\xf6rd\xfckle ri nizi', 'g\xf6rd\xfcklerinizi'),
               ('ar zulayıp', 'arzulayıp')],
    (13, 19): [('Ege men lik', 'Egemenlik')],
    (13, 36): [('bıra karak', 'bırakarak')],
    (13, 58): [('muci zeler ya pamadı', 'mucizeler yapamadı')],
    (14, 23): [('Ken disi de', 'Kendisi de')],
    (14, 35): [('O y\xf6r enin', 'O y\xf6renin')],
    (15,  6): [('Ben den sana', 'Benden sana'),
               ('vak fe dilmiş tir', 'vakfedilmiştir'),
               ('ge \xe7ersiz', 'ge\xe7ersiz')],
    (15, 17): [('Da ha', 'Daha'), ('dışa rıya', 'dışarıya')],
    (15, 31): [('tanık olduğundaşş arak', 'tanık olduğunda şaşarak')],
    (15, 32): [('aİsa \xf6ğrencilerini', 'İsa \xf6ğrencilerini')],
    (15, 33): [('mas\xd6ğrencileri', '\xd6ğrencileri'), ('do yuralım', 'doyuralım')],
    (15, 36): [('par\xe7alayıp\xf6ğren cilerine', 'par\xe7alayıp \xf6ğrencilerine')],
    (16, 28): [('Ger \xe7ek ten', 'Ger\xe7ekten'),
               ('İnsan oğlu' + RSQ + 'nun', 'İnsanoğlu' + RSQ + 'nun'),
               ('ege men liğ in de', 'egemenliğinde'),
               ('g\xf6rmeyin ce', 'g\xf6rmeyince'),
               ('tatmaya caklar dır', 'tatmayacaklardır'),
               ('tatmaya caklardır', 'tatmayacaklardır')],
    (17, 18): [('şa kirtler(\xf6ğ renci)', 'şakirtler(\xf6ğrenci)')],
    (17, 21): [('İn san oğlu', 'İnsanoğlu'), ('bıra kılıp', 'bırakılıp')],
    (18, 10): [('g\xf6r\xfc- yor', 'g\xf6r\xfcyor')],
    (19,  1): [('hare ket ederek', 'hareket ederek')],
    (19, 24): [('Tanrı' + RSQ + ' nın', 'Tanrı' + RSQ + 'nın'),
               ('iğne de liğinden', 'iğne deliğinden')],
    (20, 20): [('o ğu l ları' + RSQ + 'nın', 'oğulları' + RSQ + 'nın')],
    (20, 34): [('bir denbire', 'birdenbire')],
    (21,  8): [('Birsr\xfch\xfchalk', 'Bir s\xfcr\xfc halk'),
               ('Birs\xfcr\xfchalk', 'Bir s\xfcr\xfc halk')],
    (22, 19): [('nBa na', 'Bana')],
    (22, 43): [('\xe7ağ ırarak', '\xe7ağırarak')],
    (23,  7): [('selam laşmayı', 'selamlaşmayı')],
    (23,  8): [('Siz- ler', 'Sizler'), ('\xe7ağ rılmayacaksınız', '\xe7ağrılmayacaksınız'),
               ('ya ni', 'yani')],
    (23,  9): [('\xe7\xfcn k\xfc', '\xe7\xfcnk\xfc'), (LSQ + 'Ba banız', LSQ + 'Babanız')],
    (23, 24): [('siv risineği', 'sivrisineği')],
    (23, 32): [('\xf6l \xe7eğ ini', '\xf6l\xe7eğini')],
    (24,  1): [('O' + RSQ + '- na', 'O' + RSQ + 'na')],
    (24, 15): [('Dan i yel', 'Daniyel'), ('g\xf6rd\xfcğ\xfc n\xfczde', 'g\xf6rd\xfcğ\xfcn\xfczde')],
    (24, 45): [('n\xd6yley se', '\xd6yleyse'), ('d\xfc r\xfcst', 'd\xfcr\xfcst')],
    (25, 21): [('kal dın', 'kaldın'), ('ya pa cağım', 'yapacağım')],
    (26,  2): [('de di', 'dedi')],
    (26,  5): [('nuAma', 'Ama')],
    (26,  8): [('ol sun', 'olsun')],
    (28,  7): [('ba kın', 'bakın')],
}

# Verses where trailing reference annotation blocks must be stripped.
# The clean text ends at the first occurrence of the given sentinel.
STRIP_AFTER: dict = {
    (16, 28): 'tatmayacaklardır.',
    (20, 34): 'gittiler.',
}


def apply_patches(text: str, patches: list) -> str:
    for old, new in patches:
        text = text.replace(old, new)
    return text


def strip_annotations(text: str, sentinel: str) -> str:
    idx = text.find(sentinel)
    if idx == -1:
        return text
    return text[:idx + len(sentinel)]


def main():
    total_changed = 0
    log: list = []

    for ch_num in range(1, 29):
        ch_file = MAT_DIR / f'{ch_num}.json'
        if not ch_file.exists():
            continue
        data = json.loads(ch_file.read_text(encoding='utf-8'))
        changed = False

        for item in data.get('content', []):
            if 'v' not in item or 'text' not in item:
                continue
            v = item['v']
            original = item['text']
            text = original

            if (ch_num, v) in PATCHES:
                text = apply_patches(text, PATCHES[(ch_num, v)])

            if (ch_num, v) in STRIP_AFTER:
                text = strip_annotations(text, STRIP_AFTER[(ch_num, v)])

            if text != original:
                item['text'] = text
                changed = True
                total_changed += 1
                log.append(
                    f'MAT {ch_num}:{v}\n'
                    f'  BEFORE: {original}\n'
                    f'  AFTER:  {text}'
                )

        if changed and not DRY_RUN:
            ch_file.write_text(
                json.dumps(data, ensure_ascii=False, indent=2),
                encoding='utf-8'
            )

    prefix = '[DRY RUN] ' if DRY_RUN else ''
    print(f'{prefix}Fixed {total_changed} verses in Matthew.\n')
    for entry in log:
        print(entry)
        print()


if __name__ == '__main__':
    main()
