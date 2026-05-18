#!/usr/bin/env python3
"""
apply_user_text.py — Insert user-provided source text for:
  1. REV chapter 4  (completely missing chapter)
  2. TIT chapter 1  (verses 5-16 missing/compressed)

Applies full OCR correction to the provided raw text before writing.
"""

import json
from pathlib import Path

PROJECT  = Path(__file__).resolve().parent.parent
YYY_DIR  = PROJECT / 'data' / 'translations' / 'YYY1987'

# ---------------------------------------------------------------------------
# REV chapter 4 — corrected verse texts
# (source text parsed and OCR-corrected from user-provided material)
# ---------------------------------------------------------------------------
# Notes on verse markers in source:
#   ? = verse 2 marker     * = verse 3/8 marker
#   " = verse 4/6 marker   / = verse 7 marker
#   *"' = verse 9 marker (combined glyphs)
# v5 is incomplete — the lightning/thunder opening was lost in OCR.

REV4_VERSES = [
    (1,  "Bundan sonra gökte açık duran bir kapı gördüm. Benimle konuştuğunu işittiğim, "
         "borazan sesine benzeyen ilk ses şöyle dedi: «Buraya çık! Bundan sonra olması "
         "gereken olayları sana göstereyim.»"),

    (2,  "O anda Ruh'un beni yönetimine almasıyla gökte bir taht ve tahtın üzerinde "
         "oturan birini gördüm."),

    (3,  "Tahtta oturanın, yeşim ve kırmızı akik taşına benzer bir görünüşü vardı. "
         "Zümrüdü andıran bir gökkuşağı tahtı çevreliyordu."),

    (4,  "Tahtın etrafında yirmi dört ayrı taht vardı. Bu tahtların üzerinde, başlarında "
         "altın taçlar olan, beyaz giysilere bürünmüş yirmi dört ihtiyar oturmuştu."),

    # v5 opening (lightning/thunder) was lost in OCR; only the lamp clause survives.
    (5,  "Yanan yedi meşale vardı. Bunlar Tanrı'nın yedi ruhudur."),

    (6,  "Tahtın önünde billur gibi, sanki camdan bir deniz vardı. Tahtın ortasında ve "
         "çevresinde, önü ve arkası gözlerle kaplı dört tane canlı yaratık duruyordu."),

    # Insan → İnsan (capital I = capital İ correction)
    (7,  "Birinci yaratık aslana, ikinci yaratık danaya benziyordu. Üçüncü yaratığın "
         "yüzü İnsan yüzü gibiydi. Dördüncü yaratık uçan bir kartala benziyordu."),

    (8,  "Dört yaratığın her birinin altı kanadı vardı. Yaratıkların her yanı, "
         "kanatlarının alt tarafı bile gözlerle kaplıydı. Gece gündüz, durup dinlenmeden "
         "şöyle diyorlar: «Kutsal, kutsal, kutsaldır, var olmuş, var olan ve var olacak "
         "olan, gücü her şeye yeten Rab Tanrı!»"),

    (9,  "Canlı yaratıklar, taht üzerinde oturanı, sonsuzluklar boyunca yaşayanı "
         "yüceltip ona saygı ve şükran sundukça,"),

    (10, "yirmi dört ihtiyar, sonsuzluklar boyunca yaşayıp taht üzerinde oturanın "
         "önünde yere kapanarak O'na tapınıyorlar. Taçlarını tahtın önüne atarak "
         "diyorlar ki,"),

    (11, "«Rabbimiz ve Tanrımız! Yüceliği, saygıyı ve gücü almaya layıksın. Çünkü her "
         "şeyi sen yarattın. Hepsi senin isteğinle yaratılıp var oldu.»"),
]


# ---------------------------------------------------------------------------
# TIT 1 — verses 5-16 corrected
#
# Corruption patterns applied:
#   y→ı, Y→İ, b→ş, ö→ğ (standard pipeline)
#   pöyle→şöyle (p→ş: font renders ş as p on these pages)
#   Cünkü→Çünkü (C→Ç)
#   1örenç→iğrenç (digit 1→i, ö→ğ)
#   kirlenmi"tir→kirlenmiştir ("→ş)
#   dedildir→değildir (d→ğ: newly discovered pattern)
#   aldatyey→aldatıcı (encoding: yey=ıcı ligature)
#   Kurtaryeymyz→Kurtarıcımız (same yey=ıcı ligature in v3)
#   bobboöaz→boşboğaz (b→ş, ö→ğ)
#   ileri→işleri (semantic false-friend; b→ş that pipeline missed)
# ---------------------------------------------------------------------------
# Note: v3 currently contains merged TIT 1:4 + beginning of TIT 1:5.
# It will be trimmed to just TIT 1:4 content here.
# v4 number is absent (canonical v4 content stored as current v3).
# v13-14 are combined as v13 per the source text (marked "13-14").

TIT1_FIXES = {
    # Fix v3: trim off the v5 content; fix Kurtaryeymyz→Kurtarıcımız
    3: ("Ortak imanımıza göre öz oğlum olan Titus'a, Baba Tanrı'dan ve "
        "Kurtarıcımız Mesih İsa'dan lütuf ve esenlik olsun."),
}

TIT1_NEW_VERSES = [
    # v5: "bıraktım" — the sentence that was missing from the dataset
    # (partial form was in old v3 with "ileri" instead of "işleri")
    (5,  "Geriye kalan işleri düzene sokman ve sana buyurduğum gibi her kentte "
         "ihtiyarlar ataman için seni Girit'te bıraktım."),

    # v6: elder qualifications  (= old v4, now correctly numbered)
    (6,  "İhtiyar seçilecek kişi, eleştirilecek yönü olmayan, tek karılı biri olsun. "
         "Çocukları imanlı olmalı, sefahatle suçlanan ya da asi çocuklar olmamalı."),

    # v7: overseer qualifications
    (7,  "Gözetmen, Tanrı evinin kâhyası olduğuna göre, eleştirilecek yönü olmamalı. "
         "Dikbaşlı, tez öfkelenen, şarap düşkünü, zorba, haksız kazanç peşinde koşan "
         "biri olmamalı."),

    # v8: positive qualifications
    (8,  "Tersine, konuksever, iyiliksever, sağduyulu, adil, pak ve kendini "
         "denetleyebilen biri olmalı."),

    # v9: holding to sound doctrine  (was MISSING from dataset)
    (9,  "Hem başkalarını sağlam öğretiyle yüreklendirmek, hem de karşı çıkanları "
         "ikna edebilmek için imanlılara öğretilen güvenilir söze sımsıkı "
         "sarılmalıdır."),

    # v10: many rebellious people
    (10, "Çünkü asi, boşboğaz, aldatıcı birçok kişi vardır. Özellikle sünnet "
         "yanlıları bunlardandır."),

    # v11: silence them  (was MISSING from dataset)
    (11, "Onların ağzını kapamak gerek. Haksız kazanç uğruna, öğretmemeleri gerekeni "
         "öğreterek bazı aileleri tümüyle yıkmaktadırlar."),

    # v12: Cretan prophet quote
    (12, "Kendilerinden biri, öz peygamberlerinden biri şöyle demiştir: «Giritliler hep "
         "yalancı, azgın canavarlar, tembel oburlardır.»"),

    # v13: (source combines 13-14) rebuke them sharply
    (13, "Bu tanıklık doğrudur. Bu nedenle, Yahudi efsanelerine ve gerçek yoldan sapan "
         "kişilerin buyruklarına kulak vermemeleri, sağlam imana sahip olmaları için "
         "onları sert bir şekilde uyar."),

    # v15: to the pure all things are pure
    # bey→şey (b→ş), dedildir→değildir (d→ğ), kirlenmi"tir→kirlenmiştir
    (15, "Yüreği temiz olanlar için her şey temizdir, ama yüreği kirli olanlar ve "
         "imansızlar için hiçbir şey temiz değildir. Çünkü onların hem zihinleri, hem "
         "de vicdanları kirlenmiştir."),

    # v16: they claim to know God
    (16, "Tanrı'yı tanıdıklarını ileri sürer, ama eylemleriyle O'nu inkâr ederler. "
         "Söz dinlemeyen, hiçbir iyi işe yaramayan iğrenç kişilerdir."),
]


# ---------------------------------------------------------------------------
# Write REV chapter 4
# ---------------------------------------------------------------------------
def write_rev4():
    rev_dir = YYY_DIR / 'REV'
    rev_dir.mkdir(exist_ok=True)
    out_path = rev_dir / '4.json'

    chapter = {
        't': 'YYY1987',
        'b': 'REV',
        'c': 4,
        'content': [{'v': v, 'text': t} for v, t in REV4_VERSES],
    }
    out_path.write_text(
        json.dumps(chapter, ensure_ascii=False, indent=2),
        encoding='utf-8',
    )
    print(f'Written: {out_path}  ({len(REV4_VERSES)} verses)')


# ---------------------------------------------------------------------------
# Update TIT chapter 1
# ---------------------------------------------------------------------------
def update_tit1():
    tit_path = YYY_DIR / 'TIT' / '1.json'
    data = json.loads(tit_path.read_text(encoding='utf-8'))

    # Apply text fixes to existing verses (v3)
    for verse in data['content']:
        if verse['v'] in TIT1_FIXES:
            verse['text'] = TIT1_FIXES[verse['v']]

    # Remove old compressed v4-v10
    old_v_nums = {verse['v'] for verse in data['content']}
    data['content'] = [v for v in data['content'] if v['v'] <= 3]

    # Append new v5-v16
    for v_num, text in TIT1_NEW_VERSES:
        data['content'].append({'v': v_num, 'text': text})

    # Sort by verse number
    data['content'].sort(key=lambda v: v['v'])

    tit_path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding='utf-8',
    )
    new_vs = [v['v'] for v in data['content']]
    print(f'Updated: {tit_path}')
    print(f'  Verses: {new_vs}')
    removed = sorted(old_v_nums - set(new_vs))
    added   = sorted(set(new_vs) - old_v_nums)
    print(f'  Removed (replaced): {removed}')
    print(f'  Added:  {added}')


if __name__ == '__main__':
    write_rev4()
    update_tit1()
    print('\nDone.')
