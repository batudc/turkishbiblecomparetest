#!/usr/bin/env python3
"""Comprehensive OCR-fix patches for HKTN New Testament — full audit pass.

Covers all clear OCR error categories found during the 2026-05-17 full NT audit:
  - Apostrophe-space suffix splits (Check A)
  - Hyphen-apostrophe artifacts (Check D)
  - Stray leading characters (Check C)
  - OCR merge artifacts (Check E)
  - Known word splits (Check G)

ONLY fixes that are clearly correct with no ambiguity are included.
Unclear/garbled verses are tracked in /tmp/hktn_unclear.txt instead.

Usage:
    python3 pipeline/fix_nt_audit.py            # apply in-place
    python3 pipeline/fix_nt_audit.py --dry-run  # preview only
"""

import json, sys
from pathlib import Path

DRY_RUN  = '--dry-run' in sys.argv
HKTN_DIR = Path(__file__).parent.parent / 'data' / 'translations' / 'HKTN'

RSQ = '’'  # RIGHT SINGLE QUOTATION MARK

# (book, chapter, verse) -> [(find, replace), ...]
PATCHES: dict = {

    # ── MAT ──────────────────────────────────────────────────────────────────
    ('MAT',  5, 20): [('din bil ginleri', 'din bilginleri')],
    ('MAT',  9, 14): [('Yahya' + RSQ + '-nın', 'Yahya' + RSQ + 'nın')],
    ('MAT', 10, 15): [('Gomorro' + RSQ + '- nun', 'Gomorro' + RSQ + 'nun')],
    ('MAT', 12, 25): [('bildiğin den,', 'bildiğinden,'),
                      ('muhalefet e den', 'muhalefet eden'),
                      ('baş kaldıran', 'başkaldıran')],
    ('MAT', 12, 41): [('on lara', 'onlara')],
    ('MAT', 13, 33): [('Gö k sel Ege menlik', 'Göksel Egemenlik'),
                      ('maya la nıncaya', 'mayalanıncaya')],
    ('MAT', 13, 37): [('on ları', 'onları')],
    ('MAT', 13, 44): [('Yi ne', 'Yine'),
                      ('gizlen miş', 'gizlenmiş')],
    ('MAT', 13, 52): [('Gök sel Ege menlik', 'Göksel Egemenlik')],
    ('MAT', 17,  3): [('O' + RSQ + '- nunla', 'O' + RSQ + 'nunla')],
    ('MAT', 17,  4): [('Musa' + RSQ + ' ya', 'Musa' + RSQ + 'ya')],
    ('MAT', 18, 11): [('kur tarmaya', 'kurtarmaya')],
    ('MAT', 20, 18): [('baş kahinler', 'başkahinler')],
    ('MAT', 20, 29): [('Eriha' + RSQ + '- dan', 'Eriha' + RSQ + 'dan')],
    ('MAT', 21, 11): [('Galile' + RSQ + '-deki', 'Galile' + RSQ + 'deki')],
    ('MAT', 21, 25): [('Yahya' + RSQ + '-nın', 'Yahya' + RSQ + 'nın')],
    ('MAT', 21, 27): [('İsa' + RSQ + ' ya', 'İsa' + RSQ + 'ya')],
    ('MAT', 21, 43): [('alı nıp', 'alınıp'), ('ürü nünü', 'ürününü')],
    ('MAT', 23, 34): [('ba zısını', 'bazısını')],
    ('MAT', 25, 13): [('İn sanoğlu' + RSQ + 'nungele ceği', 'İnsanoğlu' + RSQ + 'nun geleceği')],
    ('MAT', 26, 57): [('Kayafa' + RSQ + '-nın', 'Kayafa' + RSQ + 'nın')],
    ('MAT', 26, 58): [('imam' + RSQ + '- ın', 'imam' + RSQ + 'ın'),
                      ('ka hin', 'kahin')],
    ('MAT', 26, 62): [('Baş kahin', 'Başkahin')],
    ('MAT', 26, 63): [('Tanrı' + RSQ + ' nın', 'Tanrı' + RSQ + 'nın')],
    ('MAT', 27,  3): [('O' + RSQ + ' nu', 'O' + RSQ + 'nu')],
    ('MAT', 27, 40): [('Tanrı' + RSQ + ' nın', 'Tanrı' + RSQ + 'nın'),
                      ('nidam direğin den in', 'idam direğinden in')],

    # ── MRK ──────────────────────────────────────────────────────────────────
    ('MRK',  1, 25): [('sİsa', 'İsa')],
    ('MRK',  1, 28): [('O' + RSQ + '-nun', 'O' + RSQ + 'nun')],
    ('MRK',  1, 29): [('Sinagog' + RSQ + ' tan', 'Sinagog' + RSQ + 'tan')],
    ('MRK',  1, 44): [('ha ham' + RSQ + '- a', 'haham' + RSQ + 'a')],
    ('MRK',  2, 26): [('Baş kahin', 'Başkahin'),
                      ('Tanrı' + RSQ + ' - nın', 'Tanrı' + RSQ + 'nın'),
                      ('olş tur', 'oluştur')],
    ('MRK',  2, 28): [('muBun dan böyle', 'Bundan böyle')],
    ('MRK',  3, 14): [('bu lunmaları', 'bulunmaları')],
    ('MRK',  3, 17): [('ver diği', 'verdiği')],
    ('MRK',  3, 26): [('baş kaldırırsa', 'başkaldırırsa')],
    ('MRK',  4, 26): [('Gök sel Egemenliği', 'Göksel Egemenliği')],
    ('MRK',  5, 24): [('O' + RSQ + '- nu', 'O' + RSQ + 'nu')],
    ('MRK',  5, 37): [('Yakup' + RSQ + '- un', 'Yakup' + RSQ + 'un')],
    ('MRK',  6, 15): [('Diğer le ri:', 'Diğerleri:'),
                      ('Her han gi', 'Herhangi'),
                      ('pey gamber gibidir', 'peygamber gibidir')],
    ('MRK',  6, 45): [('on lar', 'onlar'),
                      ('ön ceden', 'önceden')],
    ('MRK',  8,  5): [('nasVe onlara:', 'Ve onlara:')],
    ('MRK',  9, 12): [('aş ağ ı lanacaktır', 'aşağılanacaktır')],
    ('MRK',  9, 15): [('gör düğü', 'gördüğü')],
    ('MRK', 10, 21): [('Böy lelikle', 'Böylelikle')],
    ('MRK', 10, 46): [('Eriha' + RSQ + ' ya', 'Eriha' + RSQ + 'ya')],
    ('MRK', 12, 43): [('Ger çekten', 'Gerçekten')],
    ('MRK', 14, 26): [('asmanıSo - n ra', 'asması. Sonra')],
    ('MRK', 14, 59): [('caVe', 'Ve')],
    ('MRK', 14, 60): [('Baş kahin', 'Başkahin')],
    ('MRK', 14, 62): [('oİsa da:', 'İsa da:'),
                      ('Ben' + RSQ + ' im', 'Ben' + RSQ + 'im')],
    ('MRK', 14, 72): [('İsa' + RSQ + '-nın', 'İsa' + RSQ + 'nın')],

    # ── LUK ──────────────────────────────────────────────────────────────────
    ('LUK',  1, 79): [('O' + RSQ + '-nun', 'O' + RSQ + 'nun')],
    ('LUK',  2, 28): [('O' + RSQ + ' nu', 'O' + RSQ + 'nu')],
    ('LUK',  2, 33): [('O' + RSQ + '-nun', 'O' + RSQ + 'nun')],
    ('LUK',  3, 37): [('oMe tuşelah', 'Metuşelah')],
    ('LUK',  4, 18): [('bil dirmek', 'bildirmek'),
                      ('o lanları', 'olanları')],
    ('LUK',  4, 19): [('bil dirmek', 'bildirmek')],
    ('LUK',  4, 28): [('ler' + RSQ + ' in tümü', 'lerin tümü')],
    ('LUK',  4, 43): [('bil dir mem', 'bildirmem'),
                      ('ge re kiyor', 'gerekiyor'),
                      (', de di.', ', dedi.')],
    ('LUK',  5,  2): [('sıGölün', 'Gölün')],
    ('LUK',  5, 13): [('cüzzamkay bol du', 'cüzzam kayboldu')],
    ('LUK',  5, 26): [('Her kes', 'Herkes')],
    ('LUK',  7,  3): [('İsa' + RSQ + '- nın', 'İsa' + RSQ + 'nın')],
    ('LUK',  7, 16): [('Tü münün', 'Tümünün'),
                      ('Tan rıya', 'Tanrıya'),
                      ('hal kını', 'halkını')],
    ('LUK',  7, 29): [('duyduğundaYahya' + RSQ + 'nın', 'duyduğunda Yahya' + RSQ + 'nın')],
    ('LUK',  7, 39): [('kim se peygamber', 'kimse peygamber')],
    ('LUK',  7, 42): [('on ların', 'onların')],
    ('LUK',  8,  3): [('O' + RSQ + '-nunla', 'O' + RSQ + 'nunla')],
    ('LUK',  8, 34): [('ver diler', 'verdiler')],
    ('LUK',  8, 41): [('O' + RSQ + ' na', 'O' + RSQ + 'na')],
    ('LUK',  8, 42): [('O' + RSQ + ' nu', 'O' + RSQ + 'nu')],
    ('LUK',  8, 45): [('değ i lim', 'değilim')],
    ('LUK',  9, 22): [('İn sanoğlu', 'İnsanoğlu')],
    ('LUK',  9, 60): [('Tanrı' + RSQ + '-nın', 'Tanrı' + RSQ + 'nın')],
    ('LUK', 10, 22): [('Baba' + RSQ + '- dan', 'Baba' + RSQ + 'dan')],
    ('LUK', 10, 29): [('doğ rulamak', 'doğrulamak')],
    ('LUK', 12, 11): [('baş kanlar', 'başkanlar')],
    ('LUK', 12, 25): [('bo yuna', 'boyuna')],
    ('LUK', 13, 31): [('Ferisîlerdenba zıları', 'Ferisîlerden bazıları'),
                      ('ge le rek', 'gelerek')],
    ('LUK', 15, 24): [('bu lundu', 'bulundu')],
    ('LUK', 20, 37): [('İshak' + RSQ + '-ın', 'İshak' + RSQ + 'ın')],
    ('LUK', 22,  2): [('O' + RSQ + '-nu', 'O' + RSQ + 'nu')],
    ('LUK', 22, 56): [('O' + RSQ + ' nunla', 'O' + RSQ + 'nunla')],
    ('LUK', 22, 66): [('yaşlılarıbaş kahinlerle', 'yaşlıları başkahinlerle')],
    ('LUK', 23,  7): [('Hirodes' + RSQ + '-in', 'Hirodes' + RSQ + 'in'),
                      ('Hirodes' + RSQ + ' in', 'Hirodes' + RSQ + 'in'),
                      ('bildiğin den,', 'bildiğinden,')],
    ('LUK', 23, 12): [('Pilatus' + RSQ + ' la', 'Pilatus' + RSQ + 'la')],
    ('LUK', 23, 23): [('kal dır...!', 'kaldır...!')],
    ('LUK', 23, 29): [('Çün kü', 'Çünkü'),
                      ('doğurma mış', 'doğurmamış'),
                      ('diyecekle ri', 'diyecekleri')],
    ('LUK', 23, 32): [('O' + RSQ + ' nunla', 'O' + RSQ + 'nunla')],
    ('LUK', 23, 49): [('Galile' + RSQ + '-den', 'Galile' + RSQ + 'den'),
                      ('bu şeyleretanık', 'bu şeylere tanık')],
    ('LUK', 23, 51): [('Tanrı' + RSQ + '- nın Egemenliğ i', 'Tanrı' + RSQ + 'nın Egemenliği')],
    ('LUK', 24, 23): [('gör dük', 'gördük')],
    ('LUK', 24, 46): [('iş te', 'işte')],
    ('LUK', 24, 48): [('İş te', 'İşte')],

    # ── JHN ──────────────────────────────────────────────────────────────────
    ('JHN',  1, 36): [('İsa' + RSQ + ' nın', 'İsa' + RSQ + 'nın')],
    ('JHN',  1, 49): [('Tanrı' + RSQ + ' nın', 'Tanrı' + RSQ + 'nın')],
    ('JHN',  1, 51): [('Ger çek ten', 'Gerçekten'),
                      ('di yorum ki', 'diyorum ki')],
    ('JHN',  2,  4): [('ver di:', 'verdi:')],
    ('JHN',  2, 22): [('anımsadı lar.', 'anımsadılar.')],
    ('JHN',  3, 23): [('su lunduğundan', 'bulunduğundan')],
    ('JHN',  4, 18): [('şim diye', 'şimdiye')],
    ('JHN',  5, 11): [('Yatağ ı nı', 'Yatağını')],
    ('JHN',  5, 18): [('Tan rı' + RSQ + 'yla', 'Tanrı' + RSQ + 'yla'),
                      ('es kisinden', 'eskisinden')],
    ('JHN',  6, 41): [('İsa' + RSQ + ' nın', 'İsa' + RSQ + 'nın')],
    ('JHN',  7, 12): [('Ba zısı', 'Bazısı'),
                      ('fısıldaş malar', 'fısıldaşmalar')],
    ('JHN',  8, 54): [('Tan rımız', 'Tanrımız')],
    ('JHN',  9, 13): [('Ön ceden', 'Önceden')],
    ('JHN', 10,  6): [('İsa' + RSQ + '- nın', 'İsa' + RSQ + 'nın')],
    ('JHN', 11,  6): [('Lazar' + RSQ + '-ın', 'Lazar' + RSQ + 'ın')],
    ('JHN', 11, 19): [('ölümü ne deniyle', 'ölümü nedeniyle')],
    ('JHN', 12,  6): [('bu lunduğu', 'bulunduğu')],
    ('JHN', 12, 48): [('düş kün', 'düşkün'),
                      ('etmeyeninza ten', 'etmeyenin zaten'),
                      ('yar gıcı', 'yargıcı')],
    ('JHN', 13, 23): [('ol duğu', 'olduğu')],
    ('JHN', 13, 24): [('İsa' + RSQ + ' dan', 'İsa' + RSQ + 'dan')],
    ('JHN', 13, 31): [('sa dece:', 'sadece:')],
    ('JHN', 14,  8): [('Baba' + RSQ + '-yı', 'Baba' + RSQ + 'yı')],
    ('JHN', 14, 11): [('Baba' + RSQ + ' da', 'Baba' + RSQ + 'da'),
                      ('Baba' + RSQ + ' nın', 'Baba' + RSQ + 'nın')],
    ('JHN', 14, 13): [('Baba' + RSQ + '- nın', 'Baba' + RSQ + 'nın')],
    ('JHN', 14, 26): [('Tesel lici', 'Tesellici')],
    ('JHN', 14, 28): [('çün kü', 'çünkü'),
                      ('dön düğüm', 'döndüğüm')],
    ('JHN', 16, 13): [('gel dikten', 'geldikten'),
                      ('açıkla ya caktır.', 'açıklayacaktır.')],
    ('JHN', 18, 35): [('nuPi latus:', 'Pilatus:')],
    ('JHN', 19,  2): [('İsa' + RSQ + ' nın', 'İsa' + RSQ + 'nın'),
                      ('pe lerin', 'pelerin')],
    ('JHN', 19, 15): [('Yi ne', 'Yine')],
    ('JHN', 19, 25): [('İsa' + RSQ + '-nın', 'İsa' + RSQ + 'nın'),
                      ('al tın da', 'altında')],
    ('JHN', 19, 29): [('sap' + RSQ + ' a', 'sap' + RSQ + 'a')],
    ('JHN', 19, 30): [('al dıktan', 'aldıktan')],
    ('JHN', 19, 34): [('Yal nız', 'Yalnız')],
    ('JHN', 20,  4): [('Petrus' + RSQ + '-tan', 'Petrus' + RSQ + 'tan')],
    ('JHN', 20, 16): [('sa dece:', 'sadece:')],
    ('JHN', 20, 19): [('on lar', 'onlar')],
    ('JHN', 20, 29): [('gör meksizin', 'görmeksizin')],
    ('JHN', 21, 25): [('İsa' + RSQ + '-nın', 'İsa' + RSQ + 'nın')],

    # ── ACT ──────────────────────────────────────────────────────────────────
    ('ACT',  1,  2): [('vasıYükse liş (uruç)', 'Yükseliş (uruç)')],
    ('ACT',  1,  6): [('onaYa Rabbi', 'ona: -Ya Rabbi')],
    ('ACT',  1,  8): [('Yahudiye' + RSQ + '-ye', 'Yahudiye' + RSQ + 'ye')],
    ('ACT',  1, 21): [('İsa' + RSQ + '- nın', 'İsa' + RSQ + 'nın')],
    ('ACT',  2,  5): [('Kudüs' + RSQ + '- te', 'Kudüs' + RSQ + 'te')],
    ('ACT',  2,  7): [('Her kes', 'Herkes')],
    ('ACT',  2, 22): [('O' + RSQ + '-nun', 'O' + RSQ + 'nun')],
    ('ACT',  2, 32): [('İş te', 'İşte')],
    ('ACT',  2, 33): [('Allah' + RSQ + '- ın', 'Allah' + RSQ + 'ın')],
    ('ACT',  2, 47): [('onurlan dı rılıyorlar', 'onurlandırılıyorlar'),
                      ('ce maa te', 'cemaate'),
                      ('topluyor du.', 'topluyordu.')],
    ('ACT',  4,  6): [('baş kahin)', 'başkahin)')],
    ('ACT',  4, 21): [('bir ne den', 'bir neden')],
    ('ACT',  4, 23): [('gelerekbaş kahinlerle', 'gelerek başkahinlerle')],
    ('ACT',  9,  1): [('Rab' + RSQ + '- te', 'Rab' + RSQ + 'te')],

    # ── ROM ──────────────────────────────────────────────────────────────────
    ('ROM',  3, 12): [('Her kes', 'Herkes')],
    ('ROM',  3, 22): [('Tanrı' + RSQ + '- nın', 'Tanrı' + RSQ + 'nın')],
    ('ROM',  5, 14): [('Musa' + RSQ + '-ya', 'Musa' + RSQ + 'ya')],
    ('ROM',  7,  2): [('Yasa' + RSQ + '-dan', 'Yasa' + RSQ + 'dan')],
    ('ROM',  8, 32): [('O' + RSQ + '- nunla', 'O' + RSQ + 'nunla')],
    ('ROM', 10, 15): [('nasGönderilmedikçe', 'Gönderilmedikçe')],
    ('ROM', 11, 24): [('aÇün kü', 'Çünkü'),
                      ('aş ı lanması', 'aşılanması')],
    ('ROM', 13,  2): [('baş kal dırmış', 'başkaldırmış')],
    ('ROM', 14, 11): [('Tan rı' + RSQ + 'yı', 'Tanrı' + RSQ + 'yı')],

    # ── 1CO ──────────────────────────────────────────────────────────────────
    ('1CO',  1,  8): [('nO da', 'O da')],
    ('1CO',  1, 24): [('olsunTanrı', 'olsun Tanrı')],
    ('1CO',  2,  1): [('bil dirmek', 'bildirmek')],
    ('1CO',  9,  1): [('Rab' + RSQ + '- te', 'Rab' + RSQ + 'te')],
    ('1CO',  9, 13): [('bu lunanlar', 'bulunanlar')],
    ('1CO', 14,  9): [('zBöylece', 'Böylece')],
    ('1CO', 14, 29): [('doğ rulasınlar.', 'doğrulasınlar.')],
    ('1CO', 15, 45): [('Son raki', 'Sonraki')],

    # ── 2CO ──────────────────────────────────────────────────────────────────
    ('2CO',  3, 13): [('İsrailoğulla rı' + RSQ + ' na', 'İsrailoğulları' + RSQ + 'na')],
    ('2CO',  3, 14): [('olduğundanEski', 'olduğundan Eski'),
                      ('okunduğu sıradabu', 'okunduğu sırada bu'),
                      ('kaldırıl mamış', 'kaldırılmamış'),
                      ('kal dırılabilir', 'kaldırılabilir')],
    ('2CO',  9, 13): [('masÇünkü', 'Çünkü'),
                      ('Tanrı’yyüceltiyorlar', 'Tanrı’yı yüceltiyorlar')],
    ('2CO', 11,  3): [('sa delik', 'sadelik')],
    ('2CO', 11,  7): [('Tanrı' + RSQ + '- nın', 'Tanrı' + RSQ + 'nın')],

    # ── GAL ──────────────────────────────────────────────────────────────────
    ('GAL',  2,  4): [('içe riye', 'içeriye')],
    ('GAL',  3, 23): [('saklı kal dık.', 'saklı kaldık.')],

    # ── EPH ──────────────────────────────────────────────────────────────────
    ('EPH',  3, 14): [('rBöy le ce', 'Böylece'),
                      ('adlan-dırıldığı', 'adlandırıldığı')],
    ('EPH',  4, 18): [('Tanrı' + RSQ + '-nın', 'Tanrı' + RSQ + 'nın')],
    ('EPH',  5,  8): [('şim diyse', 'şimdiyse')],

    # ── PHP ──────────────────────────────────────────────────────────────────
    ('PHP',  2, 30): [('Mesih' + RSQ + '-in', 'Mesih' + RSQ + 'in')],
    ('PHP',  3,  9): [('Yasa' + RSQ + '- dan', 'Yasa' + RSQ + 'dan')],

    # ── COL ──────────────────────────────────────────────────────────────────
    ('COL',  2, 14): [('kal dırdırmıştır', 'kaldırmıştır')],
    ('COL',  2, 21): [('İn san', 'İnsan'),
                      ('ge le neklerine', 'geleneklerine'),
                      ('yö neliyorsunuz', 'yöneliyorsunuz')],

    # ── 1TH ──────────────────────────────────────────────────────────────────
    ('1TH',  1, 10): [('Tan rı' + RSQ + 'ya', 'Tanrı' + RSQ + 'ya')],

    # ── 2TI ──────────────────────────────────────────────────────────────────
    ('2TI',  1, 16): [('Onisifo ros' + RSQ + ' un', 'Onisifaros' + RSQ + 'un')],
    ('2TI',  1, 18): [('Rabb' + RSQ + ' ten', 'Rabb' + RSQ + 'ten')],
    ('2TI',  3, 11): [('Konya' + RSQ + ' da', 'Konya' + RSQ + 'da')],
    ('2TI',  4, 19): [('Onisifaros' + RSQ + '- un', 'Onisifaros' + RSQ + 'un')],

    # ── HEB ──────────────────────────────────────────────────────────────────
    ('HEB',  1,  4): [('saMirasçı', 'Mirasçı')],
    ('HEB',  1,  9): [('nerDürüstlüğü', 'Dürüstlüğü'),
                      ('tik sindin', 'tiksindi')],
    ('HEB',  1, 11): [('ge çer', 'geçer'),
                      ('on ları', 'onları')],
    ('HEB',  3, 16): [('Mısır' + RSQ + '-dan', 'Mısır' + RSQ + 'dan')],
    ('HEB',  5,  1): [('baş kahin', 'başkahin')],
    ('HEB',  5, 13): [('bil gisi', 'bilgisi')],
    ('HEB',  7, 12): [('Yasa' + RSQ + '- nın', 'Yasa' + RSQ + 'nın')],
    ('HEB',  7, 20): [('aracNitekim', 'Nitekim'),
                      ('kahin liği', 'kahinliği'),
                      ('olma dığı', 'olmadığı')],
    ('HEB',  7, 21): [('baş kahinsin', 'başkahinsin')],
    ('HEB',  7, 27): [('son ra da', 'sonra da'),
                      ('baş kahin', 'başkahin'),
                      ('sun arak', 'sunarak')],
    ('HEB',  8,  1): [('baş kahinimiz', 'başkahinimiz')],
    ('HEB',  8,  3): [('baş kahin', 'başkahin')],
    ('HEB',  8,  7): [('muÇünkü', 'Çünkü')],
    ('HEB',  9,  7): [('baş kahin', 'başkahin')],
    ('HEB',  9, 11): [('baş kahini', 'başkahini')],
    ('HEB', 10, 20): [('baş kahinimiz', 'başkahinimiz'),
                      ('imamımızz,', 'imamımız,')],
    ('HEB', 13, 13): [('O' + RSQ + '-nun', 'O' + RSQ + 'nun')],

    # ── JAS ──────────────────────────────────────────────────────────────────
    ('JAS',  3, 11): [('Han gi', 'Hangi'),
                      ('çı karabilir', 'çıkarabilir')],

    # ── 1PE ──────────────────────────────────────────────────────────────────
    ('1PE',  5,  2): [('Tanrı' + RSQ + ' nın', 'Tanrı' + RSQ + 'nın')],

    # ── 2PE ──────────────────────────────────────────────────────────────────
    ('2PE',  1, 21): [('Tanrı' + RSQ + '- nın', 'Tanrı' + RSQ + 'nın')],
    ('2PE',  2, 12): [('yor lar', 'yorlar')],

    # ── 1JN ──────────────────────────────────────────────────────────────────
    ('1JN',  1, 10): [('O' + RSQ + '-nu', 'O' + RSQ + 'nu')],
    ('1JN',  4,  7): [('Allah' + RSQ + '- tandır', 'Allah' + RSQ + 'tandır')],
    ('1JN',  5,  5): [('İsa' + RSQ + '- nın', 'İsa' + RSQ + 'nın')],
    ('1JN',  5, 18): [('Tanrı' + RSQ + '- dan', 'Tanrı' + RSQ + 'dan')],

    # ── 2JN ──────────────────────────────────────────────────────────────────
    ('2JN',  1,  8): [('Emeğiniziboşa', 'Emeğinizi boşa')],

    # ── JUD ──────────────────────────────────────────────────────────────────
    ('JUD',  1,  7): [('bu lu nan,', 'bulunan,')],

    # ── REV ──────────────────────────────────────────────────────────────────
    ('REV',  2, 13): [('bu lunduğu', 'bulunduğu')],
    ('REV',  8,  4): [('Tanrı' + RSQ + '- nın', 'Tanrı' + RSQ + 'nın')],
    ('REV',  8, 13): [('duy du', 'duydu')],
    ('REV',  9,  5): [('bir insansokmasgibiydi', 'bir insan sokması gibiydi'),
                      ('a cı da', 'acı da')],
    ('REV', 10,  5): [('ol duğum', 'olduğum')],
    ('REV', 12, 17): [('Tanrı' + RSQ + ' nın', 'Tanrı' + RSQ + 'nın')],
    ('REV', 20,  6): [('Tanrı' + RSQ + '- nın', 'Tanrı' + RSQ + 'nın')],
    ('REV', 22, 19): [('Kent' + RSQ + '-ten', 'Kent' + RSQ + 'ten')],
}


def apply_patches(book: str, ch: int, v: int, text: str) -> tuple[str, list[str]]:
    """Apply all patches for this verse, return (new_text, changes)."""
    key = (book, ch, v)
    if key not in PATCHES:
        return text, []
    changes = []
    for (find, replace) in PATCHES[key]:
        if find in text:
            text = text.replace(find, replace)
            changes.append(f'  {find!r} → {replace!r}')
    return text, changes


def process_book(book: str) -> int:
    book_dir = HKTN_DIR / book
    if not book_dir.exists():
        return 0
    total = 0
    for fpath in sorted(book_dir.glob('*.json'), key=lambda p: int(p.stem)):
        ch = int(fpath.stem)
        data = json.loads(fpath.read_text(encoding='utf-8'))
        changed = False
        for vobj in data.get('content', []):
            v = vobj['v']
            orig = vobj['text']
            new_text, changes = apply_patches(book, ch, v, orig)
            if changes:
                total += 1
                if DRY_RUN:
                    print(f'  [{book} {ch}:{v}]')
                    for c in changes:
                        print(c)
                else:
                    vobj['text'] = new_text
                    changed = True
        if changed and not DRY_RUN:
            fpath.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')
    return total


NT_BOOKS = [
    'MAT', 'MRK', 'LUK', 'JHN', 'ACT', 'ROM',
    '1CO', '2CO', 'GAL', 'EPH', 'PHP', 'COL',
    '1TH', '2TH', '1TI', '2TI', 'TIT', 'PHM',
    'HEB', 'JAS', '1PE', '2PE', '1JN', '2JN', '3JN', 'JUD', 'REV',
]

if DRY_RUN:
    print('=== DRY RUN — no files will be modified ===\n')
total = 0
for book in NT_BOOKS:
    n = process_book(book)
    if n:
        print(f'{book}: {n} verse(s) patched')
    total += n

print(f'\nTotal: {total} verse(s) {"would be " if DRY_RUN else ""}patched.')
