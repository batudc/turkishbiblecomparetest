#!/usr/bin/env python3
"""Reconstruct garbled ACT 7:31-60 with user-supplied correct text,
and fix MRK 2:27 ('olş tur' → 'olmuştur').

Usage:
    python3 pipeline/fix_act7_mrk2.py            # apply in-place
    python3 pipeline/fix_act7_mrk2.py --dry-run  # preview only
"""

import json, sys
from pathlib import Path

DRY_RUN  = '--dry-run' in sys.argv
HKTN_DIR = Path(__file__).parent.parent / 'data' / 'translations' / 'HKTN'

RSQ = '’'   # RIGHT SINGLE QUOTATION MARK (apostrophe / suffix sep)
LSQ = '‘'   # LEFT  SINGLE QUOTATION MARK (opening quote)

# ACT 7:31-60 — reconstructed from user-supplied correct text.
# Note: user's message had '56' written twice; second '56' = actual v57,
# user's 57→v58, 58→v59, 59→v60 (new verse, not previously in file).
ACT7_CORRECTIONS: dict = {
    31: 'Musa onu gördüğünde görünüşüne şaşırdı ve bakmak için yaklaşırken ona:',
    32: ('"Ben senin atalarının Allah' + RSQ + 'ı: İbrahim' + RSQ + 'in Allah' + RSQ + 'ı,'
         ' İshak' + RSQ + 'ın Allah' + RSQ + 'ı ve Yakup' + RSQ + 'un Allah' + RSQ + 'ıyım,'
         ' diye Rabbin sesi geldi; Musa' + RSQ + 'nın titreyerek oraya bakmaya yüreği yetmedi.'),
    33: ('Ve Rabb ona: Ayaklarından çarıklarını çıkar; çünkü durduğun yer kutsal topraktır.'),
    34: ('Gerçekten Ben Mısır' + RSQ + 'da olan halkımın zulmünü görüp, onların çağrısını duydum.'
         ' Onları kurtarmak için geldim ve şimdi gel seni Mısır' + RSQ + 'a göndereyim." dedi.'),
    35: (LSQ + 'Seni kim başkan ve yargıç yaptı' + RSQ + ' diye yalanladıkları bu Musa' + RSQ + 'yı;'
         ' hemen Allah, çalıda ona görünen meleğin eliyle baş ve kurtarıcı yaptı.'),
    36: ('Bu adam onları çıkarıp Mısır ile Kızıldenizde ve kırk yıl çöllerde harikalar ve deliller gösterdi.'),
    37: ('Ve o İsrail oğullarına: "Allahınız Rabb, kardeşlerinizden benim gibi bir peygamber çıkaracaktır;'
         ' O' + RSQ + 'nu dinleyin" diyen işte bu Musa' + RSQ + 'dır.'),
    38: ('Tur-u Sina' + RSQ + 'da kendisine söyleyen melekle ve atalarımızla beraber çölde olan'
         ' toplulukta, bize vermek için ' + LSQ + 'Yaşam Sözleri' + RSQ + ' almış olan kişi budur.'),
    39: ('Atalarımız onu dinlemek istemeyip reddettiler ve yürekleriyle Mısır' + RSQ + 'a geri döndüler.'),
    40: ('Ve Harun' + RSQ + 'a: Bize önümüzde duracak ilâhlar yap! Çünkü şu bizi Mısır' + RSQ + 'dan'
         ' çıkaran Musa' + RSQ + 'ya ne olduğunu bilmiyoruz, dediler.'),
    41: ('Böylece o günlerde bir buzağı yaptılar. Bu puta kurban kesip ellerinin işleriyle seviniyorlardı.'),
    42: ('Sonra Allah geri döndü ve onları gök varlıklarına tapınmaya terketti. Bundan ötürü'
         ' peygamberlerin kitabında: "Ey İsrail evi! Kırk yıl çölde bana sunular ve kurbanlar sundunuz mu?'),
    43: ('Molok' + RSQ + 'un çardağını ve Tanrınız Refan' + RSQ + 'ın yıldızını; yani tapınmak için'
         ' şu yaptığınız şekilleri taşıdınız. Ama Ben de sizi Babil' + RSQ + 'den daha uzağa'
         ' göndereceğim." Diye yazılmıştır.'),
    44: (LSQ + 'Tanıklık Çadırı' + RSQ + ' çölde atalarınızın arasındaydı. Çünkü Musa' + RSQ + 'ya'
         ' söyleyen: "Onu gördüğün örneğe göre yap" diye buyurmuştu.'),
    45: ('Böylece atalarımız onu sırayla taşıdılar ve Yeşu' + RSQ + 'yla beraber atalarımızın önünden'
         ' Allah' + RSQ + 'ın Davut' + RSQ + 'un günlerine dek kovduğu milletlerin toprakları içine götürdü.'),
    46: ('O da Allah' + RSQ + 'ın önünde lütuf görerek, Yakup' + RSQ + 'un Allah' + RSQ + 'ına bir yer bulmak istedi.'),
    47: ('Ama Süleyman ona bir Ev yaptı.'),
    48: ('"Yüce Tanrı elle yapılmış meskenlerde olamaz" Bundan ötürü peygamber: Rabb buyuruyor ki;'),
    49: ('"Gök tahtım ve yer de ayaklarıma basamaktır. Bana nasıl Ev yapacaksınız veya huzurumun yeri neresidir?'),
    50: ('Tüm bunları benim elim yapmadı mı?" dedi.'),
    51: ('Ey boynu sert, yürekleriyle kulakları sünnetsiz olanlar! Siz de atalarınız gibi, sürekli Kutsal Ruh' + RSQ + 'a karşı geliyorsunuz.'),
    52: ('Atalarınız peygamberlerin hangisine zulmetmedi? Onlar O doğru kişinin ortaya çıkacağını önceden'
         ' bildiren kişileri öldürdüler. Sizler de şimdi O' + RSQ + 'nun hainleri ve katilleri oldunuz.'),
    53: ('Siz Kutsal Yasa' + RSQ + 'yı (Şeriatı) meleklerin getirdiği gibi aldınız; ama tutmadınız.'),
    54: ('Yahudiler bu sözleri duyunca yürekleri kudurdu ve gazapla dişlerini sıktılar.'),
    55: ('O da Kutsal Ruhla dolu olup, göğe doğru göz dikerek Tanrı' + RSQ + 'nın yüceliğini'
         ' ve İsa' + RSQ + 'yı "O"nun sağında duruyor gördü ve:'),
    56: ('İşte ben göğü açılmış ve İnsanoğlu' + RSQ + 'nu Tanrı' + RSQ + 'nın sağında duruyor görüyorum, dedi.'),
    57: ('Onlar da yüksek sesle bağırıp kulaklarını tıkadılar ve tümü onun üzerine saldırdı.'),
    58: ('Ve onu kentten dışarıya çıkarıp recm ettiler(taşladılar). Tanıklar da kendi giysilerini'
         ' Saul(Pavlus) adındaki bir gencin ayakları yanına koydular.'),
    59: ('Ve İstafanos: Ya Rabb İsa! Ruhumu kabul et diye dua ederken onu taşladılar.'),
    60: ('Ve diz çöküp yüksek sesle: -Ya Rabbi! Bu günahı onlara sayma; diye yalvardı ve bunu'
         ' dedikten sonra uykuya vardı(öldü). Saul da onun ölümünü onaylamıştı.'),
}


def fix_act7(dry_run: bool) -> int:
    ch_file = HKTN_DIR / 'ACT' / '7.json'
    data    = json.loads(ch_file.read_text(encoding='utf-8'))
    content = data.get('content', [])

    # Map existing verse items by verse number
    verse_map: dict = {}
    for item in content:
        v = item.get('v')
        if v is not None:
            verse_map[v] = item

    changed = 0
    for v_num, new_text in ACT7_CORRECTIONS.items():
        if v_num in verse_map:
            old = verse_map[v_num]['text']
            if old != new_text:
                print(f'ACT 7:{v_num}')
                print(f'  BEFORE: {old[:100]}{"..." if len(old) > 100 else ""}')
                print(f'  AFTER:  {new_text[:100]}{"..." if len(new_text) > 100 else ""}')
                print()
                if not dry_run:
                    verse_map[v_num]['text'] = new_text
                changed += 1
        else:
            # Verse doesn't exist in file — add it
            print(f'ACT 7:{v_num}  [NEW VERSE]')
            print(f'  AFTER:  {new_text[:100]}{"..." if len(new_text) > 100 else ""}')
            print()
            if not dry_run:
                new_item = {'v': v_num, 'text': new_text}
                content.append(new_item)
            changed += 1

    if changed and not dry_run:
        # Sort content by verse number so v60 lands at the end
        data['content'] = sorted(
            content,
            key=lambda x: (x.get('v') or 0)
        )
        ch_file.write_text(
            json.dumps(data, ensure_ascii=False, indent=2),
            encoding='utf-8'
        )

    return changed


def fix_mrk2(dry_run: bool) -> int:
    ch_file = HKTN_DIR / 'MRK' / '2.json'
    data    = json.loads(ch_file.read_text(encoding='utf-8'))
    changed = 0

    for item in data.get('content', []):
        if item.get('v') == 27 and 'text' in item:
            old = item['text']
            new = old.replace('olş tur', 'olmuştur')
            if new != old:
                print(f'MRK 2:27')
                print(f'  BEFORE: {old}')
                print(f'  AFTER:  {new}')
                print()
                if not dry_run:
                    item['text'] = new
                changed += 1

    if changed and not dry_run:
        ch_file.write_text(
            json.dumps(data, ensure_ascii=False, indent=2),
            encoding='utf-8'
        )

    return changed


def main():
    prefix = '[DRY RUN] ' if DRY_RUN else ''
    total = 0
    total += fix_act7(DRY_RUN)
    total += fix_mrk2(DRY_RUN)
    print(f'{prefix}Total: {total} changes.')


if __name__ == '__main__':
    main()
