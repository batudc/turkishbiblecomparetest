#!/usr/bin/env python3
"""
Fix 9: Comprehensive retranslation of all definition files.
- Detects entries that are still in English (untranslated or partially untranslated)
- Re-translates using Google Translate with better pre/post processing
- Applies curated Turkish overrides for function words (prepositions, particles, conjunctions)
- Deduplicates repeated words in translated text
- Processes: tr2_def_grk, tr2_def_heb, tr3_nasdef_grk, tr3_nasdef_heb, tr4_sc_grk, tr4_sc_heb
"""
import json, re, time, unicodedata
from pathlib import Path
from collections import Counter

ROOT    = Path(__file__).parent.parent
STRONGS = ROOT / 'data/strongs'

# ─── Curated Turkish translations for critical function words ────────────────
# These are prepositions, particles, and conjunctions that Google Translate
# tends to leave in English or translate poorly.

OVERRIDE_DEFS = {
    # Greek prepositions
    'G1722': '-de/-da, içinde, aracılığıyla, sayesinde, ile {Genellikle esas olarak aynı anlama sahip bileşiklerde kullanılır; nadiren hareket fiilleriyle birlikte kullanılır ve ayrı (ve farklı) bir edatla (eliptik olarak) hariç, yönü belirtmez}',
    'G1519': '-e/-a, içine, için, doğru, -e kadar {Yer, zaman veya amaç bakımından hareketin hedefini gösterir}',
    'G1537': '-den/-dan, içinden, dışarı, kaynaklı {Bir kaynaktan, yerden veya durumdan ayrılmayı ifade eder; gerçek veya mecazi anlamda}',
    'G575': '-den/-dan, ayrı, uzakta, önce {Ayrılma veya kaynağı ifade eder}',
    'G1223': 'aracılığıyla, vasıtasıyla, sayesinde, yüzünden, boyunca {Araç, sebep veya zaman dilimini ifade eder; gerçek veya mecazi anlamda}',
    'G2596': '-e göre, karşı, aşağı, boyunca, için, üzerinde {Çeşitli ilişkilerde; bağlandığı duruma veya eylemin özelliğine göre değişir}',
    'G4314': '-e doğru, yanına, ile, karşısında, için {Yakınlık, yön veya ilişkiyi ifade eder; gerçek veya mecazi anlamda}',
    'G5259': 'tarafından, altında, aracılığıyla {Etken veya fail ile edilgen ilişkisini ifade eder}',
    'G1909': 'üzerinde, üstüne, hakkında, -e karşı, boyunca {Yer, zaman veya etki bakımından üstte veya üzerine konumlanmayı ifade eder}',
    'G3844': 'yanında, yanından, ötesinde, -den fazla {Yakınlık, kaynak veya karşılaştırmayı ifade eder}',
    'G4862': 'ile birlikte, beraber, -le/-la {Beraberlik veya ortaklığı ifade eder}',
    'G3326': 'ile, birlikte, sonra, arasında {Beraberlik veya ardışıklığı ifade eder}',
    'G1752': 'yüzünden, için, nedeniyle, sebebiyle',
    'G1715': 'önünde, karşısında, huzurunda, varlığında {Gerçek veya mecazi anlamda}',
    'G1725': 'önünde, karşısında, -in huzurunda {Mecazi anlamda}',
    'G1726': 'huzurunda, önünde, karşısında {Zarfsal kullanım}',
    # Greek particles and conjunctions
    'G1161': 'fakat, ama, ve, oysa, ise {Zıtlık veya geçiş ifadesi; İngilizcede çoğunlukla ifade edilmez}',
    'G1063': 'çünkü, zira, nitekim {Açıklama veya neden bildirir}',
    'G2532': 've, hem, de/da, aynı zamanda, bile {Ekleyici bağlaç}',
    'G3756': 'değil, hayır {Olumsuzluk; kesin inkâr}',
    'G3361': 'değil, olmasın {Olumsuzluk; koşullu veya niyete dayalı inkâr}',
    'G3754': 'ki, çünkü, -dığı/ndan, -diği için {Açıklama veya sebep bildiren bağlaç}',
    'G1487': 'eğer, şayet {Koşul bildiren bağlaç; gerçek veya varsayımsal}',
    'G2443': 'ki, diye, için, -sın diye {Amaç veya sonuç bildiren bağlaç}',
    'G3588': '{Belirli artikel — Türkçede karşılığı yoktur; belirli ismi işaret eder}',
    'G2228': 'ya da, veya, mi, yoksa {Seçenek bildiren bağlaç; soru cümlelerinde de kullanılır}',
    'G1065': 'gerçi, nitekim, tabii ki {Vurgu veya onay bildirir; genellikle diğer parçacıklarla birlikte kullanılır}',
    'G2260': 'daha çok, -den ziyade, belki de {Karşılaştırma veya şüphe bildirir}',
    'G3303': 'gerçi, bir yandan, doğrusu {Vurgu parçacığı; genellikle eşleşmiş bağlaçlarla kullanılır}',
    'G3767': 'o hâlde, öyleyse, bundan dolayı {Sonuç veya çıkarım bildiren bağlaç}',
    'G3779': 'böyle, bu şekilde, bunun gibi {İşaret zarfı}',
    'G5613': 'gibi, olduğu gibi, olarak {Karşılaştırma veya sıfat bildiren bağlaç}',
    'G1360': 'çünkü, zira, -diğinden, nedeniyle {Neden bildiren bağlaç; G1063 ile benzer}',
    'G1512': 'eğer belki, şayet, varsayalım ki {Koşullu veya varsayımsal bağlaç}',
    # Hebrew prepositions and particles
    'H853':  '{Belirli nesneyi işaret eden parçacık — Türkçede karşılığı yoktur}',
    'H3588': 'çünkü, zira, ki, -dığı/ndan, şayet {Açıklama, sebep veya koşul bildiren bağlaç}',
    'H3807': '-e/-a, için, doğru, ile {İletme veya yön bildiren edat}',
    'H4480': '-den/-dan, içinden, bir bölümü, bazıları {Ayrılma veya bölüm bildiren edat}',
    'H5921': 'üzerinde, hakkında, -e karşı, yanında {Yer veya ilgi bildiren edat}',
    'H5973': 'ile, birlikte, yanında {Beraberlik bildiren edat}',
    'H3808': 'değil, hayır, -me/-ma {Olumsuzluk parçacığı; kesin inkâr}',
    'H518': 'eğer, şayet, ya da {Koşul veya seçenek bildiren bağlaç}',
    'H3651': 'böyle, öyle, bu şekilde, bundan dolayı, bu nedenle {İşaret zarfı veya sonuç bildiren bağlaç}',
    'H413': '-e/-a, için, doğru, yanına {Yön veya hedef bildiren edat; G1519 ile benzer}',
    'H1004': 'ev, hane, konut, aile {Somut veya mecazi anlamda}',
    'H3027': 'el, güç, kuvvet, araç {Somut veya mecazi anlamda}',
    'H6310': 'ağız, kelam, kenar, açıklık {Somut veya mecazi anlamda}',
    'H5650': 'kul, köle, hizmetkâr {Mecazi olarak da kullanılır}',
    'H6440': 'yüz, önünde, huzurunda, varlığında {Somut veya mecazi anlamda}',
}

# ─── NAS definition overrides for key function words ─────────────────────────
OVERRIDE_NAS = {
    'G1722': 'içinde, üzerinde, aracılığıyla, ile, sayesinde',
    'G1519': '-e, içine, doğru, için',
    'G1537': '-den, içinden, kaynaklı, dışarıya',
    'G575':  '-den, ayrı, uzakta, önce',
    'G1223': 'aracılığıyla, vasıtasıyla, boyunca, sayesinde',
    'G2596': 'göre, karşı, boyunca, için',
    'G4314': 'doğru, yanında, ile, karşısında',
    'G5259': 'tarafından, altında, aracılığıyla',
    'G1909': 'üzerinde, hakkında, üzerine',
    'G3844': 'yanında, yanından, ötesinde',
    'G4862': 'ile birlikte, birlikte',
    'G3326': 'ile, sonra, arasında',
    'G1161': 'fakat, ama, ve, oysa',
    'G1063': 'çünkü, zira, nitekim',
    'G2532': 've, hem, de, bile',
    'G3756': 'değil, hayır',
    'G3361': 'değil, olmasın',
    'G3754': 'ki, çünkü, -diği için',
    'G1487': 'eğer, şayet',
    'G2443': 'ki, diye, -sın diye',
    'G2228': 'ya da, veya, yoksa',
}

# ─── English detection ────────────────────────────────────────────────────────
_ENGLISH_WORDS = frozenset('''
in at on by of to for from with into out off up down over under through
beyond above below before after between against within without toward among
upon across along behind beside besides during except per since than till
until via not no nor but and or yet so as if then is be do have get go
come give make take let put good bad great small large old new first last
same one two three a an the it its that this which who he she they them
him her we you from of set off to the be so to do is be just not
'''.split())

def _english_ratio(text: str) -> float:
    words = re.findall(r'[a-zA-Z]{2,}', text)
    if len(words) < 2:
        return 0.0
    eng = sum(1 for w in words if w.lower() in _ENGLISH_WORDS)
    return eng / len(words)

def _needs_retranslation(text: str, orig: str) -> bool:
    if not text or not text.strip():
        return True
    # If the translation is very similar to the English original, it's untranslated
    if orig:
        orig_words = set(re.findall(r'[a-zA-Z]{2,}', orig.lower()))
        tr_words   = set(re.findall(r'[a-zA-Z]{2,}', text.lower()))
        if orig_words and len(orig_words & tr_words) / len(orig_words) > 0.6:
            return True
    # If mostly English function words in meaning part
    bare = re.sub(r'\{[^}]*\}', '', text)
    bare = re.sub(r'\([^)]*\)', '', bare)
    if _english_ratio(bare) > 0.35:
        return True
    return False

# ─── Turkish deduplication ────────────────────────────────────────────────────
_TRLOW_RE = re.compile(r'[a-zA-ZçşğıöüÇŞĞİÖÜ]+', re.UNICODE)

def _tr_lower(s: str) -> str:
    return unicodedata.normalize('NFC', s).replace('İ', 'i').replace('I', 'ı').lower()

_VERB_SUFFIXES = ['yor', 'mek', 'mak', 'mış', 'miş', 'muş', 'müş']

def _word_stem(w: str) -> str:
    wl = _tr_lower(w)
    for suf in ['lar', 'ler', 'dan', 'den', 'tan', 'ten', 'nın', 'nin', 'nun', 'nün',
                'da', 'de', 'ta', 'te', 'ya', 'ye', 'yla', 'yle', 'la', 'le',
                'ı', 'i', 'u', 'ü', 'nı', 'ni', 'yı', 'yi']:
        if wl.endswith(suf) and len(wl) - len(suf) >= 3:
            return wl[:-len(suf)]
    return wl

def dedupe_definition(text: str) -> str:
    """Remove obviously repeated words within numbered definition items."""
    if not text:
        return text
    parts = re.split(r'(\{[^}]*\})', text)
    result_parts = []
    for part in parts:
        if part.startswith('{'):
            result_parts.append(part)
            continue
        # Within the main text, find numbered items
        items = re.split(r'(\d+\.\s*)', part)
        deduped_items = []
        global_seen_stems: set[str] = set()
        for i, item in enumerate(items):
            if re.match(r'\d+\.\s*$', item):
                deduped_items.append(item)
                continue
            words = item.split(',')
            seen_stems: set[str] = set()
            out_words: list[str] = []
            for word in words:
                w = word.strip()
                if not w:
                    continue
                stem = _word_stem(w)
                if stem in seen_stems or stem in global_seen_stems:
                    continue
                seen_stems.add(stem)
                global_seen_stems.add(stem)
                out_words.append(w)
            deduped_items.append(', '.join(out_words))
        result_parts.append(''.join(deduped_items))
    return ''.join(result_parts)

# ─── Google Translate ─────────────────────────────────────────────────────────
try:
    from deep_translator import GoogleTranslator
    _gt = GoogleTranslator(source='en', target='tr')
    _gt_cache: dict[str, str] = {}

    def translate(text: str) -> str:
        if not text or not text.strip():
            return text
        t = text.strip()
        if t in _gt_cache:
            return _gt_cache[t]
        for attempt in range(5):
            try:
                result = _gt.translate(t[:4900]) or t
                if result and result != t:
                    _gt_cache[t] = result
                    time.sleep(0.3)
                    return result
                # Try again with explicit Turkish context
                context = f"Türkçe çeviri (sözlük anlamı): {t}"
                result2 = _gt.translate(context[:4900]) or t
                if result2 and result2 != context:
                    result2 = re.sub(r'^Türkçe çeviri \(sözlük anlamı\):\s*', '', result2).strip()
                    _gt_cache[t] = result2
                    time.sleep(0.3)
                    return result2
                _gt_cache[t] = t
                return t
            except Exception as e:
                print(f'    [retry {attempt+1}] {e}')
                time.sleep(2 ** attempt)
        _gt_cache[t] = t
        return t

except ImportError:
    def translate(text: str) -> str:
        return text

# ─── Load source lexicons ─────────────────────────────────────────────────────
def load_lexicon_index(path: Path, prefix: str) -> dict:
    data = json.loads(path.read_text('utf-8'))
    return {f'{prefix}{e["id"]}': e for e in data}

# ─── Process a single translation file ────────────────────────────────────────
def process_file(
    lex_idx: dict,
    in_path: Path,
    out_path: Path,
    src_field: str,
    overrides: dict,
    label: str,
):
    print(f'\n  {label} — {out_path.name}')

    existing: dict[str, str] = {}
    if out_path.exists():
        existing = json.loads(out_path.read_text('utf-8'))
    else:
        existing = {}

    changed = 0
    total = len(lex_idx)

    for i, (key, entry) in enumerate(lex_idx.items()):
        # Apply overrides first
        if key in overrides:
            new_val = dedupe_definition(overrides[key])
            if existing.get(key) != new_val:
                existing[key] = new_val
                changed += 1
            continue

        orig_text = entry.get(src_field, '') or ''
        current   = existing.get(key, '')

        if not orig_text.strip():
            continue

        if not _needs_retranslation(current, orig_text):
            # Already has a good Turkish translation — but still dedupe
            deduped = dedupe_definition(current)
            if deduped != current:
                existing[key] = deduped
                changed += 1
            continue

        # Needs translation
        translated = translate(orig_text)
        deduped    = dedupe_definition(translated)
        if existing.get(key) != deduped:
            existing[key] = deduped
            changed += 1

        if (i + 1) % 200 == 0:
            out_path.write_text(json.dumps(existing, ensure_ascii=False, separators=(',', ':')), 'utf-8')
            print(f'    {i+1}/{total} — {changed} changes so far')

    out_path.write_text(json.dumps(existing, ensure_ascii=False, separators=(',', ':')), 'utf-8')
    print(f'    Done. {changed} entries changed. {len(existing)} total.')
    return changed


def main():
    print('Fix 9: Retranslating and deduplicating definition files…')

    grk_idx = load_lexicon_index(STRONGS / 'greek.json',  'G')
    heb_idx = load_lexicon_index(STRONGS / 'hebrew.json', 'H')

    # Split overrides by prefix
    grk_overrides_def = {k: v for k, v in OVERRIDE_DEFS.items() if k.startswith('G')}
    heb_overrides_def = {k: v for k, v in OVERRIDE_DEFS.items() if k.startswith('H')}
    grk_overrides_nas = {k: v for k, v in OVERRIDE_NAS.items() if k.startswith('G')}
    heb_overrides_nas = {k: v for k, v in OVERRIDE_NAS.items() if k.startswith('H')}

    total_changed = 0

    # tr2 — short_def
    print('\n=== tr2 (Tanım / short_def) ===')
    total_changed += process_file(grk_idx, STRONGS/'tr2_def_grk.json', STRONGS/'tr2_def_grk.json',
                                  'short_def', grk_overrides_def, 'Greek')
    total_changed += process_file(heb_idx, STRONGS/'tr2_def_heb.json', STRONGS/'tr2_def_heb.json',
                                  'short_def', heb_overrides_def, 'Hebrew')

    # tr3 — nas_def
    print('\n=== tr3 (NAS tanımı / nas_def) ===')
    total_changed += process_file(grk_idx, STRONGS/'tr3_nasdef_grk.json', STRONGS/'tr3_nasdef_grk.json',
                                  'nas_def', grk_overrides_nas, 'Greek')
    total_changed += process_file(heb_idx, STRONGS/'tr3_nasdef_heb.json', STRONGS/'tr3_nasdef_heb.json',
                                  'nas_def', heb_overrides_nas, 'Hebrew')

    # tr4 — strongs_def (no overrides — these are longer explanatory texts)
    print('\n=== tr4 (Strong\'s Concordance / strongs_def) ===')
    total_changed += process_file(grk_idx, STRONGS/'tr4_sc_grk.json', STRONGS/'tr4_sc_grk.json',
                                  'strongs_def', {}, 'Greek')
    total_changed += process_file(heb_idx, STRONGS/'tr4_sc_heb.json', STRONGS/'tr4_sc_heb.json',
                                  'strongs_def', {}, 'Hebrew')

    print(f'\n=== Fix 9 complete — {total_changed} total entries updated ===')


if __name__ == '__main__':
    main()
