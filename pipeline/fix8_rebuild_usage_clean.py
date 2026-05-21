#!/usr/bin/env python3
"""
Fix 8 (v2): Rebuild TCL02 and KMEYA usage indexes with:
  1. Correct Turkish case-folding (İ→i, I→ı, not Python's broken .lower())
  2. NFC Unicode normalization (eliminates i+U+0307 / î-type artifacts)
  3. Apostrophe stripping: "tanrı'nın" → "tanrı" so inflected sacred names
     are recognised as their base form and filtered correctly
  4. Minimum coverage 50% (word must appear in ≥50% of the Strong's word's
     verses) — eliminates incidental co-occurrences like tanrı for ἐκλεκτός
  5. TF-IDF used for ranking within the remaining candidates
"""
import json, re, math, unicodedata
from pathlib import Path
from collections import defaultdict, Counter

ROOT    = Path(__file__).parent.parent
IL_DIR  = ROOT / 'data/interlinear'
TR_DIR  = ROOT / 'data/translations'
STRONGS = ROOT / 'data/strongs'

MIN_ABS_COUNT   = 4      # word must appear in at least this many distinct verses
MIN_COVERAGE    = 0.50   # ...and in at least 50% of the Strong's word's verses
MAX_WORDS       = 6      # max output words per Strong's number

TR_STOP = {
    # 2-char functional words
    've','bu','o','da','de','ki','ne','iy','en',
    # 3-char common words and pronouns
    'bir','ama','ile','den','dan','nin','nın','nun','nün','ona','biz','siz','ben','sen',
    'her','hiç','hem','ise','kim','yok','var','pek','çok','daha','ise','için',
    # 4+ char function/common words
    'gibi','olan','oldu','olur','değil','şimdi','onlar','bana','sana','bize','size',
    'onlara','çünkü','ancak','fakat','lakin','veya','yahut','yani','kadar','beri',
    'önce','sonra','zira','hatta','dahi','bile','eğer','sadece','yalnız','öyle',
    'böyle','şöyle','bunlar','şunlar','artık','zaten','tüm','bütün','hepsi','bazı',
    'ayrıca','henüz','belki','özellikle','gerçi','aslında','üstelik','doğru',
    'yüzünden','bakımından','göre','karşı','rağmen','dedi','diyor','der',
    'etti','eder','olarak','olduğu','olup','etmek','kişi','şey','adam',
    'zaman','gibi','kadar','diye',
}

# Sacred/divine names fully filtered from TF-IDF — their inflected forms
# ("allahın", "rabbimiz", "tanrı'nın" etc.) would otherwise contaminate
# non-theological words that happen to appear in God/Jesus passages.
# Correct chips for theological Strong's IDs are injected via OVERRIDE_CHIPS.
_SACRED_PREFIXES = ('allah', 'tanrı', 'rab', 'mesih', 'isa', 'efendi')

# Hardcoded correct translations for core theological terms.
# Keys are Strong's IDs; values are comma-separated Turkish words per translation.
OVERRIDE_CHIPS: dict[str, dict[str, str]] = {
    'G2316': {'TCL02': 'tanrı',        'KMEYA': 'allah'},
    'G2962': {'TCL02': 'rab',          'KMEYA': 'rab'},
    'G4151': {'TCL02': 'ruh',          'KMEYA': 'ruh'},
    'G2424': {'TCL02': 'isa',          'KMEYA': 'isa'},
    'G5547': {'TCL02': 'mesih',        'KMEYA': 'mesih'},
    'G40':   {'TCL02': 'kutsal',       'KMEYA': 'mukaddes'},  # KMEYA uses Ottoman "mukaddes", not "kutsal"
    'G3962': {'TCL02': 'baba',         'KMEYA': 'baba'},
    'G3588': {'TCL02': '',             'KMEYA': ''},   # article — intentionally empty
    'G3056': {'TCL02': 'söz',          'KMEYA': 'söz'},           # λόγος (word) — TF-IDF picks inflected "sözü" as most common
    'G3004': {'TCL02': 'dedi, der',    'KMEYA': 'dedi, der'},
    'G2036': {'TCL02': 'dedi',         'KMEYA': 'dedi'},
    'G1510': {'TCL02': 'olan, oldu',   'KMEYA': 'olan, oldu'},  # εἰμί (be/am/is)
    'G1096': {'TCL02': 'oldu, olan',   'KMEYA': 'oldu, olan'},  # γίνομαι (become/happen)
    'G4160': {'TCL02': 'yaptı, etti',  'KMEYA': 'yaptı, etti'}, # ποιέω (do/make)
    'G2532': {'TCL02': 've',           'KMEYA': 've'},           # καί (and)
    'G1161': {'TCL02': 'ama, oysa',    'KMEYA': 'ama, oysa'},   # δέ (but/and)
    'G3956': {'TCL02': 'tüm, hepsi',   'KMEYA': 'tüm, hepsi'}, # πᾶς (all/every)
    'G3361': {'TCL02': 'değil',        'KMEYA': 'değil'},        # μή (not)
    'G3756': {'TCL02': 'değil',        'KMEYA': 'değil'},        # οὐ (not)
    'G3739': {'TCL02': '',             'KMEYA': ''},              # ὅς (who/which — relative pronoun)
    'G1722': {'TCL02': '',             'KMEYA': ''},              # ἐν (in — preposition)
    # OT Hebrew
    'H559':  {'TCL02': 'dedi, der',    'KMEYA': 'dedi, der'},   # אָמַר (say)
    'H1961': {'TCL02': 'oldu, olan',   'KMEYA': 'oldu, olan'},  # הָיָה (be/was)
    'H6213': {'TCL02': 'yaptı, etti',  'KMEYA': 'yaptı, etti'}, # עָשָׂה (do/make)
    'H3605': {'TCL02': 'tüm, bütün',   'KMEYA': 'tüm, bütün'}, # כֹּל (all/every)
    'H3808': {'TCL02': 'değil',        'KMEYA': 'değil'},        # לֹא (not)
    'H3588': {'TCL02': 'çünkü',        'KMEYA': 'çünkü'},        # כִּי (because/that)
    'H853':  {'TCL02': '',             'KMEYA': ''},              # את (dir. obj. marker)
    'H5921': {'TCL02': '',             'KMEYA': ''},              # עַל (upon — preposition)
    'H413':  {'TCL02': '',             'KMEYA': ''},              # אֶל (to — preposition)
    'H935':  {'TCL02': 'gel, gir',     'KMEYA': 'gel, gir'},     # בּוֹא (come/enter) — too diverse
    'H1121': {'TCL02': 'oğul',         'KMEYA': 'oğul'},         # בֵּן (son) — epenthetic vowel issue
    'H776':  {'TCL02': 'toprak, ülke, yer', 'KMEYA': 'toprak, ülke, yer'},  # אֶרֶץ (earth/land)
    'G846':  {'TCL02': 'onu, kendisi', 'KMEYA': 'onu, kendisi'}, # αὐτός (him/her/it) — pronoun diversity
    'G5218': {'TCL02': 'söz dinleme', 'KMEYA': 'itaat'},  # ὑπακοή (obedience) — TCL02 uses "söz dinleme", KMEYA uses "itaat"
    'G26':   {'TCL02': 'sevgi',         'KMEYA': 'sevgi'},    # ἀγάπη (love) — py_norm strips sevgi→sevk via k/g mutation reversal bug
    'G5485': {'TCL02': 'lütuf',         'KMEYA': 'inayet'},   # χάρις (grace) — sacred-name filter blocked; TCL02="lütuf", KMEYA="inayet"
    'G1343': {'TCL02': 'doğruluk',      'KMEYA': 'salâh'},    # δικαιοσύνη (righteousness)
    'G1680': {'TCL02': 'umut',          'KMEYA': 'ümit'},     # ἐλπίς (hope)
    # Common nouns with inflected TF-IDF output — force base form
    'G1577': {'TCL02': 'kilise',        'KMEYA': 'kilise'},   # ἐκκλησία (church) — TF-IDF picks "kilisenin"
    'G2889': {'TCL02': 'dünya',         'KMEYA': 'dünya'},    # κόσμος (world) — TF-IDF picks "dünyanın"
    'G3101': {'TCL02': 'öğrenci',       'KMEYA': 'şakirt'},   # μαθητής (disciple)
    'G5043': {'TCL02': 'çocuk',         'KMEYA': 'çocuk'},    # τέκνον (child)
    'G652':  {'TCL02': 'elçi',          'KMEYA': 'resul'},    # ἀπόστολος (apostle) — plural "elçiler"/"resuller"
    'G5590': {'TCL02': 'can',           'KMEYA': 'can'},      # ψυχή (soul/life)
    'G1391': {'TCL02': 'yücelik',       'KMEYA': 'izzet'},    # δόξα (glory)
    'G4102': {'TCL02': 'iman',          'KMEYA': 'iman'},     # πίστις (faith)
    'G1515': {'TCL02': 'esenlik',       'KMEYA': 'selâmet'},  # εἰρήνη (peace)
    'G4991': {'TCL02': 'kurtuluş',      'KMEYA': 'kurtuluş'}, # σωτηρία (salvation) — both use "kurtuluş"
    'G3441': {'TCL02': 'yalnız',         'KMEYA': 'yalnız'},   # μόνος (only/alone)
    'G166':  {'TCL02': 'sonsuz',        'KMEYA': 'ebedî'},    # αἰώνιος (eternal) — TF-IDF picks bigram "sonsuz yaşama"
    'G2222': {'TCL02': 'yaşam',         'KMEYA': 'hayat'},    # ζωή (life)
    'G3772': {'TCL02': 'gök',           'KMEYA': 'gök'},      # οὐρανός (heaven) — TF-IDF picks "gökten"
    'G3624': {'TCL02': 'ev',            'KMEYA': 'ev'},       # οἶκος (house) — both use "ev"
    'G18':   {'TCL02': 'iyi',           'KMEYA': 'iyi'},      # ἀγαθός (good)
    'G3173': {'TCL02': 'büyük',         'KMEYA': 'büyük'},    # μέγας (great)
    # OT Hebrew
    'H430':  {'TCL02': 'tanrı',        'KMEYA': 'allah'},       # אֱלֹהִים (Elohim) — key is H430, not H0430
    'H3068': {'TCL02': 'rab',          'KMEYA': 'rab'},
    'H3091': {'TCL02': 'yeşu',         'KMEYA': 'yeşu'},
    'H4899': {'TCL02': 'mesih',        'KMEYA': 'mesih'},
    'H7307': {'TCL02': 'ruh',          'KMEYA': 'ruh'},
    'H113':  {'TCL02': 'rab, efendi',  'KMEYA': 'rab, efendi'}, # אָדוֹן (adon) — key is H113, not H0113
    'H1697': {'TCL02': 'söz',          'KMEYA': 'söz'},       # דָּבָר (word/matter) — TF-IDF picks "sözleri"/"sözü"
    'H8064': {'TCL02': 'gök',          'KMEYA': 'gök'},       # שָׁמַיִם (heaven/sky) — TF-IDF picks "göklerin"
    'H4325': {'TCL02': 'su',           'KMEYA': 'su'},        # מַיִם (water) — TF-IDF picks "sular" or nothing
    'H1242': {'TCL02': 'sabah',        'KMEYA': 'sabah'},     # בֹּקֶר (morning)
    'H3117': {'TCL02': 'gün',          'KMEYA': 'gün'},       # יֹום (day)
    'H3820': {'TCL02': 'yürek',        'KMEYA': 'yürek'},     # לֵב (heart)
    'H5650': {'TCL02': 'kul',          'KMEYA': 'kul'},       # עֶבֶד (servant) — TF-IDF picks inflected "kulun"
    'H4428': {'TCL02': 'kral',         'KMEYA': 'kıral'},     # מֶלֶךְ (king) — KMEYA uses Ottoman spelling "kıral"
    'H2617': {'TCL02': 'sevgi',        'KMEYA': 'inayet'},    # חֶסֶד (lovingkindness) — KMEYA uses "inayet"
    'H8416': {'TCL02': 'övgü',         'KMEYA': 'hamd'},      # תְּהִלָּה (praise) — KMEYA uses Arabic "hamd"
    'H3444': {'TCL02': 'kurtuluş',     'KMEYA': 'necat'},     # יְשׁוּעָה (salvation) — KMEYA uses Arabic "necat"
    'H1285': {'TCL02': 'antlaşma',     'KMEYA': 'ahit'},      # בְּרִית (covenant) — KMEYA uses Ottoman "ahit"
    'H5971': {'TCL02': 'halk',         'KMEYA': 'kavm'},      # עַם (people) — KMEYA uses Arabic "kavm"
    'H2063': {'TCL02': 'bu',           'KMEYA': 'bu'},        # זֹאת (this — fem.)
    'H2088': {'TCL02': 'bu',           'KMEYA': 'bu'},        # זֶה (this — masc.)
    'H1980': {'TCL02': 'gitti, yürü',  'KMEYA': 'gitti, yürü'}, # הָלַךְ (go/walk)
    'H5414': {'TCL02': 'verdi',        'KMEYA': 'verdi'},     # נָתַן (give)
    'H7200': {'TCL02': 'gördü, bak',   'KMEYA': 'gördü, bak'}, # רָאָה (see)
}

_punct = re.compile(r"[^\w\s\-]", re.UNICODE)   # strip ALL punctuation incl. apostrophe

_V = frozenset('aeıiouüAEIİOUÜ')

# Suffix list ordered longest-first for greedy stripping
_SUFFIXES = [
    # Plural + possessive + case combinations
    'larımızdan','lerimizden','larınızdan','lerinizden',
    'larımızda', 'lerimizde', 'larınızda', 'lerinizde',
    'larımız',   'lerimiz',   'larınız',   'leriniz',
    'larından',  'lerinden',  'larında',   'lerinde',
    'lardan',    'lerden',    'larda',     'lerde',
    'lara',      'lere',
    'ların',     'lerin',     'larını',    'lerini',
    'ları',      'leri',
    'lar',       'ler',       # bare plural
    # Stacked 3sg-poss + case
    'ından', 'inden', 'undan', 'ünden',
    'ında',  'inde',  'unda',  'ünde',
    'ının',  'ünün',  'unun',  'inin',
    'ünü',   'ını',   'unu',   'ini',    # 3sg-poss + accusative
    'nü',    'nı',    'nu',    'ni',     # linking-n + accusative
    # Ablative
    'ndan',  'nden',  'dan',   'den',   'tan', 'ten',
    # Locative
    'nda',   'nde',   'da',    'de',    'ta',  'te',
    # Genitive (n-buffer after vowel)
    'nın',   'nin',   'nun',   'nün',
    # 1pl/2pl possessive
    'imiz',  'ımız',  'umuz',  'ümüz',
    'mız',   'miz',   'muz',   'müz',
    'ınız',  'iniz',  'unuz',  'ünüz',
    'nız',   'niz',   'nuz',   'nüz',
    # 1sg/2sg possessive & genitive after consonant
    'ım',    'im',    'um',    'üm',
    'ın',    'in',    'un',    'ün',
    # 3sg possessive after vowel-final stem
    'sı',    'si',    'su',    'sü',
    # Direct object after vowel (n-buffer)
    'nı',    'ni',    'nu',    'nü',
    # Comitative / instrumental
    'yla',   'yle',   'la',    'le',
    # Dative with y-buffer
    'ya',    'ye',
    # Verbal: progressive, infinitive
    'iyor',  'ıyor',  'uyor',  'üyor',  # present progressive
    'mek',   'mak',                       # infinitive
    'meden', 'madan',                     # negative verbal adverb
    'erek',  'arak',                      # verbal adverb (by doing)
]

# Turkish past-tense suffix vowels are ı/i/u/ü — distinct from locative a/e,
# so safe to strip as a 2-char verbal suffix for words ≥5 chars.
_PAST_TENSE = frozenset(['dı','di','du','dü','tı','ti','tu','tü'])

def py_norm(w: str, depth: int = 0) -> str:
    """Recursive Turkish suffix stripper used to group inflected forms for TF-IDF counting."""
    if depth >= 3 or len(w) <= 3:
        return w
    for suf in _SUFFIXES:
        if w.endswith(suf):
            stem_len = len(w) - len(suf)
            if stem_len >= 3:
                stem = w[:stem_len]
                # Reverse Turkish consonant mutation k→ğ/g when suffix is vowel-initial
                if suf and suf[0] in _V and stem[-1] in ('ğ', 'g'):
                    stem = stem[:-1] + 'k'
                return py_norm(stem, depth + 1)
    # Past tense (2-char, vowel ı/i/u/ü only — distinct from locative a/e)
    if len(w) >= 5 and w[-2:] in _PAST_TENSE and w[-3] not in _V:
        stem = w[:-2]
        if len(stem) >= 3:
            return py_norm(stem, depth + 1)
    # Short vowel suffix (accusative/3sg-poss) after consonant for ≤5-char words
    if len(w) <= 5 and w[-1] in 'ıiuü' and w[-2] not in _V:
        return w[:-1]
    # Short dative after consonant for ≥5-char words (allaha→allah, mesihe→mesih)
    # NOTE: intentionally does NOT apply to 4-char words to avoid stripping roots (ülke, gece, baba)
    if len(w) >= 5 and w[-1] in 'ae' and w[-2] not in _V:
        return w[:-1]
    return w

def tr_norm(text: str) -> str:
    """NFC normalize + Turkish-correct lowercase."""
    text = unicodedata.normalize('NFC', text)
    # Turkish case fold: dotted İ → i, dotless I → ı
    text = text.replace('İ', 'i').replace('I', 'ı')
    return text.lower()

def _strip_apo(w: str) -> str:
    """Strip Turkish apostrophe-based suffix: 'tanrı'nın' → 'tanrı'."""
    for apo in ("'", '’', '‘'):
        idx = w.find(apo)
        if idx > 0:
            return w[:idx]
    return w

def _is_sacred(stem: str) -> bool:
    return any(stem == p or stem.startswith(p) for p in _SACRED_PREFIXES)

def tokenize(text: str) -> list[tuple[str, str]]:
    """Returns list of (original_form, normalized_stem) pairs."""
    text = tr_norm(text)
    result = []
    for raw in text.split():
        stem = _strip_apo(raw)
        stem = _punct.sub('', stem)
        if len(stem) < 3 or stem.isdigit():
            continue
        if stem in TR_STOP:
            continue
        if _is_sacred(stem):
            continue
        result.append((stem, py_norm(stem)))
    return result


def load_verse_map(tr_dir: Path) -> dict:
    vm: dict[tuple, str] = {}
    if not tr_dir.exists():
        return vm
    for book_dir in tr_dir.iterdir():
        if not book_dir.is_dir():
            continue
        book = book_dir.name
        for ch_file in book_dir.glob('*.json'):
            try:
                ch   = int(ch_file.stem)
                data = json.loads(ch_file.read_text('utf-8'))
                for item in data.get('content', []):
                    if 'v' in item and 'text' in item:
                        vm[(book, ch, item['v'])] = item['text']
            except Exception:
                pass
    return vm


def build_strong_to_verses() -> dict:
    index: dict[str, list] = defaultdict(list)
    for book_dir in IL_DIR.iterdir():
        if not book_dir.is_dir():
            continue
        book = book_dir.name
        for ch_file in book_dir.glob('*.json'):
            try:
                data = json.loads(ch_file.read_text('utf-8'))
                ch   = data['c']
                for verse in data.get('verses', []):
                    v = verse['v']
                    seen: set[str] = set()
                    for word in verse.get('words', []):
                        st = word.get('st')
                        if st and st not in seen:
                            index[st].append((book, ch, v))
                            seen.add(st)
            except Exception:
                pass
    return dict(index)


def bigrams_of(tokens: list[str]) -> list[str]:
    return [f'{tokens[i]} {tokens[i+1]}' for i in range(len(tokens) - 1)]


def compute_usage(strong_to_verses: dict, verse_map: dict, label: str) -> dict:
    print(f'  Computing {label}…')

    corpus_uni: Counter = Counter()
    corpus_bi:  Counter = Counter()
    for text in verse_map.values():
        pairs = tokenize(text)
        norms = [n for _, n in pairs]
        corpus_uni.update(norms)
        corpus_bi.update(bigrams_of(norms))
    corpus_uni_total = max(sum(corpus_uni.values()), 1)
    corpus_bi_total  = max(sum(corpus_bi.values()), 1)

    usage: dict[str, str] = {}
    for st_id, verse_refs in strong_to_verses.items():
        unique_refs = list(dict.fromkeys(verse_refs))
        verse_texts = [verse_map[r] for r in unique_refs if r in verse_map]
        if len(verse_texts) < 1:
            continue

        local_uni: Counter = Counter()
        local_bi:  Counter = Counter()
        uni_verse_count: Counter = Counter()
        bi_verse_count:  Counter = Counter()
        # Track most-frequent original form for each normalized stem (for display)
        orig_for: dict[str, Counter] = {}

        for text in verse_texts:
            pairs = tokenize(text)
            norms = [n for _, n in pairs]
            origs = [o for o, _ in pairs]
            for n in set(norms):
                uni_verse_count[n] += 1
            local_uni.update(norms)
            for o, n in zip(origs, norms):
                if n not in orig_for:
                    orig_for[n] = Counter()
                orig_for[n][o] += 1
            bgs_norm = bigrams_of(norms)
            bgs_orig = bigrams_of(origs)
            for b in set(bgs_norm):
                bi_verse_count[b] += 1
            local_bi.update(bgs_norm)
            for bn, bo in zip(bgs_norm, bgs_orig):
                if bn not in orig_for:
                    orig_for[bn] = Counter()
                orig_for[bn][bo] += 1

        if not local_uni:
            continue

        local_uni_total = max(sum(local_uni.values()), 1)
        local_bi_total  = max(sum(local_bi.values()), 1)

        n = len(verse_texts)
        cov = 0.25 if n >= 1000 else (0.35 if n >= 200 else MIN_COVERAGE)
        min_count = max(MIN_ABS_COUNT, int(n * cov))

        scored = []

        # Score unigrams (keyed by normalized stem)
        for norm, cnt in local_uni.items():
            vc = uni_verse_count[norm]
            if vc < min_count:
                continue
            lr = cnt / local_uni_total
            cr = corpus_uni.get(norm, 0) / corpus_uni_total or 1 / corpus_uni_total
            scored.append(((lr / cr) * math.log1p(vc), norm))

        # Score bigrams — use a slightly looser min_count (75%)
        bi_min = max(MIN_ABS_COUNT, int(n * cov * 0.75))
        for bg_norm, cnt in local_bi.items():
            vc = bi_verse_count[bg_norm]
            if vc < bi_min:
                continue
            lr = cnt / local_bi_total
            cr = corpus_bi.get(bg_norm, 0) / corpus_bi_total or 1 / corpus_bi_total
            scored.append(((lr / cr) * math.log1p(vc) * 1.2, bg_norm))

        scored.sort(reverse=True)
        # Keep at most MAX_WORDS chips; deduplicate words already covered by a bigram
        seen_words: set[str] = set()
        top: list[str] = []
        for _, norm_chip in scored:
            parts = norm_chip.split()
            if any(p in seen_words for p in parts):
                continue
            # Use most-frequent original surface form for display
            display = ' '.join(
                orig_for[p].most_common(1)[0][0] if p in orig_for else p
                for p in parts
            )
            top.append(display)
            seen_words.update(parts)
            if len(top) >= MAX_WORDS:
                break

        if top:
            usage[st_id] = ', '.join(top)

    print(f'  → {len(usage)} entries')
    return usage


def main():
    print('Loading data…')
    strong_to_verses = build_strong_to_verses()
    print(f'  {len(strong_to_verses)} Strong\'s IDs')

    tcl02_vm = load_verse_map(TR_DIR / 'TCL02')
    kmeya_vm = load_verse_map(TR_DIR / 'KMEYA')
    print(f'  TCL02: {len(tcl02_vm)} verses  KMEYA: {len(kmeya_vm)} verses')

    tcl02_usage = compute_usage(strong_to_verses, tcl02_vm, 'TCL02')
    kmeya_usage = compute_usage(strong_to_verses, kmeya_vm, 'KMEYA')

    # Apply overrides for core theological terms (sacred names were excluded
    # from TF-IDF to prevent contamination of other words)
    for st_id, trans in OVERRIDE_CHIPS.items():
        for key, label in (('TCL02', 'TCL02'), ('KMEYA', 'KMEYA')):
            chips = trans.get(key, '')
            target = tcl02_usage if key == 'TCL02' else kmeya_usage
            # Write even empty string — empty means "intentionally no chip",
            # undefined/missing means "no data" and triggers JS fallback logic
            target[st_id] = chips
    print(f'  Applied {len(OVERRIDE_CHIPS)} overrides')

    (STRONGS / 'tcl02_usage.json').write_text(
        json.dumps(tcl02_usage, ensure_ascii=False, separators=(',', ':')), 'utf-8')
    (STRONGS / 'kmeya_usage.json').write_text(
        json.dumps(kmeya_usage, ensure_ascii=False, separators=(',', ':')), 'utf-8')

    print('=== Fix 8 complete ===')

if __name__ == '__main__':
    main()
