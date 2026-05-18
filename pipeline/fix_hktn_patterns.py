#!/usr/bin/env python3
"""Apply targeted regex-based fixes for OCR split patterns that the oracle-based
fix_hktn_splits.py cannot catch — mostly cases where one fragment is a common
Turkish word (in stopwords / oracle) or where the complete form is absent from the
reference translations for that specific verse.

Each rule is a (pattern, replacement) pair where pattern is a raw-string regex
matched against the verse text.  Replacements join the two fragments; any Turkish
inflectional suffix that follows is preserved naturally because we only match the
two split tokens, not the entire word form.

NOTE: Only merge tokens that are ALWAYS one word in Turkish.  Do NOT merge
token pairs that are legitimately two words (e.g. "her şey", "her biri").

Usage:
    python3 pipeline/fix_hktn_patterns.py            # apply in-place
    python3 pipeline/fix_hktn_patterns.py --dry-run  # print changes only
"""

import json, re, sys
from pathlib import Path

BASE     = Path(__file__).parent.parent
HKTN_DIR = BASE / 'data' / 'translations' / 'HKTN'
DRY_RUN  = '--dry-run' in sys.argv

# ── replacement rules ──────────────────────────────────────────────────────────
# Each entry: (compiled_regex, replacement_string, description)
# NO end-boundary after the second fragment so inflectional suffixes attached to
# the continuation token are preserved automatically.
# e.g. "za man" → "zaman", "za manda" → "zamanda", "za manların" → "zamanların"

RULES = []

def rule(pattern: str, replacement: str, desc: str = ''):
    RULES.append((re.compile(pattern), replacement, desc))


# ── 3-token splits (must come before their 2-token sub-patterns) ─────────────
rule(r'\bBöy\s+le\s+ce\b', 'Böylece', 'Böylece (thus, 3-part split)')
rule(r'\bböy\s+le\s+ce\b', 'böylece', 'böylece (thus, 3-part split, lowercase)')
rule(r'\bbi\s+ri\s+si',    'birisi',  'birisi (someone, 3-part split)')

# ── proper nouns (always one word) ────────────────────────────────────────────
rule(r'\bMer\s+yem\b',    'Meryem',   'Meryem (Mary)')
rule(r'\bYahu\s+da\b',    'Yahuda',   'Yahuda (Judea/Judah)')
rule(r'\bGali\s+le',      'Galile',   'Galile (Galilee, all suffixed forms)')
rule(r'\bGa\s+lile',      'Galile',   'Galile (Galilee, Ga+lile split)')
rule(r'\bİs\s+rail\b',    'İsrail',   'İsrail (Israel)')
rule(r'\bHi\s+rodes\b',   'Hirodes',  'Hirodes (Herod)')
rule(r'\bZe\s+keriya\b',  'Zekeriya', 'Zekeriya (Zechariah)')
rule(r'\bMe\s+sih’',  'Mesih’', "Mesih' (Christ, U+2019 apostrophe+suffix, all forms)")
rule(r'\bKu\s+düs',       'Kudüs',    'Kudüs (Jerusalem, Ku+düs split, all forms)')

# ── "Böylece/böyle/böyle" family ──────────────────────────────────────────────
rule(r'\bBö\s+ylece\b',   'Böylece',  'Böylece (thus, Bö+ylece split)')
rule(r'\bBöy\s+lece\b',   'Böylece',  'Böylece (thus, Böy+lece split)')
rule(r'\bböy\s+lece\b',   'böylece',  'böylece (thus, lowercase)')
rule(r'\bBöy\s+le\b',     'Böyle',    'Böyle (such/like this, uppercase)')
rule(r'\bböy\s+le\b',     'böyle',    'böyle (such/like this, lowercase)')

# ── very high-frequency words (>= 5 occurrences) ──────────────────────────────
# "şimdi" (now) — 19x
rule(r'\bŞim\s+di\b',     'Şimdi',    'Şimdi (Now, sentence-initial)')
rule(r'\bşim\s+di\b',     'şimdi',    'şimdi (now, lowercase)')
# "bundan/bunlar" — 13x, 7x
rule(r'\bBun\s+dan\b',    'Bundan',   'Bundan (From this, uppercase)')
rule(r'\bBun\s+lar\b',    'Bunlar',   'Bunlar (These, uppercase)')
# "söyle-" verb stem — 6x+ (söyleyen, söyledi, söylerken, söylüyor…)
rule(r'\bSöy\s+le',       'Söyle',    'Söyle- (say, uppercase, all inflections)')
rule(r'\bsöy\s+le',       'söyle',    'söyle- (say, lowercase, all inflections)')
rule(r'\bsöy\s+lü',       'söylü',    'söylü- (say, continuous: söylüyor, söylüyordu…)')
# "üstüne/üstünde" — 6x, 3x
rule(r'\büs\s+tüne',      'üstüne',   'üstüne (onto/upon, all inflections)')
rule(r'\büs\s+tünde',     'üstünde',  'üstünde (on top of, all inflections)')
# "büyük" (great/large) — 6x
rule(r'\bBü\s+yük\b',     'Büyük',    'Büyük (Great, uppercase standalone)')
rule(r'\bbü\s+yük',       'büyük',    'büyük (large/great, all inflections)')
# "birşey" (something, one-word form) — 5x
rule(r'\bbirş\s+ey',      'birşey',   'birşey (something, all inflections)')
# "ötürü" (because of/due to) — 5x
rule(r'\bötü\s+rü\b',     'ötürü',    'ötürü (because of/due to)')
# "inanan" (believer) — 5x
rule(r'\bina\s+nan',      'inanan',   'inanan (believer/believing, all inflections)')

# ── high-frequency words (3–4 occurrences) ────────────────────────────────────
# "şöyle" (as follows/like this) — 4x
rule(r'\bşöy\s+le\b',     'şöyle',    'şöyle (as follows/like this)')
# "sizin" (your, genitive) — 4x
rule(r'\bsi\s+zin\b',     'sizin',    'sizin (your, genitive)')
# "kulağ-" (ear, all inflections: kulağı, kulağına…) — 4x
rule(r'\bKu\s+lağ',       'Kulağ',    'Kulağ- (ear, capitalized, all inflections)')
rule(r'\bku\s+lağ',       'kulağ',    'kulağ- (ear, lowercase, all inflections)')
# "gidip" (going/having gone) — 4x
rule(r'\bgi\s+dip\b',     'gidip',    'gidip (going/having gone)')
# "hocam" (my teacher/master) — 4x
rule(r'\bho\s+cam',       'hocam',    'hocam (my teacher/master, all forms)')
# "buyruğu/buyruğa/buyruğun" (command, all inflections) — 3x
rule(r'\bbuy\s+ruğ',      'buyruğ',   'buyruğ- (command, all inflections)')
# "olağanüstü" (extraordinary) — 3x
rule(r'\bolağ\s+anüstü',  'olağanüstü', 'olağanüstü (extraordinary)')
# "şekilde" (in a manner/way) — 3x
rule(r'\bşe\s+kilde\b',   'şekilde',  'şekilde (in a manner/way)')
# "gelen" (who comes/coming) — 3x
rule(r'\bge\s+len\b',     'gelen',    'gelen (who comes/coming)')
# "Büyük" (Great, uppercase) — 3x
rule(r'\bBü\s+yük\b',     'Büyük',    'Büyük (Great, uppercase standalone)')
# "gören" (who sees/seeing) — 3x
rule(r'\bgö\s+ren\b',     'gören',    'gören (who sees/seeing)')
# "bakarak" (by looking at) — 3x
rule(r'\bba\s+karak\b',   'bakarak',  'bakarak (by looking at)')

# ── medium-frequency words (2 occurrences) ────────────────────────────────────
rule(r'\bsarsıl\s+maz\b', 'sarsılmaz', 'sarsılmaz (unshakeable/firm)')
rule(r'\bgü\s+vençle\b',  'güvençle', 'güvençle (with confidence)')
rule(r'\bden\s+li\b',     'denli',    'denli (as much as/to that extent)')
rule(r'\bge\s+lip\b',     'gelip',    'gelip (having come/coming)')
rule(r'\bkud\s+ret',      'kudret',   'kudret (power/might, all inflections)')
rule(r'\bkü\s+çük',       'küçük',    'küçük (small, all inflections)')
rule(r'\bmey\s+ve',       'meyve',    'meyve (fruit, all inflections)')
rule(r'\bmut\s+lu',       'mutlu',    'mutlu (happy/blessed, all inflections)')
rule(r'\bsec\s+de\b',     'secde',    'secde (prostration/worship)')
rule(r'\bga\s+lip\b',     'galip',    'galip (victorious)')
rule(r'\bgök\s+gü',       'gökgü',    'gökgürültüsü (thunder, gök+gü split)')
rule(r'\bhari\s+ka',      'harika',   'harika/harikalar (miracle, all inflections)')
rule(r'\bMüj\s+de',       'Müjde',    'Müjde (Gospel/Good News, all forms)')
rule(r'\bsuy\s+la\b',     'suyla',    'suyla (with water)')

# ── common Turkish words split across a space ──────────────────────────────────
# "zaman" (time/moment) — very frequent; 5 chars so below corpus threshold
rule(r'\bza\s+man',       'zaman',    'zaman (time, all inflections)')

# "herkes" (everyone) — negative lookahead avoids "her keseken" OCR artifact
rule(r'\bher\s+kes(?!eken)', 'herkes', 'herkes (everyone)')

# "olarak" (as / in the capacity of) — 6 chars, common gerund
rule(r'\bola\s+rak\b',    'olarak',   'olarak (as/being)')

# "olmak" (to be/become) and its split forms
rule(r'\bol\s+mak',       'olmak',    'olmak (to be/become, all inflections)')

# "gerek" (necessary/need)
rule(r'\bge\s+rek',       'gerek',    'gerek (necessary/need, all inflections)')

# "giysi" (clothing)
rule(r'\bgiy\s+si',       'giysi',    'giysi (clothing/garment)')

# "şeyler" (things, plural) — NOTE: "her şey" remains two words
rule(r'\bşey\s+ler',      'şeyler',   'şeyler (things, plural)')

# "olanlar" (those who are)
rule(r'\bolan\s+lar',     'olanlar',  'olanlar (those who are)')

# "biri" (one of them) — 2-part; 3-part already handled above
rule(r'\bbi\s+ri\b',      'biri',     'biri (one of them, 2-part split)')

# "başka" (other/else)
rule(r'\bba\s+şka\b',     'başka',    'başka (other)')

# "üzerinde/üzere/üzre" — upon/on
rule(r'\büze\s+rinde',    'üzerinde', 'üzerinde (on/upon)')
rule(r'\büze\s+rin\b',    'üzerin',   'üzerin (upon it)')
rule(r'\büze\s+re\b',     'üzere',    'üzere (upon/about to)')
rule(r'\büz\s+re\b',      'üzre',     'üzre (upon — archaic)')

# "davranmak/davranış" — behavior
rule(r'\bdav\s+ran',      'davran',   'davran (behave, all forms)')

# "kuşanmış" (girded/wearing)
rule(r'\bkuşan\s+mış',    'kuşanmış', 'kuşanmış (girded)')

# ── "Şöyle" (uppercase variant not covered by lowercase şöy le rule) ─────────
rule(r'\bŞöy\s+le',       'Şöyle',    'Şöyle (like this/as follows, uppercase, all forms)')

# ── "üzerine" — the -rine dative form, not covered by existing üzerin\b rule ─
rule(r'\büze\s+rine',     'üzerine',  'üzerine (onto/upon, dative inflection)')
# Update existing üzerin\b to also catch üzerin without boundary for other forms
rule(r'\büze\s+rini',     'üzerini',  'üzerini (onto him/it, accusative)')
rule(r'\büze\s+rinize',   'üzerinize','üzerinize (onto you pl., dative)')

# ── "Zaman" — uppercase variant (Za man) ──────────────────────────────────────
rule(r'\bZa\s+manı\b',    'Zamanı',   'Zamanı (His/Her Time, uppercase)')

# ── "yorum" verb ending — "-ıyorum/-uyorum/-üyorum/-iyorum" split as "yo rum" ─
rule(r'\byo\s+rum',       'yorum',    'yorum (-yorum present-cont. ending, all punct.)')

# ── "önünde" — split as "ö nünde" ────────────────────────────────────────────
rule(r'\bö\s+nünde\b',    'önünde',   'önünde (before/in front of, locative)')
rule(r'\bönün\s+de\b',    'önünde',   'önünde (before/in front of, split at ün+de)')

# ── "mezbah" (altar) — split as "mez bah" ────────────────────────────────────
rule(r'\bmez\s+bah',      'mezbah',   'mezbah (altar, all inflections)')

# ── "bizim" (our) — split as "bi zim" ────────────────────────────────────────
rule(r'\bbi\s+zim\b',     'bizim',    'bizim (our/ours)')

# ── "karşı-" verb forms — split at the consonant cluster ─────────────────────
rule(r'\bkarş\s+ılaş',    'karşılaş', 'karşılaş- (encounter/meet, all inflections)')
rule(r'\bkarş\s+ıya\b',   'karşıya',  'karşıya (to the opposite side)')

# ── "herşey" (everything, one-word form) — split as "herş ey" ────────────────
rule(r'\bherş\s+ey',      'herşey',   'herşey (everything, one-word form)')

# ── "karar" (decision) — split as "ka rar" ───────────────────────────────────
rule(r'\bka\s+rar\b',     'karar',    'karar (decision/resolution)')

# ── common verb and adjective splits ─────────────────────────────────────────
rule(r'\bÇı\s+kacak',     'Çıkacak',  'Çıkacak (will come out, uppercase)')
rule(r'\bçı\s+kacak',     'çıkacak',  'çıkacak (will come out, lowercase)')
rule(r'\böğ\s+reterek\b', 'öğreterek','öğreterek (by teaching)')
rule(r'\bsaptır\s+maya',  'saptırmaya','saptırmaya (to lead astray, all forms)')
rule(r'\bdeğ\s+nekle\b',  'değnekle', 'değnekle (with a rod/staff)')
rule(r'\bkapaya\s+mayacağı','kapayamayacağı','kapayamayacağı (cannot close, all forms)')
rule(r'\bmey\s+dan',      'meydan',   'meydan (plaza/square, all inflections)')
rule(r'\bgö\s+ründü',     'göründü',  'göründü (appeared, all punct.)')
rule(r'\bşük\s+redin',    'şükredin', 'şükredin (give thanks, all forms)')
rule(r'\bKa\s+dın',       'Kadın',    'Kadın (Woman, uppercase, all inflections)')
rule(r'\buğ\s+ramayın',   'uğramayın','uğramayın (do not enter/visit)')
rule(r'\bTar\s+çın',      'Tarçın',   'Tarçın (Cinnamon, proper name/spice)')
rule(r'\bHalelu\s+yah',   'Haleluyah','Haleluyah (Hallelujah, all forms)')
rule(r'\bkı\s+larak',     'kılarak',  'kılarak (by making/performing, all punct.)')
rule(r'\bka\s+til\b',     'katil',    'katil (murderer)')
rule(r'\bdolaşa\s+caklar','dolaşacaklar','dolaşacaklar (they will roam, all punct.)')
rule(r'\bOra\s+da\b',     'Orada',    'Orada (There, uppercase)')
rule(r'\bora\s+da\b',     'orada',    'orada (there, lowercase)')
rule(r'\bbi\s+rinin\b',   'birinin',  'birinin (of one)')
rule(r'\bCe\s+maat',      'Cemaat',   'Cemaat (congregation/church, all inflections)')
rule(r'\bgö\s+rünce',     'görünce',  'görünce (upon seeing, all forms)')
rule(r'\bhiz\s+metçi',    'hizmetçi', 'hizmetçi (servant, all inflections)')
rule(r'\bsö\s+züne\b',    'sözüne',   'sözüne (to his/her word)')
rule(r'\bkö\s+tülük',     'kötülük',  'kötülük (evil/wickedness, all inflections)')
rule(r'\bbi\s+raz\b',     'biraz',    'biraz (a bit/a little)')
rule(r'\bdiğ\s+erler',    'diğerler', 'diğerler- (others, plural, all inflections)')
rule(r'\bdiğ\s+eri\b',    'diğeri',   'diğeri (the other one)')
rule(r'\bdu\s+rumu\b',    'durumu',   'durumu (the situation/state)')
rule(r'\bka\s+panarak\b', 'kapanarak','kapanarak (by prostrating/closing)')
rule(r'\biyileş\s+medi',  'iyileşmedi','iyileşmedi (did not heal, all punct.)')
rule(r'\bge\s+lerek',     'gelerek',  'gelerek (coming, all punct.)')
rule(r'\byal\s+varan\b',  'yalvaran', 'yalvaran (pleading/begging)')
rule(r'\başağ\s+ı\b',     'aşağı',    'aşağı (downward/below, standalone)')
rule(r'\bsöylen\s+diler', 'söylendiler','söylendiler (they muttered/said, all punct.)')
rule(r'\bfaz\s+lasıyla\b','fazlasıyla','fazlasıyla (more than enough)')
rule(r'\bgö\s+türüp\b',   'götürüp',  'götürüp (having taken/brought)')
rule(r'\bgü\s+nün\b',     'günün',    'günün (of the day)')
rule(r'\bMa\s+demki\b',   'Mademki',  'Mademki (given that/since, uppercase)')
rule(r'\banım\s+sayın\b', 'anımsayın','anımsayın (remember!, imperative)')
rule(r'\banım\s+sayarak\b','anımsayarak','anımsayarak (by remembering)')
rule(r'\bso\s+ruyor',     'soruyor',  'soruyor (is asking, all forms)')
rule(r'\bhük\s+me\b',     'hükme',    'hükme (to the verdict/judgment)')
rule(r'\bke\s+derle\b',   'kederle',  'kederle (with grief/sorrow)')
rule(r'\bdövü\s+yorlar',  'dövüyorlar','dövüyorlar (they are beating, all punct.)')
rule(r'\byö\s+neltti',    'yöneltti', 'yöneltti (directed/aimed, all forms)')
rule(r'\bLa\s+tince\b',   'Latince',  'Latince (in Latin language)')
rule(r'\bsö\s+verek',     'söverek',  'söverek (by swearing/cursing)')
rule(r'\bmuy\s+du',       'muydu',    'muydu (was it?, question past)')
rule(r'\bkâ\s+seyi\b',    'kâseyi',   'kâseyi (the cup, accusative)')
rule(r'\btö\s+kez\b',     'tökez',    'tökez (stumbling-block/obstacle)')
rule(r'\bŞeh\s+rin\b',    'Şehrin',   'Şehrin (of the City, uppercase)')

# ── instrumental/comitative suffix splits (-la/-le/-yla/-yle detached) ────────
# These are always one word in Turkish; the suffix must attach to the noun.
# 3-part splits first (longest match wins sequential application):
rule(r'\bo\s+nun\s+la\b',  'onunla',   'onunla (with him/her, 3-part: o+nun+la)')
rule(r'\bon\s+la\s+r',     'onlar',    'onlar- (they, 3-part on+la+r, keeps suffix: ra/rı/rın…)')
rule(r'\bOn\s+la\s+r',     'Onlar',    'Onlar- (They, uppercase, 3-part)')
# 2-part splits:
rule(r'\bonun\s+la\b',     'onunla',   'onunla (with him/her)')
rule(r'\biman\s+la\b',     'imanla',   'imanla (with faith)')
rule(r'\bşiddet\s+le\b',   'şiddetle', 'şiddetle (with force/violence)')
rule(r'\badıy\s+la\b',     'adıyla',   'adıyla (with his/her name)')
rule(r'\bsebebiy\s+le\b',  'sebebiyle','sebebiyle (because of/for the reason)')
rule(r'\bhalkı\s+yla\b',   'halkıyla', 'halkıyla (together with his/her household)')
rule(r'\bsövgüy\s+le\b',   'sövgüyle', 'sövgüyle (with insults/abuse)')
rule(r'\belleri\s+yle\b',  'elleriyle','elleriyle (with his/her hands)')

# ── final batch: remaining patterns from scanner ──────────────────────────────

# 3-part split of "Böylece" with single-char middle token "y"
rule(r'\bBö\s+y\s+lece\b', 'Böylece', 'Böylece (thus, 3-part Bö+y+lece split)')
# 3-part split of "sözünü" (the word, accusative)
rule(r'\bsö\s+zü\s+nü\b',  'sözünü',  'sözünü (his/her word, accusative, 3-part split)')
# 3-part split of "bırakayım" (let me leave)
rule(r'\bbıra\s+ka\s+yım', 'bırakayım','bırakayım (let me leave, 3-part split)')

# Common suffix fragments still detected
rule(r'\bme\s+lerini\b',   'melerini', 'melerini (-melerini verbal noun suffix, acc.)')
rule(r'\bsöylü\s+yor',     'söylüyor', 'söylüyor (is saying, present continuous)')
rule(r'\bgökgü\s+rültüsü', 'gökgürültüsü', 'gökgürültüsü (thunder, second-stage split)')
rule(r'\bgemiy\s+le\b',    'gemiyle',  'gemiyle (by ship/boat)')
rule(r'\bHaviye\s+de\b',   'Haviyede', 'Haviyede (in Hades/Gehenna, locative)')
rule(r'\bka\s+bın',        'kabın',    'kabın (of the vessel/container, all forms)')
rule(r'\bSan\s+mıyorum',   'Sanmıyorum','Sanmıyorum (I do not think so, all punct.)')
rule(r'\beki\s+yorlar',    'ekiyorlar','ekiyorlar (they are sowing, all punct.)')
rule(r'\bsı\s+kılıyor',    'sıkılıyor','sıkılıyor (is getting bored/pressed, all forms)')
rule(r'\byücel\s+tiyorlar','yüceltiyorlar','yüceltiyorlar (they are praising, all inflections)')
rule(r'\bcak\s+sınız',     'caksınız', 'caksınız (-caksınız future-2pl suffix)')
rule(r'\bla\s+rında\b',    'larında',  'larında (-larında locative-plural suffix)')
rule(r'\bkâ\s+seyi\b',     'kâseyi',   'kâseyi (the cup/chalice, accusative)')

# ── missing space before opening quotation mark (U+2018) ─────────────────────
# e.g. "sizi'Kutsal" → "sizi 'Kutsal", "Çünkü'Taşlanırız" → "Çünkü 'Taşlanırız"
# Pattern: lowercase letter immediately followed by ' (U+2018) then uppercase.
# The replacement inserts a space between the word and the opening quote.
rule('([a-zğ\xfcşı\xf6\xe7ı])‘([A-ZĞ\xdcŞİ\xd6\xc7])',
     '\\1 ‘\\2',
     'missing space before opening ‘ quote (U+2018 + uppercase)')

# ── hamile (pregnant) ─────────────────────────────────────────────────────────
rule(r'\bhami\s+le\b',  'hamile',  'hamile (pregnant)')

# ── çocuk (child) — split at ço+cu prefix ─────────────────────────────────────
# Covers çocuğa, çocuğum, çocukları, çocuktur, etc. — "ço" is never standalone.
rule(r'\bço\s+cu', 'çocu', 'çocu- (child: çocuk/çocuğa/çocukları, initial split)')

# ── -dık/-dik/-duk verbal adjective suffix splits ─────────────────────────────
# The -DIK suffix (past verbal adjective) with case endings is always attached to
# its verb stem.  "düğün" (wedding) is deliberately excluded — it is a standalone
# noun and every düğün-form found in the data is a legitimate two-word phrase.
# 3-part splits first (dığın+da, etc.):
rule(r'(\w{2,})\s+dığın\s+da\b',    r'\1dığında',    '-dığında 3-part: stem+dığın+da')
rule(r'(\w{2,})\s+diğin\s+de\b',    r'\1diğinde',    '-diğinde 3-part: stem+diğin+de')
rule(r'(\w{2,})\s+duğun\s+da\b',    r'\1duğunda',    '-duğunda 3-part: stem+duğun+da')
# 2-part: locative (-dığında/-diğinde/-duğunda)
rule(r'(\w{2,})\s+(dığında|diğinde|duğunda)\b',          r'\1\2', '-dığında/-diğinde/-duğunda locative')
# 2-part: ablative (-dığından/-diğinden/-duğundan)
rule(r'(\w{2,})\s+(dığından|diğinden|duğundan)\b',        r'\1\2', '-dığından/-diğinden/-duğundan ablative')
# 2-part: accusative (-dığını/-diğini/-duğunu)
rule(r'(\w{2,})\s+(dığını|diğini|duğunu)\b',              r'\1\2', '-dığını/-diğini/-duğunu accusative')
# 2-part: dative (-dığına/-diğine/-duğuna)
rule(r'(\w{2,})\s+(dığına|diğine|duğuna)\b',              r'\1\2', '-dığına/-diğine/-duğuna dative')
# 2-part: 1st-person forms (-dığımda/-dığımı/-dığımız)
rule(r'(\w{2,})\s+(dığımda|diğimde|duğumda)\b',           r'\1\2', '-dığımda 1sg locative')
rule(r'(\w{2,})\s+(dığımı|diğimi|duğumu)\b',              r'\1\2', '-dığımı 1sg accusative')
rule(r'(\w{2,})\s+(dığımız|diğimiz|duğumuz)\b',           r'\1\2', '-dığımız 1pl nominative')
# 2-part: plural (-dıklarında/-diklerinde/-duklarında etc.)
rule(r'(\w{2,})\s+(dıklarında|diklerinde|duklarında)\b',  r'\1\2', '-dıklarında/-diklerinde plural locative')
rule(r'(\w{2,})\s+(dıklarından|diklerinden|duklarından)\b',r'\1\2','-dıklarından plural ablative')
rule(r'(\w{2,})\s+(dıklarını|diklerini|duklarını)\b',     r'\1\2', '-dıklarını/-diklerini plural accusative')

# ── verbal noun / infinitive suffix splits ────────────────────────────────────
# Turkish infinitives (-mek/-mak) and verbal nouns (-mesi/-ması and case forms)
# are always attached to their verb stem.  OCR detaches them as separate tokens.
#
# Multi-part splits (3+ tokens) must come before 2-part sub-patterns:
rule(r'\bko\s+r\s+u\s+mak\b',  'korumak',   'korumak (3+part: ko+r+u+mak)')
rule(r'\bde\s+ne\s+mesi\b',    'denenmesi',  'denenmesi (3-part: de+ne+mesi)')
#
# 2-part splits — specific 2-char stems (excluded by the {3,} general rule below):
rule(r'\bye\s+mek\b',   'yemek',   'yemek (food/to eat, 2-char stem)')
rule(r'\bEk\s+mek\b',  'Ekmek',   'Ekmek (bread, uppercase)')
rule(r'\bek\s+mek\b',  'ekmek',   'ekmek (bread, lowercase)')
rule(r'\bak\s+ması\b', 'akması',  'akması (its flowing, 2-char stem)')
rule(r'\bDe\s+mesi\b', 'Demesi',  'Demesi (its saying, uppercase)')
rule(r'\bde\s+mesi\b', 'demesi',  'demesi (its saying, lowercase)')
#
# 2-part splits — general rule (stem ≥ 3 chars, covers the vast majority):
# Longer suffix forms (containing mek/mak) must come before the basic mek/mak rule.
rule(r'(\w{3,})\s+(meden|madan)\b',          r'\1\2',  '-meden/-madan without-doing')
rule(r'(\w{3,})\s+(mekte|makta)\b',          r'\1\2',  '-mekte/-makta progressive')
rule(r'(\w{3,})\s+(mekten|maktan)\b',        r'\1\2',  '-mekten/-maktan ablative-inf')
rule(r'(\w{3,})\s+(mesi|ması)\b',            r'\1\2',  '-mesi/-ması verbal noun split')
rule(r'(\w{3,})\s+(mesini|masını)\b',        r'\1\2',  '-mesini/-masını accusative')
rule(r'(\w{3,})\s+(mesine|masına)\b',        r'\1\2',  '-mesine/-masına dative')
rule(r'(\w{3,})\s+(mesinde|masında)\b',      r'\1\2',  '-mesinde/-masında locative')
rule(r'(\w{3,})\s+(mesinden|masından)\b',    r'\1\2',  '-mesinden/-masından ablative')
rule(r'(\w{3,})\s+(mesiyle|masıyla)\b',      r'\1\2',  '-mesiyle/-masıyla instrumental')
rule(r'(\w{3,})\s+(mek|mak)\b',              r'\1\2',  '-mek/-mak infinitive split')
# Residual: 3-part splits leave "wordmek te" after the mek rule; catch the trailing te/ta.
rule(r'(\w{3,}mek)\s+te\b',                 r'\1te',  '-mekte (residual te after mek join)')
rule(r'(\w{3,}mak)\s+ta\b',                 r'\1ta',  '-makta (residual ta after mak join)')

# ── -lar/-ler plural suffix split across tokens ───────────────────────────────
# Turkish plural suffix (-lar/-ler) is always attached to its host word. OCR
# often breaks the suffix continuation off as a separate token.
# Longest (3-part) splits first so they match before their 2-part sub-patterns.
rule(r'(\w+la)\s+rın\s+dan\b',  r'\1rından',  '-larından 3-part: la+rın+dan')
rule(r'(\w+la)\s+rin\s+den\b',  r'\1rinden',  '-lerinden 3-part: la+rin+den')
# 2-part ablative (-larından / -lerinden / -lardan / -lerden)
rule(r'(\w+la)\s+rından\b',     r'\1rından',  '-larından 2-part: la+rından')
rule(r'(\w+la)\s+rinden\b',     r'\1rinden',  '-lerinden 2-part: la+rinden')
rule(r'(\w+lar)\s+ından\b',     r'\1ından',   '-larından 2-part: lar+ından')
rule(r'(\w+lar)\s+inden\b',     r'\1inden',   '-lerinden 2-part: lar+inden')
rule(r'(\w+ların)\s+dan\b',     r'\1dan',     '-larından 2-part: ların+dan')
rule(r'(\w+lerin)\s+den\b',     r'\1den',     '-lerinden 2-part: lerin+den')
rule(r'(\w+lar)\s+dan\b',       r'\1dan',     '-lardan 2-part: lar+dan')
rule(r'(\w+ler)\s+den\b',       r'\1den',     '-lerden 2-part: ler+den')
# 2-part dative (-larına / -lerine)
rule(r'(\w+la)\s+rına\b',       r'\1rına',    '-larına 2-part: la+rına')
rule(r'(\w+la)\s+rine\b',       r'\1rine',    '-lerine 2-part: la+rine')
rule(r'(\w+lar)\s+ına\b',       r'\1ına',     '-larına 2-part: lar+ına')
rule(r'(\w+ler)\s+ine\b',       r'\1ine',     '-lerine 2-part: ler+ine')
# 2-part locative (-larında / -lerinde)
rule(r'(\w+la)\s+rında\b',      r'\1rında',   '-larında 2-part: la+rında')
rule(r'(\w+la)\s+rinde\b',      r'\1rinde',   '-lerinde 2-part: la+rinde')
rule(r'(\w+lar)\s+ında\b',      r'\1ında',    '-larında 2-part: lar+ında')
rule(r'(\w+ler)\s+inde\b',      r'\1inde',    '-lerinde 2-part: ler+inde')
# 2-part accusative (-larını / -lerini)
rule(r'(\w+la)\s+rını\b',       r'\1rını',    '-larını 2-part: la+rını')
rule(r'(\w+la)\s+rini\b',       r'\1rini',    '-lerini 2-part: la+rini')
rule(r'(\w+lar)\s+ını\b',       r'\1ını',     '-larını 2-part: lar+ını')
rule(r'(\w+ler)\s+ini\b',       r'\1ini',     '-lerini 2-part: ler+ini')
# 2-part instrumental (-larıyla / -leriyle)
rule(r'(\w+la)\s+rıyla\b',      r'\1rıyla',   '-larıyla 2-part: la+rıyla')
rule(r'(\w+la)\s+riyle\b',      r'\1riyle',   '-leriyle 2-part: la+riyle')
rule(r'(\w+lar)\s+ıyla\b',      r'\1ıyla',    '-larıyla 2-part: lar+ıyla')
rule(r'(\w+ler)\s+iyle\b',      r'\1iyle',    '-leriyle 2-part: ler+iyle')

# ── space inserted before U+2019 apostrophe suffix after proper noun ──────────
# e.g. "Rabb 'ten" → "Rabb'ten", "İsa 'yı" → "İsa'yı"
# Must NOT affect "bu 'hoş haber'" (legitimate space before opening quote).
# Guard: only match words starting with uppercase — proper nouns take suffixes via apostrophe.
# "Rab b" split must be resolved first so the general rule can catch "Rabb 'ten".
rule(r'\bRab\s+b',  'Rabb',  'Rabb (Rab+b OCR split, all suffix forms)')
rule('([A-ZĞÜŞİÖÇ][a-zğüşıöçı]+)\\s+’([a-zğüşıöçı])',
     '\\1’\\2',
     'proper noun + spurious space before U+2019 suffix (Isa yi fix)')

# ── initial-letter detached from rest of word (OCR first-char split) ──────────
# Multi-character / 3-token splits must come before their 2-token sub-patterns.
rule(r'\bB\s+un\s+dan\b',     'Bundan',   'Bundan (B+un+dan, from this, 3-part split)')
rule(r'\bK\s+a\s+sap',       'Kasap',    'Kasap (K+a+sap, butcher, 3-part split)')
rule(r'\bA\s+ni\s+den\b',    'Aniden',   'Aniden (A+ni+den, suddenly, 3-part)')
rule(r'\bA\s+nid\s+en\b',    'Aniden',   'Aniden (A+nid+en, suddenly, 3-part)')
rule(r'\bE\s+f\s+e\s+n\s+dim\b', 'Efendim', 'Efendim (E+f+e+n+dim, my Lord, 5-part)')
rule(r'\bÜ\s+çün\s+cü',      'Üçüncü',   'Üçüncü (Ü+çün+cü, third, all inflections)')
# 2-part splits:
rule(r'\bB\s+öylece\b',      'Böylece',  'Böylece (B+öylece, thus, initial-letter split)')
rule(r'\bB\s+aba',           'Baba',     'Baba (B+aba, father, all forms)')
rule(r'\bA\s+yak',           'Ayak',     'Ayak (A+yak, foot, all inflections)')
rule(r'\bA\s+dam',           'Adam',     'Adam (A+dam, man/Adam, all inflections)')
rule(r'\bA\s+niden\b',       'Aniden',   'Aniden (A+niden, suddenly, 2-part)')
rule(r'\bA\s+teş',           'Ateş',     'Ateş (A+teş, fire, all inflections)')
rule(r'\bA\s+hit\b',         'Ahit',     'Ahit (A+hit, covenant)')
rule(r'\bA\s+dıyla\b',       'Adıyla',   'Adıyla (A+dıyla, with his/her name)')
rule(r'\bİ\s+nanç',          'İnanç',    'İnanç (İ+nanç, faith, all inflections)')

# ── verb-specific splits ───────────────────────────────────────────────────────
rule(r'\bsavu\s+rac',     'savurac',  'savuracak (will winnow)')
rule(r'\bsama\s+n',       'saman',    'saman (straw/chaff, all inflections)')
rule(r'\bvaz\s+geçir',    'vazgeçir', 'vazgeçir (give up/desist, all inflections)')
rule(r'\bvaz\s+geç\b',    'vazgeç',   'vazgeç (give up/desist)')
rule(r'\bine\s+rek\b',    'inerek',   'inerek (by descending/coming down)')


# ── main ───────────────────────────────────────────────────────────────────────

def fix_text(text: str) -> str:
    for pattern, replacement, _ in RULES:
        text = pattern.sub(replacement, text)
    return text


def main():
    total_verses  = 0
    total_changed = 0
    change_log: list = []

    for book_dir in sorted(HKTN_DIR.iterdir()):
        if not book_dir.is_dir():
            continue
        book = book_dir.name

        for ch_file in sorted(book_dir.glob('*.json'), key=lambda p: int(p.stem)):
            chapter = int(ch_file.stem)
            data    = json.loads(ch_file.read_text(encoding='utf-8'))
            content = data.get('content', [])

            changed = False
            for item in content:
                if 'text' not in item:
                    continue
                original = item['text']
                total_verses += 1
                fixed = fix_text(original)

                if fixed != original:
                    item['text'] = fixed
                    changed       = True
                    total_changed += 1
                    change_log.append(
                        f'{book} {chapter}:{item.get("v", "?")}  '
                        f'BEFORE: {original}\n'
                        f'  AFTER:  {fixed}'
                    )

            if changed and not DRY_RUN:
                ch_file.write_text(
                    json.dumps(data, ensure_ascii=False, indent=2),
                    encoding='utf-8'
                )

    print(f'{"[DRY RUN] " if DRY_RUN else ""}Fixed {total_changed} / {total_verses} verses.\n')
    for entry in change_log:
        print(entry)


if __name__ == '__main__':
    main()
