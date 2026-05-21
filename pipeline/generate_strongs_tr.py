#!/usr/bin/env python3
"""
Generate Turkish Strong's lexicon definitions using Claude claude-haiku-4-5-20251001.

Reads data/strongs/hebrew.json and data/strongs/greek.json,
translates each entry to Turkish, and saves:
  data/strongs/heb_tr.json   (H1–H8674)
  data/strongs/grk_tr.json   (G1–G5624)

Output format per entry: {"id": N, "t": "Turkish title/summary", "d": "Turkish definition", "w": "Turkish word forms"}

Resumes automatically from any partial output file.

Usage:
    python3 pipeline/generate_strongs_tr.py
    python3 pipeline/generate_strongs_tr.py --lang heb   # Hebrew only
    python3 pipeline/generate_strongs_tr.py --lang grk   # Greek only
"""
import json, sys, time, re
from pathlib import Path
import anthropic

ROOT     = Path(__file__).parent.parent
DATA_DIR = ROOT / 'data/strongs'
BATCH    = 20          # entries per API call
MODEL    = 'claude-haiku-4-5-20251001'
MAX_RETRY = 3

client = anthropic.Anthropic()

SYSTEM = (
    "Sen İbranice ve Yunanca İncil sözcüklerini Türkçeye çeviren bir İlahiyat uzmanısın. "
    "Strong's Concordance sözlüğündeki her sözcük için kısa ve öz Türkçe açıklamalar üret. "
    "Cevabını YALNIZCA geçerli JSON olarak ver, başka açıklama ekleme."
)

USER_TEMPLATE = """\
Aşağıdaki {lang} Strong's sözcükleri için Türkçe tanımlar üret.
Her sözcük için şu alanları içeren bir JSON nesnesi oluştur:
- "id": sözcüğün ID numarası (değiştirme)
- "t": Türkçe başlık/özet (5-12 kelime, sözcüğün ana anlamı)
- "d": Türkçe tanım (1-3 cümle, teolojik bağlamı da içer)
- "w": Türkçe kullanım örnekleri / KJV karşılıkları (virgülle ayrılmış liste)

Girdi sözcükleri:
{entries}

Yanıtı şu formatta ver:
[{{"id":N,"t":"...","d":"...","w":"..."}}, ...]
"""


def build_entry_text(e: dict, lang: str) -> str:
    parts = [f'ID: {e["id"]}']
    if e.get('title'):   parts.append(f'Başlık: {e["title"]}')
    if e.get('word'):    parts.append(f'Sözcük: {e["word"]}')
    if e.get('translit'):parts.append(f'Transliterasyon: {e["translit"]}')
    if e.get('short_def'):parts.append(f'Kısa tanım: {e["short_def"]}')
    if e.get('nas_def'): parts.append(f'NAS tanım: {e["nas_def"]}')
    if e.get('strongs_def'):parts.append(f'Strong\'s: {e["strongs_def"][:200]}')
    if e.get('kjv'):     parts.append(f'KJV: {e["kjv"][:150]}')
    if e.get('nasb'):    parts.append(f'NASB: {e["nasb"][:100]}')
    if e.get('pos'):     parts.append(f'Tür: {e["pos"]}')
    return ' | '.join(parts)


def call_claude(batch: list, lang_label: str) -> list | None:
    entries_text = '\n'.join(build_entry_text(e, lang_label) for e in batch)
    prompt = USER_TEMPLATE.format(lang=lang_label, entries=entries_text)

    for attempt in range(MAX_RETRY):
        try:
            msg = client.messages.create(
                model=MODEL,
                max_tokens=4096,
                system=SYSTEM,
                messages=[{'role': 'user', 'content': prompt}]
            )
            raw = msg.content[0].text.strip()
            # Extract JSON array if wrapped in markdown
            m = re.search(r'\[.*\]', raw, re.DOTALL)
            if m:
                raw = m.group(0)
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                return parsed
        except json.JSONDecodeError as e:
            print(f'  JSON parse error (attempt {attempt+1}): {e}')
        except Exception as e:
            print(f'  API error (attempt {attempt+1}): {e}')
            if attempt < MAX_RETRY - 1:
                time.sleep(2 ** attempt)
    return None


def process(in_file: Path, out_file: Path, lang_label: str):
    print(f'\n=== {lang_label} — {in_file.name} ===')
    if not in_file.exists():
        print(f'  Input not found: {in_file}'); return

    source = json.loads(in_file.read_text('utf-8'))
    # Remove error entries
    source = [e for e in source if 'error' not in e]
    print(f'  Source entries: {len(source)}')

    # Load existing output
    existing = {}
    if out_file.exists():
        try:
            existing = {e['id']: e for e in json.loads(out_file.read_text('utf-8'))}
            print(f'  Resuming: {len(existing)} already translated')
        except Exception:
            pass

    to_do = [e for e in source if e['id'] not in existing]
    print(f'  Remaining: {len(to_do)}')

    results = dict(existing)
    total   = len(source)
    done    = len(existing)

    for i in range(0, len(to_do), BATCH):
        batch = to_do[i:i + BATCH]
        translated = call_claude(batch, lang_label)
        if translated is None:
            print(f'  SKIP batch starting at index {i} (all retries failed)')
            continue

        # Map by id
        tr_map = {t['id']: t for t in translated if isinstance(t, dict) and 'id' in t}
        for e in batch:
            if e['id'] in tr_map:
                results[e['id']] = tr_map[e['id']]
            else:
                # Fallback: keep source id with empty fields
                results[e['id']] = {'id': e['id'], 't': '', 'd': '', 'w': ''}
        done += len(batch)

        pct = done / total * 100
        print(f'  {done}/{total} ({pct:.1f}%)')

        # Save every 200 entries
        if done % 200 < BATCH:
            _save(results, out_file)

        time.sleep(0.3)  # gentle rate limiting

    _save(results, out_file)
    print(f'  Done. {len(results)} entries → {out_file}')


def _save(results: dict, out_file: Path):
    lst = [results[k] for k in sorted(results)]
    out_file.write_text(json.dumps(lst, ensure_ascii=False, separators=(',', ':')), 'utf-8')
    print(f'    Saved {len(lst)} entries.')


def main():
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    lang_arg = None
    if '--lang' in sys.argv:
        idx = sys.argv.index('--lang')
        lang_arg = sys.argv[idx + 1] if idx + 1 < len(sys.argv) else None

    if lang_arg in (None, 'heb'):
        process(DATA_DIR / 'hebrew.json', DATA_DIR / 'heb_tr.json', 'İbranice')

    if lang_arg in (None, 'grk'):
        process(DATA_DIR / 'greek.json',  DATA_DIR / 'grk_tr.json', 'Yunanca')


if __name__ == '__main__':
    main()
