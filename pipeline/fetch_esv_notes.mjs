/**
 * fetch_esv_notes.mjs — Scrapes ESV Global Study Bible footnotes from BLB
 *
 * URL pattern: https://www.blueletterbible.org/esv-study-bible/notes/{blb_book}/chapter-{n}
 * Extracts: verse refs, bold scripture phrases (**...**), passage outlines
 *
 * Usage:
 *   node pipeline/fetch_esv_notes.mjs --worker 0 --total 10   # worker 0 of 10
 *   node pipeline/fetch_esv_notes.mjs --books MRK,PHP,1JN     # specific books
 *   node pipeline/fetch_esv_notes.mjs --book MAT --resume      # single book, skip existing
 */

import puppeteer from 'puppeteer';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { createRequire } from 'module';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const require   = createRequire(import.meta.url);
const PROJECT   = path.resolve(__dirname, '..');
const OUT_DIR   = path.join(PROJECT, 'data', 'commentary-en');

// USFM code → BLB ESV study notes URL slug
const BLB_BOOK = {
  GEN:'gen', EXO:'exo', LEV:'lev', NUM:'num',  DEU:'deu', JOS:'jos', JDG:'jdg', RUT:'rut',
  '1SA':'1sa','2SA':'2sa','1KI':'1ki','2KI':'2ki','1CH':'1ch','2CH':'2ch',
  EZR:'ezr', NEH:'neh', EST:'est', JOB:'job', PSA:'psa', PRO:'pro', ECC:'ecc', SNG:'sng',
  ISA:'isa', JER:'jer', LAM:'lam', EZK:'eze', DAN:'dan', HOS:'hos', JOL:'joe',
  AMO:'amo', OBA:'oba', JON:'jon', MIC:'mic', NAH:'nah', HAB:'hab', ZEP:'zep',
  HAG:'hag', ZEC:'zec', MAL:'mal',
  MAT:'mat', MRK:'mar', LUK:'luk', JHN:'jhn', ACT:'act',
  ROM:'rom','1CO':'1co','2CO':'2co', GAL:'gal', EPH:'eph', PHP:'phl', COL:'col',
  '1TH':'1th','2TH':'2th','1TI':'1ti','2TI':'2ti', TIT:'tit', PHM:'phm', HEB:'heb',
  JAS:'jas','1PE':'1pe','2PE':'2pe','1JN':'1jo','2JN':'2jo','3JN':'3jo', JUD:'jud', REV:'rev',
};

const CHAPTERS = {
  GEN:50,EXO:40,LEV:27,NUM:36,DEU:34,JOS:24,JDG:21,RUT:4,
  '1SA':31,'2SA':24,'1KI':22,'2KI':25,'1CH':29,'2CH':36,
  EZR:10,NEH:13,EST:10,JOB:42,PSA:150,PRO:31,ECC:12,SNG:8,
  ISA:66,JER:52,LAM:5,EZK:48,DAN:12,HOS:14,JOL:3,AMO:9,
  OBA:1,JON:4,MIC:7,NAH:3,HAB:3,ZEP:3,HAG:2,ZEC:14,MAL:4,
  MAT:28,MRK:16,LUK:24,JHN:21,ACT:28,ROM:16,'1CO':16,'2CO':13,
  GAL:6,EPH:6,PHP:4,COL:4,'1TH':5,'2TH':3,'1TI':6,'2TI':4,
  TIT:3,PHM:1,HEB:13,JAS:5,'1PE':5,'2PE':3,'1JN':5,'2JN':1,
  '3JN':1,JUD:1,REV:22,
};

const BOOK_ORDER = [
  'GEN','EXO','LEV','NUM','DEU','JOS','JDG','RUT','1SA','2SA',
  '1KI','2KI','1CH','2CH','EZR','NEH','EST','JOB','PSA','PRO',
  'ECC','SNG','ISA','JER','LAM','EZK','DAN','HOS','JOL','AMO',
  'OBA','JON','MIC','NAH','HAB','ZEP','HAG','ZEC','MAL',
  'MAT','MRK','LUK','JHN','ACT','ROM','1CO','2CO','GAL','EPH',
  'PHP','COL','1TH','2TH','1TI','2TI','TIT','PHM','HEB','JAS',
  '1PE','2PE','1JN','2JN','3JN','JUD','REV',
];

// ── CLI arg parsing ─────────────────────────────────────────────────────────
const args = process.argv.slice(2);
const getArg = name => { const i = args.indexOf(`--${name}`); return i >= 0 ? args[i+1] : null; };
const hasFlag = name => args.includes(`--${name}`);

let booksToProcess;
const specificBooks = getArg('books') || getArg('book');
if (specificBooks) {
  booksToProcess = specificBooks.split(',').map(b => b.trim().toUpperCase());
} else {
  const workerIdx   = parseInt(getArg('worker')  ?? '0',  10);
  const totalWorkers = parseInt(getArg('total')  ?? '1',  10);
  booksToProcess = BOOK_ORDER.filter((_, i) => i % totalWorkers === workerIdx);
}

const RESUME = hasFlag('resume');
const FORCE  = hasFlag('force'); // re-scrape even chapters that already have ** markers

// ── Note extraction logic (runs inside page.evaluate) ──────────────────────
function extractNotes() {
  const container = document.querySelector('#notesData');
  if (!container) return { passages: [], notes: [] };

  const paragraphs = Array.from(container.querySelectorAll('p'));
  const passages = [];
  const notes    = [];

  for (const p of paragraphs) {
    // 1. Extract verse ref
    const verseEl = p.querySelector('strong.verse');
    if (!verseEl) continue;
    const ref = verseEl.textContent?.trim();
    if (!ref) continue;

    // 2. Clone p, remove the verse strong to get the rest
    const clone = p.cloneNode(true);
    clone.querySelector('strong.verse')?.remove();

    // 3. Check for outline (passage/section header)
    const outlineEl = clone.querySelector('span.outline-1, span.outline-2');
    const outlineTitle = outlineEl?.textContent?.trim();
    outlineEl?.remove();

    // 4. Convert <strong class="scripture"> → **text**
    clone.querySelectorAll('strong.scripture').forEach(s => {
      s.replaceWith(`**${s.textContent}**`);
    });

    // 5. Strip remaining tags, decode entities, normalize whitespace
    const rawText = clone.innerHTML
      .replace(/<[^>]+>/g, ' ')
      .replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&nbsp;/g, ' ')
      .replace(/&#(\d+);/g, (_, n) => String.fromCharCode(parseInt(n)))
      .replace(/\s+/g, ' ').trim();

    // 6. Classify: ONLY true section headers (outline-1/2 span) go to passages.
    //    Range refs like "Matt. 18:2–4" are still verse notes — they go to notes.
    if (outlineTitle) {
      passages.push({ ref, title: outlineTitle, text: rawText });
    } else {
      if (rawText) notes.push({ ref, text: rawText });
    }
  }

  return { passages, notes };
}

// ── Main scrape function ─────────────────────────────────────────────────────
async function scrapeChapter(page, bookCode, chapter) {
  const blbName = BLB_BOOK[bookCode];
  if (!blbName) { console.log(`    ⚠ No BLB mapping for ${bookCode}`); return null; }

  const url = `https://www.blueletterbible.org/esv-study-bible/notes/${blbName}/chapter-${chapter}`;
  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      await page.goto(url, { waitUntil: 'networkidle2', timeout: 30000 });
      break;
    } catch (e) {
      if (attempt === 3) {
        process.stdout.write(`✗timeout `);
        return null;
      }
      await new Promise(r => setTimeout(r, 3000 * attempt)); // back-off on retry
    }
  }

  // Check for 404
  const is404 = await page.evaluate(() =>
    document.body?.innerText?.includes('404 Page') || document.title?.includes('404')
  );
  if (is404) {
    console.log(`    ✗ 404: ${url}`);
    return null;
  }

  const result = await page.evaluate(extractNotes);

  if (!result || (result.passages.length === 0 && result.notes.length === 0)) {
    console.log(`    ⚠ no notes found: ${url}`);
    return { passages: [], notes: [] };
  }

  return result;
}

// ── Main ─────────────────────────────────────────────────────────────────────
const browser = await puppeteer.launch({
  headless: true,
  executablePath: require('puppeteer').executablePath(),
  args: ['--no-sandbox', '--disable-setuid-sandbox'],
});
const page = await browser.newPage();
await page.setUserAgent('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36');

let ok = 0, skipped = 0, failed = 0;

for (const bookCode of booksToProcess) {
  const total = CHAPTERS[bookCode];
  if (!total) { console.log(`Unknown book: ${bookCode}`); continue; }

  console.log(`\n── ${bookCode} (${total} chapters) ──`);

  for (let ch = 1; ch <= total; ch++) {
    const outFile = path.join(OUT_DIR, bookCode, `${ch}.json`);

    if (!FORCE && fs.existsSync(outFile)) {
      const existing = JSON.parse(fs.readFileSync(outFile, 'utf8'));
      // Skip only if already has ** bold markers (fully updated)
      const alreadyBold = (existing.notes || []).some(n => n.text?.includes('**'));
      if (alreadyBold) {
        process.stdout.write(`  [${ch}] skip `);
        skipped++;
        continue;
      }
    }

    process.stdout.write(`  [${ch}] `);
    const result = await scrapeChapter(page, bookCode, ch);
    await new Promise(r => setTimeout(r, 1200)); // polite delay — avoid rate limiting

    if (!result) { failed++; continue; }

    // Merge with existing file (preserve book_intro, source)
    let existing = {};
    if (fs.existsSync(outFile)) {
      try { existing = JSON.parse(fs.readFileSync(outFile, 'utf8')); } catch (_) {}
    }

    const merged = {
      source: existing.source || 'ESV Global Study Bible (Crossway) — Used under license',
      ...(existing.book_intro   ? { book_intro:   existing.book_intro   } : {}),
      ...(existing.chapter_intro? { chapter_intro: existing.chapter_intro} : {}),
      passages: result.passages,
      notes:    result.notes,
    };

    // Derive chapter_intro from first passage if missing
    if (!merged.chapter_intro && result.passages.length > 0) {
      merged.chapter_intro = result.passages[0].title;
    }

    fs.mkdirSync(path.dirname(outFile), { recursive: true });
    fs.writeFileSync(outFile, JSON.stringify(merged, null, 2), 'utf8');

    const flag = result.notes.length === 0 && result.passages.length === 0 ? '(empty)' : `✓ ${result.notes.length}n ${result.passages.length}p`;
    process.stdout.write(`${flag}  `);
    ok++;
  }
  console.log();
}

await browser.close();
console.log(`\n─────────────────────────────────────────────`);
console.log(`Done: ${ok} saved, ${skipped} skipped, ${failed} failed`);
