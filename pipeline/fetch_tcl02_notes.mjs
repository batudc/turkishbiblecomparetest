/**
 * fetch_tcl02_notes.mjs — Re-scrape TCL02 footnotes from bible.com
 *
 * URL: https://www.bible.com/bible/170/BOOK.CH.TCL02
 * Extracts footnotes from verse-embedded note spans.
 *
 * Usage:
 *   node pipeline/fetch_tcl02_notes.mjs                    # all books
 *   node pipeline/fetch_tcl02_notes.mjs --book MAT         # single book
 *   node pipeline/fetch_tcl02_notes.mjs --worker 0 --total 4  # distributed
 *   node pipeline/fetch_tcl02_notes.mjs --resume            # skip completed chapters
 */

import puppeteer from 'puppeteer';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PROJECT   = path.resolve(__dirname, '..');
const OUT_DIR   = path.join(PROJECT, 'data', 'notes');

const BOOK_CHAPTERS = {
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

// bible.com uses the USFM code directly for most books
// Some need mapping:
const BCOM_BOOK = {
  '1SA':'1SA','2SA':'2SA','1KI':'1KI','2KI':'2KI',
  '1CH':'1CH','2CH':'2CH','1CO':'1CO','2CO':'2CO',
  '1TH':'1TH','2TH':'2TH','1TI':'1TI','2TI':'2TI',
  '1PE':'1PE','2PE':'2PE','1JN':'1JN','2JN':'2JN','3JN':'3JN',
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

// ── CLI ─────────────────────────────────────────────────────────────────────
const args = process.argv.slice(2);
const getArg = name => {
  const i = args.indexOf(`--${name}`);
  return i >= 0 ? args[i + 1] : null;
};
const RESUME  = args.includes('--resume');
const specificBook = getArg('book')?.toUpperCase();
const workerIdx = parseInt(getArg('worker') ?? '0');
const workerTotal = parseInt(getArg('total') ?? '1');

let booksToProcess = specificBook
  ? [specificBook]
  : BOOK_ORDER.filter((_, i) => i % workerTotal === workerIdx);

// ── Puppeteer setup ──────────────────────────────────────────────────────────
const browser = await puppeteer.launch({
  headless: true,
  args: ['--no-sandbox', '--disable-setuid-sandbox'],
});
const page = await browser.newPage();
await page.setUserAgent(
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
);
await page.setDefaultNavigationTimeout(45000);

// ── Extraction logic ─────────────────────────────────────────────────────────
async function extractChapterNotes(book, ch) {
  const bcomBook = BCOM_BOOK[book] || book;
  const url = `https://www.bible.com/bible/170/${bcomBook}.${ch}.TCL02`;

  try {
    await page.goto(url, { waitUntil: 'networkidle2' });
  } catch (e) {
    console.error(`  [${ch}] nav error: ${e.message.substring(0, 60)}`);
    return {};
  }

  const notes = await page.evaluate(() => {
    const result = {};

    // Find all verse spans with data-usfm
    const verseSpans = document.querySelectorAll('[data-usfm]');
    for (const vs of verseSpans) {
      const usfm = vs.dataset.usfm || '';
      // Only process verse-level elements (contain exactly two dots like MAT.14.24)
      // Skip chapter-level (MAT.14) and ref spans
      const parts = usfm.split('.');
      if (parts.length < 3) continue;
      if (vs.tagName !== 'SPAN') continue; // verses are spans

      // Get first verse number from usfm like "MAT.14.24" or "MAT.14.1+MAT.14.2"
      const firstVersePart = parts[2].split('+')[0]; // "24" or "1"
      const verseNum = parseInt(firstVersePart, 10);
      if (isNaN(verseNum)) continue;

      // Find note elements within this verse span
      const noteEls = vs.querySelectorAll('[class*="__note"]');
      for (const noteEl of noteEls) {
        // Get the body span (not the label span)
        const bodyEl = noteEl.querySelector('[class*="__body"]');
        if (!bodyEl) continue;

        // Get text, stripping the __fr verse reference prefix
        const frEl = bodyEl.querySelector('[class*="__fr"]');
        let text = bodyEl.textContent.trim();

        // Strip leading verse ref like "14:24 " from footnote type
        if (frEl) {
          const frText = frEl.textContent.trim();
          if (text.startsWith(frText)) {
            text = text.slice(frText.length).trim();
          }
        }

        // Clean up whitespace
        text = text.replace(/\s+/g, ' ').trim();
        if (!text) continue;

        // Store; if verse already has a note, combine
        if (result[verseNum]) {
          result[verseNum] += ' — ' + text;
        } else {
          result[verseNum] = text;
        }
      }
    }
    return result;
  });

  return notes;
}

// ── Main loop ────────────────────────────────────────────────────────────────
let totalChapters = 0;
let totalNotes = 0;

for (const book of booksToProcess) {
  const maxCh = BOOK_CHAPTERS[book];
  if (!maxCh) { console.log(`Unknown book: ${book}`); continue; }

  const outPath = path.join(OUT_DIR, `TCL02_${book}.json`);

  // Load existing data (for resume mode or partial updates)
  let existing = {};
  if (fs.existsSync(outPath)) {
    try { existing = JSON.parse(fs.readFileSync(outPath, 'utf8')); } catch {}
  }

  const bookData = {};
  let bookNotes = 0;

  process.stdout.write(`\n── ${book} (${maxCh}ch) ──\n`);

  for (let ch = 1; ch <= maxCh; ch++) {
    // In resume mode, skip chapters we already have
    if (RESUME && existing[ch] && Object.keys(existing[ch]).length > 0) {
      process.stdout.write(` [${ch}]skip`);
      bookData[ch] = existing[ch];
      continue;
    }

    const notes = await extractChapterNotes(book, ch);
    bookData[ch] = notes;
    const count = Object.keys(notes).length;
    bookNotes += count;
    totalNotes += count;
    process.stdout.write(` [${ch}]${count > 0 ? count + 'n' : '·'}`);

    // Small delay to be polite
    await new Promise(r => setTimeout(r, 800));
  }

  // Save book data (only chapters with at least one note)
  const toSave = {};
  for (const [ch, notes] of Object.entries(bookData)) {
    if (Object.keys(notes).length > 0) toSave[ch] = notes;
  }
  fs.writeFileSync(outPath, JSON.stringify(toSave, null, 2), 'utf8');
  process.stdout.write(`\n  → saved ${bookNotes} notes\n`);
  totalChapters += maxCh;
}

await browser.close();
console.log(`\n─── Done: ${totalChapters} chapters, ${totalNotes} notes scraped ───`);
