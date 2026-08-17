/**
 * fetch_ytc.mjs — Scrapes YTC translation + footnotes from ebible.org/turytc
 *
 * Outputs:
 *   data/translations/YTC/{BOOK}/{CHAP}.json   (verse text — replaces existing)
 *   data/notes/YTC_{BOOK}.json                  (footnotes per verse, same format as TCL02)
 *
 * Usage:
 *   node pipeline/fetch_ytc.mjs                       # all 66 books
 *   node pipeline/fetch_ytc.mjs --books MAT,MRK,JHN   # specific books
 *   node pipeline/fetch_ytc.mjs --book PSA             # single book
 *   node pipeline/fetch_ytc.mjs --resume               # skip chapters whose JSON already exists
 */

import { writeFileSync, mkdirSync, existsSync, readFileSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const PROJECT   = join(__dirname, '..');
const TRANS_DIR = join(PROJECT, 'data', 'translations', 'YTC');
const NOTES_DIR = join(PROJECT, 'data', 'notes');

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

// Some books use different URL codes or chapter padding on ebible.org
const URL_BOOK_CODE = { NAH: 'NAM' };
const URL_CHAP_WIDTH = { PSA: 3 }; // PSA uses 3-digit chapter numbers

// ── HTML parsing helpers ─────────────────────────────────────────────────────

function decodeEntities(s) {
  return s
    .replace(/&#160;/g, ' ')
    .replace(/&nbsp;/g, ' ')
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&#(\d+);/g, (_, n) => String.fromCharCode(+n))
    .replace(/&#x([0-9a-fA-F]+);/g, (_, h) => String.fromCharCode(parseInt(h, 16)));
}

function stripTags(s) {
  return s.replace(/<[^>]+>/g, '');
}

function parseChapter(html) {
  // ── 1. Parse footnotes from <div class="footnote"> ──
  const footnotes = {}; // verse_num_str → note_text
  const fnDivMatch = html.match(/<div class="footnote">([\s\S]*?)<\/div>/);
  if (fnDivMatch) {
    const fnHtml = fnDivMatch[1];
    const pRe = /<p class="([fx])"[^>]*>([\s\S]*?)<\/p>/g;
    let pm;
    while ((pm = pRe.exec(fnHtml)) !== null) {
      const para = pm[2];
      const vMatch = para.match(/href="#V(\d+)"/);
      if (!vMatch) continue;
      const vNum = vMatch[1];

      let text = '';
      const ftM = para.match(/<span class="ft">([\s\S]*?)<\/span>/);
      const xtM = para.match(/<span class="xt">([\s\S]*?)<\/span>/);
      if (ftM) text = decodeEntities(stripTags(ftM[1])).trim();
      else if (xtM) text = decodeEntities(stripTags(xtM[1])).trim();

      if (!text) continue;
      footnotes[vNum] = footnotes[vNum] ? `${footnotes[vNum]} — ${text}` : text;
    }
  }

  // ── 2. Extract verse texts ──
  // Remove inline notemark anchors (superscript + popup)
  let work = html.replace(/<a[^>]*class="notemark"[^>]*>[\s\S]*?<\/a>/g, '');

  // Cut off at the second <ul class='tnav'> (the bottom nav after last verse)
  const navHits = [...work.matchAll(/<ul class='tnav'>/g)];
  if (navHits.length >= 2) work = work.slice(0, navHits[1].index);

  // Split by verse span markers — capture group keeps the verse numbers
  // Result: [pre, "1", text1, "2", text2, ...]
  const parts = work.split(/<span class="verse" id="V(\d+)">[^<]*<\/span>/);

  const content = [];
  for (let i = 1; i + 1 < parts.length; i += 2) {
    const v = parseInt(parts[i]);
    if (v === 0) continue; // V0 is chapter label, not a verse
    const raw = parts[i + 1] || '';
    const text = decodeEntities(stripTags(raw)).replace(/\s+/g, ' ').trim();
    if (text) content.push({ v, text });
  }

  return { content, footnotes };
}

// ── Fetch with retry ─────────────────────────────────────────────────────────

async function fetchHtml(url, retries = 3) {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const res = await fetch(url, {
        headers: { 'User-Agent': 'Mozilla/5.0 (compatible; YTC-pipeline/1.0)' },
      });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      return await res.text();
    } catch (err) {
      if (attempt === retries) throw err;
      await new Promise(r => setTimeout(r, 1000 * attempt));
    }
  }
}

// ── CLI args ─────────────────────────────────────────────────────────────────

const args = process.argv.slice(2);
const resume   = args.includes('--resume');
const bookIdx  = args.indexOf('--book');
const booksIdx = args.indexOf('--books');

let targetBooks;
if (bookIdx !== -1)  targetBooks = [args[bookIdx + 1].toUpperCase()];
else if (booksIdx !== -1) targetBooks = args[booksIdx + 1].split(',').map(s => s.trim().toUpperCase());
else targetBooks = BOOK_ORDER;

// ── Main ─────────────────────────────────────────────────────────────────────

let totalChapters = 0, totalVerses = 0, totalNotes = 0, errors = 0;

for (const book of targetBooks) {
  const chapCount = CHAPTERS[book];
  if (!chapCount) { console.error(`Unknown book: ${book}`); continue; }

  mkdirSync(join(TRANS_DIR, book), { recursive: true });

  // Load existing notes file so we can merge chapter-by-chapter
  const notesPath = join(NOTES_DIR, `YTC_${book}.json`);
  let bookNotes = {};
  if (existsSync(notesPath)) {
    try { bookNotes = JSON.parse(readFileSync(notesPath, 'utf8')); } catch {}
  }

  console.log(`\n── ${book} (${chapCount} chapters) ──`);

  for (let chap = 1; chap <= chapCount; chap++) {
    const outPath = join(TRANS_DIR, book, `${chap}.json`);
    if (resume && existsSync(outPath)) {
      process.stdout.write('.');
      continue;
    }

    const chapWidth = URL_CHAP_WIDTH[book] || 2;
    const chapStr = String(chap).padStart(chapWidth, '0');
    const urlBook = URL_BOOK_CODE[book] || book;
    const url = `https://ebible.org/turytc/${urlBook}${chapStr}.htm`;

    try {
      const html = await fetchHtml(url);
      const { content, footnotes } = parseChapter(html);

      if (!content.length) throw new Error('No verses parsed');

      // Write verse JSON
      const verseJson = { t: 'YTC', b: book, c: chap, content };
      writeFileSync(outPath, JSON.stringify(verseJson));

      // Merge footnotes into bookNotes
      if (Object.keys(footnotes).length > 0) {
        bookNotes[String(chap)] = footnotes;
        totalNotes += Object.keys(footnotes).length;
      }

      totalVerses   += content.length;
      totalChapters += 1;
      process.stdout.write(Object.keys(footnotes).length ? 'N' : '.');
    } catch (err) {
      process.stdout.write('!');
      console.error(`\n  ERROR ${book} ${chap}: ${err.message}`);
      errors++;
    }

    // Polite delay — ~2 req/sec
    await new Promise(r => setTimeout(r, 500));
  }

  // Write notes file for this book
  if (Object.keys(bookNotes).length > 0) {
    writeFileSync(notesPath, JSON.stringify(bookNotes));
  }

  console.log();
}

console.log(`\nDone. ${totalChapters} chapters, ${totalVerses} verses, ${totalNotes} footnotes, ${errors} errors.`);
