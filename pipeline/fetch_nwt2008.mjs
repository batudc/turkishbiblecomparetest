/**
 * fetch_nwt2008.mjs — Re-scrapes all NWT2008 chapters from jw.org/bi12
 * The previous data was fetched from the wrong source (nwt/2025 instead of bi12).
 *
 * Usage:
 *   node pipeline/fetch_nwt2008.mjs               # all books
 *   node pipeline/fetch_nwt2008.mjs HEB            # single book
 *   node pipeline/fetch_nwt2008.mjs --resume       # skip already-fixed books
 */

import puppeteer from 'puppeteer';
import { writeFileSync, mkdirSync, existsSync, readFileSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, '..');
const OUT_DIR = join(ROOT, 'data', 'translations', 'NWT2008');

// Map USFM book code → bi12 URL path segment (extracted from jw.org/bi12/kitaplar/ listing)
const BOOK_URL = {
  GEN: 'Ba%C5%9Flang%C4%B1%C3%A7', EXO: '%C3%87%C4%B1k%C4%B1%C5%9F',
  LEV: 'Levio%C4%9Fullar%C4%B1',   NUM: 'Say%C4%B1lar',
  DEU: 'tekrar',                    JOS: 'Ye%C5%9Fu',
  JDG: 'H%C3%A2kimler',            RUT: 'rut',
  '1SA': '1-samuel',               '2SA': '2-samuel',
  '1KI': '1-krallar',              '2KI': '2-krallar',
  '1CH': '1-tarihler',             '2CH': '2-tarihler',
  EZR: 'ezra',                     NEH: 'nehemya',
  EST: 'ester',                    JOB: 'Ey%C3%BCp',
  PSA: 'mezmurlar',                PRO: '%C3%96zdeyi%C5%9Fler',
  ECC: 'vaiz',                     SNG: 'ezgiler-ezgisi',
  ISA: '%C4%B0%C5%9Faya',          JER: 'yeremya',
  LAM: 'A%C4%9F%C4%B1tlar',        EZK: 'hezekiel',
  DAN: 'daniel',                   HOS: 'Ho%C5%9Fea',
  JOL: 'yoel',                     AMO: 'amos',
  OBA: 'obadya',                   JON: 'yunus',
  MIC: 'mika',                     NAH: 'nahum',
  HAB: 'habakkuk',                 ZEP: 'tsefanya',
  HAG: 'haggay',                   ZEC: 'zekeriya',
  MAL: 'malaki',                   MAT: 'matta',
  MRK: 'markos',                   LUK: 'luka',
  JHN: 'yuhanna',                  ACT: 'El%C3%A7iler',
  ROM: 'Romal%C4%B1lar',           '1CO': '1-korintoslular',
  '2CO': '2-korintoslular',        GAL: 'Galatyal%C4%B1lar',
  EPH: 'efesoslular',              PHP: 'filipililer',
  COL: 'koloseliler',              '1TH': '1-selanikliler',
  '2TH': '2-selanikliler',         '1TI': '1-timoteos',
  '2TI': '2-timoteos',             TIT: 'titus',
  PHM: 'filimon',                  HEB: '%C4%B0braniler',
  JAS: 'yakup',                    '1PE': '1-petrus',
  '2PE': '2-petrus',               '1JN': '1-yuhanna',
  '2JN': '2-yuhanna',              '3JN': '3-yuhanna',
  JUD: 'yahuda',                   REV: 'vahiy',
};

// Chapter counts per USFM book code
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

const args = process.argv.slice(2);
const resume = args.includes('--resume');
const fromIdx = args.indexOf('--from');
const toIdx   = args.indexOf('--to');
const fromBook = fromIdx >= 0 ? args[fromIdx + 1]?.toUpperCase() : null;
const toBook   = toIdx   >= 0 ? args[toIdx   + 1]?.toUpperCase() : null;
const singleBook = args.find(a => !a.startsWith('--') && args[args.indexOf(a)-1] !== '--from' && args[args.indexOf(a)-1] !== '--to')?.toUpperCase();

function cleanText(raw) {
  return raw
    .replace(/\+|\*/g, '')           // footnote markers
    .replace(/\s+/g, ' ')
    .trim();
}

async function scrapeChapter(page, bookCode, chap) {
  const urlSlug = BOOK_URL[bookCode];
  const url = `https://www.jw.org/tr/kutuphane/kutsal-kitap/bi12/kitaplar/${urlSlug}/${chap}/`;

  await page.goto(url, { waitUntil: 'networkidle0', timeout: 30000 });
  await new Promise(r => setTimeout(r, 800));

  const result = await page.evaluate(() => {
    const verseEls = Array.from(document.querySelectorAll('.verse'));
    if (!verseEls.length) return null;

    const verses = [];
    for (const el of verseEls) {
      // Extract verse number from id e.g. v58001001 → 1
      const idMatch = el.id.match(/v\d+(\d{3})(\d{3})$/);
      if (!idMatch) continue;
      const vNum = parseInt(idMatch[2], 10);

      // Clone to manipulate
      const clone = el.cloneNode(true);
      // Remove footnote elements
      clone.querySelectorAll('a, sup, .footnoteMarker, [class*="footnote"]').forEach(n => n.remove());
      // Remove verse number label (first child sup/span with the number)
      const label = clone.querySelector('.verseNum, .label');
      if (label) label.remove();

      const text = clone.textContent.replace(/\+|\*/g, '').replace(/\s+/g, ' ').trim();
      if (text) verses.push({ v: vNum, text });
    }

    // Section headings: .sectionHeading or h3
    const headings = [];
    document.querySelectorAll('.sectionHeading, h3.sectionHeading, .groupHeading').forEach(h => {
      // Find which verse follows this heading
      let next = h.nextElementSibling;
      while (next && !next.classList.contains('verse')) next = next.nextElementSibling;
      if (next) {
        const idMatch = next.id.match(/v\d+(\d{3})(\d{3})$/);
        if (idMatch) {
          headings.push({ beforeVerse: parseInt(idMatch[2], 10), text: h.textContent.trim() });
        }
      }
    });

    return { verses, headings };
  });

  return result;
}

async function main() {
  let books;
  if (singleBook) {
    books = [singleBook];
  } else if (fromBook || toBook) {
    const s = fromBook ? BOOK_ORDER.indexOf(fromBook) : 0;
    const e = toBook   ? BOOK_ORDER.indexOf(toBook)   : BOOK_ORDER.length - 1;
    books = BOOK_ORDER.slice(s, e + 1);
  } else {
    books = BOOK_ORDER;
  }

  // Build work list
  const work = [];
  for (const code of books) {
    if (!BOOK_URL[code]) { console.log(`Unknown book: ${code}`); continue; }
    const total = CHAPTERS[code] ?? 1;
    for (let ch = 1; ch <= total; ch++) {
      if (resume) {
        const path = join(OUT_DIR, code, `${ch}.json`);
        if (existsSync(path)) {
          // Check if it looks like the wrong (NWT2025) data by comparing verse 1
          // (skip check for now — just skip existing files in resume mode)
          continue;
        }
      }
      work.push({ code, ch });
    }
  }

  const total = work.length;
  console.log(`Scraping ${total} chapters for NWT2008 (bi12) …`);
  if (total === 0) { console.log('Nothing to do.'); return; }

  const UA = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36';

  async function launchBrowser() {
    const br = await puppeteer.launch({ headless: 'new', args: ['--no-sandbox', '--disable-dev-shm-usage'] });
    const pg = await br.newPage();
    await pg.setUserAgent(UA);
    return { br, pg };
  }

  let { br: browser, pg: page } = await launchBrowser();
  let done = 0, ok = 0, fail = 0, consecutiveErrors = 0;

  for (const { code, ch } of work) {
    done++;
    process.stdout.write(`[${String(done).padStart(4)}/${total}] ${code} ${String(ch).padStart(3)} … `);

    let data = null;
    let attempted = 0;

    while (attempted < 3) {
      attempted++;
      try {
        data = await scrapeChapter(page, code, ch);
        consecutiveErrors = 0;
        break;
      } catch (err) {
        consecutiveErrors++;
        if (attempted < 3) {
          // Restart browser and retry
          process.stdout.write(`[retry${attempted}] `);
          try { await browser.close(); } catch {}
          await new Promise(r => setTimeout(r, 1500));
          ({ br: browser, pg: page } = await launchBrowser());
        } else {
          console.log(`ERROR: ${err.message}`);
          fail++;
        }
      }
    }

    if (!data) { await new Promise(r => setTimeout(r, 400)); continue; }

    if (data.verses.length === 0) {
      console.log('EMPTY');
      fail++;
      await new Promise(r => setTimeout(r, 400));
      continue;
    }

    // Build content array with section headings interleaved
    const content = [];
    for (const verse of data.verses) {
      const headingsBefore = data.headings.filter(h => h.beforeVerse === verse.v);
      for (const h of headingsBefore) content.push({ section: h.text });
      content.push({ v: verse.v, text: verse.text });
    }

    const outDir = join(OUT_DIR, code);
    mkdirSync(outDir, { recursive: true });
    writeFileSync(
      join(outDir, `${ch}.json`),
      JSON.stringify({ content }, null, 2),
      'utf8',
    );
    console.log(`✓ ${data.verses.length} verses`);
    ok++;

    // Restart browser every 100 chapters to prevent memory buildup
    if (ok % 100 === 0) {
      try { await browser.close(); } catch {}
      await new Promise(r => setTimeout(r, 1000));
      ({ br: browser, pg: page } = await launchBrowser());
    }

    // Polite delay
    await new Promise(r => setTimeout(r, 400));
  }

  await browser.close();
  console.log(`\nDone: ${ok} OK, ${fail} failed, ${total} total`);
}

main().catch(e => { console.error(e); process.exit(1); });
