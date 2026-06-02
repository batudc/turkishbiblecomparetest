/**
 * ESV Global Study Bible — Full Scraper
 * Scrapes footnotes for all 66 books from BLB and saves to data/commentary-en/
 * Skips books/chapters that already have real data.
 * Run: node scrape-all-esv.mjs
 */
import puppeteer from 'puppeteer';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DATA_ROOT = path.join(__dirname, 'data', 'commentary-en');
const CONCURRENCY = 1;    // sequential — avoid rate limiting
const DELAY_MS    = 2000; // 2s between requests

const BOOKS = [
  // OT
  { code:'GEN', blb:'gen', chapters:50,  ot:true,  name:'Genesis' },
  { code:'EXO', blb:'exo', chapters:40,  ot:true,  name:'Exodus' },
  { code:'LEV', blb:'lev', chapters:27,  ot:true,  name:'Leviticus' },
  { code:'NUM', blb:'num', chapters:36,  ot:true,  name:'Numbers' },
  { code:'DEU', blb:'deu', chapters:34,  ot:true,  name:'Deuteronomy' },
  { code:'JOS', blb:'jos', chapters:24,  ot:true,  name:'Joshua' },
  { code:'JDG', blb:'jdg', chapters:21,  ot:true,  name:'Judges' },
  { code:'RUT', blb:'rth', chapters:4,   ot:true,  name:'Ruth' },
  { code:'1SA', blb:'1sa', chapters:31,  ot:true,  name:'1 Samuel',       introSlug:'1-samuel' },
  { code:'2SA', blb:'2sa', chapters:24,  ot:true,  name:'2 Samuel',       introSlug:'2-samuel' },
  { code:'1KI', blb:'1ki', chapters:22,  ot:true,  name:'1 Kings',        introSlug:'1-kings' },
  { code:'2KI', blb:'2ki', chapters:25,  ot:true,  name:'2 Kings',        introSlug:'2-kings' },
  { code:'1CH', blb:'1ch', chapters:29,  ot:true,  name:'1 Chronicles',   introSlug:'1-chronicles' },
  { code:'2CH', blb:'2ch', chapters:36,  ot:true,  name:'2 Chronicles',   introSlug:'2-chronicles' },
  { code:'EZR', blb:'ezr', chapters:10,  ot:true,  name:'Ezra' },
  { code:'NEH', blb:'neh', chapters:13,  ot:true,  name:'Nehemiah' },
  { code:'EST', blb:'est', chapters:10,  ot:true,  name:'Esther' },
  { code:'JOB', blb:'job', chapters:42,  ot:true,  name:'Job' },
  { code:'PSA', blb:'psa', chapters:150, ot:true,  name:'Psalms' },
  { code:'PRO', blb:'pro', chapters:31,  ot:true,  name:'Proverbs' },
  { code:'ECC', blb:'ecc', chapters:12,  ot:true,  name:'Ecclesiastes' },
  { code:'SNG', blb:'sng', chapters:8,   ot:true,  name:'Song of Solomon', introSlug:'song-of-solomon' },
  { code:'ISA', blb:'isa', chapters:66,  ot:true,  name:'Isaiah' },
  { code:'JER', blb:'jer', chapters:52,  ot:true,  name:'Jeremiah' },
  { code:'LAM', blb:'lam', chapters:5,   ot:true,  name:'Lamentations' },
  { code:'EZK', blb:'eze', chapters:48,  ot:true,  name:'Ezekiel' },
  { code:'DAN', blb:'dan', chapters:12,  ot:true,  name:'Daniel' },
  { code:'HOS', blb:'hos', chapters:14,  ot:true,  name:'Hosea' },
  { code:'JOL', blb:'jol', chapters:3,   ot:true,  name:'Joel' },
  { code:'AMO', blb:'amo', chapters:9,   ot:true,  name:'Amos' },
  { code:'OBA', blb:'oba', chapters:1,   ot:true,  name:'Obadiah' },
  { code:'JON', blb:'jon', chapters:4,   ot:true,  name:'Jonah' },
  { code:'MIC', blb:'mic', chapters:7,   ot:true,  name:'Micah' },
  { code:'NAH', blb:'nah', chapters:3,   ot:true,  name:'Nahum' },
  { code:'HAB', blb:'hab', chapters:3,   ot:true,  name:'Habakkuk' },
  { code:'ZEP', blb:'zep', chapters:3,   ot:true,  name:'Zephaniah' },
  { code:'HAG', blb:'hag', chapters:2,   ot:true,  name:'Haggai' },
  { code:'ZEC', blb:'zec', chapters:14,  ot:true,  name:'Zechariah' },
  { code:'MAL', blb:'mal', chapters:4,   ot:true,  name:'Malachi' },
  // NT
  { code:'MAT', blb:'mat', chapters:28,  ot:false, name:'Matthew' },
  { code:'MRK', blb:'mrk', chapters:16,  ot:false, name:'Mark' },
  { code:'LUK', blb:'luk', chapters:24,  ot:false, name:'Luke' },
  { code:'JHN', blb:'jhn', chapters:21,  ot:false, name:'John' },
  { code:'ACT', blb:'act', chapters:28,  ot:false, name:'Acts' },
  { code:'ROM', blb:'rom', chapters:16,  ot:false, name:'Romans' },
  { code:'1CO', blb:'1co', chapters:16,  ot:false, name:'1 Corinthians',  introSlug:'1-corinthians' },
  { code:'2CO', blb:'2co', chapters:13,  ot:false, name:'2 Corinthians',  introSlug:'2-corinthians' },
  { code:'GAL', blb:'gal', chapters:6,   ot:false, name:'Galatians' },
  { code:'EPH', blb:'eph', chapters:6,   ot:false, name:'Ephesians' },
  { code:'PHP', blb:'php', chapters:4,   ot:false, name:'Philippians' },
  { code:'COL', blb:'col', chapters:4,   ot:false, name:'Colossians' },
  { code:'1TH', blb:'1th', chapters:5,   ot:false, name:'1 Thessalonians', introSlug:'1-thessalonians' },
  { code:'2TH', blb:'2th', chapters:3,   ot:false, name:'2 Thessalonians', introSlug:'2-thessalonians' },
  { code:'1TI', blb:'1ti', chapters:6,   ot:false, name:'1 Timothy',      introSlug:'1-timothy' },
  { code:'2TI', blb:'2ti', chapters:4,   ot:false, name:'2 Timothy',      introSlug:'2-timothy' },
  { code:'TIT', blb:'tit', chapters:3,   ot:false, name:'Titus' },
  { code:'PHM', blb:'phm', chapters:1,   ot:false, name:'Philemon' },
  // HEB already done — skip
  { code:'JAS', blb:'jas', chapters:5,   ot:false, name:'James' },
  { code:'1PE', blb:'1pe', chapters:5,   ot:false, name:'1 Peter',        introSlug:'1-peter' },
  { code:'2PE', blb:'2pe', chapters:3,   ot:false, name:'2 Peter',        introSlug:'2-peter' },
  { code:'1JN', blb:'1jn', chapters:5,   ot:false, name:'1 John',         introSlug:'1-john' },
  { code:'2JN', blb:'2jn', chapters:1,   ot:false, name:'2 John',         introSlug:'2-john' },
  { code:'3JN', blb:'3jn', chapters:1,   ot:false, name:'3 John',         introSlug:'3-john' },
  { code:'JUD', blb:'jud', chapters:1,   ot:false, name:'Jude' },
  { code:'REV', blb:'rev', chapters:22,  ot:false, name:'Revelation' },
];

// ── Helpers ───────────────────────────────────────────────────────────────────
function introUrl(book) {
  const slug = (book.introSlug || book.name.toLowerCase().replace(/\s+/g, '-'));
  const testament = book.ot ? 'old' : 'new';
  return `https://www.blueletterbible.org/esv-study-bible/${testament}-testament/introductions/introduction-to-${slug}.cfm`;
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function scrapeChapter(browser, book, chap) {
  const page = await browser.newPage();
  await page.setUserAgent('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36');
  try {
    const url = `https://www.blueletterbible.org/esv-study-bible/notes/${book.blb}/chapter-${chap}`;
    await page.goto(url, { waitUntil: 'networkidle2', timeout: 25000 });
    const result = await page.evaluate(() => {
      const notesDiv = document.getElementById('notesData');
      if (!notesDiv) return null;
      const passages = [], notes = [];
      Array.from(notesDiv.querySelectorAll('p')).forEach(p => {
        const verseEl = p.querySelector('strong.verse');
        const ref = verseEl ? verseEl.textContent.trim() : null;
        if (!ref) return;
        const outlineEl = p.querySelector('span.outline-1');
        const fullText = p.innerText.trim();
        if (outlineEl) {
          const outlineText = outlineEl.textContent.trim();
          const bodyText = fullText.replace(ref,'').replace(outlineText,'').replace(/^\s*[-–—]\s*/,'').trim();
          passages.push({ ref, title: outlineText, text: bodyText });
        } else {
          const noteText = fullText.replace(ref,'').replace(/^\s*/,'').trim();
          notes.push({ ref, text: noteText });
        }
      });
      return { passages, notes };
    });
    return result;
  } catch(e) {
    return null;
  } finally {
    await page.close();
  }
}

async function scrapeIntro(browser, book) {
  const page = await browser.newPage();
  await page.setUserAgent('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36');
  try {
    await page.goto(introUrl(book), { waitUntil: 'networkidle2', timeout: 25000 });
    const sections = await page.evaluate(() => {
      const result = {};
      document.querySelectorAll('h2[id]').forEach(h2 => {
        const id = h2.id;
        if (!id.includes('introduction-to')) return;
        const title = h2.textContent.trim();
        const parts = [title];
        let node = h2.nextElementSibling;
        while (node && node.tagName !== 'H2') {
          const t = node.innerText?.trim();
          if (t && t.length > 5) parts.push(t);
          node = node.nextElementSibling;
        }
        result[title] = parts.slice(1).join('\n\n');
      });
      return Object.keys(result).length > 0 ? result : null;
    });
    return sections;
  } catch(e) {
    return null;
  } finally {
    await page.close();
  }
}

// ── Passage propagation ───────────────────────────────────────────────────────
function parseChapterRange(ref) {
  const clean = ref.replace(/^[A-Za-z.\s]+/,'').trim();
  const m = clean.match(/^(\d+):\d+[–\-](\d+):/) ||
            clean.match(/^(\d+):\d+\s*[–\-]\s*(\d+):/);
  if (!m) {
    const sm = clean.match(/^(\d+):/);
    return sm ? { start: parseInt(sm[1]), end: parseInt(sm[1]) } : null;
  }
  return { start: parseInt(m[1]), end: parseInt(m[2]) };
}

function propagatePassages(allChapData) {
  // Collect all passages with their chapter ranges
  const masterPassages = [];
  allChapData.forEach((d, idx) => {
    (d.passages || []).forEach(p => {
      const range = parseChapterRange(p.ref);
      if (range) masterPassages.push({ ...p, rangeStart: range.start, rangeEnd: range.end });
    });
  });
  // For each chapter, assign applicable passages
  allChapData.forEach((d, idx) => {
    const chap = idx + 1;
    d.passages = masterPassages.filter(p => chap >= p.rangeStart && chap <= p.rangeEnd)
      .map(({ rangeStart, rangeEnd, ...p }) => p);
  });
}

// ── Main ─────────────────────────────────────────────────────────────────────
let browser = await puppeteer.launch({ headless: 'new', args: ['--no-sandbox'] });
let pageCount = 0;
const RESTART_EVERY = 25; // restart browser every N pages to avoid connection drops

async function getPage() {
  pageCount++;
  if (pageCount % RESTART_EVERY === 0) {
    try { await browser.close(); } catch(_) {}
    await sleep(1000);
    browser = await puppeteer.launch({ headless: 'new', args: ['--no-sandbox'] });
    process.stdout.write('[↺]');
  }
  const page = await browser.newPage();
  await page.setUserAgent('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36');
  return page;
}

let totalBooks = 0, totalChaps = 0, totalNotes = 0;
const startTime = Date.now();

for (const book of BOOKS) {
  const dir = path.join(DATA_ROOT, book.code);
  fs.mkdirSync(dir, { recursive: true });

  // Check if already complete (has data with notes)
  const existing = fs.existsSync(path.join(dir, '1.json'))
    ? JSON.parse(fs.readFileSync(path.join(dir, '1.json'), 'utf8'))
    : null;
  if (existing && existing.source?.includes('Crossway') && existing.notes?.length > 0) {
    process.stdout.write(`  ${book.code}: ✓ already done\n`);
    continue;
  }

  process.stdout.write(`  ${book.code} (${book.chapters} ch):`);

  // Scrape intro using getPage
  let bookIntro = null;
  try {
    const introPage = await getPage();
    try {
      await introPage.goto(introUrl(book), { waitUntil: 'networkidle2', timeout: 25000 });
      bookIntro = await introPage.evaluate(() => {
        const result = {};
        document.querySelectorAll('h2[id]').forEach(h2 => {
          if (!h2.id.includes('introduction-to')) return;
          const title = h2.textContent.trim();
          const parts = [];
          let node = h2.nextElementSibling;
          while (node && node.tagName !== 'H2') {
            const t = node.innerText?.trim();
            if (t && t.length > 5) parts.push(t);
            node = node.nextElementSibling;
          }
          if (parts.length > 0) result[title] = parts.join('\n\n');
        });
        return Object.keys(result).length > 0 ? result : null;
      });
    } finally { try { await introPage.close(); } catch(_) {} }
  } catch(_) {}
  await sleep(500);

  // Scrape all chapters in batches
  const allChapData = [];
  for (let i = 0; i < book.chapters; i += CONCURRENCY) {
    const batch = [];
    for (let j = 0; j < CONCURRENCY && i + j < book.chapters; j++) {
      const chap = i + j + 1;
      batch.push((async () => {
        const page = await getPage();
        try {
          const url = `https://www.blueletterbible.org/esv-study-bible/notes/${book.blb}/chapter-${chap}`;
          await page.goto(url, { waitUntil: 'networkidle2', timeout: 25000 });
          return await page.evaluate(() => {
            const notesDiv = document.getElementById('notesData');
            if (!notesDiv) return null;
            const passages = [], notes = [];
            Array.from(notesDiv.querySelectorAll('p')).forEach(p => {
              const verseEl = p.querySelector('strong.verse');
              const ref = verseEl ? verseEl.textContent.trim() : null;
              if (!ref) return;
              const outlineEl = p.querySelector('span.outline-1');
              const fullText = p.innerText.trim();
              if (outlineEl) {
                const outlineText = outlineEl.textContent.trim();
                const bodyText = fullText.replace(ref,'').replace(outlineText,'').replace(/^\s*[-–—]\s*/,'').trim();
                passages.push({ ref, title: outlineText, text: bodyText });
              } else {
                notes.push({ ref, text: fullText.replace(ref,'').replace(/^\s*/,'').trim() });
              }
            });
            return { passages, notes };
          });
        } catch(e) { return null; }
        finally { try { await page.close(); } catch(_) {} }
      })());
    }
    const results = await Promise.all(batch);
    results.forEach(r => allChapData.push(r || { passages: [], notes: [] }));
    process.stdout.write('.');
    await sleep(DELAY_MS);
  }

  propagatePassages(allChapData);

  let chapNotes = 0;
  allChapData.forEach((d, idx) => {
    const json = {
      source: 'ESV Global Study Bible (Crossway) — Used under license',
      book_intro: bookIntro || '',
      chapter_intro: d.passages?.length > 0 ? d.passages[0].title : '',
      passages: d.passages || [],
      notes: d.notes || []
    };
    fs.writeFileSync(path.join(dir, `${idx + 1}.json`), JSON.stringify(json, null, 2), 'utf8');
    chapNotes += json.notes.length;
  });

  totalBooks++; totalChaps += book.chapters; totalNotes += chapNotes;
  process.stdout.write(` ${book.chapters} ch, ${chapNotes} notes (${Math.round((Date.now()-startTime)/1000)}s)\n`);
}

try { await browser.close(); } catch(_) {}
console.log(`\n✅  Done: ${totalBooks} books, ${totalChaps} chapters, ${totalNotes} notes`);
