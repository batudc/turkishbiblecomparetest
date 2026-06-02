/**
 * ESV Study Bible — Chapter Notes Scraper (interactive)
 *
 * Usage:
 *   node scrape-esv-chapter.mjs HEB 1
 *   node scrape-esv-chapter.mjs HEB 2
 *
 * Steps:
 *   1. Browser opens — log in to BLB if needed
 *   2. Navigate to the ESV Study Bible page for that chapter
 *      e.g. https://www.blueletterbible.org/esv/heb/1/
 *   3. Make sure the study notes are visible (scroll down, click Tools > Study)
 *   4. Press ENTER in this terminal
 *   5. Script extracts notes and updates data/commentary-en/HEB/1.json
 */

import puppeteer from 'puppeteer';
import fs from 'fs';
import path from 'path';
import readline from 'readline';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const BOOK  = process.argv[2]?.toUpperCase();
const CHAP  = parseInt(process.argv[3], 10);

if (!BOOK || !CHAP) {
  console.error('Usage: node scrape-esv-chapter.mjs <BOOK_CODE> <CHAPTER>  e.g. HEB 2');
  process.exit(1);
}

const FILE = path.join(__dirname, 'data', 'commentary-en', BOOK, `${CHAP}.json`);
if (!fs.existsSync(FILE)) {
  console.error(`File not found: ${FILE}`);
  process.exit(1);
}

const browser = await puppeteer.launch({
  headless: false,
  defaultViewport: null,
  args: ['--start-maximized'],
});
const page = await browser.newPage();

// Start on the BLB chapter page
await page.goto(`https://www.blueletterbible.org/esv/heb/${CHAP}/`, {
  waitUntil: 'domcontentloaded',
});

console.log(`
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Browser is open on ${BOOK} chapter ${CHAP}.

  1. Log in if prompted
  2. Make the ESV Study Bible notes visible on screen
     (click a verse → Tools → Study, or scroll to the notes)
  3. When ALL the chapter/verse notes are visible, press ENTER
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
`);

await waitForEnter();
console.log('\n⏳  Extracting notes...');

const extracted = await page.evaluate(() => {
  // Try common selectors for BLB study note containers
  const SELECTORS = [
    '.studyNote', '.study-note', '.esv-note', '.esv-study',
    '[class*="studyNote"]', '[class*="study-note"]',
    '.commText', '.comm-text-body', '.commentaryText',
    '[class*="comm"]', '[class*="note-text"]',
  ];

  // Also try collecting all <p> tags under any element containing "study" in class
  let notes = [];

  for (const sel of SELECTORS) {
    const els = document.querySelectorAll(sel);
    if (els.length > 0) {
      els.forEach(el => {
        const text = el.innerText?.trim();
        if (text && text.length > 20) notes.push({ sel, text });
      });
      if (notes.length > 0) break;
    }
  }

  // Fallback: grab all paragraphs from the main content area
  if (notes.length === 0) {
    const main = document.querySelector('main, #main, #content, .content, article, [role="main"]');
    if (main) {
      main.querySelectorAll('p').forEach(p => {
        const text = p.innerText?.trim();
        if (text && text.length > 30) notes.push({ sel: 'p', text });
      });
    }
  }

  return notes;
});

await browser.close();

if (!extracted || extracted.length === 0) {
  console.error('❌  No notes found. Make sure the study notes panel is visible before pressing ENTER.');
  process.exit(1);
}

console.log(`✅  Found ${extracted.length} note elements`);

// Try to parse verse references from note text
// Format: "1:1 — note text" or "v.1 note text" or just paragraph text
const notes = extracted.map(({ text }) => {
  const verseMatch = text.match(/^(\d+:\d+(?:[–\-]\d+)?)\s*[—–:]/);
  if (verseMatch) {
    return { ref: verseMatch[1], text: text.replace(/^[^—–:]+[—–:]\s*/, '') };
  }
  return { ref: `${CHAP}`, text };
});

// Update JSON file
const data = JSON.parse(fs.readFileSync(FILE, 'utf8'));
data.notes = notes;
fs.writeFileSync(FILE, JSON.stringify(data, null, 2), 'utf8');

console.log(`✅  Saved ${notes.length} notes to ${FILE}`);

function waitForEnter() {
  return new Promise(resolve => {
    const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
    rl.question('', () => { rl.close(); resolve(); });
  });
}
