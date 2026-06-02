/**
 * ESV Study Bible — Book Introduction Scraper
 *
 * Usage:
 *   node scrape-esv-intro.mjs HEB
 *   node scrape-esv-intro.mjs MAT
 *
 * What it does:
 *   1. Opens a real Chrome browser window (not headless)
 *   2. You navigate to the book introduction page on Blue Letter Bible
 *   3. Press ENTER in this terminal when the page is ready
 *   4. Script extracts all paragraph text from the main content area
 *   5. Saves it as `book_intro` in every data/commentary-en/[BOOK]/[chap].json file
 */

import puppeteer from 'puppeteer';
import fs from 'fs';
import path from 'path';
import readline from 'readline';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const BOOK = process.argv[2]?.toUpperCase();
if (!BOOK) {
  console.error('Usage: node scrape-esv-intro.mjs <BOOK_CODE>  e.g. HEB, MAT, ROM');
  process.exit(1);
}

const DATA_DIR = path.join(__dirname, 'data', 'commentary-en', BOOK);
if (!fs.existsSync(DATA_DIR)) {
  console.error(`No data directory found at: ${DATA_DIR}`);
  console.error('Create the chapter files first before running this scraper.');
  process.exit(1);
}

// ── Open browser ─────────────────────────────────────────────────────────────
console.log('\n🔍  Opening browser...');
const browser = await puppeteer.launch({
  headless: false,
  defaultViewport: null,
  args: ['--start-maximized'],
});
const page = await browser.newPage();

// Start on the BLB ESV Study Bible landing page
await page.goto('https://www.blueletterbible.org/esv-study-bible/', {
  waitUntil: 'domcontentloaded',
});

console.log(`
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Browser is open.

  Navigate to the INTRODUCTION page for: ${BOOK}
  (e.g. click the book link, scroll to the intro section)

  When the full introduction text is visible on screen,
  come back here and press ENTER to extract it.
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
`);

// Wait for user to navigate
await waitForEnter();

console.log('\n⏳  Extracting content...');

// ── Extract all paragraph text from the page ─────────────────────────────────
// Try several common content container selectors, fall back to all <p> tags
const extracted = await page.evaluate(() => {
  const SELECTORS = [
    '.study-content',
    '.esv-study-content',
    '#study-content',
    '.intro-content',
    '#intro-content',
    'article',
    'main',
    '.content-area',
    '#content',
  ];

  let container = null;
  for (const sel of SELECTORS) {
    const el = document.querySelector(sel);
    if (el && el.innerText?.trim().length > 200) {
      container = el;
      break;
    }
  }

  // Fall back: collect all visible paragraphs on the page
  const paragraphs = container
    ? Array.from(container.querySelectorAll('p, h2, h3, h4'))
    : Array.from(document.querySelectorAll('p'));

  return paragraphs
    .map(el => el.innerText?.trim())
    .filter(t => t && t.length > 30)
    .join('\n\n');
});

if (!extracted || extracted.length < 100) {
  console.error('❌  Could not extract enough text. Try scrolling so the full intro is visible, then re-run.');
  await browser.close();
  process.exit(1);
}

console.log(`✅  Extracted ${extracted.length} characters of text.`);
console.log('\n--- Preview (first 300 chars) ---');
console.log(extracted.substring(0, 300) + '…');
console.log('---------------------------------\n');

// ── Confirm before writing ────────────────────────────────────────────────────
console.log('Does this look correct? Press ENTER to save to all chapter files, or Ctrl+C to cancel.');
await waitForEnter();

// ── Update all chapter JSON files ────────────────────────────────────────────
const files = fs.readdirSync(DATA_DIR).filter(f => f.endsWith('.json'));
let updated = 0;

for (const file of files) {
  const filePath = path.join(DATA_DIR, file);
  try {
    const data = JSON.parse(fs.readFileSync(filePath, 'utf8'));
    data.book_intro = extracted;
    fs.writeFileSync(filePath, JSON.stringify(data, null, 2), 'utf8');
    updated++;
  } catch (e) {
    console.warn(`  ⚠ Could not update ${file}: ${e.message}`);
  }
}

console.log(`\n✅  Updated book_intro in ${updated} files under data/commentary-en/${BOOK}/`);
await browser.close();
process.exit(0);

// ── Helper ────────────────────────────────────────────────────────────────────
function waitForEnter() {
  return new Promise(resolve => {
    const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
    rl.question('', () => { rl.close(); resolve(); });
  });
}
