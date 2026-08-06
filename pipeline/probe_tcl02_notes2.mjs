/**
 * probe_tcl02_notes2.mjs — Deep probe of bible.com footnote structure
 */

import puppeteer from 'puppeteer';

const browser = await puppeteer.launch({ headless: true, args: ['--no-sandbox'] });
const page = await browser.newPage();
await page.setUserAgent('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36');

await page.goto('https://www.bible.com/bible/170/MAT.14.TCL02', { waitUntil: 'networkidle2', timeout: 30000 });

const notes = await page.evaluate(() => {
  // Find all note spans
  const noteEls = document.querySelectorAll('[class*="__note"]');
  const result = [];
  for (const el of noteEls) {
    // Get the verse number — traverse up to find the verse container
    let verseEl = el.closest('[class*="__verse"]') || el.parentElement;
    let verseNum = '';
    // Look for data-usfm or data-verse attributes
    let cur = el;
    while (cur && cur !== document.body) {
      if (cur.dataset && (cur.dataset.usfm || cur.dataset.verse)) {
        verseNum = cur.dataset.usfm || cur.dataset.verse;
        break;
      }
      // Also look for id like "MAT.14.24"
      if (cur.id && /\.\d+$/.test(cur.id)) {
        verseNum = cur.id;
        break;
      }
      cur = cur.parentElement;
    }
    result.push({
      verseNum,
      outerHTML: el.outerHTML.substring(0, 400),
      textContent: el.textContent.trim(),
      classes: el.className,
    });
  }
  return result;
});

console.log('Notes found:', notes.length);
for (const n of notes) {
  console.log('\n--- Note ---');
  console.log('verseNum:', n.verseNum);
  console.log('textContent:', n.textContent.substring(0, 200));
  console.log('outerHTML:', n.outerHTML.substring(0, 300));
}

// Also look at verse structure
const verseStructure = await page.evaluate(() => {
  const verses = document.querySelectorAll('[data-usfm]');
  const sample = [];
  for (let i = 0; i < Math.min(5, verses.length); i++) {
    sample.push({
      usfm: verses[i].dataset.usfm,
      html: verses[i].outerHTML.substring(0, 300),
    });
  }
  return sample;
});

console.log('\n\n=== Verse structure ===');
for (const v of verseStructure) {
  console.log('usfm:', v.usfm);
  console.log('html:', v.html.substring(0, 200));
  console.log();
}

await browser.close();
