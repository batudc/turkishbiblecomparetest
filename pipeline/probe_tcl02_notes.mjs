/**
 * probe_tcl02_notes.mjs — Probe bible.com to understand footnote HTML structure
 * Usage: node pipeline/probe_tcl02_notes.mjs
 */

import puppeteer from 'puppeteer';

const browser = await puppeteer.launch({ headless: true, args: ['--no-sandbox'] });
const page = await browser.newPage();
await page.setUserAgent('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');

console.log('Loading MAT 14...');
await page.goto('https://www.bible.com/bible/170/MAT.14.TCL02', { waitUntil: 'networkidle2', timeout: 30000 });

// Check page title
const title = await page.title();
console.log('Title:', title);

// Try to find footnotes in the DOM
const footnoteInfo = await page.evaluate(() => {
  // Look for footnote-related elements
  const selectors = [
    '[class*="footnote"]',
    '[class*="Footnote"]',
    '[class*="note"]',
    '[data-note]',
    '[data-footnote]',
    'sup',
    '.fn',
  ];

  const results = {};
  for (const sel of selectors) {
    const els = document.querySelectorAll(sel);
    if (els.length > 0) {
      results[sel] = {
        count: els.length,
        first: els[0].outerHTML.substring(0, 200),
        text: els[0].textContent.substring(0, 100),
      };
    }
  }

  // Also get page structure
  results['_bodyHTML_sample'] = document.body.innerHTML.substring(0, 500);

  return results;
});

console.log('\nFootnote elements found:');
for (const [sel, info] of Object.entries(footnoteInfo)) {
  console.log(`\n${sel}: count=${info.count}`);
  if (info.first) console.log('  HTML:', info.first.substring(0, 150));
  if (info.text) console.log('  Text:', info.text.substring(0, 100));
}

await browser.close();
console.log('\nDone.');
