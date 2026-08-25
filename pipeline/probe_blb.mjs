/**
 * Quick probe: load a BLB chapter page and dump the HTML structure
 * so we can find the right selectors for ESV study notes.
 * Usage: node pipeline/probe_blb.mjs
 */
import puppeteer from 'puppeteer';
import { createRequire } from 'module';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const require = createRequire(import.meta.url);

const browser = await puppeteer.launch({
  headless: true,
  executablePath: require('puppeteer').executablePath(),
  args: ['--no-sandbox', '--disable-setuid-sandbox'],
});
const page = await browser.newPage();
await page.setUserAgent('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36');

// Try a chapter page
const url = 'https://www.blueletterbible.org/esv/mrk/1/1/';
console.log('Loading:', url);
await page.goto(url, { waitUntil: 'networkidle2', timeout: 30000 });

// Dump all IDs and class names that contain "note" or "study"
const info = await page.evaluate(() => {
  const hits = [];
  document.querySelectorAll('*').forEach(el => {
    const cls = el.className?.toString() || '';
    const id  = el.id || '';
    if ((cls + id).match(/note|study|comm|commentary/i)) {
      hits.push({
        tag: el.tagName,
        id,
        cls: cls.substring(0, 80),
        text: el.innerText?.substring(0, 100),
      });
    }
  });
  return hits.slice(0, 40);
});

console.log('\n=== Elements with "note|study|comm" in class/id ===');
info.forEach(h => console.log(`<${h.tag} id="${h.id}" class="${h.cls}"> → "${h.text?.substring(0,60)}"`));

// Also check for #notesData specifically
const notesData = await page.evaluate(() => {
  const el = document.querySelector('#notesData, [id*="note"], [id*="Note"], [id*="study"], [id*="Study"]');
  return el ? { found: true, id: el.id, cls: el.className, html: el.innerHTML.substring(0, 500) } : { found: false };
});
console.log('\n=== #notesData / study elements ===');
console.log(JSON.stringify(notesData, null, 2));

await browser.close();
