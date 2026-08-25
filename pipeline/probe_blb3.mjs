/**
 * Probe the ESV Study Bible notes URL pattern from BLB
 * URL: https://www.blueletterbible.org/esv-study-bible/notes/{book}/chapter-{n}
 */
import puppeteer from 'puppeteer';
import { createRequire } from 'module';
import { fileURLToPath } from 'url';
import path from 'path';

const require = createRequire(import.meta.url);

const browser = await puppeteer.launch({
  headless: true,
  executablePath: require('puppeteer').executablePath(),
  args: ['--no-sandbox', '--disable-setuid-sandbox'],
});
const page = await browser.newPage();
await page.setUserAgent('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36');

const url = 'https://www.blueletterbible.org/esv-study-bible/notes/gen/chapter-1';
console.log('Loading:', url);
await page.goto(url, { waitUntil: 'networkidle2', timeout: 30000 });
console.log('Final URL:', page.url());
console.log('Title:', await page.title());

// Dump body structure
const bodyText = await page.evaluate(() => document.body.innerText.substring(0, 2000));
console.log('\n=== Body text (first 2000 chars) ===');
console.log(bodyText);

// Find note-like elements
const noteEls = await page.evaluate(() => {
  const results = [];
  // Try common selectors
  ['p', 'div', 'section', 'article'].forEach(tag => {
    document.querySelectorAll(tag).forEach(el => {
      const text = el.innerText?.trim();
      if (text && text.length > 50 && text.length < 1000) {
        const cls = el.className?.toString() || '';
        const id = el.id || '';
        results.push({ tag, id, cls: cls.substring(0,60), text: text.substring(0, 120) });
      }
    });
  });
  return results.slice(0, 20);
});
console.log('\n=== Note-like elements ===');
noteEls.forEach(e => console.log(`<${e.tag} id="${e.id}" class="${e.cls}"> → "${e.text}"`));

// Check HTML structure at depth
const html = await page.evaluate(() => document.documentElement.innerHTML.substring(0, 3000));
console.log('\n=== Page HTML (first 3000) ===');
console.log(html);

await browser.close();
