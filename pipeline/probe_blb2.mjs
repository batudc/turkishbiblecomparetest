/**
 * Deeper probe: try clicking notes and inspecting BLB's study note panel
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

// Navigate to chapter page
await page.goto('https://www.blueletterbible.org/esv/mrk/1/1/', { waitUntil: 'networkidle2', timeout: 30000 });

// Dump page title and URL after redirect
console.log('Page URL:', page.url());
console.log('Title:', await page.title());

// Dump all nav links
const navLinks = await page.evaluate(() => {
  return Array.from(document.querySelectorAll('a[href]')).slice(0, 30).map(a => ({
    id: a.id, text: a.innerText.trim().substring(0, 40), href: a.href.substring(0, 80)
  }));
});
console.log('\n=== Nav links ===');
navLinks.forEach(l => console.log(`[${l.id}] "${l.text}" → ${l.href}`));

// Check studyDrop content
const studyDrop = await page.evaluate(() => {
  const el = document.getElementById('studyDrop');
  return el ? el.innerHTML.substring(0, 1000) : 'NOT FOUND';
});
console.log('\n=== studyDrop HTML ===');
console.log(studyDrop);

// Try navigating to a study notes URL (BLB uses /ss24/ or similar for ESV GSB)
await page.goto('https://www.blueletterbible.org/esv/mrk/1/1/ss24/', { waitUntil: 'networkidle2', timeout: 30000 });
console.log('\nAfter /ss24/ URL:', page.url());
const ss24Body = await page.evaluate(() => document.body.innerText.substring(0, 500));
console.log('Body text snippet:', ss24Body);

await browser.close();
