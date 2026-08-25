import puppeteer from 'puppeteer';
import { createRequire } from 'module';
const require = createRequire(import.meta.url);

const browser = await puppeteer.launch({
  headless: true,
  executablePath: require('puppeteer').executablePath(),
  args: ['--no-sandbox', '--disable-setuid-sandbox'],
});
const page = await browser.newPage();
await page.setUserAgent('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36');

// Check potentially tricky slugs (one chapter each)
const checks = [
  ['SNG','sng'], ['SNG','sos'], ['SNG','song'],
  ['EZK','eze'], ['EZK','ezk'],
  ['JHN','jhn'], ['JHN','joh'], ['JHN','jn'],
  ['PHM','phm'], ['PHM','plm'],
  ['OBA','oba'], ['OBA','ob'],
  ['NAH','nah'],
];

for (const [usfm, slug] of checks) {
  const url = `https://www.blueletterbible.org/esv-study-bible/notes/${slug}/chapter-1`;
  await page.goto(url, { waitUntil: 'networkidle2', timeout: 20000 });
  const container = await page.$('#notesData');
  const pCount = container ? (await container.$$('p')).length : 0;
  const status = pCount > 0 ? `✓ ${pCount} notes` : '✗ empty';
  console.log(`${usfm} → ${slug}: ${status}`);
  await new Promise(r => setTimeout(r, 300));
}
await browser.close();
