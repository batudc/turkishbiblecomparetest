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

// Try different slugs for Philippians
for (const slug of ['php', 'phi', 'phl', 'philippians']) {
  const url = `https://www.blueletterbible.org/esv-study-bible/notes/${slug}/chapter-1`;
  await page.goto(url, { waitUntil: 'networkidle2', timeout: 20000 });
  const container = await page.$('#notesData');
  const pCount = container ? (await container.$$('p')).length : 0;
  const title = await page.title();
  console.log(`${slug}: pCount=${pCount} title=${title}`);
  await new Promise(r => setTimeout(r, 300));
}
await browser.close();
