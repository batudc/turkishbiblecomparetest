import puppeteer from 'puppeteer';
import fs from 'fs';
import path from 'path';

const url = process.argv[2];
const label = process.argv[3];

if (!url) {
  console.error('Usage: node screenshot.mjs <url> [label]');
  process.exit(1);
}

const dir = './temporary screenshots';
fs.mkdirSync(dir, { recursive: true });

const existing = fs.readdirSync(dir)
  .map(f => {
    const m = f.match(/^screenshot-(\d+)/);
    return m ? parseInt(m[1], 10) : 0;
  });
const n = (existing.length ? Math.max(...existing) : 0) + 1;
const filename = label ? `screenshot-${n}-${label}.png` : `screenshot-${n}.png`;
const outPath = path.join(dir, filename);

const browser = await puppeteer.launch({
  headless: true,
  args: ['--no-sandbox', '--disable-setuid-sandbox'],
});
const page = await browser.newPage();
await page.setViewport({ width: 1440, height: 900, deviceScaleFactor: 2 });
await page.goto(url, { waitUntil: 'networkidle0', timeout: 30000 });

// Scroll through the full page first so scroll-triggered reveal animations fire
await page.evaluate(async () => {
  const step = 400;
  const delay = (ms) => new Promise(r => setTimeout(r, ms));
  let y = 0;
  const height = document.body.scrollHeight;
  while (y < height) {
    window.scrollTo({ top: y, left: 0, behavior: 'instant' });
    y += step;
    await delay(40);
  }
  window.scrollTo({ top: 0, left: 0, behavior: 'instant' });
  await delay(600);
});

await page.screenshot({ path: outPath, fullPage: true });
await browser.close();

console.log(`Saved: ${outPath}`);
