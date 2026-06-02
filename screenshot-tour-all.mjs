import puppeteer from 'puppeteer';
import { existsSync, mkdirSync, readdirSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const dir = join(__dirname, 'temporary screenshots');
if (!existsSync(dir)) mkdirSync(dir);

const existing = readdirSync(dir).filter(f => f.endsWith('.png'));
let n = existing.length + 1;

async function shot(page, label) {
  const path = join(dir, `screenshot-${n++}-${label}.png`);
  await page.screenshot({ path, fullPage: false });
  console.log(`  saved: ${path.split('/').pop()}`);
}

async function runTour(url, steps, label) {
  const browser = await puppeteer.launch({ headless: 'new', args: ['--no-sandbox'] });
  const page = await browser.newPage();
  await page.setViewport({ width: 1440, height: 900, deviceScaleFactor: 2 });
  await page.goto(url, { waitUntil: 'networkidle0', timeout: 30000 });
  await new Promise(r => setTimeout(r, 800));

  await page.click('#btn-tour');
  await new Promise(r => setTimeout(r, 400));

  for (let i = 0; i < steps; i++) {
    await shot(page, `${label}-s${i+1}`);
    if (i < steps - 1) {
      await page.click('#tour-next');
      await new Promise(r => setTimeout(r, 500));
    }
  }
  await browser.close();
  console.log(`done: ${label}`);
}

console.log('=== compare.html ===');
await runTour('http://localhost:3000/compare.html', 7, 'compare');

console.log('=== antikdil.html ===');
await runTour('http://localhost:3000/antikdil.html?book=JHN&chapter=1', 4, 'antikdil');

console.log('=== arama.html ===');
await runTour('http://localhost:3000/arama.html', 4, 'arama');
