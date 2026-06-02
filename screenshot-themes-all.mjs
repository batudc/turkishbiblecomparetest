import puppeteer from 'puppeteer';
import { existsSync, mkdirSync, readdirSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const dir = join(__dirname, 'temporary screenshots');
if (!existsSync(dir)) mkdirSync(dir);

const existing = readdirSync(dir).filter(f => f.endsWith('.png'));
let n = existing.length + 1;

const browser = await puppeteer.launch({ headless: 'new', args: ['--no-sandbox'] });
const page = await browser.newPage();
await page.setViewport({ width: 1440, height: 900, deviceScaleFactor: 2 });

for (const theme of ['light', 'dark', 'sepia']) {
  await page.goto('http://localhost:3000/interlinear.html?book=JHN&chapter=1', { waitUntil: 'networkidle0', timeout: 30000 });
  await page.evaluate((t) => {
    const r = document.querySelector(`input[name="il-theme"][value="${t}"]`);
    if (r) r.click();
  }, theme);
  await new Promise(r => setTimeout(r, 400));
  const out = join(dir, `screenshot-${n}-theme-${theme}.png`);
  await page.screenshot({ path: out, fullPage: false });
  console.log(`Screenshot saved: temporary screenshots/screenshot-${n}-theme-${theme}.png`);
  n++;
}

await browser.close();
