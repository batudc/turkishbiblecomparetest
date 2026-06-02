import puppeteer from 'puppeteer';
import { existsSync, mkdirSync, readdirSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const dir = join(__dirname, 'temporary screenshots');
if (!existsSync(dir)) mkdirSync(dir);
let n = readdirSync(dir).filter(f => f.endsWith('.png')).length + 1;

const browser = await puppeteer.launch({ headless: 'new', args: ['--no-sandbox'] });
const page = await browser.newPage();
await page.setViewport({ width: 1440, height: 900, deviceScaleFactor: 2 });

for (const theme of ['light', 'sepia', 'dark']) {
  await page.goto('http://localhost:3000/search.html', { waitUntil: 'networkidle0', timeout: 30000 });
  await page.evaluate(t => localStorage.setItem('pref-theme', t), theme);
  await page.reload({ waitUntil: 'networkidle0' });
  await new Promise(r => setTimeout(r, 300));
  await page.screenshot({ path: join(dir, `screenshot-${n}-search-theme-${theme}.png`), fullPage: false });
  console.log(`Saved screenshot-${n}-search-theme-${theme}.png`);
  n++;
}
await browser.close();
