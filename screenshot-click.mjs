import puppeteer from 'puppeteer';
import { existsSync, mkdirSync, readdirSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const dir = join(__dirname, 'temporary screenshots');
if (!existsSync(dir)) mkdirSync(dir);

const url = process.argv[2] || 'http://localhost:3000';
const selector = process.argv[3] || null;
const label = process.argv[4] ? `-${process.argv[4]}` : '';

const existing = readdirSync(dir).filter(f => f.endsWith('.png'));
const n = existing.length + 1;
const filename = `screenshot-${n}${label}.png`;
const outPath = join(dir, filename);

const browser = await puppeteer.launch({ headless: 'new', args: ['--no-sandbox'] });
const page = await browser.newPage();
await page.setViewport({ width: 1440, height: 900, deviceScaleFactor: 2 });
await page.goto(url, { waitUntil: 'networkidle0', timeout: 30000 });
if (selector) {
  await page.click(selector);
  await new Promise(r => setTimeout(r, 400));
}
await page.screenshot({ path: outPath, fullPage: false });
await browser.close();

console.log(`Screenshot saved: temporary screenshots/${filename}`);
