import puppeteer from 'puppeteer';
import { existsSync, mkdirSync, readdirSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const dir = join(__dirname, 'temporary screenshots');
if (!existsSync(dir)) mkdirSync(dir);
const n = readdirSync(dir).filter(f => f.endsWith('.png')).length + 1;

const browser = await puppeteer.launch({ headless: 'new', args: ['--no-sandbox'] });
const page = await browser.newPage();
await page.setViewport({ width: 1440, height: 900, deviceScaleFactor: 2 });

await page.goto('http://localhost:3000/search.html', { waitUntil: 'networkidle0', timeout: 30000 });
await page.click('#btn-translations');
await new Promise(r => setTimeout(r, 400));
await page.screenshot({ path: join(dir, `screenshot-${n}-ceviri-panel.png`), fullPage: false });
console.log(`Saved screenshot-${n}-ceviri-panel.png`);

await page.click('#btn-translations');
await new Promise(r => setTimeout(r, 200));
await page.click('#btn-gorunum');
await new Promise(r => setTimeout(r, 400));
await page.screenshot({ path: join(dir, `screenshot-${n+1}-gorunum-panel.png`), fullPage: false });
console.log(`Saved screenshot-${n+1}-gorunum-panel.png`);

await browser.close();
