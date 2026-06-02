import puppeteer from 'puppeteer';
import { existsSync, mkdirSync, readdirSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const dir = join(__dirname, 'temporary screenshots');
if (!existsSync(dir)) mkdirSync(dir);

const existing = readdirSync(dir).filter(f => f.endsWith('.png'));
const n = existing.length + 1;

const browser = await puppeteer.launch({ headless: 'new', args: ['--no-sandbox'] });
const page = await browser.newPage();
await page.setViewport({ width: 1440, height: 900, deviceScaleFactor: 2 });
await page.goto('http://localhost:3000/interlinear.html?book=JHN&chapter=1', { waitUntil: 'networkidle0', timeout: 30000 });

// Switch to Açık theme via JS
await page.evaluate(() => {
  document.querySelector('input[name="il-theme"][value="light"]').click();
});
await new Promise(r => setTimeout(r, 300));

const out1 = join(dir, `screenshot-${n}-acik-theme.png`);
await page.screenshot({ path: out1, fullPage: false });
console.log(`Screenshot saved: temporary screenshots/screenshot-${n}-acik-theme.png`);

// Switch to Parşömen
await page.evaluate(() => {
  document.querySelector('input[name="il-theme"][value="sepia"]').click();
});
await new Promise(r => setTimeout(r, 300));

const out2 = join(dir, `screenshot-${n+1}-parsomen-theme.png`);
await page.screenshot({ path: out2, fullPage: false });
console.log(`Screenshot saved: temporary screenshots/screenshot-${n+1}-parsomen-theme.png`);

await browser.close();
