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
await page.goto('http://localhost:3000/compare.html', { waitUntil: 'networkidle0', timeout: 30000 });

await page.click('#btn-tour');
await new Promise(r => setTimeout(r, 300));
// step 4: translations (index 3) — click next 3x
for (let i = 0; i < 3; i++) { await page.click('#tour-next'); await new Promise(r => setTimeout(r, 350)); }
await page.screenshot({ path: join(dir, `screenshot-${n++}-tour-s4-translations.png`) });

// step 7: style (index 6) — click next 3x more  
for (let i = 0; i < 3; i++) { await page.click('#tour-next'); await new Promise(r => setTimeout(r, 350)); }
await page.screenshot({ path: join(dir, `screenshot-${n++}-tour-s7-style.png`) });

// step 8: commentary (index 7)
await page.click('#tour-next'); await new Promise(r => setTimeout(r, 600));
await page.screenshot({ path: join(dir, `screenshot-${n++}-tour-s8-yorum.png`) });

await browser.close();
console.log('done');
