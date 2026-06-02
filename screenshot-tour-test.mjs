import puppeteer from 'puppeteer';
import { readdirSync, writeFileSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';
const __dirname = dirname(fileURLToPath(import.meta.url));
const outDir = join(__dirname, 'temporary screenshots');

const browser = await puppeteer.launch({ headless: 'new', args: ['--no-sandbox'] });
const page = await browser.newPage();
await page.setViewport({ width: 1400, height: 860 });
await page.goto('http://localhost:3000/compare.html', { waitUntil: 'networkidle2' });
await new Promise(r => setTimeout(r, 1500));

const nums = readdirSync(outDir).map(f => parseInt(f.match(/screenshot-(\d+)/)?.[1]||0)).filter(Boolean);
let n = Math.max(0, ...nums) + 1;

// Click tour button
await page.click('#btn-tour');
await new Promise(r => setTimeout(r, 700));
await page.screenshot({ path: join(outDir, `screenshot-${n++}-tour-s1.png`) });
console.log(`Saved step 1`);

// Step 2
await page.click('#tour-next');
await new Promise(r => setTimeout(r, 400));
await page.screenshot({ path: join(outDir, `screenshot-${n++}-tour-s2.png`) });
console.log(`Saved step 2`);

// Step 5 (columns) - click 3 more times
for (let i = 0; i < 3; i++) { await page.click('#tour-next'); await new Promise(r => setTimeout(r, 350)); }
await page.screenshot({ path: join(outDir, `screenshot-${n++}-tour-s5.png`) });
console.log(`Saved step 5`);

await browser.close();
