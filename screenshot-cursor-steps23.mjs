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

const browser = await puppeteer.launch({ headless: 'new', args: ['--no-sandbox'] });
const page = await browser.newPage();
await page.setViewport({ width: 1440, height: 900, deviceScaleFactor: 2 });
await page.goto('http://localhost:3000/compare.html', { waitUntil: 'networkidle0', timeout: 30000 });
await new Promise(r => setTimeout(r, 800));

await page.click('#btn-tour');
await new Promise(r => setTimeout(r, 400));
await page.click('#tour-next'); // step 2

// Check cursor visibility and position on the modern cat toggle
await new Promise(r => setTimeout(r, 410)); // just after cursor moved to modern cat
await shot(page, 'step2-cursor-move-to-modern');
await new Promise(r => setTimeout(r, 410)); // after click animation
await shot(page, 'step2-cursor-after-click-modern');
await new Promise(r => setTimeout(r, 800)); // cursor moved to TCL02
await shot(page, 'step2-cursor-on-tcl02');
await new Promise(r => setTimeout(r, 420));
await shot(page, 'step2-cursor-click-tcl02');

// Step 3
await page.click('#tour-next');
await new Promise(r => setTimeout(r, 400));
await shot(page, 'step3-init');
await new Promise(r => setTimeout(r, 720)); // cursor moves to TCL02 star
await shot(page, 'step3-cursor-on-tcl02star');
await new Promise(r => setTimeout(r, 420)); // click
await shot(page, 'step3-cursor-click-tcl02star');

await browser.close();
console.log('done');
