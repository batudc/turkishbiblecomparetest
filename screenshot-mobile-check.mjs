import puppeteer from 'puppeteer';
import { existsSync, mkdirSync, readdirSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const dir = join(__dirname, 'temporary screenshots');
if (!existsSync(dir)) mkdirSync(dir);

const base = 'http://localhost:3000';
const browser = await puppeteer.launch({ headless: 'new', args: ['--no-sandbox'] });

async function shot(page, label) {
  const existing = readdirSync(dir).filter(f => f.endsWith('.png'));
  const n = existing.length + 1;
  const path = join(dir, `screenshot-${n}-${label}.png`);
  await page.screenshot({ path, fullPage: false });
  console.log(`Saved: screenshot-${n}-${label}.png`);
  return path;
}

async function mobile(portrait = true) {
  const page = await browser.newPage();
  await page.setViewport({ width: portrait ? 390 : 844, height: portrait ? 844 : 390, deviceScaleFactor: 2 });
  return page;
}

// 1. arama.html portrait
let p = await mobile(true);
await p.goto(`${base}/arama.html`, { waitUntil: 'networkidle0', timeout: 20000 });
await shot(p, 'arama-portrait');
// open Çeviri panel
await p.click('#btn-translations');
await new Promise(r => setTimeout(r, 300));
await shot(p, 'arama-ceviripanel-portrait');
await p.click('#btn-translations');
await new Promise(r => setTimeout(r, 200));
// open Filtre panel
await p.click('#btn-filtreler');
await new Promise(r => setTimeout(r, 300));
await shot(p, 'arama-filtrepanel-portrait');
await p.close();

// 2. arama.html landscape
p = await mobile(false);
await p.goto(`${base}/arama.html`, { waitUntil: 'networkidle0', timeout: 20000 });
await shot(p, 'arama-landscape');
await p.close();

// 3. karsilastirma.html portrait
p = await mobile(true);
await p.goto(`${base}/karsilastirma.html`, { waitUntil: 'networkidle0', timeout: 20000 });
await shot(p, 'compare-portrait');
await p.close();

// 4. karsilastirma.html landscape
p = await mobile(false);
await p.goto(`${base}/karsilastirma.html`, { waitUntil: 'networkidle0', timeout: 20000 });
await shot(p, 'compare-landscape');
await p.close();

// 5. antikdil portrait
p = await mobile(true);
await p.goto(`${base}/antikdil.html`, { waitUntil: 'networkidle0', timeout: 20000 });
await shot(p, 'antikdil-portrait');
await p.close();

await browser.close();
console.log('Done.');
