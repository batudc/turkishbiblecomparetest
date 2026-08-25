import puppeteer from 'puppeteer';
import fs from 'fs';

const dir = '/Users/batuhandemircan/website building/temporary screenshots';
const existing = fs.readdirSync(dir).filter(f => f.match(/^screenshot-\d+/)).map(f => {
  const m = f.match(/^screenshot-(\d+)/); return m ? parseInt(m[1]) : 0;
});
const n = (existing.length ? Math.max(...existing) : 0) + 1;

const browser = await puppeteer.launch({ headless: true, args: ['--no-sandbox', '--disable-setuid-sandbox'] });
const mob = await browser.newPage();
await mob.setViewport({ width: 375, height: 812 });
await mob.goto('http://localhost:3000/karsilastirma.html', { waitUntil: 'networkidle0' });
await new Promise(r => setTimeout(r, 1000));
await mob.evaluate(() => document.getElementById('btn-style')?.click());
await new Promise(r => setTimeout(r, 600));

// Düzen bölümü görünene kadar scroll et
await mob.evaluate(() => {
  const duzen = Array.from(document.querySelectorAll('.panel-section-title')).find(el => el.textContent.trim() === 'Düzen');
  if (duzen) duzen.scrollIntoView({ block: 'start' });
});
await new Promise(r => setTimeout(r, 300));

const out = `${dir}/screenshot-${n}-layout-mobile-scroll.png`;
await mob.screenshot({ path: out });
await browser.close();
console.log(`Kaydedildi: ${out}`);
