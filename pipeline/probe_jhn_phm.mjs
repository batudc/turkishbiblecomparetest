import puppeteer from 'puppeteer';
import { createRequire } from 'module';
const require = createRequire(import.meta.url);
const browser = await puppeteer.launch({ headless: true, executablePath: require('puppeteer').executablePath(), args: ['--no-sandbox'] });
const page = await browser.newPage();
await page.setUserAgent('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36');
for (const [usfm, slug] of [['JHN','jhn'],['JHN','joh'],['PHM','phm'],['PHM','plm'],['PHM','phl']]) {
  try {
    await page.goto(`https://www.blueletterbible.org/esv-study-bible/notes/${slug}/chapter-1`, { waitUntil: 'networkidle2', timeout: 25000 });
    const cnt = await page.$('#notesData');
    const ps = cnt ? await cnt.$$('p') : [];
    console.log(`${usfm} -> ${slug}: ${ps.length > 0 ? '✓ ' + ps.length : '✗ empty'}`);
  } catch(e) { console.log(`${usfm} -> ${slug}: TIMEOUT`); }
  await new Promise(r => setTimeout(r, 400));
}
await browser.close();
