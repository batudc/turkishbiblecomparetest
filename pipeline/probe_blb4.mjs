/**
 * Get the exact HTML of note paragraphs to find bold/strong tags
 */
import puppeteer from 'puppeteer';
import { createRequire } from 'module';

const require = createRequire(import.meta.url);

const browser = await puppeteer.launch({
  headless: true,
  executablePath: require('puppeteer').executablePath(),
  args: ['--no-sandbox', '--disable-setuid-sandbox'],
});
const page = await browser.newPage();
await page.setUserAgent('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36');

await page.goto('https://www.blueletterbible.org/esv-study-bible/notes/gen/chapter-1', {
  waitUntil: 'networkidle2', timeout: 30000
});

// Get all p tags with substantial content and their innerHTML
const notes = await page.evaluate(() => {
  // Find main content area — BLB uses .tools-box or similar
  // Try to find the notes container specifically
  const allDivs = Array.from(document.querySelectorAll('div'));
  const noteContainers = allDivs.filter(d => {
    const text = d.innerText || '';
    return text.includes('Gen. 1:1') && text.length > 200 && text.length < 50000;
  });

  if (noteContainers.length === 0) {
    // fallback: grab p tags with Bible ref pattern
    return Array.from(document.querySelectorAll('p'))
      .filter(p => /^(Gen|Exo|Lev|Num|\d+:|[A-Z][a-z]{1,3}\.)/.test(p.innerText?.trim()))
      .slice(0, 20)
      .map(p => ({ innerHTML: p.innerHTML, text: p.innerText?.trim() }));
  }

  // Use the smallest container that still has all notes
  noteContainers.sort((a, b) => a.innerHTML.length - b.innerHTML.length);
  const container = noteContainers[0];

  return Array.from(container.querySelectorAll('p'))
    .slice(0, 15)
    .map(p => ({ innerHTML: p.innerHTML.substring(0, 400), text: p.innerText?.trim().substring(0, 200) }));
});

console.log('\n=== Note paragraphs with innerHTML ===');
notes.forEach((n, i) => {
  console.log(`\n[${i}] TEXT: ${n.text?.substring(0, 100)}`);
  console.log(`     HTML: ${n.innerHTML?.substring(0, 200)}`);
});

// Also try to find what wraps the note paragraphs
const containers = await page.evaluate(() => {
  const refs = [];
  document.querySelectorAll('p').forEach(p => {
    const text = p.innerText?.trim() || '';
    if (/^Gen\. 1:/.test(text) && text.length > 30) {
      let el = p;
      for (let i = 0; i < 5; i++) {
        el = el.parentElement;
        if (!el) break;
        refs.push({ level: i, tag: el.tagName, id: el.id, cls: el.className?.toString().substring(0, 60) });
      }
    }
  });
  return refs.slice(0, 15);
});
console.log('\n=== Parent chain of note paragraphs ===');
containers.forEach(c => console.log(`Level ${c.level}: <${c.tag} id="${c.id}" class="${c.cls}">`));

await browser.close();
