const SHELL_CACHE = 'kki-shell-v5';  // bumped: word study link fixes (verse tracking, consistent update)
const DATA_CACHE  = 'kki-data-v16'; // bumped: fix 171 truncated YYY1987 verses (multi-para source)

const SHELL_FILES = [
  '/',
  '/index.html',
  '/anasayfa.html',
  '/karsilastirma.html',
  '/compare.html',
  '/antikdil.html',
  '/ancient.html',
  '/arama.html',
  '/search.html',
  '/lang-switch.js',
  '/icons/icon-192x192.png',
  '/icons/icon-512x512.png',
  '/apple-touch-icon.png',
  '/data/books.json',
  '/data/index.json',
];

// ── Install: precache app shell ────────────────────────────────────────────
self.addEventListener('install', e => {
  e.waitUntil(
    caches.open(SHELL_CACHE)
      .then(c => c.addAll(SHELL_FILES))
      .then(() => self.skipWaiting())
  );
});

// ── Activate: remove old shell caches (keep data cache) ───────────────────
self.addEventListener('activate', e => {
  e.waitUntil(
    caches.keys().then(keys =>
      Promise.all(
        keys
          .filter(k => k !== SHELL_CACHE && k !== DATA_CACHE)
          .map(k => caches.delete(k))
      )
    ).then(() => self.clients.claim())
  );
});

// ── Fetch ──────────────────────────────────────────────────────────────────
self.addEventListener('fetch', e => {
  if (e.request.method !== 'GET') return;
  const url = new URL(e.request.url);
  if (!url.origin === self.location.origin) return;

  // DATA files (translations, commentary, strongs, interlinear, notes, errata)
  // Strategy: cache-first — once a chapter is read it works forever offline.
  if (url.pathname.startsWith('/data/')) {
    e.respondWith(
      caches.open(DATA_CACHE).then(cache =>
        cache.match(e.request).then(cached => {
          if (cached) return cached;
          // Not cached yet — fetch, cache, return
          return fetch(e.request).then(res => {
            if (res && res.status === 200) {
              cache.put(e.request, res.clone());
            }
            return res;
          }).catch(() => new Response('{"error":"offline"}', {
            headers: { 'Content-Type': 'application/json' }
          }));
        })
      )
    );
    return;
  }

  // APP SHELL: stale-while-revalidate
  e.respondWith(
    caches.match(e.request).then(cached => {
      const fresh = fetch(e.request).then(res => {
        if (res && res.status === 200) {
          caches.open(SHELL_CACHE).then(c => c.put(e.request, res.clone()));
        }
        return res;
      }).catch(() => null);
      return cached || fresh;
    })
  );
});

// ── Message: manual cache / uncache of a full translation ─────────────────
self.addEventListener('message', e => {
  // CACHE_TRANSLATION: sw.postMessage({ type: 'CACHE_TRANSLATION', tid, books })
  if (e.data?.type === 'CACHE_TRANSLATION') {
    const { tid, books } = e.data;
    caches.open(DATA_CACHE).then(async cache => {
      let cached = 0;
      for (const { code, chapters } of books) {
        for (let ch = 1; ch <= chapters; ch++) {
          const url = `/data/translations/${tid}/${code}/${ch}.json`;
          const already = await cache.match(url);
          if (!already) {
            try {
              const res = await fetch(url);
              if (res.ok) { await cache.put(url, res); cached++; }
            } catch (_) {}
          }
        }
      }
      const clients = await self.clients.matchAll();
      clients.forEach(c => c.postMessage({ type: 'CACHE_DONE', tid, cached }));
    });
    return;
  }

  // UNCACHE_TRANSLATION: delete all cached files for a specific tid
  if (e.data?.type === 'UNCACHE_TRANSLATION') {
    const { tid } = e.data;
    caches.open(DATA_CACHE).then(async cache => {
      const keys = await cache.keys();
      const toDelete = keys.filter(req => req.url.includes(`/data/translations/${tid}/`));
      await Promise.all(toDelete.map(req => cache.delete(req)));
      const clients = await self.clients.matchAll();
      clients.forEach(c => c.postMessage({ type: 'UNCACHE_DONE', tid }));
    });
    return;
  }
});
