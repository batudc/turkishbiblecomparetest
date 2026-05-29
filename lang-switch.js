// Language preference — runs on every page before content renders
(function() {
  const MAP = {
    // Homepages
    'index.html':         { lang: 'en', partner: 'anasayfa.html' },
    'anasayfa.html':      { lang: 'tr', partner: 'index.html' },
    // Reader pages
    'karsilastirma.html': { lang: 'tr', partner: 'compare.html' },
    'compare.html':       { lang: 'en', partner: 'karsilastirma.html' },
    'antikdil.html':      { lang: 'tr', partner: 'ancient.html' },
    'ancient.html':       { lang: 'en', partner: 'antikdil.html' },
    'arama.html':         { lang: 'tr', partner: 'search.html' },
    'search.html':        { lang: 'en', partner: 'arama.html' },
    // Info pages
    'hakkimizda.html':    { lang: 'tr', partner: 'about.html' },
    'about.html':         { lang: 'en', partner: 'hakkimizda.html' },
    'iletisim.html':      { lang: 'tr', partner: 'contact.html' },
    'contact.html':       { lang: 'en', partner: 'iletisim.html' },
    'gizlilik.html':      { lang: 'tr', partner: 'privacy.html' },
    'privacy.html':       { lang: 'en', partner: 'gizlilik.html' },
  };

  const page = location.pathname.split('/').pop() || 'index.html';
  const info = MAP[page];
  if (!info) return;

  const pref = localStorage.getItem('kki_lang');
  if (pref && pref !== info.lang) {
    location.replace(info.partner + location.search);
    return;
  }

  window._setLang = function(lang) {
    localStorage.setItem('kki_lang', lang);
    const cur = location.pathname.split('/').pop() || 'index.html';
    const inf = MAP[cur];
    if (inf && inf.lang !== lang) location.href = inf.partner + location.search;
  };

  document.addEventListener('DOMContentLoaded', function() {
    document.querySelectorAll('[data-lang-switch]').forEach(btn => {
      const target = btn.dataset.langSwitch;
      btn.classList.toggle('lang-active', target === (pref || info.lang));
      btn.addEventListener('click', () => window._setLang(target));
    });
  });
})();
