#!/usr/bin/env python3
"""Probe christiananswers.net HTML structure for YYY1987 parsing."""
import urllib.request, re

url = 'https://christiananswers.net/turkish/bible-tr/tr-mat3.html'
req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
with urllib.request.urlopen(req, timeout=15) as r:
    html = r.read().decode('utf-8', errors='replace')

# Print a relevant excerpt around verse content
start = html.find('Yahya')
if start > 0:
    print(html[max(0, start-500):start+3000])
