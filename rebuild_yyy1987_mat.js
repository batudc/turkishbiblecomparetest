#!/usr/bin/env node
/**
 * YYY1987 MAT Bible Verse Reconstruction Script (v4)
 *
 * Strategy:
 * 1. YYY verse numbers are anchors: yyyVerse.v tells us which TCL verse that chunk starts at.
 * 2. We group consecutive YYY chunks where they cover overlapping TCL ranges (merging their texts).
 * 3. Within each group, split concatenated YYY text into N sub-segments aligned to TCL verse hints.
 * 4. Splitting uses word-boundary-aware fuzzy matching on the raw text.
 * 5. Genuinely missing verses (gaps not covered by any group) get empty string.
 */

const fs = require('fs');
const path = require('path');

const ROOT = '/Users/batuhandemircan/website building/data/translations';
const YYY_DIR = path.join(ROOT, 'YYY1987', 'MAT');
const TCL_DIR = path.join(ROOT, 'TCL02', 'MAT');
const OUT_DIR = path.join(ROOT, 'YYY1987_REBUILT', 'MAT');

fs.mkdirSync(OUT_DIR, { recursive: true });

// ─── Text cleaning ────────────────────────────────────────────────────────────

function cleanText(text) {
  if (!text) return '';
  text = text.replace(/\bELÇ\b[\s\S]*/g, '');
  text = text.replace(/\bDipnotları\b[\s\S]*/g, '');
  text = text.replace(/\bKaynak ayetler?\b[\s\S]*/g, '');
  text = text.replace(/[A-ZÇŞĞÜÖİa-zçşğüöı]+\.\d+:\d+(?:-\d+)?/g, '');
  text = text.replace(/bkz\.\S*/g, '');
  text = text.replace(/\bbkz\b\.?\s*/g, '');
  text = text.replace(/\b\d+:\d+(?:-\d+)?\b/g, '');
  text = text.replace(/\b(XIV|XIII|XII|XI|IX|VIII|VII|VI|IV|III|XV|XVI|XVII|XVIII|XIX|XX|XXI|XXII|XXIII|XXIV|XXV|XXVI|XXVII|XXVIII|II|I)\b/g, '');
  text = text.replace(/(?<![A-Za-zÀ-ɏğĞşŞçÇöÖüÜıİ])\d+(?![A-Za-zÀ-ɏğĞşŞçÇöÖüÜıİ])/g, '');
  text = text.replace(/\s+/g, ' ').trim();
  return text;
}

// ─── Normalization for matching ───────────────────────────────────────────────

function normalize(str) {
  return str
    .toLowerCase()
    .replace(/İ/g, 'i').replace(/ı/g, 'i')
    .replace(/[üÜ]/g, 'u').replace(/[öÖ]/g, 'o')
    .replace(/[şŞ]/g, 's').replace(/[çÇ]/g, 'c')
    .replace(/[ğĞ]/g, 'g')
    .replace(/[^a-z0-9\s]/g, '')  // Remove non-alpha (no spaces for single-word tokens)
    .replace(/\s+/g, ' ').trim();
}

// ─── Word-aligned position finder ────────────────────────────────────────────

/**
 * Build a parallel structure: for each "word token" in rawText (split on spaces),
 * store { rawWord, normWord, startChar, endChar }
 */
function tokenize(rawText) {
  const tokens = [];
  let pos = 0;
  for (const rawWord of rawText.split(' ')) {
    tokens.push({
      rawWord,
      normWord: normalize(rawWord),
      startChar: pos,
      endChar: pos + rawWord.length
    });
    pos += rawWord.length + 1;
  }
  return tokens;
}

/**
 * Find the best word index in `tokens` (starting from tokenIdx >= minTokenIdx)
 * where the text matches the first 50 chars of `hint`.
 * Returns { tokenIdx, score } or null.
 */
function findBestMatch(tokens, hint, minTokenIdx = 0) {
  if (!hint) return null;

  const anchor = hint.slice(0, 55);
  const normAnchor = normalize(anchor);
  const anchorWords = normAnchor.split(' ').filter(w => w.length >= 2);
  if (anchorWords.length === 0) return null;

  // Helper: score between a token normWord and an anchor word
  function wordScore(tw, aw) {
    if (tw === aw) return 2;
    if (tw.length >= 3 && aw.length >= 3) {
      const prefixLen = Math.floor(Math.min(tw.length, aw.length) * 0.7);
      if (prefixLen >= 2 && (tw.startsWith(aw.slice(0, prefixLen)) || aw.startsWith(tw.slice(0, prefixLen)))) {
        return 1;
      }
    }
    return 0;
  }

  let bestScore = -1;
  let bestIdx = -1;

  for (let i = minTokenIdx; i < tokens.length; i++) {
    // GATE: Among the first 3 anchor words, at least one must match the token at or near position i.
    // This prevents "lookahead" matches (where tokens deep in the window dominate the score)
    // while allowing for minor word-level shifts between TCL and YYY translations.
    const leadAnchorWords = anchorWords.slice(0, Math.min(3, anchorWords.length));
    let gatePass = false;
    for (let g = 0; g < leadAnchorWords.length && g < 3; g++) {
      // Check token[i], token[i+1], token[i+2] against anchor[g]
      for (let d = 0; d <= Math.min(2, g + 1); d++) {
        if (i + d < tokens.length && wordScore(tokens[i + d].normWord, leadAnchorWords[g]) > 0) {
          gatePass = true;
          break;
        }
      }
      if (gatePass) break;
    }
    if (!gatePass) continue;

    let score = 0;
    const maxJ = Math.min(anchorWords.length, tokens.length - i);
    for (let j = 0; j < maxJ; j++) {
      score += wordScore(tokens[i + j].normWord, anchorWords[j]);
    }
    const normalizedScore = score / (anchorWords.length * 2);
    if (normalizedScore > bestScore) {
      bestScore = normalizedScore;
      bestIdx = i;
    }
  }

  if (bestScore < 0.25 || bestIdx < 0) return null;
  return { tokenIdx: bestIdx, score: bestScore };
}

// ─── Chunk splitter ───────────────────────────────────────────────────────────

/**
 * Split `chunkText` into exactly `tclSubVerses.length` segments.
 * Uses TCL verse texts as hints for where each verse starts.
 * Returns array of strings (trimmed).
 */
function splitChunk(chunkText, tclSubVerses) {
  const n = tclSubVerses.length;
  if (n === 0) return [];
  if (!chunkText || chunkText.trim() === '') return new Array(n).fill('');
  if (n === 1) return [chunkText.trim()];

  const tokens = tokenize(chunkText);
  if (tokens.length === 0) return new Array(n).fill('');

  // We track real split points (token indices where each verse starts).
  // A split is "real" (match found) or "virtual" (proportional fallback).
  // We also track the "first unmatched" verse index — all verses from that point
  // are treated as empty, and their text bulk is appended to the preceding verse.

  const splitTokenIdxs = [0]; // verse 0 starts at token 0
  const splitIsReal = [true];  // true = found a real match
  let firstUnmatchedVerse = -1; // index of first verse that couldn't be located

  for (let i = 1; i < n; i++) {
    const hint = tclSubVerses[i].text;
    const prevTokenIdx = splitTokenIdxs[splitTokenIdxs.length - 1];
    const minTokenIdx = prevTokenIdx + 1;

    const match = findBestMatch(tokens, hint, minTokenIdx);

    if (match && match.tokenIdx > prevTokenIdx) {
      splitTokenIdxs.push(match.tokenIdx);
      splitIsReal.push(true);
    } else {
      // No match found for verse i's hint.
      // Check: would the proportional fallback leave the previous matched verse with too few tokens?
      const frac = i / n;
      const proportional = Math.max(prevTokenIdx + 1, Math.floor(tokens.length * frac));
      const proportionalClamped = Math.min(proportional, tokens.length - 1);

      // If proportional split gives prev verse ≤ 2 tokens, it's not meaningful.
      // In this case, treat verse i as unmatched (empty) and give all remaining text to prev verse.
      const prevVerseTokenCount = proportionalClamped - prevTokenIdx;
      const remainingChars = chunkText.length - (prevTokenIdx > 0 ? tokens[prevTokenIdx].startChar : 0);
      const remainingVerses = n - (i - 1);

      if (prevVerseTokenCount <= 2 || remainingChars < remainingVerses * 8) {
        // Not meaningful to split further. Mark verse i and all after as unmatched.
        firstUnmatchedVerse = i;
        break;
      }

      // Proportional fallback
      splitTokenIdxs.push(proportionalClamped);
      splitIsReal.push(false);
    }
  }

  // Extract character ranges from token indices
  const segs = [];
  for (let i = 0; i < n; i++) {
    // If this verse is unmatched (beyond firstUnmatchedVerse), return empty
    if (firstUnmatchedVerse !== -1 && i >= firstUnmatchedVerse) {
      // The last "real" verse gets all remaining text
      if (i === firstUnmatchedVerse - 1 || (firstUnmatchedVerse === 1 && i === 0)) {
        // Already handled by the normal path below with endToken = tokens.length-1
      }
      segs.push('');
      continue;
    }

    const startTokenIdx = splitTokenIdxs[i];
    if (startTokenIdx >= tokens.length) {
      segs.push('');
      continue;
    }

    // End token: either start of next split (minus 1), or end of tokens
    let endTokenIdx;
    if (firstUnmatchedVerse !== -1 && i === firstUnmatchedVerse - 1) {
      // Last matched verse — give it ALL remaining text
      endTokenIdx = tokens.length - 1;
    } else if (i + 1 < n) {
      endTokenIdx = splitTokenIdxs[i + 1] - 1;
    } else {
      endTokenIdx = tokens.length - 1;
    }

    if (endTokenIdx < startTokenIdx) {
      segs.push('');
    } else {
      const startChar = tokens[startTokenIdx].startChar;
      const endChar = tokens[Math.min(endTokenIdx, tokens.length - 1)].endChar;
      segs.push(chunkText.slice(startChar, endChar).trim());
    }
  }

  return segs;
}

// ─── Main chapter processor ───────────────────────────────────────────────────

function processChapter(chap) {
  const yyyPath = path.join(YYY_DIR, `${chap}.json`);
  const tclPath = path.join(TCL_DIR, `${chap}.json`);
  const outPath = path.join(OUT_DIR, `${chap}.json`);

  const yyyData = JSON.parse(fs.readFileSync(yyyPath, 'utf8'));
  const tclData = JSON.parse(fs.readFileSync(tclPath, 'utf8'));

  const tclVerses = tclData.content;
  const N = tclVerses.length;

  // TCL verse number → index in tclVerses[]
  const tclVNumToIdx = new Map();
  tclVerses.forEach((tv, idx) => tclVNumToIdx.set(tv.v, idx));
  const tclVNums = tclVerses.map(tv => tv.v);
  const maxTclVNum = Math.max(...tclVNums);

  // Output array indexed by tclVerses position
  const outputTexts = new Array(N).fill('');

  // ── Group YYY chunks by their TCL coverage, merging overlapping ones ──

  const groups = []; // { tclStart, tclEnd, yyyText }

  for (let yi = 0; yi < yyyData.content.length; yi++) {
    const yv = yyyData.content[yi];
    const yyyVNum = yv.v;
    const cleanedChunk = cleanText(yv.text || '');

    // This chunk covers TCL verses from yyyVNum up to (nextYYY.v - 1)
    let tclEnd;
    if (yi + 1 < yyyData.content.length) {
      tclEnd = yyyData.content[yi + 1].v - 1;
    } else {
      tclEnd = maxTclVNum;
    }
    tclEnd = Math.min(tclEnd, maxTclVNum);

    // Check 1: verse-number overlap merge (previous group's range includes this chunk's start)
    if (groups.length > 0) {
      const lastGroup = groups[groups.length - 1];
      if (yyyVNum <= lastGroup.tclEnd) {
        lastGroup.tclEnd = Math.max(lastGroup.tclEnd, tclEnd);
        lastGroup.yyyText = (lastGroup.yyyText + ' ' + cleanedChunk).trim();
        continue;
      }
    }

    // Check 2: content-based merge — does this chunk's text actually start
    // matching the TCL verse at yyyVNum, or does it look like a continuation
    // of a verse from the previous group?
    if (groups.length > 0 && cleanedChunk.length > 0) {
      const lastGroup = groups[groups.length - 1];
      const tclVerseAtStart = tclVerses.find(tv => tv.v === yyyVNum);
      const prevGroupEndTclVerse = tclVerses.find(tv => tv.v === lastGroup.tclEnd);

      if (tclVerseAtStart && prevGroupEndTclVerse) {
        // Score: how well does this chunk START match the TCL verse it claims?
        const chunkTokens = tokenize(cleanedChunk);
        const matchToOwn = findBestMatch(chunkTokens, tclVerseAtStart.text, 0);
        const ownScore = matchToOwn ? matchToOwn.score : 0;

        // Also score: does the chunk look like it continues the PREVIOUS group's last TCL verse?
        // (i.e., the previous group ended mid-sentence and this chunk continues it)
        const matchToPrev = findBestMatch(chunkTokens, prevGroupEndTclVerse.text, 0);
        const prevScore = matchToPrev ? matchToPrev.score : 0;

        // If this chunk doesn't match its assigned TCL verse well, but the previous
        // group's text is still "open" (chunk text is short or looks like continuation),
        // merge it.
        if (ownScore < 0.15 && lastGroup.tclEnd === yyyVNum - 1) {
          // The chunk claims verse yyyVNum but doesn't match it, and the previous
          // group ends right before. Treat this chunk as overflow of the previous group.
          lastGroup.tclEnd = Math.max(lastGroup.tclEnd, tclEnd);
          lastGroup.yyyText = (lastGroup.yyyText + ' ' + cleanedChunk).trim();
          continue;
        }
      }
    }

    groups.push({ tclStart: yyyVNum, tclEnd, yyyText: cleanedChunk });
  }

  // ── For each group, split into sub-verses ──

  for (const group of groups) {
    const coveredTclVerses = tclVerses.filter(tv => tv.v >= group.tclStart && tv.v <= group.tclEnd);

    if (coveredTclVerses.length === 0) {
      // Fallback: nearest TCL verse at or after tclStart
      const fallback = tclVerses.find(tv => tv.v >= group.tclStart);
      if (fallback) {
        const idx = tclVNumToIdx.get(fallback.v);
        if (idx !== undefined && outputTexts[idx] === '') {
          outputTexts[idx] = group.yyyText;
        }
      }
      continue;
    }

    const segments = splitChunk(group.yyyText, coveredTclVerses);

    coveredTclVerses.forEach((tv, segIdx) => {
      const idx = tclVNumToIdx.get(tv.v);
      if (idx !== undefined) {
        outputTexts[idx] = segments[segIdx] || '';
      }
    });
  }

  // ── Build output ──
  const output = {
    t: 'YYY1987',
    b: 'MAT',
    c: chap,
    content: outputTexts.map((text, idx) => ({
      v: idx + 1,
      text: text
    }))
  };

  fs.writeFileSync(outPath, JSON.stringify(output, null, 2), 'utf8');
  return N;
}

// ─── Run all 28 chapters ──────────────────────────────────────────────────────

const results = [];
let succeeded = 0;

for (let chap = 1; chap <= 28; chap++) {
  try {
    const n = processChapter(chap);
    results.push(`MAT ${chap}: ${n} verses ✓`);
    succeeded++;
  } catch (err) {
    results.push(`MAT ${chap}: ERROR — ${err.message}`);
    console.error(`Chapter ${chap} error:`, err.stack);
  }
}

console.log('\n' + results.join('\n'));
console.log(`\nTotal: ${succeeded}/28 succeeded`);
