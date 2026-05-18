#!/usr/bin/env python3
"""
Translation Completeness Audit
Checks each translation directory against the canonical Protestant Bible structure.
"""

import os
import json
from pathlib import Path
from collections import defaultdict

# ── Canonical structure ──────────────────────────────────────────────────────
OT = {
    "GEN": 50, "EXO": 40, "LEV": 27, "NUM": 36, "DEU": 34,
    "JOS": 24, "JDG": 21, "RUT": 4,  "1SA": 31, "2SA": 24,
    "1KI": 22, "2KI": 25, "1CH": 29, "2CH": 36, "EZR": 10,
    "NEH": 13, "EST": 10, "JOB": 42, "PSA": 150,"PRO": 31,
    "ECC": 12, "SNG": 8,  "ISA": 66, "JER": 52, "LAM": 5,
    "EZK": 48, "DAN": 12, "HOS": 14, "JOL": 3,  "AMO": 9,
    "OBA": 1,  "JON": 4,  "MIC": 7,  "NAH": 3,  "HAB": 3,
    "ZEP": 3,  "HAG": 2,  "ZEC": 14, "MAL": 4,
}
NT = {
    "MAT": 28, "MRK": 16, "LUK": 24, "JHN": 21, "ACT": 28,
    "ROM": 16, "1CO": 16, "2CO": 13, "GAL": 6,  "EPH": 6,
    "PHP": 4,  "COL": 4,  "1TH": 5,  "2TH": 3,  "1TI": 6,
    "2TI": 4,  "TIT": 3,  "PHM": 1,  "HEB": 13, "JAS": 5,
    "1PE": 5,  "2PE": 3,  "1JN": 5,  "2JN": 1,  "3JN": 1,
    "JUD": 1,  "REV": 22,
}
CANON = {**OT, **NT}
OT_BOOKS = set(OT)
NT_BOOKS = set(NT)

TOTAL_CHAPTERS = sum(CANON.values())   # 1189

# ── Paths ────────────────────────────────────────────────────────────────────
TRANS_ROOT = Path("/Users/batuhandemircan/website building/data/translations")
OUTPUT_FILE = Path("/Users/batuhandemircan/website building/output/translation_audit.txt")
SKIP_DIRS = {"_archive", "YYY1987_REBUILT"}

# ── Build TCL02 verse index ──────────────────────────────────────────────────
def build_tcl02_verse_index():
    """Returns dict: (book, chapter) -> set of verse numbers."""
    idx = {}
    tcl02_path = TRANS_ROOT / "TCL02"
    if not tcl02_path.exists():
        return idx
    for book in CANON:
        book_path = tcl02_path / book
        if not book_path.exists():
            continue
        for ch in range(1, CANON[book] + 1):
            ch_file = book_path / f"{ch}.json"
            if not ch_file.exists():
                continue
            try:
                data = json.loads(ch_file.read_text(encoding="utf-8"))
            except Exception:
                continue
            verses = {item["v"] for item in data.get("content", []) if "v" in item}
            idx[(book, ch)] = verses
    return idx

# ── Audit one translation ────────────────────────────────────────────────────
def audit_translation(trans_dir: Path, tcl02_idx: dict) -> dict:
    name = trans_dir.name

    present_books = set()
    missing_books = set()
    missing_chapters = defaultdict(list)  # book -> [ch, ...]
    verse_flags = defaultdict(list)       # book -> [(ch, missing_count), ...]
    total_present_ch = 0
    total_expected_ch = 0

    for book, max_ch in CANON.items():
        total_expected_ch += max_ch
        book_path = trans_dir / book
        if not book_path.exists():
            missing_books.add(book)
            continue

        present_books.add(book)
        for ch in range(1, max_ch + 1):
            ch_file = book_path / f"{ch}.json"
            if not ch_file.exists():
                missing_chapters[book].append(ch)
                continue
            total_present_ch += 1

            # Per-verse check against TCL02
            ref_verses = tcl02_idx.get((book, ch))
            if ref_verses is None:
                continue
            try:
                data = json.loads(ch_file.read_text(encoding="utf-8"))
            except Exception:
                continue
            trans_verses = {item["v"] for item in data.get("content", []) if "v" in item}
            missing_v = ref_verses - trans_verses
            if len(missing_v) > 3:
                verse_flags[book].append((ch, len(missing_v)))

    coverage_pct = (total_present_ch / total_expected_ch * 100) if total_expected_ch else 0

    ot_present = present_books & OT_BOOKS
    nt_present = present_books & NT_BOOKS
    if nt_present and not ot_present:
        scope = "NT-only"
    elif ot_present and not nt_present:
        scope = "OT-only"
    else:
        scope = "Full Bible"

    return {
        "name": name,
        "coverage_pct": coverage_pct,
        "total_present_ch": total_present_ch,
        "total_expected_ch": total_expected_ch,
        "present_books": present_books,
        "missing_books": missing_books,
        "missing_chapters": dict(missing_chapters),
        "verse_flags": dict(verse_flags),
        "scope": scope,
        "book_count": len(present_books),
    }

# ── Formatting helpers ───────────────────────────────────────────────────────
def fmt_missing_books(missing_books: set) -> str:
    if not missing_books:
        return "none"
    ordered = [b for b in CANON if b in missing_books]
    return " ".join(ordered)

def fmt_missing_chapters(missing_chapters: dict) -> str:
    """Compact representation: show details if <5 total, else show count."""
    total = sum(len(v) for v in missing_chapters.values())
    if total == 0:
        return "none"
    if total > 5:
        return f"{total} chapters missing across {len(missing_chapters)} books"
    parts = []
    for book in CANON:
        if book in missing_chapters:
            chs = missing_chapters[book]
            parts.append(f"{book}:{','.join(str(c) for c in chs)}")
    return "  ".join(parts)

def fmt_verse_flags(verse_flags: dict) -> str:
    if not verse_flags:
        return "none"
    parts = []
    for book in CANON:
        if book in verse_flags:
            for ch, cnt in verse_flags[book]:
                parts.append(f"{book} ch{ch} (-{cnt}v)")
    return ", ".join(parts) if parts else "none"

# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    print("Building TCL02 verse index …", flush=True)
    tcl02_idx = build_tcl02_verse_index()
    print(f"  TCL02 index: {len(tcl02_idx)} chapters indexed\n")

    results = []
    for entry in sorted(TRANS_ROOT.iterdir()):
        if not entry.is_dir():
            continue
        if entry.name in SKIP_DIRS:
            print(f"  SKIPPED: {entry.name}")
            continue
        print(f"  Auditing {entry.name} …", flush=True)
        r = audit_translation(entry, tcl02_idx)
        results.append(r)

    # Sort by coverage % descending
    results.sort(key=lambda x: -x["coverage_pct"])

    lines = []
    SEP = "─" * 90

    # ── Summary table ────────────────────────────────────────────────────────
    lines.append("TRANSLATION COMPLETENESS AUDIT")
    lines.append(f"Canonical total: {len(CANON)} books  |  {TOTAL_CHAPTERS} chapters")
    lines.append(f"TCL02 chapters indexed: {len(tcl02_idx)}")
    lines.append(SEP)
    header = f"{'Translation':<20} {'Cov%':>6}  {'Books':>5}  {'Scope':<12}  {'Ch-Miss':>7}  Verse flags"
    lines.append(header)
    lines.append(SEP)

    for r in results:
        miss_ch_total = sum(len(v) for v in r["missing_chapters"].values())
        vflag_count = sum(len(v) for v in r["verse_flags"].values())
        vflag_str = f"{vflag_count} chapter(s)" if vflag_count else "—"
        lines.append(
            f"{r['name']:<20} {r['coverage_pct']:>5.1f}%  "
            f"{r['book_count']:>5}  {r['scope']:<12}  {miss_ch_total:>7}  {vflag_str}"
        )

    lines.append(SEP)

    # ── Per-translation detail ────────────────────────────────────────────────
    lines.append("")
    lines.append("PER-TRANSLATION DETAILS  (missing items only)")
    lines.append(SEP)

    for r in results:
        lines.append(f"[ {r['name']} ]  {r['coverage_pct']:.1f}% coverage  |  {r['scope']}")
        lines.append(f"  Books present : {r['book_count']} / {len(CANON)}")

        if r["missing_books"]:
            lines.append(f"  Missing books : {fmt_missing_books(r['missing_books'])}")
        else:
            lines.append(f"  Missing books : none")

        mc_str = fmt_missing_chapters(r["missing_chapters"])
        lines.append(f"  Missing chaps : {mc_str}")

        vf_str = fmt_verse_flags(r["verse_flags"])
        lines.append(f"  Verse flags   : {vf_str}")
        lines.append("")

    lines.append(SEP)
    lines.append("END OF AUDIT")

    output_text = "\n".join(lines)

    # Print to stdout
    print("\n" + output_text)

    # Save to file
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_FILE.write_text(output_text, encoding="utf-8")
    print(f"\n[Saved to {OUTPUT_FILE}]")

if __name__ == "__main__":
    main()
