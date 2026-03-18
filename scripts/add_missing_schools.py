#!/usr/bin/env python3
"""
Add all high schools from education_affiliations that have 1+ players but are not
in schools.json. Derives schools from education_affiliations (highSchool, prepSchool, academy).
"""

from __future__ import annotations

import json
import re
import unicodedata
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"

# Likely colleges/programs to skip (not high schools)
SKIP_PATTERNS = [
    "mississippi", "tennessee", "east tennessee", "hofstra",
    "merchant marine", "oklahoma military academy", "jc)", "j.c.)",
]

# Map observed names to existing school IDs (avoid duplicates)
SCHOOL_ALIAS_TO_ID = {
    "dominguez hs": "s-dominguez",
    "dominguez high school": "s-dominguez",
    "south kent hs": "s-south-kent",
    "south kent high school": "s-south-kent",
    "farragut academy hs": "s-farragut-chicago",
    "farragut academy high school": "s-farragut-chicago",
    "bloomington high school north": "s-bloomington-north",
}

# Infer region from parenthetical like (FL), (TX), (CA)
STATE_ABBREV = {
    "al": "Alabama", "ak": "Alaska", "az": "Arizona", "ar": "Arkansas", "ca": "California",
    "co": "Colorado", "ct": "Connecticut", "de": "Delaware", "fl": "Florida", "ga": "Georgia",
    "hi": "Hawaii", "id": "Idaho", "il": "Illinois", "in": "Indiana", "ia": "Iowa",
    "ks": "Kansas", "ky": "Kentucky", "la": "Louisiana", "me": "Maine", "md": "Maryland",
    "ma": "Massachusetts", "mi": "Michigan", "mn": "Minnesota", "ms": "Mississippi",
    "mo": "Missouri", "mt": "Montana", "ne": "Nebraska", "nv": "Nevada", "nh": "New Hampshire",
    "nj": "New Jersey", "nm": "New Mexico", "ny": "New York", "nc": "North Carolina",
    "nd": "North Dakota", "oh": "Ohio", "ok": "Oklahoma", "or": "Oregon", "pa": "Pennsylvania",
    "ri": "Rhode Island", "sc": "South Carolina", "sd": "South Dakota", "tn": "Tennessee",
    "tx": "Texas", "ut": "Utah", "vt": "Vermont", "va": "Virginia", "wa": "Washington",
    "wv": "West Virginia", "wi": "Wisconsin", "wy": "Wyoming", "dc": "District of Columbia",
}


def slugify(value: str) -> str:
    text = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    text = text.lower().replace("&", " and ")
    text = re.sub(r"\([^)]*\)", " ", text)
    text = re.sub(r"\bhs\b", "high school", text)
    text = re.sub(r"\bst\.\b", "saint", text)
    text = re.sub(r"[^a-z0-9]+", "-", text)
    return re.sub(r"-+", "-", text).strip("-")


def normalize_name(value: str) -> str:
    return slugify(value)


def strip_trailing_state(key: str) -> str:
    """Remove trailing -fl, -tx, -oh etc. from slug for matching."""
    return re.sub(r"-(al|ak|az|ar|ca|co|ct|de|fl|ga|hi|id|il|in|ia|ks|ky|la|me|md|ma|mi|mn|ms|mo|mt|ne|nv|nh|nj|nm|ny|nc|nd|oh|ok|or|pa|ri|sc|sd|tn|tx|ut|vt|va|wa|wv|wi|wy|dc)$", "", key)


def build_school_lookup(schools: list[dict]) -> tuple[dict[str, dict], set[str]]:
    lookup: dict[str, dict] = {}
    all_keys: set[str] = set()
    for school in schools:
        for name in [
            school["name"],
            school["name"].replace("High School", "HS"),
            school["name"].replace(" High School", ""),
            school["name"].replace("Saint", "St."),
            school["name"].replace("Preparatory", "Prep"),
        ]:
            key = normalize_name(name)
            if key and len(key) > 2:
                lookup.setdefault(key, school)
                all_keys.add(key)
                all_keys.add(strip_trailing_state(key))
    return lookup, all_keys


def normalize_alias(value: str) -> str:
    """Normalize free-form alias keys for robust matching."""
    return re.sub(r"-+", "-", slugify(value.replace(".", ""))).strip("-")


def infer_region_from_name(name: str) -> tuple[str | None, str | None]:
    """Extract (FL), (TX) etc. from name. Returns (region, None)."""
    m = re.search(r"\(([A-Za-z]{2})\)", name)
    if m:
        abbr = m.group(1).lower()
        return STATE_ABBREV.get(abbr, m.group(1)), None
    m = re.search(r"\((PA|NJ|NY|NC|OH|CA|TX|FL|GA|IL|IN|CT|VA|MS|MN)\)", name, re.I)
    if m:
        abbr = m.group(1).lower()
        return STATE_ABBREV.get(abbr, m.group(1)), None
    return None, None


def should_skip(name: str) -> bool:
    n = name.lower()
    return any(p in n for p in SKIP_PATTERNS) or n in ("tennessee", "mississippi")


def main() -> int:
    schools = json.load(open(DATA_DIR / "schools.json", encoding="utf-8"))
    ed_aff = json.load(open(DATA_DIR / "education_affiliations.json", encoding="utf-8"))

    school_lookup, existing_normalized = build_school_lookup(schools)
    existing_ids = {s["id"] for s in schools}
    alias_keys = {normalize_alias(k) for k in SCHOOL_ALIAS_TO_ID}

    # Collect unique institution names with 1+ players
    from collections import Counter
    name_counts: Counter[str] = Counter()
    for row in ed_aff:
        if row.get("educationType") in ("highSchool", "prepSchool", "academy"):
            name = (row.get("institutionName") or "").strip()
            if name:
                name_counts[name] += 1

    added: list[dict] = []
    for name, count in name_counts.items():
        if should_skip(name):
            continue
        key = normalize_name(name)
        if not key:
            continue
        # Skip if mapped to existing school
        if normalize_alias(name) in alias_keys:
            continue
        core = strip_trailing_state(key)
        if core in alias_keys:
            continue
        # Skip if exact or core matches existing
        if key in existing_normalized or core in existing_normalized:
            continue
        region, _ = infer_region_from_name(name)
        clean_name = re.sub(r"\s*\([^)]*\)\s*$", "", name).strip()
        if not clean_name:
            clean_name = name
        school_id = "s-" + slugify(clean_name)[:50].strip("-")
        if school_id in existing_ids:
            base = school_id
            for i in range(1, 100):
                school_id = f"{base}-{i}"
                if school_id not in existing_ids:
                    break
        existing_ids.add(school_id)
        existing_normalized.add(key)
        added.append({
            "id": school_id,
            "name": clean_name,
            "city": "",
            "country": "US",
            "region": region or "",
            "sportFocus": ["basketball", "football", "baseball"],
            "leagues": ["NBA", "NFL", "MLB", "NHL"],
            "metadata": {"addedFrom": "education_affiliations", "playerCount": count},
        })

    if not added:
        print("No new schools to add.")
        return 0

    # Merge and sort
    all_schools = schools + added
    all_schools.sort(key=lambda s: s["name"].lower())
    json.dump(all_schools, open(DATA_DIR / "schools.json", "w", encoding="utf-8"), indent=2, ensure_ascii=False)
    print(f"Added {len(added)} schools:")
    for s in added:
        print(f"  {s['id']}: {s['name']} ({s['metadata']['playerCount']} players)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
