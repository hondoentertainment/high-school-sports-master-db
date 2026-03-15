#!/usr/bin/env python3
"""
Wikidata + Sports Reference enrichment for athletes missing birth info.

Wikidata: For athletes missing birthDate, birthCity, or countryOfBirth:
  - Searches via wbsearchentities, fetches claims (P569, P19, P27) via wbgetentities
  - Fills birthDate, birthCity, birthRegion, countryOfBirth; adds wikidataId to metadata
  - Cache: .cache/wikidata_enrich.json (avoids repeat API calls)
  - Rate limit: 1–2s between requests (--rate 1.5 default)

Sports Reference: Merges manual IDs from data/sports_reference_ids.json into metadata
  (bbrefId, pfrId, hockeyReferenceId, basketballReferenceId). No bulk API.

Usage:
  npm run enrich                    # Full run (all candidates; takes hours)
  python scripts/enrich_wikidata.py --sample 100   # Validate with 100
  python scripts/enrich_wikidata.py --limit 500   # Process first 500
  python scripts/enrich_wikidata.py --rate 2      # Slower rate limit

Priority: NBA (missing birthDate) and NHL first. Only fills null/empty; never overwrites.
Preserves metadata.source; adds metadata.enrichedFrom.
"""

import argparse
import json
import re
import time
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
CACHE_DIR = ROOT / ".cache"
CACHE_FILE = CACHE_DIR / "wikidata_enrich.json"

# Wikidata property IDs
P_BIRTH_DATE = "P569"
P_BIRTH_PLACE = "P19"
P_COUNTRY_CITIZENSHIP = "P27"

# Q-id to ISO 3166-1 alpha-2 (common athlete birth countries)
WIKIDATA_COUNTRY_MAP = {
    "Q30": "US",   # United States
    "Q16": "CA",   # Canada
    "Q142": "FR",  # France
    "Q183": "DE",  # Germany
    "Q145": "GB",  # United Kingdom
    "Q17": "JP",   # Japan
    "Q408": "AU",  # Australia
    "Q39": "CH",   # Switzerland
    "Q159": "RU",  # Russia
    "Q213": "CZ",  # Czech Republic
    "Q36": "PL",   # Poland
    "Q34": "SE",   # Sweden
    "Q33": "FI",   # Finland
    "Q20": "NO",   # Norway
    "Q35": "DK",   # Denmark
    "Q29": "ES",   # Spain
    "Q38": "IT",   # Italy
    "Q41": "GR",   # Greece
    "Q403": "RS",  # Serbia
    "Q215": "SK",  # Slovakia
    "Q211": "SI",  # Slovenia
    "Q37": "LT",   # Lithuania
    "Q40": "AT",   # Austria
    "Q55": "NL",   # Netherlands
    "Q96": "MX",   # Mexico
    "Q414": "BR",  # Brazil
    "Q750": "CO",  # Colombia
    "Q298": "CL",  # Chile
    "Q77": "UY",   # Uruguay
    "Q733": "PY",  # Paraguay
    "Q419": "PE",  # Peru
    "Q241": "CU",  # Cuba
    "Q786": "DO",  # Dominican Republic
    "Q242": "PR",  # Puerto Rico
    "Q79": "EG",   # Egypt
    "Q148": "CN",  # China
    "Q865": "TW",  # Taiwan
    "Q884": "KR",  # South Korea
    "Q668": "IN",  # India
    "Q43": "TR",   # Turkey
    "Q399": "AM",  # Armenia
    "Q229": "CY",  # Cyprus
    "Q28": "HU",   # Hungary
    "Q218": "RO",   # Romania
    "Q219": "BG",  # Bulgaria
    "Q224": "HR",  # Croatia
    "Q225": "BA",  # Bosnia and Herzegovina
    "Q244": "BY",  # Belarus
    "Q212": "UA",  # Ukraine
    "Q902": "BD",  # Bangladesh
    "Q843": "KZ",  # Kazakhstan
    "Q796": "IQ",  # Iraq
    "Q794": "IR",  # Iran
    "Q1246": "XK", # Kosovo
    "Q929": "CF",  # Central African Republic
    "Q1039": "BI", # Burundi
    "Q114": "KE",  # Kenya
    "Q1028": "AO", # Angola
    "Q1025": "MA", # Morocco
    "Q1029": "CI", # Ivory Coast
    "Q1033": "NG", # Nigeria
    "Q1036": "UG", # Uganda
    "Q1037": "RW", # Rwanda
    "Q657": "TD",  # Chad
    "Q1041": "SN", # Senegal
    "Q1042": "SO", # Somalia
    "Q1044": "SD", # Sudan
    "Q1045": "SZ", # Eswatini
    "Q258": "ZA",  # South Africa
    "Q954": "ZW",  # Zimbabwe
    "Q117": "SE",  # Sweden (alias)
    "Q928": "PH",  # Philippines
    "Q424": "KH",  # Cambodia
    "Q881": "VN",  # Vietnam
    "Q836": "MM",  # Myanmar
    "Q836": "MM",  # Myanmar
    "Q819": "LA",  # Laos
    "Q869": "TH",  # Thailand
    "Q1045": "SZ", # Eswatini
}


def _none(v):
    if v is None or v == "" or (isinstance(v, str) and not v.strip()):
        return None
    s = str(v).strip()
    return s if s and s.lower() != "nan" else None


def load_cache() -> dict:
    CACHE_DIR.mkdir(exist_ok=True)
    if not CACHE_FILE.exists():
        return {}
    try:
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_cache(cache: dict) -> None:
    CACHE_DIR.mkdir(exist_ok=True)
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=2, ensure_ascii=False)


def load_athletes() -> list[dict]:
    """Load athletes from per-league JSON files."""
    merged = []
    for name in ["athletes-nba.json", "athletes-nhl.json", "athletes-nfl.json", "athletes-mlb.json"]:
        path = DATA_DIR / name
        if path.exists():
            try:
                with open(path, "r", encoding="utf-8") as f:
                    merged.extend(json.load(f))
            except Exception:
                pass
    return merged


def save_athletes_by_league(athletes: list[dict]) -> None:
    """Save athletes back to per-league files."""
    by_league: dict[str, list[dict]] = {"NBA": [], "NFL": [], "MLB": [], "NHL": []}
    for a in athletes:
        league = a.get("league", "")
        if league in by_league:
            by_league[league].append(a)
    for league, lst in by_league.items():
        path = DATA_DIR / f"athletes-{league.lower()}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(lst, f, indent=2, ensure_ascii=False, allow_nan=False)


def _has_value(val) -> bool:
    v = _none(val)
    return v is not None and len(v) > 0


def needs_enrichment_fixed(athlete: dict) -> bool:
    """True if athlete is missing birthDate, birthCity, or countryOfBirth."""
    return (
        not _has_value(athlete.get("birthDate"))
        or not _has_value(athlete.get("birthCity"))
        or not _has_value(athlete.get("countryOfBirth"))
    )


def priority_score(athlete: dict) -> int:
    """Higher = more priority. NBA missing birthDate and NHL sparse profiles first."""
    score = 0
    league = athlete.get("league", "")
    if league == "NBA":
        score += 10
    if league == "NHL":
        score += 8
    if not _has_value(athlete.get("birthDate")):
        score += 5
    if not _has_value(athlete.get("birthCity")) and not _has_value(athlete.get("countryOfBirth")):
        score += 3
    return score


def wikidata_search(session: requests.Session, name: str, league: str) -> str | None:
    """Search Wikidata for entity; return Q-id or None."""
    try:
        resp = session.get(
            "https://www.wikidata.org/w/api.php",
            params={
                "action": "wbsearchentities",
                "search": name,
                "language": "en",
                "limit": 20,
                "format": "json",
            },
            timeout=15,
            headers={"User-Agent": "HighSchoolSports/1.0 (wikidata-enrich)"},
        )
        resp.raise_for_status()
        data = resp.json()
        results = data.get("search", [])
        sport_keywords = {
            "NBA": ["basketball", "nba"],
            "NHL": ["hockey", "nhl"],
            "NFL": ["football", "nfl"],
            "MLB": ["baseball", "mlb"],
        }
        keywords = sport_keywords.get(league, [])
        for r in results:
            desc = (r.get("description") or "").lower()
            for kw in keywords:
                if kw in desc:
                    return r.get("id")
        if results:
            return results[0].get("id")
    except Exception:
        pass
    return None


def wikidata_get_entities(session: requests.Session, ids: list[str]) -> dict:
    """Fetch entity data including claims and labels for given Q-ids."""
    if not ids:
        return {}
    ids_str = "|".join(ids[:50])
    try:
        resp = session.get(
            "https://www.wikidata.org/w/api.php",
            params={
                "action": "wbgetentities",
                "ids": ids_str,
                "props": "claims|labels",
                "languages": "en",
                "format": "json",
            },
            timeout=30,
            headers={"User-Agent": "HighSchoolSports/1.0 (wikidata-enrich)"},
        )
        resp.raise_for_status()
        return resp.json().get("entities", {})
    except Exception:
        return {}


def parse_wikidata_date(time_val: str) -> str | None:
    """Convert Wikidata time (+1947-10-01T00:00:00Z) to YYYY-MM-DD."""
    if not time_val:
        return None
    m = re.match(r"\+?(-?\d{4})-(\d{2})-(\d{2})", time_val)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    return None


def resolve_place_qid(session: requests.Session, cache: dict, qid: str, label_cache: dict) -> tuple[str | None, str | None, str | None]:
    """
    Resolve a place Q-id to (birthCity, birthRegion, countryOfBirth).
    May need to fetch the entity; place can be city/region/country.
    """
    if not qid:
        return None, None, None
    cache_key = f"place:{qid}"
    if cache_key in cache:
        return tuple(cache[cache_key])

    entities = wikidata_get_entities(session, [qid])
    ent = entities.get(qid, {})
    if not ent or "missing" in ent:
        cache[cache_key] = [None, None, None]
        return None, None, None

    labels = ent.get("labels", {})
    label = (labels.get("en") or {}).get("value") if isinstance(labels.get("en"), dict) else None
    if not label and labels:
        first = next(iter(labels.values()), None)
        label = first.get("value") if isinstance(first, dict) else None

    claims = ent.get("claims", {})
    country_qid = None
    region_qid = None
    # P17 = country
    for stmt in claims.get("P17", [])[:1]:
        try:
            country_qid = stmt.get("mainsnak", {}).get("datavalue", {}).get("value", {}).get("id")
            break
        except Exception:
            pass
    # P131 = located in (administrative entity)
    for stmt in claims.get("P131", [])[:1]:
        try:
            region_qid = stmt.get("mainsnak", {}).get("datavalue", {}).get("value", {}).get("id")
            break
        except Exception:
            pass

    country = WIKIDATA_COUNTRY_MAP.get(qid) if qid in WIKIDATA_COUNTRY_MAP else None
    if not country and country_qid:
        country = WIKIDATA_COUNTRY_MAP.get(country_qid)
    if not country and country_qid and country_qid not in label_cache:
        label_cache[country_qid] = None
        ents = wikidata_get_entities(session, [country_qid])
        e = ents.get(country_qid, {})
        lbl = (e.get("labels", {}).get("en") or {}).get("value")
        if isinstance(lbl, str):
            label_cache[country_qid] = lbl
        # Map common country names to codes
        from_upper = {
            "UNITED STATES": "US", "USA": "US", "CANADA": "CA", "CA": "CA",
            "FRANCE": "FR", "GERMANY": "DE", "UNITED KINGDOM": "GB", "JAPAN": "JP",
            "AUSTRALIA": "AU", "SWITZERLAND": "CH", "RUSSIA": "RU", "CZECH REPUBLIC": "CZ",
            "POLAND": "PL", "SWEDEN": "SE", "FINLAND": "FI", "NORWAY": "NO", "DENMARK": "DK",
            "SPAIN": "ES", "ITALY": "IT", "GREECE": "GR", "SERBIA": "RS", "SLOVAKIA": "SK",
        }
        if lbl and lbl.upper() in from_upper:
            country = from_upper[lbl.upper()]

    if qid in WIKIDATA_COUNTRY_MAP:
        country = WIKIDATA_COUNTRY_MAP[qid]
        city, region = None, None
    elif label:
        # Heuristic: if it's a known country, use as country; else city
        if country:
            city = label
            region = None
        else:
            city = label
            region = None
    else:
        city, region = None, None

    result = [city, region, country]
    cache[cache_key] = result
    return tuple(result)


def extract_entity_data(entities: dict, qid: str, session: requests.Session, cache: dict, label_cache: dict) -> dict:
    """
    Extract birthDate, birthCity, birthRegion, countryOfBirth from entity claims.
    """
    out = {}
    ent = entities.get(qid, {})
    if not ent:
        return out
    claims = ent.get("claims", {})
    # P569 birth date
    for stmt in claims.get(P_BIRTH_DATE, [])[:1]:
        try:
            time_val = stmt.get("mainsnak", {}).get("datavalue", {}).get("value", {}).get("time")
            if time_val:
                out["birthDate"] = parse_wikidata_date(time_val)
                break
        except Exception:
            pass
    # P27 country of citizenship
    for stmt in claims.get(P_COUNTRY_CITIZENSHIP, [])[:1]:
        try:
            cq = stmt.get("mainsnak", {}).get("datavalue", {}).get("value", {}).get("id")
            if cq and cq in WIKIDATA_COUNTRY_MAP:
                out["countryOfBirth"] = WIKIDATA_COUNTRY_MAP[cq]
                break
        except Exception:
            pass
    # P19 birth place
    for stmt in claims.get(P_BIRTH_PLACE, [])[:1]:
        try:
            pq = stmt.get("mainsnak", {}).get("datavalue", {}).get("value", {}).get("id")
            if pq:
                city, region, country = resolve_place_qid(session, cache, pq, label_cache)
                if city and not out.get("birthCity"):
                    out["birthCity"] = city
                if region and not out.get("birthRegion"):
                    out["birthRegion"] = region
                if country and not out.get("countryOfBirth"):
                    out["countryOfBirth"] = country
                break
        except Exception:
            pass
    return out


def enrich_one(
    athlete: dict,
    session: requests.Session,
    cache: dict,
    label_cache: dict,
    rate_limit_sec: float = 1.5,
) -> bool:
    """
    Enrich one athlete from Wikidata. Returns True if any field was filled.
    Only fills null/empty; never overwrites.
    """
    name = _none(athlete.get("name"))
    league = athlete.get("league", "")
    if not name:
        return False

    cache_key = f"{name}|{league}"
    if cache_key not in cache:
        time.sleep(rate_limit_sec)
        wd_id = wikidata_search(session, name, league)
        cache[cache_key] = {"id": wd_id, "data": None}
        if wd_id:
            time.sleep(rate_limit_sec)
            entities = wikidata_get_entities(session, [wd_id])
            data = extract_entity_data(entities, wd_id, session, cache, label_cache)
            cache[cache_key]["data"] = data
        save_cache(cache)

    entry = cache.get(cache_key, {})
    wd_id = entry.get("id")
    data = entry.get("data") or {}
    if not data and not wd_id:
        return False

    changed = False
    meta = athlete.setdefault("metadata", {})
    if wd_id and not meta.get("wikidataId"):
        meta["wikidataId"] = wd_id
        changed = True

    for field in ["birthDate", "birthCity", "birthRegion", "countryOfBirth"]:
        val = data.get(field)
        if val and not _has_value(athlete.get(field)):
            athlete[field] = val
            changed = True

    if changed:
        sources = meta.get("enrichedFrom") or []
        if isinstance(sources, str):
            sources = [sources]
        if "wikidata" not in sources:
            sources.append("wikidata")
        meta["enrichedFrom"] = sources

    return changed


def merge_sports_reference(athletes: list[dict]) -> int:
    """
    Merge manual Sports Reference IDs from data/sports_reference_ids.json.
    Only fills metadata.bbrefId, pfrId, hockeyReferenceId, basketballReferenceId where missing.
    """
    path = DATA_DIR / "sports_reference_ids.json"
    if not path.exists():
        return 0
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return 0
    entries = data.get("entries", [])
    by_key = {(e.get("name", "").strip().lower(), e.get("league", "")): e for e in entries if e.get("name") and e.get("league")}
    changed = 0
    ref_keys = ["bbrefId", "pfrId", "hockeyReferenceId", "basketballReferenceId"]
    for athlete in athletes:
        key = (athlete.get("name", "").strip().lower(), athlete.get("league", ""))
        ref = by_key.get(key)
        if not ref:
            continue
        meta = athlete.setdefault("metadata", {})
        athlete_changed = False
        for k in ref_keys:
            if ref.get(k) and not _has_value(meta.get(k)):
                meta[k] = ref[k]
                changed += 1
                athlete_changed = True
        if athlete_changed:
            sources = meta.get("enrichedFrom") or []
            if isinstance(sources, str):
                sources = [sources]
            if "sports_reference" not in sources:
                sources.append("sports_reference")
            meta["enrichedFrom"] = sources
    return changed


def main():
    parser = argparse.ArgumentParser(description="Wikidata enrichment for athlete profiles")
    parser.add_argument("--sample", type=int, default=0, help="Process only N athletes (for validation)")
    parser.add_argument("--limit", type=int, default=0, help="Max athletes to process (0=all)")
    parser.add_argument("--rate", type=float, default=1.5, help="Seconds between API requests")
    args = parser.parse_args()

    athletes = load_athletes()
    candidates = [a for a in athletes if needs_enrichment_fixed(a)]
    candidates.sort(key=priority_score, reverse=True)

    total = len(candidates)
    take = args.sample or args.limit or total
    to_process = candidates[:take]

    print(f"Athletes missing birth info: {total}")
    print(f"Processing: {len(to_process)} (sample={args.sample}, limit={args.limit})")

    cache = load_cache()
    label_cache = {}
    session = requests.Session()
    changed_count = 0

    for i, athlete in enumerate(to_process):
        if enrich_one(athlete, session, cache, label_cache, args.rate):
            changed_count += 1
        if (i + 1) % 50 == 0:
            print(f"  Processed {i + 1}/{len(to_process)}, enriched {changed_count}")

    # Merge Sports Reference IDs from manual mapping
    sr_changed = merge_sports_reference(athletes)

    # Athletes were mutated in place; save back to per-league files
    athletes.sort(key=lambda a: (a.get("league", ""), a.get("id", "")))
    save_athletes_by_league(athletes)
    print(f"\nEnriched {changed_count} athletes (Wikidata), {sr_changed} (Sports Reference). Cache: {CACHE_FILE}")


if __name__ == "__main__":
    main()
