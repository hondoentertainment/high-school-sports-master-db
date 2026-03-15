#!/usr/bin/env python3
"""
Ingest ALL players ever from NHL, NBA, MLB, NFL into the master database.
Uses public APIs and datasets. Run: python scripts/ingest_all_players.py
"""

import json
import os
import re
import subprocess
import sys
from pathlib import Path

# Project root
ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
OUTPUT = DATA_DIR / "athletes.json"


def era_from_years(years_str: str | None, start_year: int | None) -> str:
    """Classify era from years active or debut year."""
    year = None
    if start_year:
        year = start_year
    elif years_str:
        match = re.search(r"(\d{4})", str(years_str))
        if match:
            year = int(match.group(1))
    if year is None:
        return "modern"
    if year < 1950:
        return "pioneer"
    if year < 1990:
        return "golden age"
    return "modern"


def country_from_birth(birth_place: str | None) -> str:
    """Infer country code from birth place string."""
    if not birth_place:
        return "US"
    place = (birth_place or "").upper()
    if "USA" in place or "UNITED STATES" in place or ", US" in place:
        return "US"
    if "CANADA" in place or ", CA" in place or "ONTARIO" in place or "QUEBEC" in place:
        return "CA"
    if "DOMINICAN" in place or "DOM REP" in place:
        return "DO"
    if "VENEZUELA" in place:
        return "VE"
    if "PUERTO RICO" in place:
        return "PR"
    if "JAPAN" in place or "JPN" in place:
        return "JP"
    if "SWEDEN" in place:
        return "SE"
    if "FINLAND" in place:
        return "FI"
    if "RUSSIA" in place or "USSR" in place:
        return "RU"
    if "CZECH" in place:
        return "CZ"
    if "GERMANY" in place:
        return "DE"
    if "FRANCE" in place:
        return "FR"
    if "AUSTRALIA" in place:
        return "AU"
    if "MEXICO" in place:
        return "MX"
    if "CUBA" in place:
        return "CU"
    if "SOUTH KOREA" in place or "KOREA" in place:
        return "KR"
    return "US"


# --- NBA ---
def fetch_nba_players() -> list[dict]:
    """Fetch all NBA/ABA players via nba_api."""
    try:
        from nba_api.stats.endpoints import commonallplayers

        # Current season + all historical
        all_players = []
        seen_ids = set()
        # NBA API returns players per season; we need to iterate or use is_only_current_season=0
        resp = commonallplayers.CommonAllPlayers(is_only_current_season=0)
        df = resp.get_data_frames()[0]
        for _, row in df.iterrows():
            pid = str(row.get("PERSON_ID", ""))
            if pid in seen_ids:
                continue
            seen_ids.add(pid)
            from_year = row.get("FROM_YEAR") or row.get("MIN_YEAR")
            to_year = row.get("TO_YEAR") or row.get("MAX_YEAR")
            years = f"{from_year}-{to_year}" if from_year and to_year else ""
            all_players.append(
                {
                    "id": f"a-nba-{pid}",
                    "name": str(row.get("DISPLAY_FIRST_LAST", row.get("FULL_NAME", "Unknown"))),
                    "league": "NBA",
                    "sport": "basketball",
                    "nationality": "US",
                    "countryOfBirth": "US",
                    "yearsActive": years,
                    "era": era_from_years(years, int(from_year) if from_year else None),
                    "position": str(row.get("POSITION", "")) or None,
                    "teams": [],
                    "metadata": {"nbaId": pid},
                }
            )
        return all_players
    except ImportError:
        print("Run: pip install nba_api")
        return []
    except Exception as e:
        print(f"NBA fetch error: {e}")
        return []


# --- NFL ---
def fetch_nfl_players() -> list[dict]:
    """Fetch all NFL players from nflverse CSV."""
    import urllib.request

    url = "https://github.com/nflverse/nflverse-data/releases/download/players/players.csv"
    try:
        with urllib.request.urlopen(url, timeout=60) as r:
            text = r.read().decode("utf-8")
    except Exception as e:
        print(f"NFL fetch error: {e}")
        return []

    lines = [l for l in text.strip().split("\n") if l]
    if not lines:
        return []
    header = [h.strip('"') for h in lines[0].split(",")]
    rows = []
    for line in lines[1:]:
        vals = _parse_csv_line(line) if '"' in line else [x.strip() for x in line.split(",")]
        rows.append(dict(zip(header, vals + [""] * (len(header) - len(vals)))))

    athletes = []
    seen = set()
    for i, r in enumerate(rows):
        nfl_id = str(r.get("gsis_id") or r.get("nfl_id") or i)
        if nfl_id in seen or nfl_id == "nan":
            continue
        seen.add(nfl_id)
        name = (r.get("display_name") or r.get("name") or f"{r.get('first_name','')} {r.get('last_name','')}".strip()).strip()
        if not name or name == "nan":
            continue
        birth = r.get("birth_place") or ""
        draft_yr = r.get("draft_year")
        try:
            year = int(draft_yr) if draft_yr and str(draft_yr).replace(".0", "").isdigit() else None
        except (ValueError, TypeError):
            year = None
        athletes.append(
            {
                "id": f"a-nfl-{nfl_id}",
                "name": name,
                "league": "NFL",
                "sport": "football",
                "nationality": country_from_birth(birth),
                "countryOfBirth": country_from_birth(birth),
                "yearsActive": str(draft_yr) if draft_yr else None,
                "era": era_from_years(None, year),
                "position": r.get("position") or None,
                "teams": [],
                "metadata": {"nflId": nfl_id},
            }
        )
    return athletes


# --- MLB ---
def _parse_csv_line(line: str) -> list[str]:
    vals, cur, inq = [], "", False
    for c in line:
        if c == '"':
            inq = not inq
        elif c == "," and not inq:
            vals.append(cur.strip('"'))
            cur = ""
        else:
            cur += c
    vals.append(cur.strip('"'))
    return vals


def fetch_mlb_players() -> list[dict]:
    """Fetch MLB players from Chadwick Bureau register (mlb_played = MLB only)."""
    import urllib.request

    # Chadwick register: people-0..9, people-a..f
    suffixes = [str(i) for i in range(10)] + ["a", "b", "c", "d", "e", "f"]
    base = "https://raw.githubusercontent.com/chadwickbureau/register/master/data"
    athletes = []
    for suf in suffixes:
        url = f"{base}/people-{suf}.csv"
        try:
            with urllib.request.urlopen(url, timeout=60) as r:
                text = r.read().decode("utf-8")
        except Exception as e:
            print(f"  MLB: Skip {url}: {e}")
            continue
        lines = [l for l in text.strip().split("\n") if l]
        if not lines:
            continue
        header = _parse_csv_line(lines[0])
        for line in lines[1:]:
            vals = _parse_csv_line(line)
            r = dict(zip(header, vals + [""] * (len(header) - len(vals))))
            # Only MLB players (have mlb_played_first)
            if not r.get("mlb_played_first", "").strip():
                continue
            pid = r.get("key_mlbam") or r.get("key_person") or r.get("key_bbref", "").strip()
            if not pid:
                continue
            # Avoid duplicates (key_mlbam can repeat across files)
            name = f"{r.get('name_first','')} {r.get('name_last','')}".strip()
            if not name:
                name = r.get("name_given") or pid
            year = None
            mlb_first = r.get("mlb_played_first", "")
            if mlb_first and mlb_first.isdigit():
                year = int(mlb_first)
            athletes.append(
                {
                    "id": f"a-mlb-{pid}",
                    "name": name,
                    "league": "MLB",
                    "sport": "baseball",
                    "nationality": "US",
                    "countryOfBirth": "US",
                    "yearsActive": f"{r.get('mlb_played_first','')}-{r.get('mlb_played_last','')}" or None,
                    "era": era_from_years(None, year),
                    "position": None,
                    "teams": [],
                    "metadata": {"mlbId": pid},
                }
            )
    # Deduplicate by id
    seen = set()
    unique = []
    for a in athletes:
        if a["id"] not in seen:
            seen.add(a["id"])
            unique.append(a)
    return unique


# --- NHL ---
def fetch_nhl_players() -> list[dict]:
    """Fetch all-time NHL players from the packaged registry."""
    helper = ROOT / "scripts" / "export_nhl_players.mjs"
    try:
        result = subprocess.run(
            ["node", str(helper)],
            capture_output=True,
            text=True,
            check=True,
        )
        players = json.loads(result.stdout)
    except Exception as e:
        print(f"NHL fetch error: {e}")
        return []

    return players


def merge_with_existing(existing: list[dict], ingested: list[dict], league: str) -> list[dict]:
    """Merge ingested players with existing; prefer existing for duplicates by name+league."""
    by_key = {}
    for a in existing:
        if a.get("league") == league:
            key = a.get("name", "").strip().lower()
            by_key[key] = a
    for a in ingested:
        key = a.get("name", "").strip().lower()
        if key not in by_key:
            by_key[key] = a
    return [a for a in by_key.values() if a.get("league") == league]


def main():
    os.makedirs(DATA_DIR, exist_ok=True)

    # Load existing athletes (to preserve affiliations and manually added data)
    existing_path = DATA_DIR / "athletes.json"
    existing = []
    if existing_path.exists():
        with open(existing_path, "r", encoding="utf-8") as f:
            existing = json.load(f)

    print("Fetching NBA players...")
    nba = fetch_nba_players()
    print(f"  NBA: {len(nba)} players")

    print("Fetching NFL players...")
    nfl = fetch_nfl_players()
    print(f"  NFL: {len(nfl)} players")

    print("Fetching MLB players...")
    mlb = fetch_mlb_players()
    print(f"  MLB: {len(mlb)} players")

    print("Fetching NHL players...")
    nhl = fetch_nhl_players()
    print(f"  NHL: {len(nhl)} players")

    # Build merged list: existing first (keep IDs for affiliations), then add ingested
    # Avoid duplicates by (name_normalized, league)
    existing_ids = {a["id"] for a in existing}
    existing_keys = {(a.get("name", "").strip().lower(), a.get("league", "")) for a in existing}
    all_athletes = list(existing)

    def add_league(league: str, players: list[dict]):
        for p in players:
            key = (p.get("name", "").strip().lower(), league)
            if key in existing_keys:
                continue
            pid = p.get("id", "")
            if pid in existing_ids:
                continue
            existing_keys.add(key)
            existing_ids.add(pid)
            all_athletes.append(p)

    add_league("NBA", nba)
    add_league("NFL", nfl)
    add_league("MLB", mlb)
    add_league("NHL", nhl)

    # Sort: by league, then by id
    def sort_key(a):
        lid = a.get("id", "")
        m = re.match(r"a-(nba|nfl|mlb|nhl)-(\d+)", lid)
        if m:
            league_order = {"NBA": 0, "NFL": 1, "MLB": 2, "NHL": 3}.get(m.group(1).upper(), 4)
            num = int(m.group(2))
            return (league_order, num)
        return (4, 0)

    all_athletes.sort(key=lambda a: (a.get("league", ""), a.get("id", "")))

    with open(OUTPUT, "w", encoding="utf-8") as f:
        json.dump(all_athletes, f, indent=2, ensure_ascii=False)

    print(f"\nTotal: {len(all_athletes)} athletes")
    print(f"Written to {OUTPUT}")
    for league in ["NBA", "NFL", "MLB", "NHL"]:
        count = sum(1 for a in all_athletes if a.get("league") == league)
        print(f"  {league}: {count}")


if __name__ == "__main__":
    main()
