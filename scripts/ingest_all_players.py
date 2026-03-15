#!/usr/bin/env python3
"""
Ingest ALL players ever from NHL, NBA, MLB, NFL into the master database.
Uses public APIs and datasets. Run: python scripts/ingest_all_players.py

Athlete IDs are deterministic: a-{league}-{externalId} derived from source APIs
(nbaId, nflId/gsis_id, mlbId/key_mlbam, nhlId) so re-runs do not change IDs.
"""

import json
import math
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests

# Project root
ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
OUTPUT = DATA_DIR / "athletes.json"
CACHE_DIR = ROOT / ".cache"


COUNTRY_CODE_MAP = {
    "USA": "US",
    "UNITED STATES": "US",
    "UNITED STATES OF AMERICA": "US",
    "CANADA": "CA",
    "CAN": "CA",
    "JAPAN": "JP",
    "JPN": "JP",
    "PUERTO RICO": "PR",
    "DOMINICAN REPUBLIC": "DO",
    "DOM REP": "DO",
    "VENEZUELA": "VE",
    "MEXICO": "MX",
    "CUBA": "CU",
    "NIGERIA": "NG",
    "NEW ZEALAND": "NZ",
    "AUSTRALIA": "AU",
    "FRANCE": "FR",
    "GERMANY": "DE",
    "SPAIN": "ES",
    "ITALY": "IT",
    "GREECE": "GR",
    "SERBIA": "RS",
    "SLOVENIA": "SI",
    "LITHUANIA": "LT",
    "LATVIA": "LV",
    "ESTONIA": "EE",
    "FINLAND": "FI",
    "SWEDEN": "SE",
    "NORWAY": "NO",
    "DENMARK": "DK",
    "RUSSIA": "RU",
    "USSR": "RU",
    "CZECH REPUBLIC": "CZ",
    "CZECHIA": "CZ",
    "CZE": "CZ",
    "SLOVAKIA": "SK",
    "SVK": "SK",
    "BELARUS": "BY",
    "UKRAINE": "UA",
    "SWITZERLAND": "CH",
    "CHE": "CH",
    "AUSTRIA": "AT",
    "NETHERLANDS": "NL",
    "UNITED KINGDOM": "GB",
    "ENGLAND": "GB",
    "SCOTLAND": "GB",
    "WALES": "GB",
    "SOUTH KOREA": "KR",
    "KOREA": "KR",
    "BRAZIL": "BR",
    "ARGENTINA": "AR",
    "COLOMBIA": "CO",
    "CURACAO": "CW",
    "ARUBA": "AW",
    "PANAMA": "PA",
    "BAHAMAS": "BS",
    "JAMAICA": "JM",
    "U.S. VIRGIN ISLANDS": "VI",
    "VIRGIN ISLANDS": "VI",
    "CHINA": "CN",
    "TAIWAN": "TW",
    "SOUTH AFRICA": "ZA",
    "CAMEROON": "CM",
    "SENEGAL": "SN",
}

# Notable players for Wikidata enrichment when missing (name, league)
NOTABLE_PLAYERS = [
    ("LeBron James", "NBA"),
    ("Michael Jordan", "NBA"),
    ("Kobe Bryant", "NBA"),
    ("Stephen Curry", "NBA"),
    ("Kevin Durant", "NBA"),
    ("Magic Johnson", "NBA"),
    ("Larry Bird", "NBA"),
    ("Tom Brady", "NFL"),
    ("Peyton Manning", "NFL"),
    ("Jerry Rice", "NFL"),
    ("Walter Payton", "NFL"),
    ("Joe Montana", "NFL"),
    ("Patrick Mahomes", "NFL"),
    ("Wayne Gretzky", "NHL"),
    ("Mario Lemieux", "NHL"),
    ("Bobby Orr", "NHL"),
    ("Sidney Crosby", "NHL"),
    ("Alex Ovechkin", "NHL"),
    ("Connor McDavid", "NHL"),
]


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
        return None
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
    return None


def none_if_empty(value):
    """Convert empty-like source values to None."""
    if value is None:
        return None
    if isinstance(value, float):
        if math.isnan(value):
            return None
        return value
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None
    return text


def normalize_country_code(value: str | None) -> str | None:
    """Normalize country names/codes into a compact code when possible."""
    text = none_if_empty(value)
    if not text:
        return None
    upper = text.upper()
    if len(upper) == 2:
        return upper
    if len(upper) == 3 and upper in {"USA", "CAN", "JPN", "SWE", "FIN", "RUS", "CZE", "SVK", "CHE"}:
        return COUNTRY_CODE_MAP.get(upper, upper)
    return COUNTRY_CODE_MAP.get(upper, upper)


def localized_text(value):
    """Extract a string from plain text or NHL-style localized objects."""
    if value is None:
        return None
    if isinstance(value, dict):
        return none_if_empty(value.get("default") or value.get("fr"))
    return none_if_empty(value)


def set_education(player: dict, *, school: str | None = None, last_affiliation: str | None = None, college: str | None = None) -> None:
    """Populate best-effort education fields from league profile data."""
    education = dict(player.get("education") or {})
    candidate_values = []
    if school:
        candidate_values.append(school)
    if last_affiliation:
        candidate_values.append(last_affiliation.split("/")[0].strip())
    if college:
        candidate_values.append(college)

    for value in candidate_values:
        text = none_if_empty(value)
        if not text:
            continue
        upper = text.upper()
        if " HS" in upper or "HIGH SCHOOL" in upper:
            education.setdefault("highSchool", [])
            if text not in education["highSchool"]:
                education["highSchool"].append(text)
        elif "PREP" in upper or "PREPARATORY" in upper:
            education.setdefault("prepSchool", [])
            if text not in education["prepSchool"]:
                education["prepSchool"].append(text)
        elif "ACADEMY" in upper:
            education.setdefault("academy", [])
            if text not in education["academy"]:
                education["academy"].append(text)
        else:
            education.setdefault("college", [])
            if text not in education["college"]:
                education["college"].append(text)

    last_text = none_if_empty(last_affiliation)
    if last_text:
        education["lastAffiliation"] = last_text

    if education:
        player["education"] = education


def merge_player_record(target: dict, source: dict) -> None:
    """Merge league-derived data into a pinned athlete while preserving the pinned ID."""
    for key, value in source.items():
        if key == "id" or value in (None, "", [], {}):
            continue
        if key == "metadata":
            target.setdefault("metadata", {}).update(value)
            continue
        if key == "education":
            education = dict(target.get("education") or {})
            for edu_key, edu_value in (value or {}).items():
                if isinstance(edu_value, list):
                    education.setdefault(edu_key, [])
                    for item in edu_value:
                        if item not in education[edu_key]:
                            education[edu_key].append(item)
                elif edu_value:
                    education[edu_key] = edu_value
            if education:
                target["education"] = education
            continue
        if key in {"teams", "awards", "honors"}:
            target.setdefault(key, [])
            for item in value:
                if item not in target[key]:
                    target[key].append(item)
            continue
        target[key] = value


def load_cache(name: str) -> dict:
    CACHE_DIR.mkdir(exist_ok=True)
    path = CACHE_DIR / name
    if not path.exists():
        return {}
    try:
        with open(path, "r", encoding="utf-8") as file:
            return json.load(file)
    except Exception:
        return {}


def save_cache(name: str, payload: dict) -> None:
    CACHE_DIR.mkdir(exist_ok=True)
    with open(CACHE_DIR / name, "w", encoding="utf-8") as file:
        json.dump(payload, file, ensure_ascii=False)


def load_json_from_git(git_path: str) -> list[dict]:
    """Load JSON from the last committed version of a file."""
    try:
        result = subprocess.run(
            ["git", "show", f"HEAD:{git_path}"],
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=True,
        )
        return json.loads(result.stdout)
    except Exception:
        return []


def load_json_with_git_fallback(path: Path, git_path: str) -> list[dict]:
    """Load JSON from disk, falling back to HEAD when the working copy is empty or invalid."""
    try:
        if path.exists() and path.stat().st_size > 0:
            with open(path, "r", encoding="utf-8") as file:
                return json.load(file)
    except Exception:
        pass

    return load_json_from_git(git_path)


# --- NBA ---
def fetch_nba_players() -> list[dict]:
    """Fetch all historical NBA players via PlayerIndex."""
    try:
        from nba_api.stats.endpoints import playerindex

        frame = playerindex.PlayerIndex(historical_nullable=1, timeout=60).get_data_frames()[0]
        all_players = []
        seen_ids = set()
        for _, row in frame.iterrows():
            pid = str(row.get("PERSON_ID", ""))
            if pid in seen_ids:
                continue
            seen_ids.add(pid)
            from_year = row.get("FROM_YEAR") or row.get("MIN_YEAR")
            to_year = row.get("TO_YEAR") or row.get("MAX_YEAR")
            years = f"{from_year}-{to_year}" if from_year and to_year else ""
            team_abbr = none_if_empty(row.get("TEAM_ABBREVIATION"))
            team_name = none_if_empty(row.get("TEAM_NAME"))
            country = normalize_country_code(row.get("COUNTRY"))
            all_players.append(
                {
                    "id": f"a-nba-{pid}",
                    "name": f"{row.get('PLAYER_FIRST_NAME', '')} {row.get('PLAYER_LAST_NAME', '')}".strip() or "Unknown",
                    "league": "NBA",
                    "sport": "basketball",
                    "nationality": country,
                    "countryOfBirth": country,
                    "yearsActive": years,
                    "era": era_from_years(years, int(from_year) if from_year else None),
                    "position": str(row.get("POSITION", "")) or None,
                    "height": none_if_empty(row.get("HEIGHT")),
                    "weight": none_if_empty(row.get("WEIGHT")),
                    "teams": [team_name or team_abbr] if team_name or team_abbr else [],
                    "education": {"college": [none_if_empty(row.get("COLLEGE"))]} if none_if_empty(row.get("COLLEGE")) else None,
                    "metadata": {
                        "nbaId": pid,
                        "source": "nba_api",
                        "playerSlug": none_if_empty(row.get("PLAYER_SLUG")),
                        "jerseyNumber": none_if_empty(row.get("JERSEY_NUMBER")),
                        "teamAbbreviation": team_abbr,
                        "draftYear": none_if_empty(row.get("DRAFT_YEAR")),
                        "draftRound": none_if_empty(row.get("DRAFT_ROUND")),
                        "draftNumber": none_if_empty(row.get("DRAFT_NUMBER")),
                    },
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
        draft_yr = r.get("draft_year")
        rookie_season = none_if_empty(r.get("rookie_season"))
        last_season = none_if_empty(r.get("last_season"))
        latest_team = none_if_empty(r.get("latest_team"))
        try:
            year = int(rookie_season or draft_yr) if (rookie_season or draft_yr) and str(rookie_season or draft_yr).replace(".0", "").isdigit() else None
        except (ValueError, TypeError):
            year = None
        years_active = None
        if rookie_season and last_season:
            years_active = f"{rookie_season}-{last_season}"
        elif rookie_season:
            years_active = rookie_season
        elif draft_yr:
            years_active = str(draft_yr)
        athletes.append(
            {
                "id": f"a-nfl-{nfl_id}",
                "name": name,
                "league": "NFL",
                "sport": "football",
                "nationality": None,
                "countryOfBirth": None,
                "birthDate": none_if_empty(r.get("birth_date")),
                "yearsActive": years_active,
                "era": era_from_years(None, year),
                "position": r.get("position") or None,
                "height": none_if_empty(r.get("height")),
                "weight": none_if_empty(r.get("weight")),
                "teams": [latest_team] if latest_team else [],
                "education": {"college": [none_if_empty(r.get("college_name"))]} if none_if_empty(r.get("college_name")) else None,
                "active": True if none_if_empty(r.get("status")) in {"ACT", "DEV", "RES", "ROSTER", "CUT", "TRD"} else None,
                "metadata": {
                    "nflId": nfl_id,
                    "source": "nflverse",
                    "positionGroup": none_if_empty(r.get("position_group")),
                    "collegeName": none_if_empty(r.get("college_name")),
                    "draftTeam": none_if_empty(r.get("draft_team")),
                    "status": none_if_empty(r.get("status")),
                    "yearsOfExperience": none_if_empty(r.get("years_of_experience")),
                    "esbId": none_if_empty(r.get("esb_id")),
                    "pfrId": none_if_empty(r.get("pfr_id")),
                    "pffId": none_if_empty(r.get("pff_id")),
                    "espnId": none_if_empty(r.get("espn_id")),
                    "smartId": none_if_empty(r.get("smart_id")),
                    "draftYear": none_if_empty(r.get("draft_year")),
                    "draftRound": none_if_empty(r.get("draft_round")),
                    "draftPick": none_if_empty(r.get("draft_pick")),
                },
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
            suffix = none_if_empty(r.get("name_suffix"))
            name = f"{r.get('name_first','')} {r.get('name_last','')}".strip()
            if suffix:
                name = f"{name} {suffix}".strip()
            if not name:
                name = r.get("name_given") or pid
            year = None
            mlb_first = r.get("mlb_played_first", "")
            if mlb_first and mlb_first.isdigit():
                year = int(mlb_first)
            mlb_last = none_if_empty(r.get("mlb_played_last"))
            athletes.append(
                {
                    "id": f"a-mlb-{pid}",
                    "name": name,
                    "league": "MLB",
                    "sport": "baseball",
                    "nationality": None,
                    "countryOfBirth": None,
                    "yearsActive": f"{mlb_first}-{mlb_last}" if mlb_first and mlb_last else mlb_first or mlb_last,
                    "era": era_from_years(None, year),
                    "position": None,
                    "birthDate": "-".join(part.zfill(2) if idx else part for idx, part in enumerate([
                        none_if_empty(r.get("birth_year")) or "",
                        none_if_empty(r.get("birth_month")) or "",
                        none_if_empty(r.get("birth_day")) or "",
                    ])) if none_if_empty(r.get("birth_year")) and none_if_empty(r.get("birth_month")) and none_if_empty(r.get("birth_day")) else None,
                    "teams": [],
                    "metadata": {
                        "mlbId": pid,
                        "source": "chadwick_register",
                        "personKey": none_if_empty(r.get("key_person")),
                        "bbrefId": none_if_empty(r.get("key_bbref")),
                        "retroId": none_if_empty(r.get("key_retro")),
                        "fangraphsId": none_if_empty(r.get("key_fangraphs")),
                        "wikidataId": none_if_empty(r.get("key_wikidata")),
                        "birthYear": none_if_empty(r.get("birth_year")),
                        "birthMonth": none_if_empty(r.get("birth_month")),
                        "birthDay": none_if_empty(r.get("birth_day")),
                        "deathYear": none_if_empty(r.get("death_year")),
                    },
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
            encoding="utf-8",
            errors="replace",
            check=True,
        )
        players = json.loads(result.stdout)
    except Exception as e:
        print(f"NHL fetch error: {e}")
        return []

    return players


def enrich_nba_players(players: list[dict]) -> None:
    """Enrich NBA players with cached CommonPlayerInfo responses."""
    cache = load_cache("nba_commonplayerinfo.json")
    targets = []
    for player in players:
        if player.get("league") != "NBA":
            continue
        nba_id = none_if_empty(player.get("metadata", {}).get("nbaId")) or player["id"].replace("a-nba-", "")
        if nba_id:
            targets.append((player, str(nba_id)))

    missing_ids = sorted({nba_id for _, nba_id in targets if nba_id not in cache})
    if missing_ids:
        print(f"Enriching NBA profiles: {len(missing_ids)} uncached")
        from nba_api.stats.endpoints import commonplayerinfo

        completed = 0

        def fetch_profile(nba_id: str):
            frame = commonplayerinfo.CommonPlayerInfo(player_id=nba_id, timeout=60).get_data_frames()[0]
            rows = frame.to_dict(orient="records")
            return nba_id, (rows[0] if rows else None)

        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = {executor.submit(fetch_profile, nba_id): nba_id for nba_id in missing_ids}
            for future in as_completed(futures):
                nba_id = futures[future]
                try:
                    _, profile = future.result()
                except Exception:
                    profile = None
                cache[nba_id] = profile
                completed += 1
                if completed % 250 == 0:
                    print(f"  NBA enriched {completed}/{len(missing_ids)}")
                    save_cache("nba_commonplayerinfo.json", cache)
        save_cache("nba_commonplayerinfo.json", cache)

    for player, nba_id in targets:
        profile = cache.get(str(nba_id)) or {}
        if not profile:
            continue
        country = normalize_country_code(profile.get("COUNTRY"))
        position = none_if_empty(profile.get("POSITION"))
        team_name = none_if_empty(profile.get("TEAM_NAME"))
        from_year = none_if_empty(profile.get("FROM_YEAR"))
        to_year = none_if_empty(profile.get("TO_YEAR"))

        if country:
            player["nationality"] = country
            player["countryOfBirth"] = player.get("countryOfBirth") or country
        if position:
            player["position"] = position
        if from_year and to_year:
            player["yearsActive"] = f"{from_year}-{to_year}"
            try:
                player["era"] = era_from_years(player["yearsActive"], int(from_year))
            except ValueError:
                pass
        if team_name and team_name not in player.get("teams", []):
            player.setdefault("teams", []).append(team_name)
        if none_if_empty(profile.get("BIRTHDATE")):
            player["birthDate"] = none_if_empty(profile.get("BIRTHDATE"))
        if none_if_empty(profile.get("HEIGHT")):
            player["height"] = none_if_empty(profile.get("HEIGHT"))
        if none_if_empty(profile.get("WEIGHT")):
            player["weight"] = none_if_empty(profile.get("WEIGHT"))
        player["active"] = profile.get("ROSTERSTATUS") == "Active"
        player["education"] = {}
        set_education(
            player,
            school=none_if_empty(profile.get("SCHOOL")),
            last_affiliation=none_if_empty(profile.get("LAST_AFFILIATION")),
        )
        honors = list(player.get("honors") or [])
        if profile.get("GREATEST_75_FLAG") == "Y" and "NBA 75th Anniversary Team" not in honors:
            honors.append("NBA 75th Anniversary Team")
        if honors:
            player["honors"] = honors

        player.setdefault("metadata", {}).update(
            {
                "source": "nba_api",
                "school": none_if_empty(profile.get("SCHOOL")),
                "lastAffiliation": none_if_empty(profile.get("LAST_AFFILIATION")),
                "draftYear": none_if_empty(profile.get("DRAFT_YEAR")),
                "draftRound": none_if_empty(profile.get("DRAFT_ROUND")),
                "draftNumber": none_if_empty(profile.get("DRAFT_NUMBER")),
                "jerseyNumber": none_if_empty(profile.get("JERSEY")),
                "teamAbbreviation": none_if_empty(profile.get("TEAM_ABBREVIATION")),
                "greatest75Flag": none_if_empty(profile.get("GREATEST_75_FLAG")),
            }
        )


def enrich_mlb_players(players: list[dict]) -> None:
    """Enrich MLB players with bulk MLB Stats API profiles."""
    cache = load_cache("mlb_people.json")
    search_cache = load_cache("mlb_name_search.json")
    targets = []
    session = requests.Session()

    def resolve_mlbam_by_name(name: str) -> str | None:
        cached = search_cache.get(name)
        if cached is not None:
            return cached
        response = session.get(
            "https://statsapi.mlb.com/api/v1/people/search",
            params={"sportIds": 1, "names": name},
            timeout=60,
        )
        response.raise_for_status()
        people = response.json().get("people", [])
        exact = next((person for person in people if person.get("fullName") == name), None)
        resolved = str(exact["id"]) if exact else None
        search_cache[name] = resolved
        return resolved

    for player in players:
        if player.get("league") != "MLB":
            continue
        metadata = player.get("metadata", {}) or {}
        mlbam = none_if_empty(metadata.get("mlbId"))
        if not (mlbam and str(mlbam).isdigit()):
            try:
                mlbam = resolve_mlbam_by_name(player["name"])
            except Exception:
                mlbam = None
            if mlbam:
                player.setdefault("metadata", {})["mlbId"] = mlbam
        if mlbam and str(mlbam).isdigit():
            targets.append((player, str(mlbam)))
    save_cache("mlb_name_search.json", search_cache)

    missing_ids = sorted({mlbam for _, mlbam in targets if mlbam not in cache})
    if missing_ids:
        print(f"Enriching MLB profiles: {len(missing_ids)} uncached")
        batch_size = 25
        for index in range(0, len(missing_ids), batch_size):
            batch = missing_ids[index:index + batch_size]
            response = session.get(
                "https://statsapi.mlb.com/api/v1/people",
                params={"personIds": ",".join(batch), "hydrate": "currentTeam"},
                timeout=60,
            )
            response.raise_for_status()
            found = {str(person["id"]): person for person in response.json().get("people", [])}
            for mlbam in batch:
                cache[mlbam] = found.get(mlbam)
            if (index // batch_size + 1) % 50 == 0:
                print(f"  MLB enriched {min(index + batch_size, len(missing_ids))}/{len(missing_ids)}")
                save_cache("mlb_people.json", cache)
        save_cache("mlb_people.json", cache)

    for player, mlbam in targets:
        profile = cache.get(mlbam) or {}
        if not profile:
            continue
        country = normalize_country_code(profile.get("birthCountry"))
        position = ((profile.get("primaryPosition") or {}).get("name"))
        current_team = ((profile.get("currentTeam") or {}).get("name"))

        if country:
            player["nationality"] = country
            player["countryOfBirth"] = country
        if position:
            player["position"] = position
        if current_team and current_team not in player.get("teams", []):
            player.setdefault("teams", []).append(current_team)
        player["birthDate"] = player.get("birthDate") or none_if_empty(profile.get("birthDate"))
        player["birthCity"] = player.get("birthCity") or none_if_empty(profile.get("birthCity"))
        player["birthRegion"] = player.get("birthRegion") or none_if_empty(profile.get("birthStateProvince"))
        player["height"] = player.get("height") or none_if_empty(profile.get("height"))
        player["weight"] = player.get("weight") or none_if_empty(profile.get("weight"))
        bat_side = none_if_empty((profile.get("batSide") or {}).get("description"))
        pitch_hand = none_if_empty((profile.get("pitchHand") or {}).get("description"))
        if bat_side or pitch_hand:
            parts = []
            if bat_side:
                parts.append(f"Bats: {bat_side}")
            if pitch_hand:
                parts.append(f"Throws: {pitch_hand}")
            player["handedness"] = player.get("handedness") or "; ".join(parts)
        player["active"] = profile.get("active")

        player.setdefault("metadata", {}).update(
            {
                "source": "mlb_stats_api",
                "mlbDebutDate": none_if_empty(profile.get("mlbDebutDate")),
                "batSide": bat_side,
                "pitchHand": pitch_hand,
                "active": profile.get("active"),
            }
        )


def enrich_nfl_players(players: list[dict]) -> None:
    """Normalize NFL bulk fields into the shared athlete shape."""
    for player in players:
        if player.get("league") != "NFL":
            continue
        metadata = player.get("metadata", {}) or {}
        college = none_if_empty(metadata.get("collegeName"))
        if college:
            set_education(player, college=college)


def enrich_nhl_players(players: list[dict]) -> None:
    """Enrich NHL players with profile details from the NHL landing endpoint."""
    cache = load_cache("nhl_landing.json")
    targets = []
    for player in players:
        if player.get("league") != "NHL":
            continue
        metadata = player.get("metadata", {}) or {}
        nhl_id = none_if_empty(metadata.get("nhlId")) or player["id"].replace("a-nhl-", "")
        if nhl_id and str(nhl_id).isdigit():
            targets.append((player, str(nhl_id)))

    missing_ids = sorted({nhl_id for _, nhl_id in targets if nhl_id not in cache})
    if missing_ids:
        print(f"Enriching NHL profiles: {len(missing_ids)} uncached")
        completed = 0

        def fetch_profile(nhl_id: str):
            response = requests.get(
                f"https://api-web.nhle.com/v1/player/{nhl_id}/landing",
                timeout=30,
                headers={"User-Agent": "HighSchoolSports/1.0"},
            )
            response.raise_for_status()
            return nhl_id, response.json()

        with ThreadPoolExecutor(max_workers=48) as executor:
            futures = {executor.submit(fetch_profile, nhl_id): nhl_id for nhl_id in missing_ids}
            for future in as_completed(futures):
                nhl_id = futures[future]
                try:
                    _, profile = future.result()
                except Exception:
                    profile = None
                cache[nhl_id] = profile
                completed += 1
                if completed % 1000 == 0:
                    print(f"  NHL enriched {completed}/{len(missing_ids)}")
                    save_cache("nhl_landing.json", cache)
        save_cache("nhl_landing.json", cache)

    for player, nhl_id in targets:
        profile = cache.get(nhl_id) or {}
        if not profile:
            continue
        country = normalize_country_code(profile.get("birthCountry"))
        position = none_if_empty(profile.get("position"))
        team_name = localized_text(profile.get("fullTeamName")) or none_if_empty(profile.get("currentTeamAbbrev"))

        if country:
            player["nationality"] = country
            player["countryOfBirth"] = country
        if position:
            player["position"] = position
        if team_name and team_name not in player.get("teams", []):
            player.setdefault("teams", []).append(team_name)
        player["birthDate"] = player.get("birthDate") or none_if_empty(profile.get("birthDate"))
        player["birthCity"] = player.get("birthCity") or localized_text(profile.get("birthCity"))
        player["birthRegion"] = player.get("birthRegion") or localized_text(profile.get("birthStateProvince"))
        if profile.get("heightInInches") is not None:
            player["height"] = player.get("height") or str(profile.get("heightInInches"))
        if profile.get("weightInPounds") is not None:
            player["weight"] = player.get("weight") or profile.get("weightInPounds")
        player["handedness"] = player.get("handedness") or none_if_empty(profile.get("shootsCatches"))
        player["active"] = profile.get("isActive")
        honors = list(player.get("honors") or [])
        if profile.get("inHHOF") and "Hockey Hall of Fame" not in honors:
            honors.append("Hockey Hall of Fame")
        if profile.get("inTop100AllTime") and "NHL Top 100 All-Time" not in honors:
            honors.append("NHL Top 100 All-Time")
        if honors:
            player["honors"] = honors
        awards = list(player.get("awards") or [])
        for award in profile.get("awards") or []:
            trophy = localized_text((award.get("trophy") or {}).get("name")) or localized_text(award.get("name")) or localized_text(award.get("title"))
            if trophy and trophy not in awards:
                awards.append(trophy)
        if awards:
            player["awards"] = awards

        player.setdefault("metadata", {}).update(
            {
                "source": "nhl_landing",
                "sweaterNumber": none_if_empty(profile.get("sweaterNumber")),
                "headshot": none_if_empty(profile.get("headshot")),
                "draftDetails": profile.get("draftDetails"),
            }
        )


def enrich_wikidata_notable(players: list[dict]) -> None:
    """Fetch wikidataId for notable athletes where missing via Wikidata Search API."""
    cache = load_cache("wikidata_search.json")
    session = requests.Session()
    by_key = {(p.get("name", "").strip(), p.get("league", "")): p for p in players}

    for name, league in NOTABLE_PLAYERS:
        key = (name, league)
        cache_key = f"{name}|{league}"
        if cache_key in cache:
            wikidata_id = cache[cache_key]
        else:
            try:
                resp = session.get(
                    "https://www.wikidata.org/w/api.php",
                    params={"action": "wbsearchentities", "search": name, "language": "en", "format": "json"},
                    timeout=15,
                    headers={"User-Agent": "HighSchoolSports/1.0"},
                )
                resp.raise_for_status()
                data = resp.json()
                results = data.get("search", [])
                wikidata_id = None
                for r in results:
                    desc = (r.get("description") or "").lower()
                    if league == "NBA" and ("basketball" in desc or "nba" in desc):
                        wikidata_id = r.get("id")
                        break
                    if league == "NFL" and ("football" in desc or "nfl" in desc):
                        wikidata_id = r.get("id")
                        break
                    if league == "NHL" and ("hockey" in desc or "nhl" in desc):
                        wikidata_id = r.get("id")
                        break
                    if league == "MLB" and ("baseball" in desc or "mlb" in desc):
                        wikidata_id = r.get("id")
                        break
                if not wikidata_id and results:
                    wikidata_id = results[0].get("id")
                cache[cache_key] = wikidata_id
                save_cache("wikidata_search.json", cache)
                if wikidata_id:
                    time.sleep(0.3)
            except Exception as e:
                cache[cache_key] = None
                save_cache("wikidata_search.json", cache)
                continue

        player = by_key.get((name.strip(), league))
        if player and wikidata_id:
            meta = player.setdefault("metadata", {})
            if not meta.get("wikidataId"):
                meta["wikidataId"] = wikidata_id


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

    # Load existing athletes and affiliations so school-linked athlete IDs remain stable.
    existing_path = DATA_DIR / "athletes.json"
    affiliations_path = DATA_DIR / "affiliations.json"
    existing = load_json_with_git_fallback(existing_path, "data/athletes.json")
    committed_existing = load_json_from_git("data/athletes.json")
    affiliations = load_json_with_git_fallback(affiliations_path, "data/affiliations.json")

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

    # Preserve only athletes that are already referenced by affiliations.
    pinned_ids = {row["athleteId"] for row in affiliations}
    pinned_lookup = {athlete["id"]: athlete for athlete in committed_existing if athlete["id"] in pinned_ids}
    for athlete in existing:
        if athlete["id"] in pinned_ids:
            pinned_lookup[athlete["id"]] = athlete
    pinned_athletes = list(pinned_lookup.values())
    pinned_keys = {(a.get("name", "").strip().lower(), a.get("league", "")) for a in pinned_athletes}
    pinned_by_key = {(a.get("name", "").strip().lower(), a.get("league", "")): a for a in pinned_athletes}
    all_athletes = list(pinned_athletes)
    existing_ids = {a["id"] for a in pinned_athletes}

    def add_league(league: str, players: list[dict]):
        for p in players:
            key = (p.get("name", "").strip().lower(), league)
            if key in pinned_keys:
                merge_player_record(pinned_by_key[key], p)
                continue
            pid = p.get("id", "")
            if pid in existing_ids:
                continue
            existing_ids.add(pid)
            all_athletes.append(p)

    add_league("NBA", nba)
    add_league("NFL", nfl)
    add_league("MLB", mlb)
    add_league("NHL", nhl)

    enrich_nfl_players(all_athletes)
    enrich_nba_players(all_athletes)
    enrich_mlb_players(all_athletes)
    enrich_nhl_players(all_athletes)
    enrich_wikidata_notable(all_athletes)

    # Set provenance: ensure source and lastIngestedAt
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    for athlete in all_athletes:
        meta = athlete.setdefault("metadata", {})
        if not meta.get("source"):
            league = athlete.get("league", "")
            meta["source"] = {"NBA": "nba_api", "NFL": "nflverse", "MLB": "mlb_stats_api", "NHL": "nhl_landing"}.get(league, "unknown")
        meta["lastIngestedAt"] = now_iso

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
        json.dump(all_athletes, f, indent=2, ensure_ascii=False, allow_nan=False)

    metadata_path = DATA_DIR / "ingest_metadata.json"
    with open(metadata_path, "w", encoding="utf-8") as f:
        json.dump(
            {"lastIngestedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())},
            f,
            indent=2,
        )

    print(f"\nTotal: {len(all_athletes)} athletes")
    print(f"Written to {OUTPUT}")
    for league in ["NBA", "NFL", "MLB", "NHL"]:
        count = sum(1 for a in all_athletes if a.get("league") == league)
        print(f"  {league}: {count}")


if __name__ == "__main__":
    main()
