#!/usr/bin/env python3
"""
Add inferred school affiliations from normalized education links.

Rules:
- Source rows come from data/education_affiliations.json
- Only high-school-like education types are considered
- Only rows with institutionId matching an existing school are considered
- Existing athlete-school pairs are preserved (no duplicates)
"""

from __future__ import annotations

import json
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"

SPORT_BY_LEAGUE = {
    "NBA": "basketball",
    "NFL": "football",
    "MLB": "baseball",
    "NHL": "hockey",
}

ELIGIBLE_EDUCATION_TYPES = {"highSchool", "prepSchool", "academy"}


def load_json(name: str):
    with open(DATA_DIR / name, "r", encoding="utf-8") as file:
        return json.load(file)


def save_json(name: str, payload) -> None:
    with open(DATA_DIR / name, "w", encoding="utf-8") as file:
        json.dump(payload, file, indent=2, ensure_ascii=False)


def load_athletes() -> list[dict]:
    merged: list[dict] = []
    for name in ("athletes-nba.json", "athletes-nfl.json", "athletes-mlb.json", "athletes-nhl.json"):
        path = DATA_DIR / name
        if not path.exists():
            continue
        merged.extend(load_json(name))
    return merged


def next_affiliation_id(existing: list[dict], index: int) -> str:
    max_num = 0
    for row in existing:
        match = re.match(r"^af-(\d+)$", str(row.get("id", "")))
        if match:
            max_num = max(max_num, int(match.group(1)))
    value = max_num + index
    width = max(3, len(str(value)))
    return f"af-{value:0{width}d}"


def main() -> int:
    affiliations = load_json("affiliations.json")
    education_affiliations = load_json("education_affiliations.json")
    schools = load_json("schools.json")
    athletes = load_athletes()

    athlete_by_id = {row["id"]: row for row in athletes}
    school_ids = {row["id"] for row in schools}

    # Treat athlete-school as unique for inferred inserts.
    existing_pairs = {(row.get("athleteId"), row.get("schoolId")) for row in affiliations}

    additions: list[dict] = []
    skipped_missing_school = 0
    skipped_missing_athlete = 0

    for edu in education_affiliations:
        if edu.get("educationType") not in ELIGIBLE_EDUCATION_TYPES:
            continue
        school_id = edu.get("institutionId")
        athlete_id = edu.get("athleteId")
        if not school_id or not str(school_id).startswith("s-"):
            continue
        if school_id not in school_ids:
            skipped_missing_school += 1
            continue
        athlete = athlete_by_id.get(athlete_id)
        if not athlete:
            skipped_missing_athlete += 1
            continue
        pair = (athlete_id, school_id)
        if pair in existing_pairs:
            continue

        league = athlete.get("league") or edu.get("league")
        sport = athlete.get("sport") or SPORT_BY_LEAGUE.get(league, "")
        if not league or not sport:
            continue

        row = {
            "id": "",  # assigned after collecting
            "schoolId": school_id,
            "athleteId": athlete_id,
            "type": "alumni",
            "league": league,
            "sport": sport,
            "notes": f"Inferred from athlete education ({edu.get('educationType')}).",
            "sources": ["athlete_education"],
            "metadata": {
                "derivedFromEducationAffiliationId": edu.get("id"),
                "educationType": edu.get("educationType"),
            },
        }

        if edu.get("yearsAttended"):
            row["yearsAttended"] = edu.get("yearsAttended")
        if isinstance(edu.get("graduated"), int):
            row["graduated"] = edu.get("graduated")

        existing_pairs.add(pair)
        additions.append(row)

    if not additions:
        print("No new affiliations to add.")
        return 0

    for i, row in enumerate(additions, start=1):
        row["id"] = next_affiliation_id(affiliations, i)

    merged = affiliations + additions
    merged.sort(key=lambda row: row["id"])
    save_json("affiliations.json", merged)

    print(f"Added {len(additions)} affiliations.")
    print(f"Skipped missing school refs: {skipped_missing_school}")
    print(f"Skipped missing athlete refs: {skipped_missing_athlete}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
