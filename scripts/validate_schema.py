#!/usr/bin/env python3
"""
Validate all JSON data files against the expected schema structure.

Checks:
- Athletes: id, name, league (required); no duplicate IDs
- Schools: id, name, country (required)
- Colleges: id, name (required)
- Affiliations: valid schoolId and athleteId references
- Education affiliations: valid athleteId, institutionId when present

Run: python scripts/validate_schema.py
"""

from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"

LEAGUES = {"NBA", "NFL", "MLB", "NHL"}


def load_json(name: str) -> list | dict:
    path = DATA_DIR / name
    if not path.exists():
        return [] if "affiliations" in name or "education" in name else {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def main() -> int:
    errors: list[str] = []

    # Load data
    athletes = load_json("athletes.json")
    schools = load_json("schools.json")
    colleges_raw = load_json("colleges.json")
    colleges = colleges_raw if isinstance(colleges_raw, list) else []
    affiliations = load_json("affiliations.json")
    education_affiliations = load_json("education_affiliations.json")

    athlete_list = athletes if isinstance(athletes, list) else []
    school_list = schools if isinstance(schools, list) else []
    college_list = colleges if isinstance(colleges, list) else []
    affiliation_list = affiliations if isinstance(affiliations, list) else []
    edu_aff_list = education_affiliations if isinstance(education_affiliations, list) else []

    athlete_ids = {a.get("id") for a in athlete_list}
    school_ids = {s.get("id") for s in school_list}
    college_ids = {c.get("id") for c in college_list}

    # Duplicate athlete IDs
    seen_ids: set[str] = set()
    for a in athlete_list:
        aid = a.get("id")
        if aid and aid in seen_ids:
            errors.append(f"Athlete duplicate ID: {aid}")
        if aid:
            seen_ids.add(aid)

    # Athletes: required id, name, league
    for i, a in enumerate(athlete_list):
        if not isinstance(a, dict):
            errors.append(f"Athlete[{i}]: not an object")
            continue
        if not a.get("id"):
            errors.append(f"Athlete[{i}]: missing id (name={a.get('name', '?')})")
        if not a.get("name"):
            errors.append(f"Athlete[{i}]: missing name (id={a.get('id', '?')})")
        if a.get("league") not in LEAGUES:
            errors.append(f"Athlete[{i}]: invalid or missing league (id={a.get('id')})")

    # Schools: required id, name, country
    for i, s in enumerate(school_list):
        if not isinstance(s, dict):
            errors.append(f"School[{i}]: not an object")
            continue
        if not s.get("id"):
            errors.append(f"School[{i}]: missing id")
        if not s.get("name"):
            errors.append(f"School[{i}]: missing name (id={s.get('id')})")
        if not s.get("country"):
            errors.append(f"School[{i}]: missing country (id={s.get('id')})")

    # Colleges: required id, name
    for i, c in enumerate(college_list):
        if not isinstance(c, dict):
            errors.append(f"College[{i}]: not an object")
            continue
        if not c.get("id"):
            errors.append(f"College[{i}]: missing id")
        if not c.get("name"):
            errors.append(f"College[{i}]: missing name (id={c.get('id')})")

    # Affiliations: valid schoolId, athleteId
    for i, af in enumerate(affiliation_list):
        if not isinstance(af, dict):
            errors.append(f"Affiliation[{i}]: not an object")
            continue
        sid = af.get("schoolId")
        aid = af.get("athleteId")
        if sid and sid not in school_ids:
            errors.append(f"Affiliation[{i}]: schoolId '{sid}' not found in schools")
        if aid and aid not in athlete_ids:
            errors.append(f"Affiliation[{i}]: athleteId '{aid}' not found in athletes")

    # Education affiliations: valid athleteId, institutionId when present
    for i, ea in enumerate(edu_aff_list):
        if not isinstance(ea, dict):
            errors.append(f"EducationAffiliation[{i}]: not an object")
            continue
        aid = ea.get("athleteId")
        iid = ea.get("institutionId")
        if aid and aid not in athlete_ids:
            errors.append(f"EducationAffiliation[{i}]: athleteId '{aid}' not found in athletes")
        if iid and iid not in college_ids and iid not in school_ids:
            errors.append(f"EducationAffiliation[{i}]: institutionId '{iid}' not found in colleges or schools")

    if errors:
        print("Schema validation FAILED")
        for e in errors[:50]:
            print(f"  {e}")
        if len(errors) > 50:
            print(f"  ... and {len(errors) - 50} more")
        return 1

    print("Schema validation OK")
    print(f"  Athletes: {len(athlete_list)}")
    print(f"  Schools: {len(school_list)}")
    print(f"  Colleges: {len(college_list)}")
    print(f"  Affiliations: {len(affiliation_list)}")
    print(f"  Education affiliations: {len(edu_aff_list)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
