#!/usr/bin/env python3
import csv
import json
from pathlib import Path
from typing import Any

ROOT = Path("/home/claw/projects/hackathon")
OUTPUT_DIR = ROOT / "output"
MATCHED_INPUT_PATH = OUTPUT_DIR / "events_kassir_matched.json"
MATCHED_CSV_PATH = OUTPUT_DIR / "kepler_events_matched.csv"
UNMATCHED_CSV_PATH = OUTPUT_DIR / "kepler_events_unmatched.csv"
MATCHED_GEOJSON_PATH = OUTPUT_DIR / "kepler_events_matched.geojson"
SUMMARY_PATH = OUTPUT_DIR / "kepler_events_export_summary.json"

MATCHED_FIELDS = [
    "id",
    "title",
    "category",
    "tags",
    "venue",
    "startDate",
    "endDate",
    "match_status",
    "match_score",
    "place_name",
    "place_type",
    "address_full",
    "district",
    "lat",
    "lon",
    "osm_type",
    "osm_id",
    "osm_url",
    "source_provider",
    "source_url",
    "raw_title",
    "raw_date_text",
    "scraped_at",
]

UNMATCHED_FIELDS = [
    "id",
    "title",
    "category",
    "tags",
    "venue",
    "startDate",
    "endDate",
    "match_status",
    "match_score",
    "match_second_score",
    "match_reasons",
    "source_provider",
    "source_url",
    "raw_title",
    "raw_date_text",
    "scraped_at",
]


def load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: Path, payload: Any) -> None:
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def flatten_common_fields(event: dict[str, Any]) -> dict[str, Any]:
    source_meta = event.get("sourceMeta") or {}
    return {
        "id": event.get("id") or "",
        "title": event.get("title") or "",
        "category": event.get("category") or "",
        "tags": ", ".join(event.get("tags") or []),
        "venue": event.get("venue") or "",
        "startDate": event.get("startDate") or "",
        "endDate": event.get("endDate") or "",
        "source_provider": source_meta.get("provider") or "",
        "source_url": source_meta.get("url") or "",
        "raw_title": source_meta.get("rawTitle") or "",
        "raw_date_text": source_meta.get("rawDateText") or "",
        "scraped_at": source_meta.get("scrapedAt") or "",
    }


def flatten_matched_event(event: dict[str, Any]) -> dict[str, Any]:
    row = flatten_common_fields(event)
    match = event.get("match") or {}
    place = match.get("place") or {}
    row.update(
        {
            "match_status": match.get("status") or "",
            "match_score": match.get("score") or 0,
            "place_name": place.get("name") or "",
            "place_type": place.get("place_type") or "",
            "address_full": place.get("address_full") or "",
            "district": place.get("district") or "",
            "lat": place.get("lat") or "",
            "lon": place.get("lon") or "",
            "osm_type": place.get("osm_type") or "",
            "osm_id": place.get("osm_id") or "",
            "osm_url": place.get("osm_url") or "",
        }
    )
    return row


def flatten_unmatched_event(event: dict[str, Any]) -> dict[str, Any]:
    row = flatten_common_fields(event)
    match = event.get("match") or {}
    row.update(
        {
            "match_status": match.get("status") or "",
            "match_score": match.get("score") or 0,
            "match_second_score": match.get("second_score") or 0,
            "match_reasons": " | ".join(match.get("reasons") or []),
        }
    )
    return row


def build_geojson(rows: list[dict[str, Any]]) -> dict[str, Any]:
    features = []
    for row in rows:
        properties = {key: value for key, value in row.items() if key not in {"lat", "lon"}}
        features.append(
            {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [row["lon"], row["lat"]],
                },
                "properties": properties,
            }
        )
    return {"type": "FeatureCollection", "features": features}


def main() -> None:
    events = load_json(MATCHED_INPUT_PATH)

    matched_rows = []
    unmatched_rows = []
    for event in events:
        match = event.get("match") or {}
        place = match.get("place")
        if place:
            matched_rows.append(flatten_matched_event(event))
        else:
            unmatched_rows.append(flatten_unmatched_event(event))

    write_csv(MATCHED_CSV_PATH, matched_rows, MATCHED_FIELDS)
    write_csv(UNMATCHED_CSV_PATH, unmatched_rows, UNMATCHED_FIELDS)
    save_json(MATCHED_GEOJSON_PATH, build_geojson(matched_rows))

    summary = {
        "matched_rows": len(matched_rows),
        "unmatched_rows": len(unmatched_rows),
        "matched_csv": str(MATCHED_CSV_PATH),
        "unmatched_csv": str(UNMATCHED_CSV_PATH),
        "matched_geojson": str(MATCHED_GEOJSON_PATH),
        "places_geojson": str(OUTPUT_DIR / "krasnoyarsk_places.geojson"),
    }
    save_json(SUMMARY_PATH, summary)
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
