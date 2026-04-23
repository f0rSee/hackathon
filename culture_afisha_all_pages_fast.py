#!/usr/bin/env python3
import argparse
import csv
import json
import re
from pathlib import Path
from typing import Any

import requests

BASE_URL = "https://www.culture.ru/afisha/krasnoyarskiy-kray-krasnoyarsk"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36"
}


def fetch_page(page: int) -> dict[str, Any]:
    response = requests.get(BASE_URL, params={"page": page}, headers=HEADERS, timeout=60)
    response.raise_for_status()
    match = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', response.text, re.S)
    if not match:
        raise RuntimeError(f"Не найден __NEXT_DATA__ на странице {page}")
    return json.loads(match.group(1))


def extract_place_fields(item: dict[str, Any]) -> tuple[str, str]:
    places = item.get("places") or []
    place_titles = []
    addresses = []
    for place in places:
        if not isinstance(place, dict):
            continue
        title = (place.get("title") or "").strip()
        address = (place.get("address") or "").strip()
        if title:
            place_titles.append(title)
        if address:
            addresses.append(address)
    return " || ".join(place_titles), " || ".join(addresses)


def extract_event_date(item: dict[str, Any]) -> str:
    raw_date = item.get("date")
    if isinstance(raw_date, dict):
        end = str(raw_date.get("endDate") or raw_date.get("date") or "")[:10]
        start = str(raw_date.get("startDate") or raw_date.get("date") or "")[:10]
        if end:
            return end
        if start:
            return start
    if isinstance(raw_date, str) and raw_date:
        return raw_date[:10]
    seance_end = item.get("seanceEndDate")
    if isinstance(seance_end, str) and len(seance_end) >= 10:
        return seance_end[:10]
    return ""


def collect_rows(max_pages: int | None = None) -> list[dict[str, Any]]:
    first = fetch_page(1)
    total_pages = first["props"]["pageProps"]["events"]["pagination"]["total"]
    if max_pages is not None:
        total_pages = min(total_pages, max_pages)

    rows = []
    for page in range(1, total_pages + 1):
        data = first if page == 1 else fetch_page(page)
        items = data["props"]["pageProps"]["events"]["items"]
        for item in items:
            price = item.get("price") or {}
            places_theatre, adress = extract_place_fields(item)
            event_date = extract_event_date(item)
            rows.append(
                {
                    "source_page": page,
                    "event_id": item.get("_id", ""),
                    "title": item.get("title", ""),
                    "price_min": price.get("min", ""),
                    "price_max": price.get("max", ""),
                    "places_theatre": places_theatre,
                    "adress": adress,
                    "event_date": event_date,
                    "date_source": "list_end_date_preferred",
                }
            )
    return rows


def write_csv(rows: list[dict[str, Any]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "source_page",
        "event_id",
        "title",
        "price_min",
        "price_max",
        "places_theatre",
        "adress",
        "event_date",
        "date_source",
    ]
    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Быстрый сбор всех страниц афиши culture.ru из __NEXT_DATA__")
    parser.add_argument("--output", default="output/culture_afisha_all_pages.csv")
    parser.add_argument("--max-pages", type=int, default=None)
    args = parser.parse_args()

    rows = collect_rows(max_pages=args.max_pages)
    write_csv(rows, Path(args.output))
    print(f"Сохранено строк: {len(rows)}")
    print(f"Файл: {args.output}")


if __name__ == "__main__":
    main()
