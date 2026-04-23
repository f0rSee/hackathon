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
    response = requests.get(BASE_URL, params={"page": page}, headers=HEADERS, timeout=30)
    response.raise_for_status()

    match = re.search(
        r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
        response.text,
        re.S,
    )
    if not match:
        raise RuntimeError("Не найден __NEXT_DATA__ на странице")

    return json.loads(match.group(1))


def extract_place_fields(item: dict[str, Any]) -> tuple[str, str]:
    places = item.get("places") or []
    place_titles: list[str] = []
    addresses: list[str] = []

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
        date_value = raw_date.get("date") or raw_date.get("startDate")
        if date_value:
            return str(date_value)[:10]

    if isinstance(raw_date, str) and raw_date:
        return raw_date[:10]

    seance_end = item.get("seanceEndDate")
    if isinstance(seance_end, str) and len(seance_end) >= 10:
        return seance_end[:10]

    return ""


def extract_rows(data: dict[str, Any], source_page: int) -> list[dict[str, Any]]:
    items = data["props"]["pageProps"]["events"]["items"]
    rows: list[dict[str, Any]] = []

    for item in items:
        price = item.get("price") or {}
        place_theatre, adress = extract_place_fields(item)
        event_date = extract_event_date(item)
        rows.append(
            {
                "source_page": source_page,
                "event_id": item.get("_id", ""),
                "title": item.get("title", ""),
                "price_min": price.get("min", ""),
                "price_max": price.get("max", ""),
                "places_theatre": place_theatre,
                "adress": adress,
                "event_date": event_date,
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
    ]
    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Парсинг одной страницы афиши culture.ru из __NEXT_DATA__")
    parser.add_argument("--page", type=int, default=1, help="Номер страницы афиши")
    parser.add_argument(
        "--output",
        default="output/culture_afisha_page_1.csv",
        help="Путь к выходному CSV",
    )
    args = parser.parse_args()

    data = fetch_page(args.page)
    rows = extract_rows(data, source_page=args.page)
    write_csv(rows, Path(args.output))

    print(f"Сохранено строк: {len(rows)}")
    print(f"Файл: {args.output}")


if __name__ == "__main__":
    main()
