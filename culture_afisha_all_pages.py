#!/usr/bin/env python3
import argparse
import csv
import json
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import requests

BASE_URL = "https://www.culture.ru/afisha/krasnoyarskiy-kray-krasnoyarsk"
EVENT_BASE_URL = "https://www.culture.ru/events/{event_id}/{slug}"
LOCATION = "krasnoyarskiy-kray-krasnoyarsk"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36"
}


def extract_next_data(html: str) -> dict[str, Any]:
    match = re.search(
        r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
        html,
        re.S,
    )
    if not match:
        raise RuntimeError("Не найден __NEXT_DATA__ на странице")
    return json.loads(match.group(1))


def fetch_json(url: str, params: dict[str, Any] | None = None, retries: int = 3, timeout: int = 30) -> dict[str, Any]:
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            response = requests.get(url, params=params, headers=HEADERS, timeout=timeout)
            response.raise_for_status()
            return extract_next_data(response.text)
        except Exception as exc:
            last_error = exc
            if attempt == retries:
                raise
            time.sleep(attempt)
    assert last_error is not None
    raise last_error


def fetch_page(page: int) -> dict[str, Any]:
    return fetch_json(BASE_URL, params={"page": page}, timeout=30)


def build_event_url(item: dict[str, Any]) -> str:
    event_id = item.get("urlEventId") or item.get("_id")
    slug = item.get("name") or ""
    query = urlencode({"location": LOCATION})
    return f"{EVENT_BASE_URL.format(event_id=event_id, slug=slug)}?{query}"


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


def normalize_iso_date(value: Any) -> str:
    if isinstance(value, str) and len(value) >= 10:
        return value[:10]
    return ""


def derive_dates_from_detail(item: dict[str, Any]) -> tuple[str, str, str, str]:
    url = build_event_url(item)
    data = fetch_json(url, timeout=20)
    event = data.get("props", {}).get("pageProps", {}).get("event", {})

    seances = event.get("seances") or []
    if seances:
        starts = sorted({normalize_iso_date(s.get("localeStartDate") or s.get("startDate")) for s in seances if normalize_iso_date(s.get("localeStartDate") or s.get("startDate"))})
        ends = sorted({normalize_iso_date(s.get("localeEndDate") or s.get("endDate") or s.get("localeStartDate") or s.get("startDate")) for s in seances if normalize_iso_date(s.get("localeEndDate") or s.get("endDate") or s.get("localeStartDate") or s.get("startDate"))})
        date_from = starts[0] if starts else ""
        date_to = ends[-1] if ends else date_from
        if date_from and date_to and date_from != date_to:
            return date_from, date_to, f"с {date_from} по {date_to}", "detail_seances_range"
        if date_from:
            return date_from, date_from, date_from, "detail_seances_single"

    date_from = normalize_iso_date(event.get("startDate"))
    date_to = normalize_iso_date(event.get("endDate"))
    if date_from and date_to and date_from != date_to:
        return date_from, date_to, f"с {date_from} по {date_to}", "detail_event_range"
    if date_from:
        return date_from, date_to or date_from, date_from, "detail_event_single"

    raise RuntimeError("В карточке события не найдены даты")


def derive_dates_fallback(item: dict[str, Any]) -> tuple[str, str, str, str]:
    raw_date = item.get("date")
    if isinstance(raw_date, dict):
        date_from = normalize_iso_date(raw_date.get("startDate") or raw_date.get("date"))
        date_to = normalize_iso_date(raw_date.get("endDate") or raw_date.get("date"))
        if date_from and date_to and date_from != date_to:
            return date_from, date_to, f"с {date_from} по {date_to}", "list_date_range"
        if date_from:
            return date_from, date_to or date_from, date_from, "list_date_single"

    if isinstance(raw_date, str) and raw_date:
        date_value = normalize_iso_date(raw_date)
        if date_value:
            return date_value, date_value, date_value, "list_date_single"

    seance_end = normalize_iso_date(item.get("seanceEndDate"))
    if seance_end:
        return seance_end, seance_end, seance_end, "list_seanceEndDate"

    return "", "", "", "missing"


def enrich_item(item: dict[str, Any], source_page: int) -> dict[str, Any]:
    price = item.get("price") or {}
    place_theatre, adress = extract_place_fields(item)

    try:
        event_date_from, event_date_to, event_date_text, date_source = derive_dates_from_detail(item)
    except Exception:
        event_date_from, event_date_to, event_date_text, date_source = derive_dates_fallback(item)

    return {
        "source_page": source_page,
        "event_id": item.get("_id", ""),
        "title": item.get("title", ""),
        "price_min": price.get("min", ""),
        "price_max": price.get("max", ""),
        "places_theatre": place_theatre,
        "adress": adress,
        "event_date_from": event_date_from,
        "event_date_to": event_date_to,
        "event_date_text": event_date_text,
        "date_source": date_source,
    }


def collect_all_items(max_pages: int | None = None) -> list[tuple[int, dict[str, Any]]]:
    first_page = fetch_page(1)
    total_pages = first_page["props"]["pageProps"]["events"]["pagination"]["total"]
    if max_pages is not None:
        total_pages = min(total_pages, max_pages)

    page_items: list[tuple[int, dict[str, Any]]] = []
    for item in first_page["props"]["pageProps"]["events"]["items"]:
        page_items.append((1, item))

    for page in range(2, total_pages + 1):
        data = fetch_page(page)
        for item in data["props"]["pageProps"]["events"]["items"]:
            page_items.append((page, item))

    return page_items


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
        "event_date_from",
        "event_date_to",
        "event_date_text",
        "date_source",
    ]
    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Парсинг всех страниц афиши culture.ru из __NEXT_DATA__")
    parser.add_argument("--output", default="output/culture_afisha_all_pages.csv", help="Путь к выходному CSV")
    parser.add_argument("--max-pages", type=int, default=None, help="Ограничить число страниц для теста")
    parser.add_argument("--workers", type=int, default=4, help="Количество потоков для карточек событий")
    args = parser.parse_args()

    page_items = collect_all_items(max_pages=args.max_pages)
    rows: list[dict[str, Any]] = []

    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as executor:
        futures = [executor.submit(enrich_item, item, source_page) for source_page, item in page_items]
        for future in as_completed(futures):
            rows.append(future.result())

    rows.sort(key=lambda r: (int(r["source_page"]), str(r["event_id"])))
    write_csv(rows, Path(args.output))

    print(f"Сохранено строк: {len(rows)}")
    print(f"Файл: {args.output}")


if __name__ == "__main__":
    main()
