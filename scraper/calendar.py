import re
import time
import requests
from bs4 import BeautifulSoup
from typing import Generator
from models.database import insert_event, get_all_event_fis_ids

BASE_URL = "https://firstskisport.com/cross-country/calendar.php"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}


def parse_date(date_str: str, season_year: int) -> str:
    """
    Convert date like '29.11' to ISO date.

    Season year is the ending year (e.g., 2026 = 2025-2026 season).
    - Nov-Dec dates are in season_year - 1
    - Jan-Apr dates are in season_year
    """
    if not date_str:
        return None
    match = re.match(r"(\d{1,2})\.(\d{1,2})", date_str.strip())
    if match:
        day, month = int(match.group(1)), int(match.group(2))
        # Nov-Dec = previous year, Jan-Apr = season year
        if month >= 8:
            actual_year = season_year - 1
        else:
            actual_year = season_year
        return f"{actual_year}-{month:02d}-{day:02d}"
    return None


def extract_event_id(href: str) -> int:
    """Extract event ID from results URL."""
    match = re.search(r"id=(\d+)", href)
    return int(match.group(1)) if match else None


def scrape_calendar_page(year: int, gender: str) -> Generator[dict, None, None]:
    """
    Scrape a single calendar page for a year and gender.

    Args:
        year: Season year
        gender: 'M' for men (no g param), 'W' for women (g=w param)
    """
    params = {"y": year}
    if gender == "W":
        params["g"] = "w"

    try:
        response = requests.get(BASE_URL, params=params, headers=HEADERS, timeout=30)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Error fetching calendar {year}/{gender}: {e}")
        return

    soup = BeautifulSoup(response.text, "lxml")

    table = soup.find("table", class_="tablesorter")
    if not table:
        print(f"No table found for {year}/{gender}")
        return

    rows = table.find_all("tr")
    current_date = None
    current_location = None
    current_country = None
    current_series = None

    for row in rows:
        cells = row.find_all("td")
        if len(cells) < 7:
            continue

        date_cell = cells[0].get_text(strip=True)
        if date_cell:
            current_date = parse_date(date_cell, year)

        location_cell = cells[2]
        location_text = location_cell.get_text(strip=True)
        if location_text:
            current_location = location_text
            img = location_cell.find("img")
            if img and img.get("src"):
                country_match = re.search(r"/(\w{3})\.", img["src"])
                current_country = country_match.group(1).upper() if country_match else None

            series_span = location_cell.find("span", class_="serie")
            current_series = series_span.get_text(strip=True) if series_span else None

        discipline_cell = cells[3]
        discipline = discipline_cell.get_text(strip=True)

        results_link = None
        for cell in cells:
            results_link = cell.find("a", href=re.compile(r"results\.php"))
            if results_link:
                break

        if results_link:
            event_id = extract_event_id(results_link["href"])
        else:
            # Generate temp ID for upcoming events without result pages
            # Use negative hash to avoid collision with real IDs
            key = f"{current_date}_{discipline}_{current_location}_{gender}"
            event_id = -abs(hash(key)) % 1000000

        if not event_id:
            continue

        yield {
            "fis_id": event_id,
            "date": current_date,
            "location": current_location,
            "country": current_country,
            "discipline": discipline,
            "series": current_series,
            "gender": gender,
            "season": year
        }


def scrape_calendars(start_year: int, end_year: int, delay: float = 1.0) -> list[int]:
    """Scrape calendar pages for a range of years. Returns list of event IDs."""
    existing_ids = get_all_event_fis_ids()
    new_event_ids = []

    for year in range(start_year, end_year + 1):
        for gender in ["M", "W"]:
            print(f"Scraping calendar: {year} {gender}...")

            for event_data in scrape_calendar_page(year, gender):
                if event_data["fis_id"] not in existing_ids:
                    insert_event(**event_data)
                    new_event_ids.append(event_data["fis_id"])
                    existing_ids.add(event_data["fis_id"])
                    loc = event_data['location'].encode('ascii', 'replace').decode('ascii')
                    print(f"  Added: {event_data['date']} - {event_data['discipline']} ({loc}) [{gender}]")

            time.sleep(delay)

    print(f"\nTotal new events added: {len(new_event_ids)}")
    return new_event_ids
