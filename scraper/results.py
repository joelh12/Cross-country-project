import re
import time
import requests
from bs4 import BeautifulSoup
from typing import Optional
from models.database import (
    get_db, insert_athlete, insert_result,
    get_event_by_fis_id, get_scraped_event_ids
)

BASE_URL = "https://firstskisport.com/cross-country/results.php"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}


def normalize_whitespace(text: str) -> str:
    """Collapse repeated whitespace and trim."""
    return re.sub(r"\s+", " ", text).strip()


def parse_time_to_seconds(time_str: str) -> Optional[float]:
    """Convert time string to seconds. Handles formats like '25:31.8' or '1:02:31.8'."""
    if not time_str or time_str.strip() in ["", "-", "DNF", "DNS", "DSQ", "LAP"]:
        return None

    time_str = time_str.strip().lstrip("+")

    parts = time_str.split(":")
    try:
        if len(parts) == 3:
            hours, mins, secs = parts
            return float(hours) * 3600 + float(mins) * 60 + float(secs)
        elif len(parts) == 2:
            mins, secs = parts
            return float(mins) * 60 + float(secs)
        elif len(parts) == 1:
            return float(parts[0])
    except ValueError:
        return None
    return None


def extract_athlete_id(href: str) -> Optional[int]:
    """Extract athlete ID from profile URL."""
    match = re.search(r"id=(\d+)", href)
    return int(match.group(1)) if match else None


def extract_athlete_name(athlete_link) -> str:
    """
    Extract athlete name from profile link.

    Handles pages that separate first/last names across spans and pages that
    render the full name directly in the anchor text.
    """
    first_name = athlete_link.find("span", class_=re.compile(r"first", re.IGNORECASE))
    last_name = athlete_link.find("span", class_=re.compile(r"last", re.IGNORECASE))

    if first_name and last_name:
        first_text = normalize_whitespace(first_name.get_text(" ", strip=True))
        last_text = normalize_whitespace(last_name.get_text(" ", strip=True))
        full_name = normalize_whitespace(f"{first_text} {last_text}")
        if full_name:
            return full_name

    # Use explicit separator so adjacent HTML nodes don't get concatenated.
    return normalize_whitespace(athlete_link.get_text(" ", strip=True))


def scrape_results_page(event_fis_id: int, gender: str) -> list[dict]:
    """
    Scrape results for a single event.

    Args:
        event_fis_id: The event ID from the site
        gender: 'M' for men (no g param), 'W' for women (g=w param)
    """
    params = {"id": event_fis_id}
    if gender == "W":
        params["g"] = "w"

    try:
        response = requests.get(BASE_URL, params=params, headers=HEADERS, timeout=30)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"  Error fetching results for event {event_fis_id}: {e}")
        return []

    soup = BeautifulSoup(response.text, "lxml")
    results = []

    table = soup.find("table", class_="tablesorter")
    if not table:
        print(f"  No results table found for event {event_fis_id}")
        return []

    rows = table.find("tbody")
    if not rows:
        rows = table
    rows = rows.find_all("tr")

    for row in rows:
        cells = row.find_all("td")
        if len(cells) < 5:
            continue

        try:
            position_text = cells[0].get_text(strip=True)
            position = int(position_text) if position_text.isdigit() else None

            bib_text = cells[1].get_text(strip=True)
            bib = int(bib_text) if bib_text.isdigit() else None

            athlete_cell = cells[2]
            athlete_link = athlete_cell.find("a", href=re.compile(r"athlete"))
            if not athlete_link:
                continue

            athlete_name = extract_athlete_name(athlete_link)
            athlete_fis_id = extract_athlete_id(athlete_link["href"])

            birth_year_text = cells[3].get_text(strip=True)
            birth_year = int(birth_year_text) if birth_year_text.isdigit() else None

            nation_cell = cells[4]
            nation_img = nation_cell.find("img")
            nation = None
            if nation_img and nation_img.get("src"):
                nation_match = re.search(r"/(\w{3})\.", nation_img["src"])
                nation = nation_match.group(1).upper() if nation_match else None
            if not nation:
                nation = nation_cell.get_text(strip=True)[:3].upper() if nation_cell.get_text(strip=True) else None

            time_display = cells[5].get_text(strip=True) if len(cells) > 5 else None
            time_seconds = parse_time_to_seconds(time_display)

            points = None
            if len(cells) > 6:
                points_text = cells[6].get_text(strip=True)
                points = int(points_text) if points_text.isdigit() else None

            results.append({
                "athlete_fis_id": athlete_fis_id,
                "athlete_name": athlete_name,
                "birth_year": birth_year,
                "nation": nation,
                "position": position,
                "bib": bib,
                "time_seconds": time_seconds,
                "time_display": time_display,
                "points": points,
                "gender": gender.upper()
            })

        except (ValueError, IndexError, AttributeError) as e:
            continue

    return results


def scrape_event_results(event_fis_id: int, event_db_id: int, gender: str) -> int:
    """Scrape and store results for a single event. Returns number of results added."""
    results = scrape_results_page(event_fis_id, gender)

    count = 0
    for r in results:
        athlete_id = insert_athlete(
            fis_id=r["athlete_fis_id"],
            name=r["athlete_name"],
            birth_year=r["birth_year"],
            nation=r["nation"],
            gender=r["gender"]
        )

        insert_result(
            event_id=event_db_id,
            athlete_id=athlete_id,
            position=r["position"],
            bib=r["bib"],
            time_seconds=r["time_seconds"],
            time_display=r["time_display"],
            points=r["points"],
            gender=r["gender"]
        )
        count += 1

    return count


def scrape_all_results(delay: float = 1.0, limit: Optional[int] = None):
    """Scrape results for all events that haven't been scraped yet."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, fis_id, date, discipline, location, gender
            FROM events
            ORDER BY date DESC
        """)
        events = cursor.fetchall()

    scraped_event_ids = get_scraped_event_ids()

    events_to_scrape = [e for e in events if e[0] not in scraped_event_ids]

    if limit:
        events_to_scrape = events_to_scrape[:limit]

    print(f"Found {len(events_to_scrape)} events to scrape results for")

    total_results = 0
    for i, event in enumerate(events_to_scrape, 1):
        db_id, fis_id, date, discipline, location, gender = event
        loc = location.encode('ascii', 'replace').decode('ascii') if location else ''
        print(f"[{i}/{len(events_to_scrape)}] Scraping: {date} - {discipline} ({loc}) [{gender}]...")

        count = scrape_event_results(fis_id, db_id, gender)
        total_results += count
        print(f"  Added {count} results")

        time.sleep(delay)

    print(f"\nTotal results added: {total_results}")
    return total_results
