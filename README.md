# FIS Cross-Country Results Scraper + Elo Ratings

A Python project that scrapes cross-country skiing race data from [firstskisport.com](https://firstskisport.com/cross-country/), stores it in SQLite, and builds multi-dimensional Elo ratings to rank athletes and simulate predictions.

## What This Project Does

- Scrapes race calendars by season and gender.
- Scrapes race results and athlete metadata.
- Stores structured data in a local SQLite database.
- Builds Elo ratings across multiple dimensions: `overall`, `sprint`, `distance`, `classic`, `freestyle`, and combined discipline-technique ratings (for example `classic_sprint`).
- Supports head-to-head matchup predictions.
- Supports historical ranking snapshots by date.
- Includes backtesting tools for evaluating prediction quality.

## Tech Stack

- Python 3
- SQLite
- `requests`
- `beautifulsoup4`
- `lxml`

## Project Structure

```text
.
|-- main.py
|-- requirements.txt
|-- data/
|   `-- skiing.db
|-- models/
|   |-- database.py
|   |-- elo.py
|   `-- backtest.py
`-- scraper/
    |-- calendar.py
    `-- results.py
```

## Setup

1. Clone this repository.
2. Create and activate a virtual environment.
3. Install dependencies.

```bash
python -m venv .venv
# Windows
.venv\Scripts\activate
# macOS/Linux
source .venv/bin/activate

pip install -r requirements.txt
```

## Quick Start

Initialize database schema:

```bash
python main.py init
```

Scrape calendar events (default range is 2020 to 2025):

```bash
python main.py calendar --start-year 2020 --end-year 2025
```

Scrape race results for events in the DB:

```bash
python main.py results
```

Build Elo ratings from all scraped results:

```bash
python main.py elo-build
```

Show top ratings:

```bash
python main.py elo-rankings --type overall --top 20
```

## CLI Commands

### Data Ingestion

- `python main.py init`  
  Create core tables (`athletes`, `events`, `results`).

- `python main.py calendar --start-year 2020 --end-year 2025 --delay 1.0`  
  Scrape season calendars for men and women.

- `python main.py results --delay 1.0 --limit 100`  
  Scrape results for unscraped events (optional limit for testing).

- `python main.py all --start-year 2020 --end-year 2025 --delay 1.0`  
  Run calendar + results in one command.

- `python main.py stats`  
  Print DB stats (events, athletes, results, date range, gender split, top nations).

### Elo Ratings

- `python main.py elo-build`  
  Recompute all Elo ratings from historical race results.

- `python main.py elo-rankings --type overall --gender m --top 20 --active 12 --decay`  
  Show top Elo rankings with optional filters: `--type` (`overall`, `sprint`, `distance`, `classic`, `freestyle`, `classic_sprint`, `freestyle_sprint`, `classic_distance`, `freestyle_distance`), `--gender` (`m` or `w`), `--active` (last N months), and `--decay` (inactivity adjustment).

- `python main.py elo-athlete "Johannes Hoesflot Klaebo"`  
  Show all rating dimensions for one athlete (name partial match supported).

- `python main.py elo-matchup "Athlete A" "Athlete B" --type sprint`  
  Predict head-to-head win probability.

- `python main.py elo-history 2023-02-01 --gender w --top 30`  
  View historical top ratings at a specific date.

### Backtesting

- `python main.py backtest --start 2020-01-01 --end 2025-12-31 --gender m --min-participants 10`  
  Evaluate historical prediction quality and print top-1/top-3/top-5 hit rates, Brier score, calibration buckets, and discipline-level metrics.

Use `--overall-only` to disable discipline-specific ratings in backtest predictions.

## Database

Default database path:

`data/skiing.db`

Core tables:

- `athletes`
- `events`
- `results`
- `elo_ratings`
- `elo_history`

## Notes and Limitations

- Source HTML structure can change and break parsers.
- Scraping speed is intentionally throttled via `--delay`.
- Elo quality depends on data completeness and discipline classification heuristics.
- Upcoming events without official result IDs may use temporary generated IDs.

## Portfolio Highlights

- End-to-end data pipeline: ingestion, storage, modeling, and evaluation.
- Practical feature engineering with discipline-specific ratings.
- Reproducible CLI workflow suitable for automation and further experimentation.

## Future Improvements

- Add unit/integration tests for parsers and Elo update logic.
- Add retry/backoff and logging for scraper resilience.
- Export rankings/backtest outputs to CSV or dashboards.
- Add a small API or Streamlit UI for interactive exploration.