import sqlite3
from pathlib import Path
from typing import Optional
from contextlib import contextmanager

DB_PATH = Path(__file__).parent.parent / "data" / "skiing.db"


def get_connection() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


@contextmanager
def get_db():
    conn = get_connection()
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db():
    with get_db() as conn:
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS athletes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fis_id INTEGER UNIQUE,
                name TEXT NOT NULL,
                birth_year INTEGER,
                nation TEXT,
                gender TEXT CHECK(gender IN ('M', 'W'))
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fis_id INTEGER UNIQUE NOT NULL,
                date TEXT,
                location TEXT,
                country TEXT,
                discipline TEXT,
                series TEXT,
                gender TEXT CHECK(gender IN ('M', 'W')),
                season INTEGER
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id INTEGER NOT NULL,
                athlete_id INTEGER NOT NULL,
                position INTEGER,
                bib INTEGER,
                time_seconds REAL,
                time_display TEXT,
                points INTEGER,
                gender TEXT CHECK(gender IN ('M', 'W')),
                FOREIGN KEY (event_id) REFERENCES events(id),
                FOREIGN KEY (athlete_id) REFERENCES athletes(id),
                UNIQUE(event_id, athlete_id)
            )
        """)

        cursor.execute("CREATE INDEX IF NOT EXISTS idx_events_season ON events(season)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_results_athlete ON results(athlete_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_results_event ON results(event_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_athletes_nation ON athletes(nation)")


def insert_athlete(fis_id: Optional[int], name: str, birth_year: Optional[int],
                   nation: Optional[str], gender: str) -> int:
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO athletes (fis_id, name, birth_year, nation, gender)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(fis_id) DO UPDATE SET
                name = excluded.name,
                birth_year = COALESCE(excluded.birth_year, athletes.birth_year),
                nation = COALESCE(excluded.nation, athletes.nation),
                gender = COALESCE(excluded.gender, athletes.gender)
            RETURNING id
        """, (fis_id, name, birth_year, nation, gender))
        result = cursor.fetchone()
        return result[0] if result else None


def insert_event(fis_id: int, date: str, location: str, country: Optional[str],
                 discipline: str, series: Optional[str], gender: str, season: int) -> int:
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO events (fis_id, date, location, country, discipline, series, gender, season)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(fis_id) DO UPDATE SET
                date = excluded.date,
                location = excluded.location,
                country = COALESCE(excluded.country, events.country),
                discipline = excluded.discipline,
                series = COALESCE(excluded.series, events.series),
                gender = excluded.gender,
                season = excluded.season
            RETURNING id
        """, (fis_id, date, location, country, discipline, series, gender, season))
        result = cursor.fetchone()
        return result[0] if result else None


def insert_result(event_id: int, athlete_id: int, position: Optional[int],
                  bib: Optional[int], time_seconds: Optional[float],
                  time_display: Optional[str], points: Optional[int], gender: str):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO results (event_id, athlete_id, position, bib, time_seconds, time_display, points, gender)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(event_id, athlete_id) DO UPDATE SET
                position = excluded.position,
                bib = excluded.bib,
                time_seconds = excluded.time_seconds,
                time_display = excluded.time_display,
                points = excluded.points
        """, (event_id, athlete_id, position, bib, time_seconds, time_display, points, gender))


def get_event_by_fis_id(fis_id: int) -> Optional[dict]:
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM events WHERE fis_id = ?", (fis_id,))
        row = cursor.fetchone()
        return dict(row) if row else None


def get_athlete_by_fis_id(fis_id: int) -> Optional[dict]:
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM athletes WHERE fis_id = ?", (fis_id,))
        row = cursor.fetchone()
        return dict(row) if row else None


def get_all_event_fis_ids() -> set:
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT fis_id FROM events")
        return {row[0] for row in cursor.fetchall()}


def get_scraped_event_ids() -> set:
    """Get event IDs that already have results scraped."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT event_id FROM results")
        return {row[0] for row in cursor.fetchall()}
