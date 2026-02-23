"""
Elo Rating System for Cross-Country Skiing

This implements a multi-competitor Elo system using pairwise comparisons.
Each race result updates ratings based on head-to-head outcomes.

Key features:
- Separate ratings by discipline type (Sprint vs Distance)
- Separate ratings by technique (Classic vs Freestyle)
- K-factor adjusts based on athlete experience
- Handles DNF/DNS appropriately
"""

import math
from datetime import datetime, timedelta
from typing import Optional
from models.database import get_db

# Configuration
DEFAULT_RATING = 1500
K_FACTOR_NEW = 40        # Higher K for new athletes (faster adjustment)
K_FACTOR_ESTABLISHED = 20  # Lower K for established athletes
RACES_UNTIL_ESTABLISHED = 15
DECAY_RATE_PER_YEAR = 50  # Rating points lost per year of inactivity

# Discipline classification
SPRINT_KEYWORDS = ['sprint', 'team sprint']
DISTANCE_KEYWORDS = ['10 km', '15 km', '20 km', '30 km', '50 km', 'mass start', 'pursuit', 'duathlon', 'skiathlon']
CLASSIC_KEYWORDS = [' c ', ' c,', 'classic', ' c mass', ' c pursuit']
FREESTYLE_KEYWORDS = [' f ', ' f,', 'free', ' f mass', ' f pursuit']


def classify_discipline(discipline: str) -> tuple[str, str]:
    """
    Classify a race discipline into (type, technique).

    Returns:
        (type, technique) where:
        - type: 'sprint' or 'distance'
        - technique: 'classic', 'freestyle', or 'mixed'
    """
    disc_lower = discipline.lower()

    # Determine sprint vs distance
    is_sprint = any(kw in disc_lower for kw in SPRINT_KEYWORDS)
    race_type = 'sprint' if is_sprint else 'distance'

    # Determine technique - check keywords and also check if ends with ' c' or ' f'
    is_classic = any(kw in disc_lower for kw in CLASSIC_KEYWORDS) or disc_lower.endswith(' c')
    is_freestyle = any(kw in disc_lower for kw in FREESTYLE_KEYWORDS) or disc_lower.endswith(' f')

    if is_classic and not is_freestyle:
        technique = 'classic'
    elif is_freestyle and not is_classic:
        technique = 'freestyle'
    else:
        technique = 'mixed'  # Duathlon, pursuit with both, etc.

    return race_type, technique


def expected_score(rating_a: float, rating_b: float) -> float:
    """Calculate expected score for player A against player B."""
    return 1 / (1 + math.pow(10, (rating_b - rating_a) / 400))


def get_k_factor(race_count: int) -> float:
    """Get K-factor based on athlete's experience."""
    if race_count < RACES_UNTIL_ESTABLISHED:
        # Linear interpolation from K_NEW to K_ESTABLISHED
        progress = race_count / RACES_UNTIL_ESTABLISHED
        return K_FACTOR_NEW - (K_FACTOR_NEW - K_FACTOR_ESTABLISHED) * progress
    return K_FACTOR_ESTABLISHED


def calculate_elo_updates(results: list[dict], ratings: dict, race_counts: dict) -> dict:
    """
    Calculate Elo rating updates for a race using pairwise comparisons.

    Args:
        results: List of {athlete_id, position} sorted by position
        ratings: Current ratings {athlete_id: rating}
        race_counts: Number of races per athlete {athlete_id: count}

    Returns:
        Dictionary of rating changes {athlete_id: delta}
    """
    # Filter out DNF/DNS (position is None or very high)
    valid_results = [r for r in results if r['position'] is not None and r['position'] < 900]

    if len(valid_results) < 2:
        return {}

    # Initialize deltas
    deltas = {r['athlete_id']: 0.0 for r in valid_results}

    # Compare each pair
    n = len(valid_results)
    for i in range(n):
        for j in range(i + 1, n):
            a = valid_results[i]  # Higher finisher (better position = lower number)
            b = valid_results[j]  # Lower finisher

            a_id, b_id = a['athlete_id'], b['athlete_id']
            a_rating = ratings.get(a_id, DEFAULT_RATING)
            b_rating = ratings.get(b_id, DEFAULT_RATING)

            # A beat B (A gets score=1, B gets score=0)
            expected_a = expected_score(a_rating, b_rating)
            expected_b = 1 - expected_a

            # K-factor based on experience (use average of both)
            k_a = get_k_factor(race_counts.get(a_id, 0))
            k_b = get_k_factor(race_counts.get(b_id, 0))

            # Scale down K since we do many comparisons per race
            # Divide by sqrt(n) to normalize for field size
            scale = 1 / math.sqrt(n)

            # Update deltas
            deltas[a_id] += k_a * scale * (1 - expected_a)
            deltas[b_id] += k_b * scale * (0 - expected_b)

    return deltas


def init_elo_tables():
    """Create Elo rating tables in the database."""
    with get_db() as conn:
        cursor = conn.cursor()

        # Main ratings table - current ratings
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS elo_ratings (
                athlete_id INTEGER PRIMARY KEY,
                overall_rating REAL DEFAULT 1500,
                sprint_rating REAL DEFAULT 1500,
                distance_rating REAL DEFAULT 1500,
                classic_rating REAL DEFAULT 1500,
                freestyle_rating REAL DEFAULT 1500,
                classic_sprint_rating REAL DEFAULT 1500,
                freestyle_sprint_rating REAL DEFAULT 1500,
                classic_distance_rating REAL DEFAULT 1500,
                freestyle_distance_rating REAL DEFAULT 1500,
                race_count INTEGER DEFAULT 0,
                last_race_date TEXT,
                FOREIGN KEY (athlete_id) REFERENCES athletes(id)
            )
        """)

        # Rating history for tracking progression
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS elo_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                athlete_id INTEGER,
                event_id INTEGER,
                date TEXT,
                rating_before REAL,
                rating_after REAL,
                rating_type TEXT,  -- 'overall', 'sprint', 'distance', 'classic', 'freestyle'
                FOREIGN KEY (athlete_id) REFERENCES athletes(id),
                FOREIGN KEY (event_id) REFERENCES events(id)
            )
        """)

        cursor.execute("CREATE INDEX IF NOT EXISTS idx_elo_history_athlete ON elo_history(athlete_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_elo_history_date ON elo_history(date)")


def get_current_ratings() -> dict:
    """
    Get all current ratings from database.

    Returns:
        Dictionary with keys: overall, sprint, distance, classic, freestyle,
        classic_sprint, freestyle_sprint, classic_distance, freestyle_distance, counts
    """
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT athlete_id, overall_rating, sprint_rating, distance_rating,
                   classic_rating, freestyle_rating, classic_sprint_rating,
                   freestyle_sprint_rating, classic_distance_rating,
                   freestyle_distance_rating, race_count
            FROM elo_ratings
        """)

        ratings = {
            'overall': {}, 'sprint': {}, 'distance': {},
            'classic': {}, 'freestyle': {},
            'classic_sprint': {}, 'freestyle_sprint': {},
            'classic_distance': {}, 'freestyle_distance': {},
            'counts': {}
        }

        for row in cursor.fetchall():
            aid = row[0]
            ratings['overall'][aid] = row[1]
            ratings['sprint'][aid] = row[2]
            ratings['distance'][aid] = row[3]
            ratings['classic'][aid] = row[4]
            ratings['freestyle'][aid] = row[5]
            ratings['classic_sprint'][aid] = row[6]
            ratings['freestyle_sprint'][aid] = row[7]
            ratings['classic_distance'][aid] = row[8]
            ratings['freestyle_distance'][aid] = row[9]
            ratings['counts'][aid] = row[10]

        return ratings


def update_ratings_for_event(event_id: int, date: str, discipline: str, results: list[dict]):
    """
    Update all relevant ratings for a single event.

    Args:
        event_id: Database event ID
        date: Event date
        discipline: Race discipline string
        results: List of {athlete_id, position}
    """
    race_type, technique = classify_discipline(discipline)

    # Get current ratings
    ratings = get_current_ratings()
    counts = ratings['counts']

    # Calculate updates for each relevant rating type
    overall_deltas = calculate_elo_updates(results, ratings['overall'], counts)

    # Type-specific (sprint or distance)
    type_deltas = calculate_elo_updates(results, ratings[race_type], counts)

    # Technique-specific (classic or freestyle) - only if not mixed
    if technique in ('classic', 'freestyle'):
        tech_deltas = calculate_elo_updates(results, ratings[technique], counts)
        # Combined rating (e.g., classic_sprint, freestyle_distance)
        combined_key = f"{technique}_{race_type}"
        combined_deltas = calculate_elo_updates(results, ratings[combined_key], counts)
    else:
        tech_deltas = {}
        combined_deltas = {}
        combined_key = None

    # Apply updates to database
    with get_db() as conn:
        cursor = conn.cursor()

        for athlete_id in overall_deltas:
            old_overall = ratings['overall'].get(athlete_id, DEFAULT_RATING)
            new_overall = old_overall + overall_deltas[athlete_id]

            old_type = ratings[race_type].get(athlete_id, DEFAULT_RATING)
            new_type = old_type + type_deltas.get(athlete_id, 0)

            new_count = counts.get(athlete_id, 0) + 1

            # Build the update query dynamically based on what ratings apply
            type_col = f"{race_type}_rating"

            # Base upsert for overall + type rating
            cursor.execute(f"""
                INSERT INTO elo_ratings (athlete_id, overall_rating, {type_col}, race_count, last_race_date)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(athlete_id) DO UPDATE SET
                    overall_rating = ?,
                    {type_col} = ?,
                    race_count = ?,
                    last_race_date = ?
            """, (athlete_id, new_overall, new_type, new_count, date,
                  new_overall, new_type, new_count, date))

            # Update technique rating if applicable
            if technique in ('classic', 'freestyle'):
                old_tech = ratings[technique].get(athlete_id, DEFAULT_RATING)
                new_tech = old_tech + tech_deltas.get(athlete_id, 0)
                tech_col = f"{technique}_rating"
                cursor.execute(f"""
                    UPDATE elo_ratings SET {tech_col} = ? WHERE athlete_id = ?
                """, (new_tech, athlete_id))

                # Update combined rating
                old_combined = ratings[combined_key].get(athlete_id, DEFAULT_RATING)
                new_combined = old_combined + combined_deltas.get(athlete_id, 0)
                combined_col = f"{combined_key}_rating"
                cursor.execute(f"""
                    UPDATE elo_ratings SET {combined_col} = ? WHERE athlete_id = ?
                """, (new_combined, athlete_id))

            # Record history for overall rating
            cursor.execute("""
                INSERT INTO elo_history (athlete_id, event_id, date, rating_before, rating_after, rating_type)
                VALUES (?, ?, ?, ?, ?, 'overall')
            """, (athlete_id, event_id, date, old_overall, new_overall))


def process_all_results():
    """Process all historical results to build Elo ratings."""
    init_elo_tables()

    # Clear existing ratings
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM elo_ratings")
        cursor.execute("DELETE FROM elo_history")

    # Get all events with results, ordered by date
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT DISTINCT e.id, e.date, e.discipline, e.gender
            FROM events e
            INNER JOIN results r ON e.id = r.event_id
            WHERE e.date IS NOT NULL
            ORDER BY e.date, e.id
        """)
        events = cursor.fetchall()

    print(f"Processing {len(events)} events...")

    for i, (event_id, date, discipline, gender) in enumerate(events):
        # Get results for this event
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT athlete_id, position
                FROM results
                WHERE event_id = ?
                ORDER BY position
            """, (event_id,))
            results = [{'athlete_id': row[0], 'position': row[1]} for row in cursor.fetchall()]

        if results:
            update_ratings_for_event(event_id, date, discipline, results)

        if (i + 1) % 50 == 0:
            print(f"  Processed {i + 1}/{len(events)} events...")

    print(f"Done! Processed {len(events)} events.")


def calculate_decayed_rating(rating: float, last_race_date: str, as_of_date: Optional[str] = None) -> float:
    """
    Calculate rating with decay applied for inactivity.

    Args:
        rating: Raw rating from database
        last_race_date: Date of last race (ISO format)
        as_of_date: Calculate decay as of this date (default: today)

    Returns:
        Rating with decay applied, minimum of DEFAULT_RATING
    """
    if not last_race_date:
        return rating

    try:
        last_race = datetime.strptime(last_race_date, "%Y-%m-%d")
        if as_of_date:
            reference_date = datetime.strptime(as_of_date, "%Y-%m-%d")
        else:
            reference_date = datetime.now()

        days_inactive = (reference_date - last_race).days
        if days_inactive <= 0:
            return rating

        years_inactive = days_inactive / 365.0
        decay = years_inactive * DECAY_RATE_PER_YEAR

        # Don't decay below default rating
        decayed = max(rating - decay, DEFAULT_RATING)
        return decayed
    except ValueError:
        return rating


def get_top_rated(n: int = 20, rating_type: str = 'overall', gender: Optional[str] = None,
                  active_months: Optional[int] = None, apply_decay: bool = False) -> list[dict]:
    """
    Get top N rated athletes.

    Args:
        n: Number of athletes to return
        rating_type: Which rating to sort by
        gender: Filter by gender ('M' or 'W')
        active_months: Only include athletes who raced within this many months
        apply_decay: Apply rating decay for inactivity
    """
    column_map = {
        'overall': 'overall_rating',
        'sprint': 'sprint_rating',
        'distance': 'distance_rating',
        'classic': 'classic_rating',
        'freestyle': 'freestyle_rating',
        'classic_sprint': 'classic_sprint_rating',
        'freestyle_sprint': 'freestyle_sprint_rating',
        'classic_distance': 'classic_distance_rating',
        'freestyle_distance': 'freestyle_distance_rating'
    }
    column = column_map.get(rating_type, 'overall_rating')

    with get_db() as conn:
        cursor = conn.cursor()

        # Build query with optional active filter
        conditions = ["e.race_count >= 5"]
        params = []

        if gender:
            conditions.append("a.gender = ?")
            params.append(gender)

        if active_months:
            cutoff_date = (datetime.now() - timedelta(days=active_months * 30)).strftime("%Y-%m-%d")
            conditions.append("e.last_race_date >= ?")
            params.append(cutoff_date)

        where_clause = " AND ".join(conditions)

        cursor.execute(f"""
            SELECT a.name, a.nation, e.{column}, e.race_count, a.gender, e.last_race_date
            FROM elo_ratings e
            JOIN athletes a ON e.athlete_id = a.id
            WHERE {where_clause}
            ORDER BY e.{column} DESC
            LIMIT ?
        """, params + [n * 3 if apply_decay else n])  # Fetch more if applying decay (reordering)

        results = []
        for row in cursor.fetchall():
            rating = row[2]
            if apply_decay:
                rating = calculate_decayed_rating(rating, row[5])

            results.append({
                'name': row[0],
                'nation': row[1],
                'rating': round(rating),
                'races': row[3],
                'gender': row[4],
                'last_race': row[5]
            })

        # Re-sort by decayed rating if decay was applied
        if apply_decay:
            results.sort(key=lambda x: x['rating'], reverse=True)

        return results[:n]


def get_athlete_rating(athlete_id: int) -> Optional[dict]:
    """Get all ratings for a specific athlete."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT a.name, a.nation, a.gender,
                   e.overall_rating, e.sprint_rating, e.distance_rating,
                   e.classic_rating, e.freestyle_rating,
                   e.classic_sprint_rating, e.freestyle_sprint_rating,
                   e.classic_distance_rating, e.freestyle_distance_rating,
                   e.race_count
            FROM elo_ratings e
            JOIN athletes a ON e.athlete_id = a.id
            WHERE e.athlete_id = ?
        """, (athlete_id,))
        row = cursor.fetchone()

        if row:
            return {
                'name': row[0],
                'nation': row[1],
                'gender': row[2],
                'overall': round(row[3]),
                'sprint': round(row[4]),
                'distance': round(row[5]),
                'classic': round(row[6]),
                'freestyle': round(row[7]),
                'classic_sprint': round(row[8]),
                'freestyle_sprint': round(row[9]),
                'classic_distance': round(row[10]),
                'freestyle_distance': round(row[11]),
                'races': row[12]
            }
        return None


def get_ratings_at_date(target_date: str, n: int = 20, gender: Optional[str] = None) -> list[dict]:
    """
    Get top ratings as of a specific historical date.

    Uses the elo_history table to find ratings at that point in time.

    Args:
        target_date: Date in ISO format (YYYY-MM-DD)
        n: Number of athletes to return
        gender: Filter by gender ('M' or 'W')

    Returns:
        List of athletes with their ratings as of that date
    """
    with get_db() as conn:
        cursor = conn.cursor()

        # Get the most recent rating for each athlete as of target_date
        gender_filter = "AND a.gender = ?" if gender else ""
        params = [target_date, target_date]
        if gender:
            params.append(gender)
        params.append(n)

        cursor.execute(f"""
            SELECT a.name, a.nation, h.rating_after, a.gender, h.date,
                   (SELECT COUNT(*) FROM elo_history h2
                    WHERE h2.athlete_id = h.athlete_id AND h2.date <= ?) as race_count
            FROM elo_history h
            JOIN athletes a ON h.athlete_id = a.id
            WHERE h.rating_type = 'overall'
              AND h.date <= ?
              AND h.id = (
                  SELECT MAX(h3.id) FROM elo_history h3
                  WHERE h3.athlete_id = h.athlete_id
                    AND h3.rating_type = 'overall'
                    AND h3.date <= ?
              )
              {gender_filter}
            ORDER BY h.rating_after DESC
            LIMIT ?
        """, [target_date] + params)

        return [
            {
                'name': row[0],
                'nation': row[1],
                'rating': round(row[2]),
                'gender': row[3],
                'last_race': row[4],
                'races': row[5]
            }
            for row in cursor.fetchall()
        ]


def predict_matchup(athlete_a_id: int, athlete_b_id: int, race_type: str = 'overall') -> dict:
    """Predict head-to-head probability between two athletes."""
    column_map = {
        'overall': 'overall_rating',
        'sprint': 'sprint_rating',
        'distance': 'distance_rating',
        'classic': 'classic_rating',
        'freestyle': 'freestyle_rating',
        'classic_sprint': 'classic_sprint_rating',
        'freestyle_sprint': 'freestyle_sprint_rating',
        'classic_distance': 'classic_distance_rating',
        'freestyle_distance': 'freestyle_distance_rating'
    }
    column = column_map.get(race_type, 'overall_rating')

    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT e.athlete_id, a.name, e.{column}
            FROM elo_ratings e
            JOIN athletes a ON e.athlete_id = a.id
            WHERE e.athlete_id IN (?, ?)
        """, (athlete_a_id, athlete_b_id))

        rows = cursor.fetchall()
        if len(rows) != 2:
            return {'error': 'Athletes not found'}

        data = {row[0]: {'name': row[1], 'rating': row[2]} for row in rows}
        a_data = data.get(athlete_a_id)
        b_data = data.get(athlete_b_id)

        if not a_data or not b_data:
            return {'error': 'Athletes not found'}

        prob_a = expected_score(a_data['rating'], b_data['rating'])

        return {
            'athlete_a': a_data['name'],
            'athlete_b': b_data['name'],
            'rating_a': round(a_data['rating']),
            'rating_b': round(b_data['rating']),
            'win_prob_a': round(prob_a * 100, 1),
            'win_prob_b': round((1 - prob_a) * 100, 1)
        }
