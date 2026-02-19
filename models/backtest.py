"""
Backtesting module for Elo predictions.

Tests how well the Elo ratings predict actual race outcomes
by simulating predictions using only data available before each race.
"""

import math
from typing import Optional
from models.database import get_db
from models.elo import expected_score, classify_discipline, DEFAULT_RATING

# Scale factor for converting Elo to win probability
# Lower value = more spread in probabilities, higher = more concentrated on favorites
ELO_SCALE = 400


def get_ratings_before_event(event_id: int, event_date: str, rating_type: str = 'overall') -> dict:
    """
    Get ratings for all athletes as they were BEFORE a specific event.

    For overall ratings, uses elo_history to find ratings at that point in time.
    For discipline-specific ratings, we use a proportional estimate based on
    current discipline rating vs current overall rating, applied to historical overall.
    """
    with get_db() as conn:
        cursor = conn.cursor()

        # Get historical overall ratings (these are accurate point-in-time)
        cursor.execute("""
            SELECT h.athlete_id, h.rating_after
            FROM elo_history h
            WHERE h.rating_type = 'overall'
              AND h.date < ?
              AND h.id = (
                  SELECT MAX(h2.id) FROM elo_history h2
                  WHERE h2.athlete_id = h.athlete_id
                    AND h2.rating_type = 'overall'
                    AND h2.date < ?
              )
        """, (event_date, event_date))

        historical_overall = {row[0]: row[1] for row in cursor.fetchall()}

        # If we just need overall, return it
        if rating_type == 'overall':
            return historical_overall

        # For discipline-specific ratings, we need to estimate historical values
        # Get current ratings to compute discipline offset from overall
        column_map = {
            'sprint': 'sprint_rating',
            'distance': 'distance_rating',
            'classic': 'classic_rating',
            'freestyle': 'freestyle_rating',
            'classic_sprint': 'classic_sprint_rating',
            'freestyle_sprint': 'freestyle_sprint_rating',
            'classic_distance': 'classic_distance_rating',
            'freestyle_distance': 'freestyle_distance_rating'
        }
        column = column_map.get(rating_type)

        if not column:
            return historical_overall

        cursor.execute(f"""
            SELECT athlete_id, overall_rating, {column}
            FROM elo_ratings
        """)

        # Calculate offset for each athlete: discipline_rating - overall_rating
        # Apply this offset to historical overall rating
        result = {}
        current_data = {row[0]: (row[1], row[2]) for row in cursor.fetchall()}

        for athlete_id, hist_overall in historical_overall.items():
            if athlete_id in current_data:
                curr_overall, curr_discipline = current_data[athlete_id]
                # Discipline offset captures athlete's relative strength in this discipline
                offset = curr_discipline - curr_overall
                # Apply offset to historical overall (capped to reasonable bounds)
                offset = max(-300, min(300, offset))
                result[athlete_id] = hist_overall + offset
            else:
                result[athlete_id] = hist_overall

        return result


def predict_race_outcome(ratings: dict, participants: list[int]) -> list[tuple[int, float]]:
    """
    Predict finish order based on ratings using Bradley-Terry model.

    Uses softmax over Elo ratings to calculate true multi-competitor
    win probability: P(i wins) = exp(r_i/scale) / sum(exp(r_j/scale))

    Returns list of (athlete_id, predicted_win_probability) sorted by probability.
    """
    if not participants:
        return []

    # Get ratings for all participants
    participant_ratings = []
    for athlete_id in participants:
        rating = ratings.get(athlete_id, DEFAULT_RATING)
        participant_ratings.append((athlete_id, rating))

    # Calculate softmax probabilities (Bradley-Terry model)
    # First, find max rating for numerical stability
    max_rating = max(r for _, r in participant_ratings)

    # Calculate exp(rating / scale) for each, shifted by max for stability
    exp_values = {}
    for athlete_id, rating in participant_ratings:
        exp_values[athlete_id] = math.exp((rating - max_rating) / ELO_SCALE)

    # Sum of all exp values
    total_exp = sum(exp_values.values())

    # Calculate actual probabilities
    probabilities = {}
    for athlete_id, exp_val in exp_values.items():
        probabilities[athlete_id] = exp_val / total_exp

    # Sort by probability descending
    sorted_probs = sorted(probabilities.items(), key=lambda x: x[1], reverse=True)
    return sorted_probs


def run_backtest(start_date: str = "2020-01-01", end_date: str = "2025-12-31",
                 gender: Optional[str] = None, min_participants: int = 10,
                 use_discipline_ratings: bool = True) -> dict:
    """
    Run backtest on historical races.

    Args:
        start_date: Start of backtest period
        end_date: End of backtest period
        gender: Filter by gender ('M' or 'W')
        min_participants: Minimum participants for a race to be included
        use_discipline_ratings: Use discipline-specific ratings for each race type

    Returns:
        Dictionary with backtest results and metrics
    """
    with get_db() as conn:
        cursor = conn.cursor()

        # Get all events in the period
        gender_filter = "AND e.gender = ?" if gender else ""
        params = [start_date, end_date]
        if gender:
            params.append(gender)

        cursor.execute(f"""
            SELECT e.id, e.date, e.discipline, e.gender, e.location,
                   COUNT(r.id) as participant_count
            FROM events e
            JOIN results r ON e.id = r.event_id
            WHERE e.date >= ? AND e.date <= ?
              AND e.date IS NOT NULL
              {gender_filter}
            GROUP BY e.id
            HAVING participant_count >= ?
            ORDER BY e.date
        """, params + [min_participants])

        events = cursor.fetchall()

    results = {
        'total_races': 0,
        'predictions': [],
        'top1_correct': 0,
        'top3_correct': 0,
        'top5_correct': 0,
        'calibration_buckets': {i: {'predicted': 0, 'actual': 0} for i in range(0, 100, 10)},
        'brier_scores': [],
        'by_discipline': {}
    }

    print(f"Backtesting {len(events)} races from {start_date} to {end_date}...")

    for i, (event_id, date, discipline, evt_gender, location, _) in enumerate(events):
        # Skip first few races (not enough history)
        if date < "2020-03-01":
            continue

        # Determine which rating type to use
        race_type, technique = classify_discipline(discipline)

        if use_discipline_ratings and technique in ('classic', 'freestyle'):
            rating_type = f"{technique}_{race_type}"
        elif use_discipline_ratings:
            rating_type = race_type  # Just sprint or distance for mixed technique
        else:
            rating_type = 'overall'

        # Get pre-race ratings
        ratings = get_ratings_before_event(event_id, date, rating_type)

        if not ratings:
            continue

        # Get actual results for this event
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT athlete_id, position
                FROM results
                WHERE event_id = ? AND position IS NOT NULL AND position < 900
                ORDER BY position
            """, (event_id,))
            actual_results = cursor.fetchall()

        if len(actual_results) < min_participants:
            continue

        participants = [r[0] for r in actual_results]
        actual_winner = actual_results[0][0]
        actual_top3 = set(r[0] for r in actual_results[:3])
        actual_top5 = set(r[0] for r in actual_results[:5])

        # Make prediction
        predictions = predict_race_outcome(ratings, participants)

        if not predictions:
            continue

        predicted_winner = predictions[0][0]
        predicted_top3 = set(p[0] for p in predictions[:3])
        predicted_top5 = set(p[0] for p in predictions[:5])

        # Score prediction
        results['total_races'] += 1

        if predicted_winner == actual_winner:
            results['top1_correct'] += 1

        if actual_winner in predicted_top3:
            results['top3_correct'] += 1

        if actual_winner in predicted_top5:
            results['top5_correct'] += 1

        # Calibration: track win probability vs actual outcome
        win_prob = predictions[0][1]
        bucket = min(int(win_prob * 100) // 10 * 10, 90)
        results['calibration_buckets'][bucket]['predicted'] += 1
        if predicted_winner == actual_winner:
            results['calibration_buckets'][bucket]['actual'] += 1

        # Brier score for top prediction
        actual_outcome = 1 if predicted_winner == actual_winner else 0
        brier = (win_prob - actual_outcome) ** 2
        results['brier_scores'].append(brier)

        # Track by discipline type
        combined = f"{technique}_{race_type}" if technique != 'mixed' else race_type

        if combined not in results['by_discipline']:
            results['by_discipline'][combined] = {
                'races': 0, 'top1_correct': 0, 'top3_correct': 0
            }

        results['by_discipline'][combined]['races'] += 1
        if predicted_winner == actual_winner:
            results['by_discipline'][combined]['top1_correct'] += 1
        if actual_winner in predicted_top3:
            results['by_discipline'][combined]['top3_correct'] += 1

        # Store prediction details
        results['predictions'].append({
            'event_id': event_id,
            'date': date,
            'discipline': discipline,
            'predicted_winner': predicted_winner,
            'actual_winner': actual_winner,
            'correct': predicted_winner == actual_winner,
            'win_prob': win_prob
        })

        if (i + 1) % 50 == 0:
            print(f"  Processed {i + 1}/{len(events)} races...")

    # Calculate summary metrics
    if results['total_races'] > 0:
        results['top1_accuracy'] = results['top1_correct'] / results['total_races']
        results['top3_accuracy'] = results['top3_correct'] / results['total_races']
        results['top5_accuracy'] = results['top5_correct'] / results['total_races']
        results['avg_brier_score'] = sum(results['brier_scores']) / len(results['brier_scores'])

        # Calculate calibration
        results['calibration'] = {}
        for bucket, data in results['calibration_buckets'].items():
            if data['predicted'] > 0:
                results['calibration'][bucket] = {
                    'count': data['predicted'],
                    'expected': (bucket + 5) / 100,  # midpoint of bucket
                    'actual': data['actual'] / data['predicted']
                }

    return results


def print_backtest_results(results: dict):
    """Print formatted backtest results."""
    if results['total_races'] == 0:
        print("No races to backtest.")
        return

    print(f"\n{'='*60}")
    print("BACKTEST RESULTS")
    print(f"{'='*60}\n")

    print(f"Total races analyzed: {results['total_races']}")
    print(f"\nPrediction Accuracy:")
    print(f"  Winner correct:      {results['top1_correct']:>4} / {results['total_races']} ({results['top1_accuracy']*100:.1f}%)")
    print(f"  Winner in top 3:     {results['top3_correct']:>4} / {results['total_races']} ({results['top3_accuracy']*100:.1f}%)")
    print(f"  Winner in top 5:     {results['top5_correct']:>4} / {results['total_races']} ({results['top5_accuracy']*100:.1f}%)")

    print(f"\nBrier Score: {results['avg_brier_score']:.4f}")
    print("  (Lower is better. 0.25 = random, 0 = perfect)")

    print(f"\nCalibration (predicted vs actual win rate):")
    for bucket in sorted(results['calibration'].keys()):
        data = results['calibration'][bucket]
        if data['count'] >= 5:  # Only show buckets with enough samples
            bar = '#' * int(data['actual'] * 20)
            print(f"  {bucket:>2}-{bucket+10:<2}%: {data['actual']*100:>5.1f}% actual (n={data['count']:>3}) {bar}")

    print(f"\nAccuracy by Discipline:")
    for disc, data in sorted(results['by_discipline'].items(), key=lambda x: x[1]['races'], reverse=True):
        if data['races'] >= 5:
            acc = data['top1_correct'] / data['races'] * 100
            t3_acc = data['top3_correct'] / data['races'] * 100
            print(f"  {disc:<20}: {acc:>5.1f}% winner, {t3_acc:>5.1f}% top3 (n={data['races']})")
