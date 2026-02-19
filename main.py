#!/usr/bin/env python3
"""
FIS Cross-Country Skiing Results Scraper

Scrapes race results from firstskisport.com and stores them in a SQLite database.
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from models.database import init_db, get_db
from scraper.calendar import scrape_calendars
from scraper.results import scrape_all_results
from models.elo import process_all_results, get_top_rated, get_athlete_rating, init_elo_tables, predict_matchup, get_ratings_at_date
from models.backtest import run_backtest, print_backtest_results


def cmd_init(args):
    """Initialize the database."""
    print("Initializing database...")
    init_db()
    print("Database initialized successfully!")


def cmd_scrape_calendar(args):
    """Scrape calendar pages."""
    init_db()
    print(f"Scraping calendars from {args.start_year} to {args.end_year}...")
    scrape_calendars(args.start_year, args.end_year, delay=args.delay)


def cmd_scrape_results(args):
    """Scrape results for all events."""
    init_db()
    print("Scraping results for events...")
    scrape_all_results(delay=args.delay, limit=args.limit)


def cmd_scrape_all(args):
    """Scrape both calendar and results."""
    init_db()
    print(f"Scraping calendars from {args.start_year} to {args.end_year}...")
    scrape_calendars(args.start_year, args.end_year, delay=args.delay)
    print("\nScraping results for all events...")
    scrape_all_results(delay=args.delay)


def cmd_stats(args):
    """Show database statistics."""
    init_db()
    with get_db() as conn:
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM events")
        events = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM athletes")
        athletes = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM results")
        results = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(DISTINCT event_id) FROM results")
        events_with_results = cursor.fetchone()[0]

        cursor.execute("SELECT MIN(date), MAX(date) FROM events")
        date_range = cursor.fetchone()

        cursor.execute("""
            SELECT gender, COUNT(*) FROM events GROUP BY gender
        """)
        events_by_gender = cursor.fetchall()

        cursor.execute("""
            SELECT gender, COUNT(*) FROM results GROUP BY gender
        """)
        results_by_gender = cursor.fetchall()

        cursor.execute("""
            SELECT nation, COUNT(*) as cnt FROM athletes
            GROUP BY nation ORDER BY cnt DESC LIMIT 10
        """)
        top_nations = cursor.fetchall()

    print("\n=== Database Statistics ===\n")
    print(f"Events:              {events}")
    print(f"Events with results: {events_with_results}")
    print(f"Athletes:            {athletes}")
    print(f"Results:             {results}")
    print(f"Date range:          {date_range[0]} to {date_range[1]}")

    print("\nEvents by gender:")
    for gender, count in events_by_gender:
        label = "Men" if gender == "M" else "Women"
        print(f"  {label}: {count}")

    print("\nResults by gender:")
    for gender, count in results_by_gender:
        label = "Men" if gender == "M" else "Women"
        print(f"  {label}: {count}")

    print("\nTop 10 nations by athletes:")
    for nation, count in top_nations:
        print(f"  {nation}: {count}")


def cmd_elo_build(args):
    """Build Elo ratings from historical results."""
    init_db()
    print("Building Elo ratings from historical results...")
    process_all_results()


def cmd_elo_rankings(args):
    """Show Elo rankings."""
    init_elo_tables()

    rating_type = args.type
    gender = args.gender.upper() if args.gender else None
    active_months = args.active

    print(f"\n=== Top {args.top} {rating_type.title()} Ratings", end="")
    if gender:
        print(f" ({('Men' if gender == 'M' else 'Women')})", end="")
    if active_months:
        print(f" [Active last {active_months} months]", end="")
    if args.decay:
        print(f" [With decay]", end="")
    print(" ===\n")

    rankings = get_top_rated(args.top, rating_type, gender, active_months, args.decay)

    if not rankings:
        print("No ratings found. Run 'python main.py elo-build' first.")
        return

    print(f"{'Rank':<5} {'Name':<25} {'Nation':<6} {'Rating':<7} {'Races':<6} {'Last Race':<12} {'Gender'}")
    print("-" * 75)
    for i, r in enumerate(rankings, 1):
        name = r['name'].encode('ascii', 'replace').decode('ascii')[:24]
        last_race = r.get('last_race', '')[:10] if r.get('last_race') else ''
        print(f"{i:<5} {name:<25} {r['nation']:<6} {r['rating']:<7} {r['races']:<6} {last_race:<12} {r['gender']}")


def cmd_elo_athlete(args):
    """Show ratings for a specific athlete."""
    init_elo_tables()

    # Search for athlete by name
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, name, nation, gender FROM athletes
            WHERE name LIKE ?
            LIMIT 10
        """, (f"%{args.name}%",))
        matches = cursor.fetchall()

    if not matches:
        print(f"No athlete found matching '{args.name}'")
        return

    if len(matches) > 1:
        print(f"Multiple matches for '{args.name}':")
        for aid, name, nation, gender in matches:
            print(f"  [{aid}] {name} ({nation}) - {gender}")
        print("\nUse a more specific name or athlete ID.")
        return

    athlete_id = matches[0][0]
    rating = get_athlete_rating(athlete_id)

    if not rating:
        print(f"No ratings found for this athlete. They may not have enough races.")
        return

    print(f"\n=== {rating['name']} ({rating['nation']}) - {rating['gender']} ===\n")
    print(f"  Overall:            {rating['overall']}")
    print(f"  Sprint:             {rating['sprint']}")
    print(f"  Distance:           {rating['distance']}")
    print(f"  Classic:            {rating['classic']}")
    print(f"  Freestyle:          {rating['freestyle']}")
    print(f"  Classic Sprint:     {rating['classic_sprint']}")
    print(f"  Freestyle Sprint:   {rating['freestyle_sprint']}")
    print(f"  Classic Distance:   {rating['classic_distance']}")
    print(f"  Freestyle Distance: {rating['freestyle_distance']}")
    print(f"  Races:              {rating['races']}")


def cmd_elo_matchup(args):
    """Predict head-to-head matchup between two athletes."""
    init_elo_tables()

    # Find first athlete
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT a.id, a.name, a.nation FROM athletes a
            JOIN elo_ratings e ON a.id = e.athlete_id
            WHERE a.name LIKE ?
            LIMIT 5
        """, (f"%{args.athlete1}%",))
        matches1 = cursor.fetchall()

    if not matches1:
        print(f"No rated athlete found matching '{args.athlete1}'")
        return
    if len(matches1) > 1:
        print(f"Multiple matches for '{args.athlete1}':")
        for aid, name, nation in matches1:
            print(f"  {name} ({nation})")
        return

    # Find second athlete
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT a.id, a.name, a.nation FROM athletes a
            JOIN elo_ratings e ON a.id = e.athlete_id
            WHERE a.name LIKE ?
            LIMIT 5
        """, (f"%{args.athlete2}%",))
        matches2 = cursor.fetchall()

    if not matches2:
        print(f"No rated athlete found matching '{args.athlete2}'")
        return
    if len(matches2) > 1:
        print(f"Multiple matches for '{args.athlete2}':")
        for aid, name, nation in matches2:
            print(f"  {name} ({nation})")
        return

    result = predict_matchup(matches1[0][0], matches2[0][0], args.type)

    if 'error' in result:
        print(result['error'])
        return

    print(f"\n=== {args.type.title()} Matchup Prediction ===\n")
    name1 = result['athlete_a'].encode('ascii', 'replace').decode('ascii')
    name2 = result['athlete_b'].encode('ascii', 'replace').decode('ascii')
    print(f"  {name1}: {result['rating_a']} ({result['win_prob_a']}%)")
    print(f"  {name2}: {result['rating_b']} ({result['win_prob_b']}%)")


def cmd_elo_history(args):
    """Show Elo rankings at a specific historical date."""
    init_elo_tables()

    gender = args.gender.upper() if args.gender else None

    print(f"\n=== Top {args.top} Ratings as of {args.date}", end="")
    if gender:
        print(f" ({('Men' if gender == 'M' else 'Women')})", end="")
    print(" ===\n")

    rankings = get_ratings_at_date(args.date, args.top, gender)

    if not rankings:
        print(f"No ratings found for date {args.date}.")
        return

    print(f"{'Rank':<5} {'Name':<25} {'Nation':<6} {'Rating':<7} {'Races':<6} {'Gender'}")
    print("-" * 60)
    for i, r in enumerate(rankings, 1):
        name = r['name'].encode('ascii', 'replace').decode('ascii')[:24]
        print(f"{i:<5} {name:<25} {r['nation']:<6} {r['rating']:<7} {r['races']:<6} {r['gender']}")


def cmd_backtest(args):
    """Run backtest on historical predictions."""
    init_elo_tables()

    gender = args.gender.upper() if args.gender else None

    results = run_backtest(
        start_date=args.start,
        end_date=args.end,
        gender=gender,
        min_participants=args.min_participants,
        use_discipline_ratings=not args.overall_only
    )

    print_backtest_results(results)


def main():
    parser = argparse.ArgumentParser(
        description="FIS Cross-Country Skiing Results Scraper & Elo Ratings"
    )
    subparsers = parser.add_subparsers(dest="command", help="Commands")

    init_parser = subparsers.add_parser("init", help="Initialize the database")
    init_parser.set_defaults(func=cmd_init)

    cal_parser = subparsers.add_parser("calendar", help="Scrape calendar pages")
    cal_parser.add_argument("--start-year", type=int, default=2020, help="Start year")
    cal_parser.add_argument("--end-year", type=int, default=2025, help="End year")
    cal_parser.add_argument("--delay", type=float, default=1.0, help="Delay between requests")
    cal_parser.set_defaults(func=cmd_scrape_calendar)

    res_parser = subparsers.add_parser("results", help="Scrape results for events")
    res_parser.add_argument("--delay", type=float, default=1.0, help="Delay between requests")
    res_parser.add_argument("--limit", type=int, help="Limit number of events to scrape")
    res_parser.set_defaults(func=cmd_scrape_results)

    all_parser = subparsers.add_parser("all", help="Scrape calendar and results")
    all_parser.add_argument("--start-year", type=int, default=2020, help="Start year")
    all_parser.add_argument("--end-year", type=int, default=2025, help="End year")
    all_parser.add_argument("--delay", type=float, default=1.0, help="Delay between requests")
    all_parser.set_defaults(func=cmd_scrape_all)

    stats_parser = subparsers.add_parser("stats", help="Show database statistics")
    stats_parser.set_defaults(func=cmd_stats)

    # Elo commands
    elo_build_parser = subparsers.add_parser("elo-build", help="Build Elo ratings from results")
    elo_build_parser.set_defaults(func=cmd_elo_build)

    elo_rank_parser = subparsers.add_parser("elo-rankings", help="Show Elo rankings")
    elo_rank_parser.add_argument("--type", choices=[
        'overall', 'sprint', 'distance', 'classic', 'freestyle',
        'classic_sprint', 'freestyle_sprint', 'classic_distance', 'freestyle_distance'
    ], default='overall', help="Rating type")
    elo_rank_parser.add_argument("--gender", choices=['m', 'w'], help="Filter by gender")
    elo_rank_parser.add_argument("--top", type=int, default=20, help="Number of athletes to show")
    elo_rank_parser.add_argument("--active", type=int, metavar="MONTHS",
                                  help="Only show athletes active in last N months")
    elo_rank_parser.add_argument("--decay", action="store_true",
                                  help="Apply rating decay for inactive athletes")
    elo_rank_parser.set_defaults(func=cmd_elo_rankings)

    elo_athlete_parser = subparsers.add_parser("elo-athlete", help="Show athlete ratings")
    elo_athlete_parser.add_argument("name", help="Athlete name (partial match)")
    elo_athlete_parser.set_defaults(func=cmd_elo_athlete)

    elo_matchup_parser = subparsers.add_parser("elo-matchup", help="Predict head-to-head matchup")
    elo_matchup_parser.add_argument("athlete1", help="First athlete name")
    elo_matchup_parser.add_argument("athlete2", help="Second athlete name")
    elo_matchup_parser.add_argument("--type", choices=[
        'overall', 'sprint', 'distance', 'classic', 'freestyle',
        'classic_sprint', 'freestyle_sprint', 'classic_distance', 'freestyle_distance'
    ], default='overall', help="Rating type for prediction")
    elo_matchup_parser.set_defaults(func=cmd_elo_matchup)

    elo_history_parser = subparsers.add_parser("elo-history", help="Show ratings at a historical date")
    elo_history_parser.add_argument("date", help="Date in YYYY-MM-DD format")
    elo_history_parser.add_argument("--gender", choices=['m', 'w'], help="Filter by gender")
    elo_history_parser.add_argument("--top", type=int, default=20, help="Number of athletes to show")
    elo_history_parser.set_defaults(func=cmd_elo_history)

    backtest_parser = subparsers.add_parser("backtest", help="Backtest predictions on historical races")
    backtest_parser.add_argument("--start", default="2020-01-01", help="Start date (YYYY-MM-DD)")
    backtest_parser.add_argument("--end", default="2025-12-31", help="End date (YYYY-MM-DD)")
    backtest_parser.add_argument("--gender", choices=['m', 'w'], help="Filter by gender")
    backtest_parser.add_argument("--min-participants", type=int, default=10, help="Minimum race participants")
    backtest_parser.add_argument("--overall-only", action="store_true",
                                  help="Use only overall ratings (not discipline-specific)")
    backtest_parser.set_defaults(func=cmd_backtest)

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        return

    args.func(args)


if __name__ == "__main__":
    main()
