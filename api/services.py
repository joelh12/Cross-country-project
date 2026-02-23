import json
import re
import threading
import traceback
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime
from typing import Callable, Optional
from uuid import uuid4

from models.backtest import run_backtest
from models.database import DB_PATH, get_db, init_db
from models.elo import (
    get_athlete_rating,
    get_ratings_at_date,
    get_top_rated,
    init_elo_tables,
    predict_matchup,
    process_all_results,
)
from scraper.calendar import scrape_calendars
from scraper.results import scrape_all_results

_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="api-job")
_futures: dict[str, Future] = {}
_futures_lock = threading.Lock()


def _now_utc_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _format_athlete_name(name: Optional[str]) -> Optional[str]:
    if not name:
        return name
    # Fix merged tokens like "KlæboJohannes" in legacy scraped data.
    spaced = re.sub(r"([a-zà-öø-ÿ])([A-ZÀ-ÖØ-Þ])", r"\1 \2", name)
    return re.sub(r"\s+", " ", spaced).strip()


def ensure_api_tables() -> None:
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS api_jobs (
                id TEXT PRIMARY KEY,
                job_type TEXT NOT NULL,
                status TEXT NOT NULL CHECK(status IN ('queued', 'running', 'succeeded', 'failed')),
                created_at TEXT NOT NULL,
                started_at TEXT,
                finished_at TEXT,
                params_json TEXT NOT NULL,
                result_json TEXT,
                error_text TEXT
            )
            """
        )
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_api_jobs_status ON api_jobs(status)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_api_jobs_created_at ON api_jobs(created_at DESC)")


def initialize_runtime() -> None:
    init_db()
    init_elo_tables()
    ensure_api_tables()


def get_stats() -> dict:
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
        min_date, max_date = cursor.fetchone()

        cursor.execute("SELECT gender, COUNT(*) FROM events GROUP BY gender ORDER BY gender")
        events_by_gender = [{"gender": row[0], "count": row[1]} for row in cursor.fetchall()]

        cursor.execute("SELECT gender, COUNT(*) FROM results GROUP BY gender ORDER BY gender")
        results_by_gender = [{"gender": row[0], "count": row[1]} for row in cursor.fetchall()]

        cursor.execute(
            """
            SELECT nation, COUNT(*) as cnt
            FROM athletes
            WHERE nation IS NOT NULL AND TRIM(nation) != ''
            GROUP BY nation
            ORDER BY cnt DESC
            LIMIT 10
            """
        )
        top_nations = [{"nation": row[0], "count": row[1]} for row in cursor.fetchall()]

    return {
        "events": events,
        "events_with_results": events_with_results,
        "athletes": athletes,
        "results": results,
        "min_date": min_date,
        "max_date": max_date,
        "events_by_gender": events_by_gender,
        "results_by_gender": results_by_gender,
        "top_nations": top_nations,
    }


def get_rankings(
    rating_type: str,
    top: int,
    gender: Optional[str],
    active_months: Optional[int],
    decay: bool,
) -> dict:
    normalized_gender = gender.upper() if gender else None
    rows = get_top_rated(top, rating_type, normalized_gender, active_months, decay)
    items = [
        {
            "rank": index + 1,
            "name": _format_athlete_name(row["name"]),
            "nation": row["nation"],
            "rating": row["rating"],
            "races": row["races"],
            "gender": row["gender"],
            "last_race": row.get("last_race"),
        }
        for index, row in enumerate(rows)
    ]

    return {
        "rating_type": rating_type,
        "gender": normalized_gender,
        "top": top,
        "active_months": active_months,
        "decay": decay,
        "items": items,
    }


def search_athletes(query: str, limit: int, rated_only: bool = False) -> dict:
    with get_db() as conn:
        cursor = conn.cursor()
        where = "WHERE a.name LIKE ?"
        params = [f"%{query}%"]
        if rated_only:
            where += " AND e.athlete_id IS NOT NULL"

        cursor.execute(
            f"""
            SELECT a.id, a.name, a.nation, a.gender, a.birth_year,
                   CASE WHEN e.athlete_id IS NULL THEN 0 ELSE 1 END AS has_rating
            FROM athletes a
            LEFT JOIN elo_ratings e ON e.athlete_id = a.id
            {where}
            ORDER BY has_rating DESC, a.name ASC
            LIMIT ?
            """,
            params + [limit],
        )
        rows = cursor.fetchall()

    items = [
        {
            "id": row[0],
            "name": _format_athlete_name(row[1]),
            "nation": row[2],
            "gender": row[3],
            "birth_year": row[4],
            "has_rating": bool(row[5]),
        }
        for row in rows
    ]
    return {"query": query, "limit": limit, "items": items}


def get_athlete(athlete_id: int) -> Optional[dict]:
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT id, fis_id, name, nation, gender, birth_year
            FROM athletes
            WHERE id = ?
            """,
            (athlete_id,),
        )
        row = cursor.fetchone()
        if not row:
            return None

    ratings = get_athlete_rating(athlete_id)
    if ratings and "name" in ratings:
        ratings["name"] = _format_athlete_name(ratings["name"])
    return {
        "id": row[0],
        "fis_id": row[1],
        "name": _format_athlete_name(row[2]),
        "nation": row[3],
        "gender": row[4],
        "birth_year": row[5],
        "ratings": ratings,
    }


def get_history(target_date: str, top: int, gender: Optional[str]) -> list[dict]:
    normalized_gender = gender.upper() if gender else None
    rows = get_ratings_at_date(target_date, top, normalized_gender)
    return [
        {
            "rank": index + 1,
            "name": _format_athlete_name(row["name"]),
            "nation": row["nation"],
            "rating": row["rating"],
            "gender": row["gender"],
            "last_race": row["last_race"],
            "races": row["races"],
        }
        for index, row in enumerate(rows)
    ]


def _find_rated_athlete(identifier: str) -> tuple[Optional[dict], list[dict]]:
    with get_db() as conn:
        cursor = conn.cursor()
        if identifier.isdigit():
            cursor.execute(
                """
                SELECT a.id, a.name, a.nation
                FROM athletes a
                JOIN elo_ratings e ON e.athlete_id = a.id
                WHERE a.id = ?
                LIMIT 10
                """,
                (int(identifier),),
            )
        else:
            cursor.execute(
                """
                SELECT a.id, a.name, a.nation
                FROM athletes a
                JOIN elo_ratings e ON e.athlete_id = a.id
                WHERE a.name LIKE ?
                ORDER BY a.name
                LIMIT 10
                """,
                (f"%{identifier}%",),
            )
        matches = [{"id": row[0], "name": row[1], "nation": row[2]} for row in cursor.fetchall()]

    if len(matches) == 1:
        return matches[0], matches
    return None, matches


def build_matchup(identifier_a: str, identifier_b: str, rating_type: str) -> dict:
    athlete_a, matches_a = _find_rated_athlete(identifier_a)
    if not athlete_a:
        raise ValueError(
            json.dumps(
                {
                    "message": f"Could not resolve athlete 'a' from '{identifier_a}'",
                    "matches": matches_a,
                }
            )
        )

    athlete_b, matches_b = _find_rated_athlete(identifier_b)
    if not athlete_b:
        raise ValueError(
            json.dumps(
                {
                    "message": f"Could not resolve athlete 'b' from '{identifier_b}'",
                    "matches": matches_b,
                }
            )
        )

    if athlete_a["id"] == athlete_b["id"]:
        raise ValueError(json.dumps({"message": "Athletes 'a' and 'b' must be different"}))

    prediction = predict_matchup(athlete_a["id"], athlete_b["id"], rating_type)
    if "error" in prediction:
        raise ValueError(json.dumps({"message": prediction["error"]}))

    return {
        "rating_type": rating_type,
        "athlete_a": {
            "athlete_id": athlete_a["id"],
            "name": _format_athlete_name(athlete_a["name"]),
            "nation": athlete_a["nation"],
            "rating": prediction["rating_a"],
            "win_probability": prediction["win_prob_a"],
        },
        "athlete_b": {
            "athlete_id": athlete_b["id"],
            "name": _format_athlete_name(athlete_b["name"]),
            "nation": athlete_b["nation"],
            "rating": prediction["rating_b"],
            "win_probability": prediction["win_prob_b"],
        },
    }


def _create_job_record(job_type: str, params: dict) -> str:
    job_id = str(uuid4())
    now = _now_utc_iso()
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO api_jobs (id, job_type, status, created_at, params_json)
            VALUES (?, ?, 'queued', ?, ?)
            """,
            (job_id, job_type, now, json.dumps(params)),
        )
    return job_id


def _update_job_record(job_id: str, **fields) -> None:
    if not fields:
        return
    columns = []
    values = []
    for key, value in fields.items():
        columns.append(f"{key} = ?")
        values.append(value)
    values.append(job_id)

    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(f"UPDATE api_jobs SET {', '.join(columns)} WHERE id = ?", values)


def get_job(job_id: str) -> Optional[dict]:
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT id, job_type, status, created_at, started_at, finished_at,
                   params_json, result_json, error_text
            FROM api_jobs
            WHERE id = ?
            """,
            (job_id,),
        )
        row = cursor.fetchone()

    if not row:
        return None

    return {
        "id": row[0],
        "job_type": row[1],
        "status": row[2],
        "created_at": row[3],
        "started_at": row[4],
        "finished_at": row[5],
        "params": json.loads(row[6]) if row[6] else {},
        "result": json.loads(row[7]) if row[7] else None,
        "error": row[8],
    }


def list_jobs(limit: int = 50, status: Optional[str] = None, job_type: Optional[str] = None) -> list[dict]:
    conditions = []
    params = []

    if status:
        conditions.append("status = ?")
        params.append(status)
    if job_type:
        conditions.append("job_type = ?")
        params.append(job_type)

    where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(
            f"""
            SELECT id, job_type, status, created_at, started_at, finished_at,
                   params_json, result_json, error_text
            FROM api_jobs
            {where_clause}
            ORDER BY created_at DESC
            LIMIT ?
            """,
            params + [limit],
        )
        rows = cursor.fetchall()

    jobs = []
    for row in rows:
        jobs.append(
            {
                "id": row[0],
                "job_type": row[1],
                "status": row[2],
                "created_at": row[3],
                "started_at": row[4],
                "finished_at": row[5],
                "params": json.loads(row[6]) if row[6] else {},
                "result": json.loads(row[7]) if row[7] else None,
                "error": row[8],
            }
        )
    return jobs


def _run_job(job_id: str, job_func: Callable[[dict], dict], params: dict) -> None:
    _update_job_record(job_id, status="running", started_at=_now_utc_iso())
    try:
        result = job_func(params)
        _update_job_record(
            job_id,
            status="succeeded",
            finished_at=_now_utc_iso(),
            result_json=json.dumps(result),
            error_text=None,
        )
    except Exception as exc:
        _update_job_record(
            job_id,
            status="failed",
            finished_at=_now_utc_iso(),
            error_text=f"{type(exc).__name__}: {exc}",
            result_json=json.dumps({"traceback": traceback.format_exc()}),
        )
        raise


def submit_job(job_type: str, params: dict, job_func: Callable[[dict], dict]) -> dict:
    initialize_runtime()
    job_id = _create_job_record(job_type, params)
    future = _executor.submit(_run_job, job_id, job_func, params)

    with _futures_lock:
        _futures[job_id] = future

    def _cleanup(_future: Future) -> None:
        with _futures_lock:
            _futures.pop(job_id, None)

    future.add_done_callback(_cleanup)
    job = get_job(job_id)
    if job is None:
        raise RuntimeError(f"Failed to retrieve created job '{job_id}'")
    return job


def _calendar_job(params: dict) -> dict:
    init_db()
    new_event_ids = scrape_calendars(
        params["start_year"],
        params["end_year"],
        delay=params["delay"],
    )
    return {"new_events": len(new_event_ids)}


def _results_job(params: dict) -> dict:
    init_db()
    added = scrape_all_results(delay=params["delay"], limit=params.get("limit"))
    return {"results_added": added}


def _elo_build_job(params: dict) -> dict:
    del params
    init_db()
    init_elo_tables()
    process_all_results()
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM elo_ratings")
        rated_athletes = cursor.fetchone()[0]
        cursor.execute("SELECT MAX(last_race_date) FROM elo_ratings")
        last_race_date = cursor.fetchone()[0]
    return {
        "rated_athletes": rated_athletes,
        "last_race_date": last_race_date,
    }


def _compact_backtest(results: dict) -> dict:
    by_discipline = {}
    for key, value in results.get("by_discipline", {}).items():
        races = value.get("races", 0)
        top1 = value.get("top1_correct", 0)
        top3 = value.get("top3_correct", 0)
        by_discipline[key] = {
            "races": races,
            "top1_accuracy": round((top1 / races), 4) if races else None,
            "top3_accuracy": round((top3 / races), 4) if races else None,
        }

    return {
        "total_races": results.get("total_races", 0),
        "top1_accuracy": round(results.get("top1_accuracy", 0), 4) if results.get("total_races") else None,
        "top3_accuracy": round(results.get("top3_accuracy", 0), 4) if results.get("total_races") else None,
        "top5_accuracy": round(results.get("top5_accuracy", 0), 4) if results.get("total_races") else None,
        "avg_brier_score": round(results.get("avg_brier_score", 0), 6) if results.get("brier_scores") else None,
        "by_discipline": by_discipline,
    }


def _backtest_job(params: dict) -> dict:
    init_db()
    init_elo_tables()

    gender = params.get("gender")
    normalized_gender = gender.upper() if gender else None

    full_results = run_backtest(
        start_date=params["start"],
        end_date=params["end"],
        gender=normalized_gender,
        min_participants=params["min_participants"],
        use_discipline_ratings=not params["overall_only"],
    )
    return _compact_backtest(full_results)


def submit_calendar_job(params: dict) -> dict:
    return submit_job("calendar", params, _calendar_job)


def submit_results_job(params: dict) -> dict:
    return submit_job("results", params, _results_job)


def submit_elo_build_job(params: dict) -> dict:
    return submit_job("elo-build", params, _elo_build_job)


def submit_backtest_job(params: dict) -> dict:
    return submit_job("backtest", params, _backtest_job)


def get_database_path() -> str:
    return str(DB_PATH)
