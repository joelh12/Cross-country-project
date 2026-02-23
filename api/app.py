import json
import os
from datetime import datetime
from typing import Optional

from fastapi import Depends, FastAPI, Header, HTTPException, Query, status

from api.schemas import (
    AthleteResponse,
    AthleteSearchResponse,
    BacktestJobRequest,
    CalendarJobRequest,
    HealthResponse,
    HistoryResponse,
    JobsListResponse,
    JobResponse,
    JobStatus,
    MatchupResponse,
    RankingsResponse,
    RatingType,
    ResultsJobRequest,
    StatsResponse,
)
from api.services import (
    build_matchup,
    get_athlete,
    get_database_path,
    get_history,
    get_job,
    list_jobs as list_jobs_service,
    get_rankings,
    get_stats,
    initialize_runtime,
    search_athletes,
    submit_backtest_job,
    submit_calendar_job,
    submit_elo_build_job,
    submit_results_job,
)

app = FastAPI(
    title="Cross-Country Skiing API",
    version="1.0.0",
    description="API for race data ingestion, Elo rankings, matchup predictions, and backtesting.",
)


@app.on_event("startup")
def on_startup() -> None:
    initialize_runtime()


def _model_dump(model: object) -> dict:
    if hasattr(model, "model_dump"):
        return model.model_dump()  # Pydantic v2
    return model.dict()  # Pydantic v1


def _normalize_gender(gender: Optional[str]) -> Optional[str]:
    if gender is None:
        return None
    normalized = gender.upper()
    if normalized not in ("M", "W"):
        raise HTTPException(status_code=400, detail="gender must be one of: M, W")
    return normalized


def require_api_key(x_api_key: Optional[str] = Header(default=None)) -> None:
    expected = os.getenv("CROSS_COUNTRY_API_KEY")
    if not expected:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="CROSS_COUNTRY_API_KEY is not configured on the server",
        )
    if x_api_key != expected:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key",
        )


@app.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    return HealthResponse(status="ok", database=get_database_path())


@app.get("/v1/stats", response_model=StatsResponse)
def stats() -> StatsResponse:
    return StatsResponse(**get_stats())


@app.get("/v1/rankings", response_model=RankingsResponse)
def rankings(
    rating_type: RatingType = Query(default="overall", alias="type"),
    gender: Optional[str] = Query(default=None),
    top: int = Query(default=20, ge=1, le=500),
    active: Optional[int] = Query(default=None, ge=1, le=120),
    decay: bool = Query(default=False),
) -> RankingsResponse:
    payload = get_rankings(
        rating_type=rating_type,
        top=top,
        gender=_normalize_gender(gender),
        active_months=active,
        decay=decay,
    )
    return RankingsResponse(**payload)


@app.get("/v1/athletes/search", response_model=AthleteSearchResponse)
def athlete_search(
    q: str = Query(min_length=2),
    limit: int = Query(default=10, ge=1, le=100),
    rated_only: bool = Query(default=False),
) -> AthleteSearchResponse:
    return AthleteSearchResponse(**search_athletes(q, limit=limit, rated_only=rated_only))


@app.get("/v1/athletes/{athlete_id}", response_model=AthleteResponse)
def athlete_details(athlete_id: int) -> AthleteResponse:
    data = get_athlete(athlete_id)
    if data is None:
        raise HTTPException(status_code=404, detail=f"Athlete {athlete_id} not found")
    return AthleteResponse(**data)


@app.get("/v1/matchup", response_model=MatchupResponse)
def matchup(
    a: str = Query(description="Athlete id or name fragment"),
    b: str = Query(description="Athlete id or name fragment"),
    rating_type: RatingType = Query(default="overall", alias="type"),
) -> MatchupResponse:
    try:
        payload = build_matchup(a, b, rating_type)
    except ValueError as exc:
        try:
            detail = json.loads(str(exc))
        except json.JSONDecodeError:
            detail = {"message": str(exc)}
        raise HTTPException(status_code=400, detail=detail) from exc

    return MatchupResponse(**payload)


@app.get("/v1/history", response_model=HistoryResponse)
def history(
    date: str = Query(description="YYYY-MM-DD"),
    gender: Optional[str] = Query(default=None),
    top: int = Query(default=20, ge=1, le=500),
) -> HistoryResponse:
    try:
        parsed_date = datetime.strptime(date, "%Y-%m-%d").date()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="date must be YYYY-MM-DD") from exc

    items = get_history(date, top=top, gender=_normalize_gender(gender))
    return HistoryResponse(date=parsed_date, gender=_normalize_gender(gender), top=top, items=items)


@app.post("/v1/jobs/calendar", response_model=JobResponse, dependencies=[Depends(require_api_key)])
def create_calendar_job(body: CalendarJobRequest) -> JobResponse:
    if body.end_year < body.start_year:
        raise HTTPException(status_code=400, detail="end_year must be >= start_year")
    return JobResponse(**submit_calendar_job(_model_dump(body)))


@app.post("/v1/jobs/results", response_model=JobResponse, dependencies=[Depends(require_api_key)])
def create_results_job(body: ResultsJobRequest) -> JobResponse:
    return JobResponse(**submit_results_job(_model_dump(body)))


@app.post("/v1/jobs/elo-build", response_model=JobResponse, dependencies=[Depends(require_api_key)])
def create_elo_build_job() -> JobResponse:
    return JobResponse(**submit_elo_build_job({}))


@app.post("/v1/jobs/backtest", response_model=JobResponse, dependencies=[Depends(require_api_key)])
def create_backtest_job(body: BacktestJobRequest) -> JobResponse:
    try:
        datetime.strptime(body.start, "%Y-%m-%d")
        datetime.strptime(body.end, "%Y-%m-%d")
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="start/end must be YYYY-MM-DD") from exc
    if body.start > body.end:
        raise HTTPException(status_code=400, detail="start must be <= end")
    return JobResponse(**submit_backtest_job(_model_dump(body)))


@app.get("/v1/jobs/{job_id}", response_model=JobResponse, dependencies=[Depends(require_api_key)])
def job_status(job_id: str) -> JobResponse:
    job = get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
    return JobResponse(**job)


@app.get("/v1/jobs", response_model=JobsListResponse, dependencies=[Depends(require_api_key)])
def list_jobs(
    limit: int = Query(default=20, ge=1, le=200),
    status_filter: Optional[JobStatus] = Query(default=None, alias="status"),
    job_type: Optional[str] = Query(default=None, alias="type"),
) -> JobsListResponse:
    jobs = list_jobs_service(limit=limit, status=status_filter, job_type=job_type)
    return JobsListResponse(items=[JobResponse(**job) for job in jobs])
