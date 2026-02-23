from datetime import date
from typing import Literal, Optional

from pydantic import BaseModel, Field


RatingType = Literal[
    "overall",
    "sprint",
    "distance",
    "classic",
    "freestyle",
    "classic_sprint",
    "freestyle_sprint",
    "classic_distance",
    "freestyle_distance",
]

JobStatus = Literal["queued", "running", "succeeded", "failed"]


class HealthResponse(BaseModel):
    status: Literal["ok"]
    database: str


class NationCount(BaseModel):
    nation: str
    count: int


class GenderCount(BaseModel):
    gender: Literal["M", "W"]
    count: int


class StatsResponse(BaseModel):
    events: int
    events_with_results: int
    athletes: int
    results: int
    min_date: Optional[str]
    max_date: Optional[str]
    events_by_gender: list[GenderCount]
    results_by_gender: list[GenderCount]
    top_nations: list[NationCount]


class RankingEntry(BaseModel):
    rank: int
    name: str
    nation: Optional[str]
    rating: int
    races: int
    gender: Literal["M", "W"]
    last_race: Optional[str]


class RankingsResponse(BaseModel):
    rating_type: RatingType
    gender: Optional[Literal["M", "W"]]
    top: int
    active_months: Optional[int]
    decay: bool
    items: list[RankingEntry]


class AthleteSearchItem(BaseModel):
    id: int
    name: str
    nation: Optional[str]
    gender: Literal["M", "W"]
    birth_year: Optional[int]
    has_rating: bool


class AthleteSearchResponse(BaseModel):
    query: str
    limit: int
    items: list[AthleteSearchItem]


class AthleteResponse(BaseModel):
    id: int
    fis_id: Optional[int]
    name: str
    nation: Optional[str]
    gender: Literal["M", "W"]
    birth_year: Optional[int]
    ratings: Optional[dict]


class MatchupSide(BaseModel):
    athlete_id: int
    name: str
    nation: Optional[str]
    rating: int
    win_probability: float


class MatchupResponse(BaseModel):
    rating_type: RatingType
    athlete_a: MatchupSide
    athlete_b: MatchupSide


class HistoryEntry(BaseModel):
    rank: int
    name: str
    nation: Optional[str]
    rating: int
    gender: Literal["M", "W"]
    last_race: Optional[str]
    races: int


class HistoryResponse(BaseModel):
    date: date
    gender: Optional[Literal["M", "W"]]
    top: int
    items: list[HistoryEntry]


class CalendarJobRequest(BaseModel):
    start_year: int = Field(default=2020, ge=1900, le=2100)
    end_year: int = Field(default=2026, ge=1900, le=2100)
    delay: float = Field(default=1.0, ge=0.0, le=30.0)


class ResultsJobRequest(BaseModel):
    delay: float = Field(default=1.0, ge=0.0, le=30.0)
    limit: Optional[int] = Field(default=None, ge=1, le=100000)


class EloBuildJobRequest(BaseModel):
    pass


class BacktestJobRequest(BaseModel):
    start: str = "2020-01-01"
    end: str = "2025-12-31"
    gender: Optional[Literal["M", "W", "m", "w"]] = None
    min_participants: int = Field(default=10, ge=2, le=500)
    overall_only: bool = False


class JobResponse(BaseModel):
    id: str
    job_type: str
    status: JobStatus
    created_at: str
    started_at: Optional[str]
    finished_at: Optional[str]
    params: dict
    result: Optional[dict]
    error: Optional[str]


class JobsListResponse(BaseModel):
    items: list[JobResponse]
