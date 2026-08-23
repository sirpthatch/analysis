"""Configuration for the marathon-results ETL.

Windows are expressed as {name: days}. Adding a window here is the only change
required to featurize it - Stage 2 computes rolling aggregates per city-day, so a
new window costs one more pass over ~1M rows rather than a re-scan per race.
"""
from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class WeatherConfig:
    # Training windows, measured backwards from race day: [race_date - N, race_date).
    # Race day itself is excluded - it is a covariate, not training.
    WINDOWS: dict = field(default_factory=lambda: {"full": 90, "peak": 30})

    # Fraction of a window's days that must be observed for its features to be used.
    MIN_WINDOW_COVERAGE: float = 0.8

    # Precipitation threshold in INCHES. The legacy config said 0.2 "(inches)" but
    # the stored data was millimetres, so the old day-counts thresholded at 0.2mm -
    # any measurable trace. See migrate_legacy_weather.py.
    PRECIP_THRESHOLD_IN: float = 0.2

    HEAT_DAY_F: float = 80.0     # temp_max at or above -> heat exposure day
    COLD_DAY_F: float = 40.0     # temp_max at or below -> cold exposure day
    FREEZE_F: float = 32.0       # temp_min at or below -> freeze day

    # Heat acclimatization decays; recent exposure counts for more than a flat mean.
    ACCLIM_HALFLIFE_DAYS: int = 21

    # Days that plausibly disrupt training altogether.
    HOSTILE_PRECIP_IN: float = 0.2
    HOSTILE_COLD_F: float = 25.0
    HOSTILE_HEAT_F: float = 90.0

    # The archive actually starts 1999-01-01; the old value of 2000-01-01 discarded
    # roughly 15 months of usable early-season races.
    EARLIEST_WEATHER_DATE: datetime = datetime(1999, 1, 1)


@dataclass
class JoinConfig:
    DEDUP_STRATEGY: str = "keep_first"   # 'keep_first' | 'keep_last' | 'keep_all'
    NORMALIZE_CASE: bool = True


@dataclass
class PanelConfig:
    MIN_TIME_MIN: float = 120.0
    MAX_TIME_MIN: float = 420.0
    MIN_AGE: int = 18
    MAX_AGE: int = 85
    MIN_INSTANCE_FINISHERS: int = 100
    MIN_INSTANCE_CITIES: int = 5
    MIN_CITY_RACE_YEARS: int = 3
