"""Verified constants for the OMNY fare-cap analysis.

Every dataset id, column name and figure in this module was confirmed against
the live endpoint on 2026-09-28. Anything that could NOT be verified in that
session is marked UNVERIFIED and must be sourced before it appears in the
writeup. Do not trust these as still-current: re-run `pytest tests/` to check
the recorded figures against the live APIs before publishing.
"""

# ---------------------------------------------------------------------------
# Socrata domains. Note the split: the ridership data is STATE open data, the
# Fair Fares enrollment data is CITY open data. Different domains, same API.
# ---------------------------------------------------------------------------
DOMAIN_STATE = "data.ny.gov"
DOMAIN_CITY = "data.cityofnewyork.us"

# ---------------------------------------------------------------------------
# Datasets. Verified 2026-09-28 unless noted.
# ---------------------------------------------------------------------------
DATASETS = {
    # --- Load-bearing. Columns confirmed by pulling records. ---
    "subway_2025": {
        "id": "5wq4-mkjj",
        "domain": DOMAIN_STATE,
        "name": "MTA Subway Hourly Ridership: Beginning 2025",
        "rows": 45_243_727,          # verified
        "rows_updated_at": "2026-09-23",  # verified (epoch 1790174781)
        "coverage_end": "2026-09-16",      # verified via max(transit_timestamp)
        "verified": True,
    },
    "subway_2020_2024": {
        "id": "wujg-7c2s",
        "domain": DOMAIN_STATE,
        "name": "MTA Subway Hourly Ridership: 2020-2024",
        "rows": None,
        "rows_updated_at": "2026-03-10",  # from catalog index, not metadata call
        "coverage_end": "2024-12-31",      # assumed from title, NOT verified
        "verified": True,  # queried successfully; schema matches subway_2025
    },
    "fair_fares_enrollees": {
        "id": "3tw8-6si8",
        "domain": DOMAIN_CITY,
        "name": "Fair Fares Enrollees (HRA)",
        "rows": 92,                   # verified
        "coverage": "2019-01-01 .. 2026-08-01",  # verified via min/max(month)
        "verified": True,
    },
    # --- Catalog-listed only. Ids came from the catalog search; columns NOT
    # --- confirmed. Verify before use.
    "subway_2017_2019": {"id": "t69i-h2me", "domain": DOMAIN_STATE, "verified": False},
    "bus_2025": {"id": "gxb3-akrn", "domain": DOMAIN_STATE, "verified": True},  # columns checked 2026-09-28
    "bus_2020_2024": {"id": "kv7t-n8in", "domain": DOMAIN_STATE, "verified": True},  # columns checked 2026-09-28
    "bus_fare_evasion": {"id": "uv5h-dfhp", "domain": DOMAIN_STATE, "verified": True},  # columns checked 2026-09-28
    "reduced_fare_metrocard": {"id": "v8fq-z483", "domain": DOMAIN_STATE, "verified": False},
    # --- Added 2026-09-28 on the first networked run. Columns confirmed from the
    # --- metadata endpoint and by pulling records.
    "subway_fare_evasion": {"id": "6kj3-ijvb", "domain": DOMAIN_STATE, "verified": True},
    "statement_of_operations": {"id": "yg77-3tkj", "domain": DOMAIN_STATE, "verified": True},
    "daily_ridership": {"id": "sayj-mze2", "domain": DOMAIN_STATE, "verified": True},
    "od_2024": {"id": "jsu2-fbtj", "domain": DOMAIN_STATE, "verified": True},
    "od_2026": {"id": "28vm-gjqr", "domain": DOMAIN_STATE, "verified": True},
}

# Columns confirmed present on both subway ridership datasets (identical schema).
RIDERSHIP_COLUMNS = [
    "transit_timestamp",
    "transit_mode",
    "station_complex_id",
    "station_complex",
    "borough",
    "payment_method",       # 'omny' | 'metrocard'
    "fare_class_category",
    "ridership",
    "transfers",
    "latitude",
    "longitude",
    "georeference",
]

FAIR_FARES_COLUMNS = ["month", "total_fair_fares_enrollees"]

# ---------------------------------------------------------------------------
# Fare schedule. Source: MTA board adoption, effective January 2026.
# https://www.mta.info/press-release/mta-board-adopts-fare-and-toll-increases-take-effect-january-2026
# ---------------------------------------------------------------------------
FARE_2026 = {
    "base": 3.00,
    "base_prior": 2.90,
    "reduced": 1.50,
    "reduced_prior": 1.45,
    "express_bus": 7.25,
    "express_bus_prior": 7.00,
    "weekly_cap": 35.00,
    "weekly_cap_reduced": 17.50,
}

# The MTA's own language: the 7-Day, 30-Day and Express Bus Plus unlimited
# passes "retire and be replaced with the automatic fare cap for all riders."
# The replacement cap is WEEKLY ONLY. Nothing replaced the 30-day product.
RETIRED_PASSES = ["MetroCard 7-Day Unlimited", "MetroCard 30-Day Unlimited",
                  "MetroCard Express Bus Plus Unlimited"]

# UNVERIFIED — do not publish without a primary source. I could not find the
# final sticker price of the 30-Day Unlimited before retirement in an MTA fare
# schedule or board document. $132 at a $2.90 base fare is the widely-repeated
# figure and is arithmetically consistent (132 / 2.90 = 45.5 rides breakeven),
# but it is not sourced. Pull it from an MTA fare schedule or board minutes.
METROCARD_30DAY_FINAL_PRICE_UNVERIFIED = 132.00

# Effective Transit Alliance's proposal, via Streetsblog 2026-02-18:
# "A cap of 46 rides over 30 days would at least restore the status quo."
ETA_PROPOSED_CAP_RIDES = 46

# ---------------------------------------------------------------------------
# Comparison windows. Sixteen days, same calendar position, two years apart.
# Chosen because subway_2025 coverage ends 2026-09-16 — this is the widest
# like-for-like window available. Both bounds are half-open [start, end).
# ---------------------------------------------------------------------------
WINDOWS = {
    2024: ("2024-09-01", "2024-09-17"),
    2026: ("2026-09-01", "2026-09-17"),
}
WINDOW_DAYS = 16

# Which dataset serves which window.
WINDOW_DATASET = {2024: "subway_2020_2024", 2026: "subway_2025"}

# ---------------------------------------------------------------------------
# Fare class figures recorded 2026-09-28. tests/test_verified_figures.py
# re-queries these against the live API. A mismatch is news, not a bug:
# it means the MTA restated the data.
# ---------------------------------------------------------------------------
RECORDED_FARE_CLASS_RIDERSHIP = {
    2024: {
        "Metrocard - Fair Fare": 1_918_087,
        "Metrocard - Full Fare": 6_671_554,
        "Metrocard - Other": 1_918_729,
        "Metrocard - Seniors & Disability": 1_811_049,
        "Metrocard - Students": 2,
        "Metrocard - Unlimited 30-Day": 3_981_746,
        "Metrocard - Unlimited 7-Day": 4_181_883,
        "OMNY - Fair Fare": 262,
        "OMNY - Full Fare": 30_363_420,
        "OMNY - Other": 251_321,
        "OMNY - Seniors & Disability": 201_844,
        "OMNY - Students": 2_343_858,
    },
    2026: {
        "Metrocard - Fair Fare": 3,
        "Metrocard - Full Fare": 55_699,
        "Metrocard - Other": 205_082,
        "Metrocard - Seniors & Disability": 4_665,
        "Metrocard - Unlimited 30-Day": 34,
        "OMNY - Fair Fare": 2_426_999,
        "OMNY - Full Fare": 48_900_551,
        "OMNY - Other": 2_345_608,
        "OMNY - Seniors & Disability": 3_394_573,
        "OMNY - Students": 1_571_206,
    },
}

# Re-recorded 2026-09-28 on the first live run. The scoping-session figures
# (53,599,970 / 58,876,993) came from `transit_timestamp > 'YYYY-09-01'`, which
# silently drops the 00:00 hour of Sept 1. They are NOT a restatement: the live
# API still returns them exactly for the strict-inequality filter. The figures
# here are for the half-open WINDOWS below, which is what the code queries.
# Both splits reconcile exactly to their independent window totals.
RECORDED_WINDOW_TOTALS = {2024: 53_643_755, 2026: 58_904_420}

# The unlimited-pass categories. Note 'Metrocard - Unlimited 7-Day' is ABSENT
# from the 2026 schema entirely — not zero, gone. Any code that groups on
# fare_class_category across both windows must tolerate missing categories.
UNLIMITED_CATEGORIES = ["Metrocard - Unlimited 30-Day", "Metrocard - Unlimited 7-Day"]
MONTHLY_PASS_CATEGORY = "Metrocard - Unlimited 30-Day"

# Recorded Fair Fares enrollment, most recent months (verified 2026-09-28).
RECORDED_FAIR_FARES = {
    "2026-08-01": 382_820,
    "2026-07-01": 384_372,
    "2026-06-01": 384_914,
    "2026-05-01": 383_377,
    "2026-04-01": 381_558,
}
