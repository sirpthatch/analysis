#!/usr/bin/env bash
# Training-weather pipeline, end to end. Stages are resumable: process.py skips
# partitions that already exist, so delete a stage's output directory to rebuild it.
set -euo pipefail
cd "$(dirname "$0")"
PY=.venv/bin/python

echo "== Stage 0: city roster =="
$PY src/build_city_roster.py --target-coverage 0.72

echo "== Stage 1: weather ingest =="
# Migrate the existing 109-city archive (applies the mm -> inch fix).
PYTHONPATH=src $PY src/migrate_legacy_weather.py
# Expansion beyond those cities is quota-bound; see the note in fetch_weather.py.
#   $PY src/fetch_weather.py --variables lean --api-key "$OPENMETEO_API_KEY"

echo "== Stage 0b: runner identity =="
(cd src && ../$PY process_runner_identity.py)

echo "== Stage 2: rolling window features =="
(cd src && ../$PY process.py ingest --src ../data/weather_daily \
    --out ../data/city_window_features --name process_city_windows.CityWindowEtlModule)

echo "== Stage 2b: record-level join =="
(cd src && ../$PY process.py ingest --src ../data/join_input \
    --out ../data/joined_race_data_v3 --name process_join.JoinEtlModule)

echo "== Stage 3: training + race-day weather =="
(cd src && ../$PY process_training_weather.py)

echo "== Stage 4: analysis panel =="
(cd src && ../$PY build_analysis_panel.py)

echo "== Stage 5: estimation =="
(cd src && ../$PY estimate_training_effect.py --spec all)

echo "== Stage 5b: hometown weather hypothesis (cell-level) =="
(cd src && ../$PY analyze_hometown_weather.py --min-n 5)

echo "== tests =="
$PY -m pytest tests/test_process_city_windows.py tests/test_process_join.py \
    tests/test_build_analysis_panel.py -q
