# Retired modules

Superseded by the Stage 2/3 pipeline. Kept for reference; not imported anywhere.

| file | replaced by | why |
|---|---|---|
| `process_weather_features.py` | `process_city_windows.py` + `process_training_weather.py` | Re-sliced each hometown's full weather history once per (race, date, city) group under a thread pool. Windows, the >5-record gate and the precip threshold were hardcoded in `_process_race_date_group`, duplicating the config-driven helpers that only the tests reached. The replacement computes rolling aggregates once per city-day. **Parity verified**: temperature features and precipitation totals match this module's output (`featurized_race_data_v3.csv`) to floating point. |
| `process_race_weather_join.py` | `process_training_weather.py` | Unfinished — `process_files` ended in `return 1/0`. |
| `enrich_race_day_weather.py` | `process_training_weather.py` (`attach_race_day`) | Standalone script outside the EtlModule framework; folded into Stage 3. |
| `test_process_weather_features.py` | `tests/test_process_city_windows.py` | Tested the retired module. Two of its cases (`TestFileTypeDetection`) already failed against that module before this work; the rest referenced config fields renamed in `etl_config.py` (`FULL_TRAINING_DAYS` -> `WINDOWS`, `PRECIP_THRESHOLD` -> `PRECIP_THRESHOLD_IN`). |

The precipitation-day counts intentionally do **not** match: the old threshold compared
0.2 against millimetres, so it counted any measurable trace as a rain day
(32.3 of 90 days on average, vs 12.7 at a true 0.2 inch threshold).
