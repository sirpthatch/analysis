"""OMNY fare-cap analysis: what replaced the 30-Day Unlimited MetroCard.

Entry points:
    python src/collect.py all          # cache every aggregate pull
    python src/analyze.py --all        # composition, arithmetic, stations
    pytest tests/                      # re-verify recorded figures vs live API
"""

from . import constants, collect, fareclass, stations  # noqa: F401

__all__ = ["constants", "collect", "fareclass", "stations"]
