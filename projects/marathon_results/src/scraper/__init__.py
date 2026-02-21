"""Marathon results scraper for marathonguide.com"""

from .scraper import scrape_race_results
from .batch import batch_scrape_races

__all__ = ['scrape_race_results', 'batch_scrape_races']
