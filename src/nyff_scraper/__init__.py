"""
NYFF Scraper - Extract film lineup data and enrich with IMDb and trailer information.

Author: Jack Murphy
"""

__version__ = "0.1.0"
__author__ = "Jack Murphy"
__description__ = "Scrape NYFF film data and enrich with IMDb and YouTube trailer information"

from .scraper import NYFFScraper
from .imdb_enricher import IMDbEnricher
from .trailer_enricher import TrailerEnricher
from .exporters import JSONExporter, CSVExporter, MarkdownExporter

__all__ = [
    "NYFFScraper",
    "IMDbEnricher", 
    "TrailerEnricher",
    "JSONExporter",
    "CSVExporter", 
    "MarkdownExporter"
]