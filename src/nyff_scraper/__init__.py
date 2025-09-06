"""
NYFF Scraper - Extract film lineup data and enrich with IMDb, trailer, and metadata classification information.

Author: Jack Murphy
"""

__version__ = "0.1.0"
__author__ = "Jack Murphy"
__description__ = "Scrape NYFF film data and enrich with IMDb and YouTube trailer information"

from .scraper import NYFFScraper
from .imdb_enricher import IMDbEnricher
from .trailer_enricher import TrailerEnricher
from .metadata_enricher import MetadataEnricher
from .distribution_scorer import DistributionLikelihoodScorer
from .exporters import JSONExporter, CSVExporter, MarkdownExporter

__all__ = [
    "NYFFScraper",
    "IMDbEnricher", 
    "TrailerEnricher",
    "MetadataEnricher",
    "DistributionLikelihoodScorer",
    "JSONExporter",
    "CSVExporter", 
    "MarkdownExporter"
]