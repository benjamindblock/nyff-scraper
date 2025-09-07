"""
Custom exceptions for NYFF Scraper.

Author: Jack Murphy
"""


class NYFFScraperError(Exception):
    """Base exception for NYFF Scraper errors."""
    pass


class NetworkError(NYFFScraperError):
    """Raised when network requests fail."""
    pass


class CacheError(NYFFScraperError):
    """Raised when cache operations fail."""
    pass


class ParsingError(NYFFScraperError):
    """Raised when HTML parsing fails."""
    pass


class DataExtractionError(NYFFScraperError):
    """Raised when data extraction from elements fails."""
    pass


class ExportError(NYFFScraperError):
    """Raised when data export operations fail."""
    pass