"""
NYFF web scraper for extracting film lineup data.

Author: Jack Murphy
"""

import requests
from bs4 import BeautifulSoup
import os
import time
import re
import logging
import random
import gzip
import io
import zlib
from typing import List, Dict, Optional, Tuple
from urllib.parse import urljoin
from .exceptions import NetworkError, CacheError, ParsingError, DataExtractionError

# Optional import for brotli support
try:
    import brotli
    HAS_BROTLI = True
except ImportError:
    HAS_BROTLI = False

logger = logging.getLogger(__name__)

# Constants
DEFAULT_URL = "https://www.filmlinc.org/nyff/nyff63-lineup/"
DEFAULT_BACKUP_URL = "https://web.archive.org/web/https://www.filmlinc.org/nyff/nyff63-lineup/"  # Most recent snapshot
DEFAULT_CACHE_MAX_AGE_MINUTES = 45
DEFAULT_REQUEST_TIMEOUT = 30
DEFAULT_REQUEST_DELAY = 2
METADATA_YEAR_LENGTH = 4

# Rotating user agents to appear more like different browsers
USER_AGENTS = [
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:120.0) Gecko/20100101 Firefox/120.0',
]


class NYFFScraper:
    """Scraper for NYFF film lineup pages."""

    def __init__(self, cache_dir: str = "cache") -> None:
        """Initialize the scraper with optional caching.

        Args:
            cache_dir: Directory to cache web requests
        """
        self.session = requests.Session()
        self._setup_session()
        self.cache_dir = cache_dir
        self.ensure_cache_dir()

    def _setup_session(self) -> None:
        """Configure session with headers to avoid Cloudflare blocking."""
        # Use a random user agent
        user_agent = random.choice(USER_AGENTS)
        
        # Add basic headers that mimic a real browser
        self.session.headers.update({
            'User-Agent': user_agent,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
        })
        
        logger.debug(f"Using User-Agent: {user_agent}")

    def ensure_cache_dir(self) -> None:
        """Create cache directory if it doesn't exist.

        Raises:
            OSError: If directory creation fails
        """
        if not os.path.exists(self.cache_dir):
            os.makedirs(self.cache_dir)

    def _decode_response_content(self, response) -> str:
        """Decode response content, handling compression and encoding.
        
        Args:
            response: requests Response object
            
        Returns:
            Decoded HTML content as string
        """
        try:
            # Debug: log response headers
            logger.debug(f"Response headers: {dict(response.headers)}")
            logger.debug(f"Response encoding: {response.encoding}")
            
            # Check if response is compressed
            content_encoding = response.headers.get('content-encoding', '').lower()
            raw_content = response.content
            
            logger.debug(f"Content-Encoding: '{content_encoding}', Content length: {len(raw_content)}")
            
            if content_encoding == 'gzip':
                logger.debug("Decompressing gzipped response")
                try:
                    raw_content = gzip.decompress(raw_content)
                except gzip.BadGzipFile:
                    logger.warning("Failed to decompress gzipped content, using raw content")
            elif content_encoding == 'deflate':
                logger.debug("Decompressing deflated response")
                try:
                    raw_content = zlib.decompress(raw_content)
                except zlib.error:
                    logger.warning("Failed to decompress deflated content, using raw content")
            elif content_encoding == 'br':
                logger.debug("Decompressing brotli response")
                if HAS_BROTLI:
                    try:
                        raw_content = brotli.decompress(raw_content)
                    except brotli.error:
                        logger.warning("Failed to decompress brotli content, using raw content")
                else:
                    logger.warning("Brotli decompression not available, using raw content")
            
            # Decode to string
            if response.encoding:
                encoding = response.encoding
            else:
                # Try to detect encoding from content
                encoding = 'utf-8'
                # Look for charset in content-type header
                content_type = response.headers.get('content-type', '')
                if 'charset=' in content_type:
                    try:
                        encoding = content_type.split('charset=')[1].split(';')[0].strip()
                    except IndexError:
                        pass
            
            content = raw_content.decode(encoding, errors='replace')
            
            # Validate that content looks like HTML
            content_start = content.strip()[:100].lower()
            if content_start and not (content_start.startswith('<!doctype') or 
                                    content_start.startswith('<html') or
                                    '<html' in content_start):
                logger.warning(f"Response doesn't appear to be valid HTML. Content starts with: {content_start[:50]}...")
                # Return None to trigger backup URL fallback
                return None
            
            return content
            
        except Exception as e:
            logger.error(f"Error decoding response content: {e}")
            # Fallback to response.text
            try:
                return response.text
            except Exception as fallback_error:
                logger.error(f"Fallback to response.text also failed: {fallback_error}")
                return ""

    def get_cached_or_fetch(
            self,
            url: str,
            filename: str,
            max_age_minutes: Optional[int] = None,
            force_refresh: bool = False) -> Optional[str]:
        """Get content from cache or fetch from URL.

        Args:
            url: URL to fetch content from
            filename: Cache filename to use for storage
            max_age_minutes: Maximum age of cache file in minutes. If None, no age limit is applied
            force_refresh: If True, ignore cache and always fetch fresh content

        Returns:
            HTML content as string if successful, None if fetch failed

        Raises:
            NetworkError: If both primary and backup network requests fail
        """
        cache_file_path = os.path.join(self.cache_dir, filename)

        # Check if we should use cached file
        should_use_cache = False
        if os.path.exists(cache_file_path) and not force_refresh:
            if max_age_minutes is None:
                should_use_cache = True
            else:
                # Check file age
                file_age_seconds = time.time() - os.path.getmtime(cache_file_path)
                file_age_minutes = file_age_seconds / 60
                if file_age_minutes <= max_age_minutes:
                    should_use_cache = True
                else:
                    logger.info(
                        f"Cache file {filename} is {
                            file_age_minutes:.1f} minutes old (max: {max_age_minutes}), refreshing")

        if should_use_cache:
            logger.info(f"Loading from cache: {filename}")
            with open(cache_file_path, 'r', encoding='utf-8') as f:
                return f.read()

        logger.info(f"Fetching: {url}")
        
        # Retry logic with exponential backoff
        max_retries = 3
        base_delay = DEFAULT_REQUEST_DELAY
        
        for attempt in range(max_retries):
            try:
                # Add some jitter to avoid synchronized requests
                pre_delay = base_delay * (2 ** attempt) + random.uniform(0.1, 0.5)
                if attempt > 0:
                    logger.info(f"Retry attempt {attempt + 1}/{max_retries} after {pre_delay:.1f}s delay")
                    time.sleep(pre_delay)
                
                # Rotate user agent for retries to avoid fingerprinting
                if attempt > 0:
                    new_user_agent = random.choice(USER_AGENTS)
                    self.session.headers['User-Agent'] = new_user_agent
                    logger.debug(f"Switched to User-Agent: {new_user_agent}")
                
                response = self.session.get(url, timeout=DEFAULT_REQUEST_TIMEOUT)
                response.raise_for_status()
                
                # Handle compressed responses
                content = self._decode_response_content(response)
                
                # Only cache and return if content is valid
                if content is not None:
                    # Cache the content
                    os.makedirs(self.cache_dir, exist_ok=True)
                    with open(cache_file_path, 'w', encoding='utf-8') as f:
                        f.write(content)

                    logger.info(f"Successfully fetched {url} on attempt {attempt + 1}")
                    
                    # Be nice to servers - longer delay after successful fetch
                    time.sleep(random.uniform(1.0, 2.0))
                    return content
                else:
                    logger.warning(f"Invalid content received from {url}, will not cache")
                    # Continue to next attempt or fall through to return None

            except (requests.RequestException, requests.Timeout) as e:
                logger.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
                
                # Don't sleep after the last attempt
                if attempt < max_retries - 1:
                    # Check if it's a Cloudflare-related error
                    if hasattr(e, 'response') and e.response is not None:
                        if e.response.status_code in [403, 503, 429]:
                            logger.warning(f"Cloudflare-related error {e.response.status_code}, using longer delay")
                            time.sleep(random.uniform(5.0, 10.0))
                        elif e.response.status_code == 429:  # Rate limited
                            logger.warning("Rate limited, using extra long delay")
                            time.sleep(random.uniform(10.0, 20.0))
        
        logger.error(f"All {max_retries} attempts failed for {url}")
        return None

    def scrape_nyff_lineup(
            self,
            url: Optional[str] = None,
            backup_url: Optional[str] = None,
            force_refresh: bool = False) -> List[Dict]:
        """Scrape NYFF lineup page for films and showtimes.

        Args:
            url: URL to scrape (defaults to NYFF 2025 lineup)
            backup_url: Backup URL to scrape (defaults to Wayback Machine NYFF 2025 lineup)
            force_refresh: Force refresh of NYFF cache

        Returns:
            List of film dictionaries
        """
        url = url or DEFAULT_URL
        backup_url = backup_url or DEFAULT_BACKUP_URL

        # Use cache age limit for NYFF lineup
        content = self.get_cached_or_fetch(
            url,
            "nyff_lineup.html",
            max_age_minutes=DEFAULT_CACHE_MAX_AGE_MINUTES,
            force_refresh=force_refresh)

        # Use the backup URL if the primary failed
        if content is None:
            logger.info(
                "Primary URL failed, trying Archive.org backup URLs...")
            
            # Try multiple Archive.org URL patterns for getting recent snapshots
            backup_urls = [
                backup_url,  # Most recent snapshot
                "https://web.archive.org/web/2/https://www.filmlinc.org/nyff/nyff63-lineup/",  # Alternative most recent
                "https://web.archive.org/web/20250831221540/https://www.filmlinc.org/nyff/nyff63-lineup/",  # Known working snapshot
            ]
            
            for i, backup in enumerate(backup_urls, 1):
                logger.info(f"Trying backup URL {i}/{len(backup_urls)}: {backup}")
                content = self.get_cached_or_fetch(
                    backup,
                    f"nyff_lineup_backup_{i}.html",
                    max_age_minutes=DEFAULT_CACHE_MAX_AGE_MINUTES,
                    force_refresh=force_refresh)
                if content:
                    break
                    
        if not content:
            logger.warning("No content retrieved from primary or any backup URLs")
            return []

        try:
            soup = BeautifulSoup(content, 'html.parser')
        except Exception as e:
            raise ParsingError(f"Failed to parse HTML content: {e}") from e
        films = []

        # Look for film containers based on NYFF structure
        film_containers = soup.select(
            'div.py-8.lg\\:py-10.border-b.border-border')

        logger.info(f"Found {len(film_containers)} potential film containers")

        for container in film_containers:
            film_data = self.extract_film_data(container)
            if film_data:
                films.append(film_data)

        logger.info(f"Extracted {len(films)} films from NYFF lineup")
        return films

    def extract_film_data(self, element) -> Optional[Dict]:
        """Extract film data from a film element.

        Args:
            element: BeautifulSoup element containing film data

        Returns:
            Dictionary of film data or None if extraction fails
        """
        try:
            # Look for the film title link
            title_link = element.select_one('a[href*="/nyff2025/films/"]')
            if not title_link:
                return None

            # Extract title
            title_div = title_link.select_one('div')
            if not title_div:
                return None

            title = title_div.get_text(strip=True)
            if not title:
                return None

            # Extract director
            director = None
            director_elem = title_link.find_next('p')
            if director_elem:
                director_text = director_elem.get_text(strip=True)
                director = director_text if director_text else None

            # Extract description
            description = None
            prose_section = element.select_one('.typography.prose p')
            if prose_section:
                description_text = prose_section.get_text(strip=True)
                description = description_text if description_text else None

            # Extract showtimes
            showtimes = self.extract_showtimes(element)

            # Extract metadata (year, country, runtime)
            year, country, runtime = self.extract_metadata(element)

            return {
                "title": title,
                "director": director,
                "description": description,
                "year": year,
                "country": country,
                "runtime": runtime,
                "nyff_showtimes": showtimes
            }

        except Exception as e:
            logger.error(f"Error extracting film data: {e}")
            return None

    def extract_metadata(self,
                         element) -> Tuple[Optional[str],
                                           Optional[str],
                                           Optional[str]]:
        """Extract year, country, and runtime metadata.

        Args:
            element: BeautifulSoup element containing metadata

        Returns:
            Tuple of (year, country, runtime), each can be None if not found
        """
        year = None
        country = None
        runtime = None

        # Strategy 1: Look for flex container with metadata paragraphs
        flex_container = element.select_one('div.flex.flex-wrap')
        if flex_container:
            metadata_ps = flex_container.select(
                'p[data-typography-mobile="body-xs"]')

            for p in metadata_ps:
                text = p.get_text(strip=True)
                # Remove the separator "|" from text
                clean_text = text.replace('|', '').strip()

                # Check if it's a year (4 digits)
                if clean_text.isdigit() and len(clean_text) == METADATA_YEAR_LENGTH:
                    year = clean_text
                # Check if it contains "minutes" (runtime)
                elif 'minute' in clean_text.lower():
                    runtime = clean_text
                # Check if it contains "subtitle" (usually follows country)
                elif 'subtitle' in clean_text.lower():
                    # This is the subtitle line, skip it
                    continue
                # Otherwise, assume it's country (if not already found and not
                # empty)
                elif country is None and clean_text and clean_text not in [year, runtime]:
                    country = clean_text

        # Strategy 2: Fallback to original method
        if year is None and country is None and runtime is None:
            metadata_ps = element.select('p[data-typography-mobile="body-xs"]')
            for p in metadata_ps:
                text = p.get_text(strip=True)
                if '|' in text:
                    # This is likely the year|country|runtime line
                    parts = [part.strip() for part in text.split('|')]
                    if len(parts) >= 1 and parts[0].isdigit():
                        year = parts[0]
                    if len(parts) >= 2:
                        country = parts[1]
                    if len(parts) >= 3 and (
                        'minute' in parts[2].lower() or parts[2].replace(
                            ' ', '').isdigit()):
                        runtime = parts[2]
                    break

        return year, country, runtime

    def extract_showtimes(self, element) -> List[Dict]:
        """Extract showtime information from film element.

        Args:
            element: BeautifulSoup element containing showtime data

        Returns:
            List of showtime dictionaries
        """
        showtimes = []

        # Look for the showtimes section
        showtime_section = element.select_one('div.flex.flex-col.gap-2.mt-4')
        if not showtime_section:
            return showtimes

        # Find date sections
        date_sections = showtime_section.select(
            'div.flex.flex-col.gap-2.border-t.border-border.pt-2')

        for date_section in date_sections:
            # Extract date
            date_elem = date_section.select_one(
                'p[data-typography-mobile="d-eyebrow-sm"]')
            date = None
            if date_elem:
                date_text = date_elem.get_text(strip=True)
                date = date_text if date_text else None

            # Extract time buttons
            time_buttons = date_section.select('button')
            for button in time_buttons:
                button_text = button.get_text(strip=True)

                # Extract time from button text
                time_match = re.search(
                    r'\d{1,2}:\d{2}\s*(?:AM|PM)', button_text)
                showtime_time = None
                if time_match:
                    showtime_time = time_match.group()

                # Check for special notes
                notes = []
                if 'Q&A' in button_text:
                    notes.append('Q&A')
                if 'Intro' in button_text:
                    notes.append('Intro')

                # Check availability - multiple ways a showtime can be sold out
                button_classes = ' '.join(button.get('class', []))
                is_disabled = (
                    button.get('disabled') is not None or
                    'cursor-not-allowed' in button_classes or
                    'disabled' in button_classes
                )

                # Check for line-through styling on child elements (new
                # structure)
                has_linethrough = bool(button.select('.line-through'))

                is_available = not (is_disabled or has_linethrough)

                if showtime_time:
                    showtime_data = {
                        "date": date,
                        "time": showtime_time,
                        "venue": "TBA",
                        "notes": notes,
                        "available": is_available,
                        "raw_text": button_text
                    }
                    showtimes.append(showtime_data)

        return showtimes
