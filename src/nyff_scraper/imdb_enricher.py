"""
IMDb enricher for adding production company and distributor information.

Author: Jack Murphy
"""

import os
import requests
from bs4 import BeautifulSoup
import re
import time
import logging
import json
from difflib import SequenceMatcher
from typing import List, Dict, Optional, Tuple
from urllib.parse import quote
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class IMDbEnricher:
    """Enricher for adding IMDb production company and distributor data."""

    def __init__(self, cache_dir: str = "cache"):
        """Initialize the enricher with optional caching.

        Args:
            cache_dir: Directory to cache web requests
        """
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        self.cache_dir = cache_dir

    def search_imdb(
            self,
            film_title: str,
            year: str = "2025",
            director: str = "") -> Optional[str]:
        """Search IMDb for film and return IMDb ID.

        Args:
            film_title: Title of the film to search for
            year: Year to include in search (defaults to 2025)
            director: Director name to include for more accurate search

        Returns:
            IMDb ID (e.g., "tt1234567") or None if not found
        """
        def normalize_title(title: str) -> str:
            """Normalize title for search by handling special characters."""
            # Replace smart quotes and similar characters
            title = title.replace(''', "'").replace(''', "'")
            title = title.replace('"', '"').replace('"', '"')
            title = title.replace('—', '-').replace('–', '-')
            # Keep apostrophes, hyphens, and common punctuation for better matching
            # Only remove truly problematic characters
            title = re.sub(r'[^\w\s\'\-\.\:\!\?]', ' ', title)
            # Collapse multiple spaces
            title = re.sub(r'\s+', ' ', title).strip()
            return title

        # Normalize title for search
        clean_title = normalize_title(film_title)

        # Strategy 1: Use find search (most reliable for NYFF films)
        # This provides better results than advanced search for festival films
        search_attempts = []

        if director:
            clean_director = normalize_title(director)
            search_attempts.append(f"{clean_title} {clean_director} {year}")

        search_attempts.append(f"{clean_title} {year}")
        search_attempts.append(clean_title)

        for attempt_num, search_query in enumerate(search_attempts, 1):
            search_url = f"https://www.imdb.com/find/?q={
                quote(search_query)}&s=tt&ttype=ft"
            underscored_film_title = re.sub(r'[^\w]', '_', film_title)
            filename = f"imdb_search_{underscored_film_title}_attempt_{attempt_num}.html"
            content = self.get_cached_or_fetch(search_url, filename)

            if not content:
                continue

            soup = BeautifulSoup(content, 'html.parser')

            # Look for search results with improved selectors - most specific first
            result_selectors = [
                'a.ipc-metadata-list-summary-item__t[href*="/title/tt"]',  # Most specific - modern IMDb with title links
                'a.ipc-metadata-list-summary-item__t',  # Modern IMDb layout  
                '.find-title-result a[href*="/title/tt"]',  # Find results page title links
                'a[href*="/title/tt"]',  # Generic IMDb title links
                '.findResult .result_text a',  # Legacy layout
                '.titleResult a',  # Legacy layout
                '.find-section .findResult h3.findResult-text a',  # Legacy layout
                '.cli-title'  # Alternative layout
            ]

            for selector in result_selectors:
                links = soup.select(selector)
                logger.debug(f"Selector '{selector}' found {len(links)} links")
                for link in links:
                    href = link.get('href', '')
                    logger.debug(f"Checking link href: {href}")
                    match = re.search(r'/title/(tt\d+)/', href)
                    if match:
                        imdb_id = match.group(1)
                        link_title = link.get_text(strip=True)
                        logger.debug(f"Found IMDb ID {imdb_id} for title '{link_title}'")

                        validation_result = self._validate_search_result(
                                link, film_title, year, director)
                        
                        logger.debug(f"Validation result for {imdb_id}: {validation_result}")
                        
                        if validation_result == "valid":
                            # If we have a director to validate, check it on the main page
                            if director and director.strip():
                                found_director = self.get_director_from_imdb_page(imdb_id)
                                if found_director and self.validate_director_match(found_director, director):
                                    logger.info(
                                        f"Found IMDb ID for '{film_title}' (find search attempt {attempt_num}, director validated): {imdb_id}")
                                    return imdb_id
                                elif found_director:
                                    logger.warning(
                                        f"Director mismatch for '{film_title}' (IMDb: '{found_director}' vs expected: '{director}'), continuing search")
                                    continue
                                else:
                                    logger.warning(
                                        f"Could not find director for '{film_title}' on IMDb page, accepting anyway: {imdb_id}")
                                    return imdb_id
                            else:
                                # No director to validate, title + year match is sufficient
                                logger.info(
                                    f"Found IMDb ID for '{film_title}' (find search attempt {attempt_num}): {imdb_id}")
                                return imdb_id
                        # If validation_result == "invalid", continue to next result

            if attempt_num == 1 and director:
                logger.warning(
                    f"No IMDb ID found for '{film_title}' with director '{director}', trying without director")
            elif attempt_num == 2:
                logger.warning(
                    f"No IMDb ID found for '{film_title}' with year, trying title only")

        # Strategy 2: Fall back to advanced search if find search fails
        logger.info(f"Find search failed, trying advanced search for '{film_title}'")
        advanced_search_url = f"https://www.imdb.com/search/title/?title={
            quote(clean_title)}&release_date={year}-01-01,{year}-12-31"
        underscored_film_title = re.sub(r'[^\w]', '_', film_title)
        filename = f"imdb_advanced_search_{underscored_film_title}.html"
        content = self.get_cached_or_fetch(advanced_search_url, filename)

        if content:
            soup = BeautifulSoup(content, 'html.parser')

            # Advanced search result selectors
            advanced_selectors = [
                '.titleColumn h3 a',
                '.cli-title a',
                '.ipc-title a',
                'h3.ipc-title a',
                '.lister-item-header a'
            ]

            for selector in advanced_selectors:
                links = soup.select(selector)
                for link in links:
                    href = link.get('href', '')
                    match = re.search(r'/title/(tt\d+)/', href)
                    if match:
                        imdb_id = match.group(1)
                        link_text = link.get_text(strip=True)

                        # Validate result by checking if title is reasonably similar
                        if self._is_title_match(link_text, film_title) and self._validate_advanced_result(link, film_title, year, director):
                            # If we have a director to validate, check it on the main page  
                            if director and director.strip():
                                found_director = self.get_director_from_imdb_page(imdb_id)
                                if found_director and self.validate_director_match(found_director, director):
                                    logger.info(
                                        f"Found IMDb ID for '{film_title}' via advanced search (director validated): {imdb_id}")
                                    return imdb_id
                                elif found_director:
                                    logger.debug(
                                        f"Director mismatch in advanced search for '{film_title}' (IMDb: '{found_director}' vs expected: '{director}'), continuing")
                                    continue
                                else:
                                    logger.info(
                                        f"Found IMDb ID for '{film_title}' via advanced search (no director found): {imdb_id}")
                                    return imdb_id
                            else:
                                logger.info(
                                    f"Found IMDb ID for '{film_title}' via advanced search: {imdb_id}")
                                return imdb_id

        logger.warning(
            f"No IMDb ID found for '{film_title}' after all search attempts")
        return None

    def _is_title_match(
            self,
            found_title: str,
            expected_title: str,
            threshold: float = 0.7) -> bool:
        """Check if found title is similar enough to expected title."""
        try:

            # Normalize both titles for comparison
            found_clean = re.sub(r'[^\w\s]', ' ', found_title.lower().strip())
            expected_clean = re.sub(
                r'[^\w\s]', ' ', expected_title.lower().strip())

            # Calculate similarity ratio
            similarity = SequenceMatcher(
                None, found_clean, expected_clean).ratio()

            # Also check if expected title is contained in found title (handles
            # subtitles)
            contains_match = expected_clean in found_clean or found_clean in expected_clean

            return similarity >= threshold or contains_match
        except Exception:
            # Fallback to simple string matching if comparison fails
            found_clean = re.sub(r'[^\w\s]', ' ', found_title.lower().strip())
            expected_clean = re.sub(
                r'[^\w\s]', ' ', expected_title.lower().strip())
            return expected_clean in found_clean or found_clean in expected_clean

    def _validate_advanced_result(
            self,
            link_element,
            expected_title: str,
            expected_year: str,
            expected_director: str = "") -> bool:
        """Validate advanced search results."""
        try:
            # Get the parent container which usually has year and director info
            parent = link_element.find_parent()
            while parent and parent.name not in [
                    'li', 'div', 'article'] and parent.find_parent():
                parent = parent.find_parent()

            if not parent:
                return True  # If we can't find parent context, assume it's okay

            parent_text = parent.get_text()

            # Check for year in the parent text
            year_pattern = r'\b' + re.escape(expected_year) + r'\b'
            year_found = re.search(year_pattern, parent_text)

            # If director is provided, check for director match
            director_found = True
            if expected_director:
                director_words = expected_director.lower().split()
                # Check if any significant part of director name appears
                director_found = any(word in parent_text.lower()
                                     for word in director_words if len(word) > 2)

            return bool(year_found) and director_found
        except Exception:
            return True  # If validation fails, assume it's okay

    def _validate_search_result(
            self,
            link_element,
            expected_title: str,
            expected_year: str,
            expected_director: str = "") -> str:
        """Validate if a search result matches our expectations.
        
        Returns:
            "valid" if title and year match (director checked later on main page)
            "invalid" if title or year clearly don't match
        """
        try:
            # Get the title from the link
            link_title = link_element.get_text(strip=True)
            
            # Check title similarity first
            if not self._is_title_match(link_title, expected_title):
                logger.debug(f"Title mismatch: '{link_title}' vs '{expected_title}'")
                return "invalid"
            
            # Get the result text and surrounding context
            parent_text = link_element.find_parent().get_text(
                strip=True) if link_element.find_parent() else ""

            # Check if year matches (allow year to be part of longer text)
            year_found = re.search(expected_year, parent_text)

            if year_found:
                # Title + year match is sufficient - we'll validate director on the main page
                return "valid"
            else:
                logger.debug(f"Year mismatch: looking for '{expected_year}' in '{parent_text}'")
                return "invalid"
                
        except Exception as e:
            logger.debug(f"Validation error: {e}")
            return "invalid"

    def get_theatrical_release_date(self, imdb_id: str) -> Optional[str]:
        """Get theatrical release date from IMDb main page.

        Args:
            imdb_id: IMDb ID (e.g., "tt1234567")

        Returns:
            Theatrical release date string or None if not found
        """
        url = f"https://www.imdb.com/title/{imdb_id}/"
        filename = f"imdb_main_{imdb_id}.html"
        content = self.get_cached_or_fetch(url, filename)

        if not content:
            return None

        soup = BeautifulSoup(content, 'html.parser')

        # Look for release date information
        release_selectors = [
            'li[data-testid="title-pc-principal-credit"]:contains("Release date")',
            '.titlereference-overview-section li:contains("Release")',
            '[data-testid="title-details-releasedate"]',
            'span:contains("Release date") + span',
            'h4:contains("Release Date") + ul li']

        for selector in release_selectors:
            try:
                elements = soup.select(selector)
                for element in elements:
                    text = element.get_text(strip=True)
                    # Look for date patterns like "March 15, 2024", "15 March
                    # 2024", etc.
                    date_match = re.search(
                        r'(\d{1,2}\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4})',
                        text,
                        re.I)
                    if date_match:
                        return date_match.group(1)
                    # Alternative format: Month Day, Year
                    date_match = re.search(
                        r'((?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4})',
                        text,
                        re.I)
                    if date_match:
                        return date_match.group(1)
            except Exception:
                continue

        # Fallback: look for any date in the general structure
        try:
            # Look for structured data or meta tags
            script_tags = soup.find_all('script', type='application/ld+json')
            for script in script_tags:
                try:
                    data = json.loads(script.string)
                    if isinstance(data, dict) and 'datePublished' in data:
                        return data['datePublished']
                    elif isinstance(data, list):
                        for item in data:
                            if isinstance(
                                    item, dict) and 'datePublished' in item:
                                return item['datePublished']
                except (json.JSONDecodeError, AttributeError):
                    continue
        except Exception:
            pass

        logger.debug(f"No theatrical release date found for {imdb_id}")
        return None

    def get_director_from_imdb_page(self, imdb_id: str) -> Optional[str]:
        """Get director from IMDb main page.

        Args:
            imdb_id: IMDb ID (e.g., "tt1234567")

        Returns:
            Director name(s) or None if not found
        """
        url = f"https://www.imdb.com/title/{imdb_id}/"
        filename = f"imdb_main_{imdb_id}.html"
        content = self.get_cached_or_fetch(url, filename)

        if not content:
            return None

        soup = BeautifulSoup(content, 'html.parser')

        # Look for director information in various locations
        director_selectors = [
            # Modern IMDb layout
            'li[data-testid="title-pc-principal-credit"]:has(span:contains("Director")) .ipc-metadata-list-summary-item__t',
            'li[data-testid="title-pc-principal-credit"]:has(span:contains("Directors")) .ipc-metadata-list-summary-item__t',
            # Alternative selectors
            '.titlereference-overview-section li:contains("Director") a',
            'a[href*="/name/nm"]:contains("Director")',
            # JSON-LD structured data
        ]

        directors = []
        
        # Try CSS selectors first
        for selector in director_selectors:
            try:
                if ':contains(' in selector:
                    # Handle pseudo-selectors manually since BeautifulSoup doesn't support them
                    # Look for director credit sections
                    credit_items = soup.select('li[data-testid="title-pc-principal-credit"]')
                    for item in credit_items:
                        text_content = item.get_text().lower()
                        if 'director' in text_content:
                            links = item.select('.ipc-metadata-list-summary-item__t')
                            directors.extend([link.get_text(strip=True) for link in links])
                else:
                    elements = soup.select(selector)
                    directors.extend([elem.get_text(strip=True) for elem in elements])
            except Exception:
                continue

        # Try JSON-LD structured data
        if not directors:
            try:
                script_tags = soup.find_all('script', type='application/ld+json')
                for script in script_tags:
                    try:
                        data = json.loads(script.string)
                        if isinstance(data, dict) and 'director' in data:
                            director_data = data['director']
                            if isinstance(director_data, list):
                                directors.extend([d.get('name', '') for d in director_data if isinstance(d, dict) and 'name' in d])
                            elif isinstance(director_data, dict) and 'name' in director_data:
                                directors.append(director_data['name'])
                        elif isinstance(data, list):
                            for item in data:
                                if isinstance(item, dict) and 'director' in item:
                                    director_data = item['director']
                                    if isinstance(director_data, list):
                                        directors.extend([d.get('name', '') for d in director_data if isinstance(d, dict) and 'name' in d])
                                    elif isinstance(director_data, dict) and 'name' in director_data:
                                        directors.append(director_data['name'])
                    except (json.JSONDecodeError, AttributeError, KeyError):
                        continue
            except Exception:
                pass

        # Clean up and return
        directors = [d for d in directors if d and d.strip()]
        if directors:
            director_str = ', '.join(directors)
            logger.debug(f"Found director(s) for {imdb_id}: {director_str}")
            return director_str
        
        logger.debug(f"No director found for {imdb_id}")
        return None

    def validate_director_match(self, found_director: str, expected_director: str) -> bool:
        """Validate if found director matches expected director.
        
        Args:
            found_director: Director from IMDb page
            expected_director: Expected director from film data
            
        Returns:
            True if directors match (allowing for multiple directors, name variations)
        """
        if not found_director or not expected_director:
            return False
            
        # Normalize both director strings
        found_clean = re.sub(r'[^\w\s]', ' ', found_director.lower()).strip()
        expected_clean = re.sub(r'[^\w\s]', ' ', expected_director.lower()).strip()
        
        # Split expected directors by common separators
        expected_parts = re.split(r'\s+(?:and|&)\s+|,\s*', expected_clean)
        expected_parts = [part.strip() for part in expected_parts if part.strip()]
        
        # Check if any expected director appears in the found director
        for expected_part in expected_parts:
            if len(expected_part) > 2:  # Avoid very short matches
                # Check both full name match and surname match
                words = expected_part.split()
                if len(words) >= 2:
                    # Check if both first and last name appear
                    if all(word in found_clean for word in words):
                        return True
                    # Check if last name appears (for "Baumbach" matching "Noah Baumbach")
                    surname = words[-1]
                    if len(surname) > 3 and surname in found_clean:
                        return True
                elif expected_part in found_clean:
                    return True
        
        return False

    def _get_festival_date_range(self, films: List[Dict]) -> Tuple[Optional[datetime], Optional[datetime]]:
        """Calculate festival date range from film screening dates.
        
        Args:
            films: List of film dictionaries with date/time information
            
        Returns:
            Tuple of (earliest_date, latest_date) or (None, None) if no dates found
        """
        dates = []
        
        # Extract dates from film data
        for film in films:
            # Look for date/time fields in the film data
            for field in ['date', 'datetime', 'screening_date', 'showtime_date']:
                date_value = film.get(field)
                if date_value:
                    parsed_date = self._parse_date_from_string(str(date_value))
                    if parsed_date:
                        dates.append(parsed_date)
        
        if not dates:
            logger.warning("No festival dates found in film data - cannot determine festival window")
            return None, None
        
        earliest_date = min(dates)
        latest_date = max(dates)
        
        logger.info(f"Detected festival date range: {earliest_date.strftime('%Y-%m-%d')} to {latest_date.strftime('%Y-%m-%d')}")
        return earliest_date, latest_date

    def _parse_date_from_string(self, date_str: str) -> Optional[datetime]:
        """Parse a date from various string formats.
        
        Args:
            date_str: Date string to parse
            
        Returns:
            Parsed datetime or None if parsing fails
        """
        if not date_str:
            return None
            
        # Common date formats
        date_formats = [
            '%Y-%m-%d',           # "2025-09-26"
            '%Y-%m-%d %H:%M',     # "2025-09-26 19:30"
            '%B %d, %Y',          # "September 26, 2025"
            '%d %B %Y',           # "26 September 2025"
            '%m/%d/%Y',           # "09/26/2025"
            '%m-%d-%Y',           # "09-26-2025"
        ]
        
        for fmt in date_formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        
        # Try to extract date from longer strings (like "Thursday, September 26, 2025 at 7:30 PM")
        date_patterns = [
            r'(\d{4}-\d{2}-\d{2})',
            r'((?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4})',
            r'(\d{1,2}\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4})'
        ]
        
        for pattern in date_patterns:
            match = re.search(pattern, date_str, re.I)
            if match:
                date_part = match.group(1)
                parsed = self._parse_date_from_string(date_part)
                if parsed:
                    return parsed
        
        return None

    def _is_festival_date(self, date_str: str, films: List[Dict]) -> bool:
        """Check if a date falls within the festival window (dynamically calculated).
        
        Args:
            date_str: Date string to check
            films: List of films to calculate festival window from
            
        Returns:
            True if the date is within the festival window
        """
        if not date_str:
            return False
        
        parsed_date = self._parse_date_from_string(date_str)
        if not parsed_date:
            return False
        
        # Get the festival date range
        earliest_date, latest_date = self._get_festival_date_range(films)
        
        if not earliest_date or not latest_date:
            # This shouldn't happen if films data contains screening dates
            logger.error("No festival dates found in film data - cannot determine festival window")
            return False
        
        fest_start = earliest_date
        fest_end = latest_date
        
        is_festival_date = fest_start.date() <= parsed_date.date() <= fest_end.date()
        
        if is_festival_date:
            logger.info(f"Date '{date_str}' falls within festival window ({fest_start.strftime('%Y-%m-%d')} to {fest_end.strftime('%Y-%m-%d')}) - treating as festival premiere, not theatrical release")
        
        return is_festival_date

    def get_country_and_runtime(self, imdb_id: str) -> tuple[str, str]:
        """Get country and runtime from IMDb main page.

        Args:
            imdb_id: IMDb ID (e.g., "tt1234567")

        Returns:
            Tuple of (country, runtime) strings
        """
        url = f"https://www.imdb.com/title/{imdb_id}/"
        filename = f"imdb_main_{imdb_id}.html"
        content = self.get_cached_or_fetch(url, filename)

        if not content:
            return "", ""

        soup = BeautifulSoup(content, 'html.parser')
        country = ""
        runtime = ""

        # Strategy 1: Extract from JSON-LD structured data and other script
        # tags
        try:
            # Check all script tags for both JSON-LD and other JSON data
            script_tags = soup.find_all('script')
            for script in script_tags:
                try:
                    script_content = script.string or ""

                    # Handle JSON-LD scripts
                    if script.get('type') == 'application/ld+json':
                        data = json.loads(script_content)
                        if isinstance(data, dict):
                            # Get runtime from duration field (ISO 8601 format
                            # like "PT2H11M")
                            if 'duration' in data and not runtime:
                                duration = data['duration']
                                runtime = self._parse_iso_duration(duration)

                    # Handle other script tags that might contain
                    # countriesOfOrigin
                    elif 'countriesOfOrigin' in script_content and not country:
                        # Look for countriesOfOrigin pattern
                        country_match = re.search(
                            r'"countriesOfOrigin":\s*\{\s*"countries":\s*\[\s*\{\s*"id":\s*"([^"]+)"',
                            script_content)
                        if country_match:
                            # Use country code directly (AR, IT, US, etc.)
                            country = country_match.group(1)

                except (json.JSONDecodeError, AttributeError, TypeError):
                    continue
        except Exception:
            pass

        # Strategy 2: Extract runtime from meta description (e.g., "2h 11m")
        if not runtime:
            try:
                og_desc = soup.find('meta', property='og:description')
                if og_desc:
                    desc_content = og_desc.get('content', '')
                    # Look for patterns like "2h 11m" or "131 min"
                    time_match = re.search(
                        r'(\d+h\s*\d+m|\d+\s*min)', desc_content)
                    if time_match:
                        runtime = time_match.group(1)
            except Exception:
                pass

        # Strategy 3: Look for country in the page content
        if not country:
            try:
                # Look for country information in various places
                country_selectors = [
                    '[data-testid="title-details-origin"]',
                    'a[href*="/country/"]',
                    'li:contains("Country")',
                ]

                for selector in country_selectors:
                    try:
                        if ':contains(' in selector:
                            # Handle text-based selector
                            elements = soup.find_all('li')
                            for elem in elements:
                                elem_text = elem.get_text().lower()
                                if 'country' in elem_text and 'origin' in elem_text:
                                    # Extract country from this element
                                    links = elem.find_all('a')
                                    for link in links:
                                        href = link.get('href', '')
                                        if '/country/' in href:
                                            country = link.get_text(strip=True)
                                            break
                                    if country:
                                        break
                        else:
                            elements = soup.select(selector)
                            for element in elements:
                                country = element.get_text(strip=True)
                                if country:
                                    break
                        if country:
                            break
                    except Exception:
                        continue
            except Exception:
                pass

        logger.debug(
            f"Extracted from {imdb_id}: country='{country}', runtime='{runtime}'")
        return country, runtime

    def _parse_iso_duration(self, duration: str) -> str:
        """Parse ISO 8601 duration format to human readable.

        Args:
            duration: ISO duration like "PT2H11M"

        Returns:
            Human readable duration like "2h 11m"
        """
        try:
            # Parse PT2H11M format
            match = re.match(r'PT(?:(\d+)H)?(?:(\d+)M)?', duration)
            if match:
                hours, minutes = match.groups()
                parts = []
                if hours:
                    parts.append(f"{hours}h")
                if minutes:
                    parts.append(f"{minutes}m")
                return " ".join(parts) if parts else ""
        except Exception:
            pass
        return ""

    def get_company_credits(self, imdb_id: str) -> Dict[str, List[str]]:
        """Get production companies and distributors from IMDb company credits.

        Args:
            imdb_id: IMDb ID (e.g., "tt1234567")

        Returns:
            Dictionary with 'production_companies' and 'distributors' lists
        """
        url = f"https://www.imdb.com/title/{imdb_id}/companycredits/"
        filename = f"imdb_credits_{imdb_id}.html"
        content = self.get_cached_or_fetch(url, filename)

        if not content:
            return {"production_companies": [], "distributors": []}

        soup = BeautifulSoup(content, 'html.parser')

        production_companies = []
        distributors = []

        # Method 1: Look for specific section headers
        sections = soup.find_all(['h4', 'h3'], string=re.compile(
            r'production|distributor', re.I))

        for section in sections:
            section_text = section.get_text().lower()

            # Find the list of companies following this section
            next_element = section.find_next_sibling()
            companies = []

            while next_element and next_element.name not in ['h3', 'h4']:
                if next_element.name == 'ul':
                    companies.extend([li.get_text(strip=True)
                                     for li in next_element.find_all('li')])
                elif next_element.name == 'p':
                    companies.append(next_element.get_text(strip=True))

                next_element = next_element.find_next_sibling()

            if 'production' in section_text:
                production_companies.extend(companies)
            elif 'distributor' in section_text:
                distributors.extend(companies)

        # Method 2: Look for company links directly if no sections found
        if not production_companies and not distributors:
            company_links = soup.select('a[href*="/company/"]')

            for link in company_links:
                company_name = link.get_text(strip=True)
                # Try to determine if it's production or distribution based on
                # context
                context = link.find_parent().get_text().lower()

                if any(word in context for word in ['production', 'produced']):
                    production_companies.append(company_name)
                elif any(word in context for word in ['distribution', 'distributed']):
                    distributors.append(company_name)
                else:
                    # Default to production if unclear
                    production_companies.append(company_name)

        # Clean up duplicates and empty strings
        production_companies = list(
            set([pc for pc in production_companies if pc.strip()]))
        distributors = list(set([d for d in distributors if d.strip()]))

        logger.info(
            f"Found {
                len(production_companies)} production companies and {
                len(distributors)} distributors for {imdb_id}")

        return {
            "production_companies": production_companies,
            "distributors": distributors
        }

    def should_skip_imdb_lookup(self, film: Dict) -> tuple[bool, str]:
        """Check if we should skip IMDb lookup for this film.

        Args:
            film: Film dictionary

        Returns:
            Tuple of (should_skip, reason)
        """
        title = film.get('title', '')
        director = film.get('director', '')
        description = film.get('description', '')

        # Skip for dual films (containing '+' and multiple directors)
        if '+' in title and '/' in director:
            return True, "dual film screening"

        director_count = self._count_directors(director)

        # Skip for films with 3+ directors
        if director_count >= 3:
            return True, f"film with {director_count} directors"

        # Skip for Currents Programs with 3+ directors
        currents_indicators = {'currents', 'short', 'shorts'}
        text_to_check = f"{title} {description}".lower()

        if director_count >= 3 and any(
                indicator in text_to_check for indicator in currents_indicators):
            return True, "Currents program with multiple directors"

        return False, ""

    def _count_directors(self, director: str) -> int:
        """Count the number of directors in a director string.

        Args:
            director: Director string (e.g., "John Doe and Jane Smith")

        Returns:
            Number of directors detected
        """
        if not director:
            return 0

        # Use regex to split by multiple separators in one pass
        parts = re.split(r'\s+(?:and|&)\s+|,\s*|/', director)

        # Filter and count valid director names (non-empty with letters)
        return sum(1 for part in parts if part.strip()
                   and re.search(r'[A-Za-z]', part.strip()))

    def _extract_year_from_film(self, film: Dict) -> str:
        """Extract year for IMDb search.
        
        For NYFF films, default to 2025 since most are upcoming releases.
        
        Args:
            film: Film dictionary containing screening information
            
        Returns:
            Year as string, defaults to "2025" for NYFF films
        """
        # Always default to 2025 for NYFF films since most are upcoming releases
        return "2025"

    def _get_festival_start_date(self, films: List[Dict]) -> Optional[datetime]:
        """Get the first date of the festival by finding earliest screening across all films.
        
        Args:
            films: List of all film dictionaries with screening information
            
        Returns:
            Earliest screening date as datetime, or None if no dates found
        """
        all_dates = []
        
        for film in films:
            screenings = film.get('screenings', [])
            for screening in screenings:
                date = self._parse_date_from_string(screening)
                if date:
                    all_dates.append(date)
        
        if all_dates:
            earliest = min(all_dates)
            logger.info(f"Festival start date determined as: {earliest.strftime('%Y-%m-%d')}")
            return earliest
        
        logger.warning("No festival dates found in any films")
        return None
    
    def _is_festival_debut(self, film: Dict, imdb_theatrical_date: str, festival_start_date: datetime) -> bool:
        """Check if film is making its debut at the festival.
        
        A film is considered a festival debut if its IMDb theatrical release 
        date is within 3 weeks of the festival start date.
        
        Args:
            film: Film dictionary with screening information  
            imdb_theatrical_date: IMDb theatrical release date string
            festival_start_date: First date of the festival
            
        Returns:
            True if this is likely a festival debut, False otherwise
        """
        if not imdb_theatrical_date or not festival_start_date:
            return False
            
        # Parse IMDb theatrical date
        imdb_date = self._parse_date_from_string(imdb_theatrical_date)
        if not imdb_date:
            return False
            
        # Check if IMDb release is within 3 weeks (21 days) of festival start
        days_difference = abs((imdb_date - festival_start_date).days)
        is_debut = days_difference <= 21
        
        if is_debut:
            logger.info(f"'{film.get('title')}' is likely a festival debut - Festival: {festival_start_date.strftime('%Y-%m-%d')}, IMDb: {imdb_date.strftime('%Y-%m-%d')} ({days_difference} days apart)")
        else:
            logger.debug(f"'{film.get('title')}' is not a festival debut - Festival: {festival_start_date.strftime('%Y-%m-%d')}, IMDb: {imdb_date.strftime('%Y-%m-%d')} ({days_difference} days apart)")
            
        return is_debut

    def enrich_films(self, films: List[Dict], limit: int = None) -> List[Dict]:
        """Enrich films with IMDb production company and distributor data.

        Args:
            films: List of film dictionaries to enrich
            limit: Optional limit on number of films to process

        Returns:
            List of enriched film dictionaries
        """
        processed_films = []

        # Limit number of films for testing
        if limit:
            films = films[:limit]
            logger.info(
                f"Processing limited set of {
                    len(films)} films for testing")
        
        # Calculate festival start date once for all debut checks
        festival_start_date = self._get_festival_start_date(films)

        for i, film in enumerate(films):
            logger.info(
                f"Processing film {i + 1}/{len(films)}: {film['title']}")

            # Check if we should skip IMDb lookup
            should_skip, skip_reason = self.should_skip_imdb_lookup(film)

            if should_skip:
                logger.info(
                    f"Skipping IMDb lookup for '{
                        film['title']}': {skip_reason}")
                film['production_companies'] = []
                film['distributors'] = []
                film['imdb_id'] = None
                film['theatrical_release_date'] = None
                film['likely_theatrical'] = False
                processed_films.append(film)
                continue

            # Extract year from film screenings
            screening_year = self._extract_year_from_film(film)
            
            # Search IMDb with director for better accuracy
            director = film.get('director', '')
            imdb_id = self.search_imdb(
                film['title'], screening_year, director)

            if imdb_id:
                # Get company credits
                credits = self.get_company_credits(imdb_id)
                film.update(credits)
                film['imdb_id'] = imdb_id

                # Get theatrical release date
                theatrical_release_date = self.get_theatrical_release_date(
                    imdb_id)
                film['theatrical_release_date'] = theatrical_release_date
                
                # Check if this is a festival debut
                is_debut = self._is_festival_debut(film, theatrical_release_date, festival_start_date)
                film['is_festival_debut'] = is_debut

                # Get country and runtime from IMDb if not already present
                if not film.get('country') or not film.get('runtime'):
                    country, runtime = self.get_country_and_runtime(imdb_id)
                    if not film.get('country'):
                        film['country'] = country
                    if not film.get('runtime'):
                        film['runtime'] = runtime
            else:
                film['production_companies'] = []
                film['distributors'] = []
                film['imdb_id'] = None
                film['theatrical_release_date'] = None

            # Determine likely_theatrical status based on multiple criteria
            production_count = len(film.get('production_companies', []))
            distributor_count = len(film.get('distributors', []))
            theatrical_release_date = film.get('theatrical_release_date')
            
            # Check if the release date is a real theatrical release or just festival premiere
            has_valid_release_date = False
            if theatrical_release_date:
                if self._is_festival_date(theatrical_release_date, films):
                    # This is just a festival premiere, not a real theatrical release
                    has_valid_release_date = False
                    logger.info(f"'{film['title']}' release date '{theatrical_release_date}' is festival premiere - not counting as theatrical release")
                else:
                    has_valid_release_date = True

            # A film is likely theatrical if:
            # 1. It has a confirmed non-festival theatrical release date, OR
            # 2. It has significant production backing (multiple companies) or distributors
            film['likely_theatrical'] = (
                has_valid_release_date or
                production_count > 2 or
                distributor_count >= 1
            )

            # Log the reasoning for debugging
            if has_valid_release_date:
                logger.info(
                    f"'{film['title']}' marked as likely_theatrical=True due to confirmed release date: {theatrical_release_date}")
            elif production_count > 2 or distributor_count >= 1:
                logger.info(
                    f"'{film['title']}' marked as likely_theatrical=True due to {production_count} production companies and {distributor_count} distributors")
            else:
                logger.info(
                    f"'{film['title']}' marked as likely_theatrical=False - no release date and limited production backing")

            processed_films.append(film)

        return processed_films

    def get_cached_or_fetch(self, url: str, filename: str) -> str:
        """Get content from cache or fetch from URL.

        Args:
            url: URL to fetch
            filename: Cache filename

        Returns:
            HTML content as string
        """
        cache_path = os.path.join(self.cache_dir, filename)

        if os.path.exists(cache_path):
            logger.info(f"Loading from cache: {filename}")
            with open(cache_path, 'r', encoding='utf-8') as f:
                return f.read()

        logger.info(f"Fetching: {url}")
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            content = response.text

            # Cache the content
            with open(cache_path, 'w', encoding='utf-8') as f:
                f.write(content)

            # Be nice to servers
            time.sleep(1)
            return content

        except requests.RequestException as e:
            logger.error(f"Error fetching {url}: {e}")
            return ""
