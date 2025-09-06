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
from typing import List, Dict, Optional
from urllib.parse import urljoin

logger = logging.getLogger(__name__)


class NYFFScraper:
    """Scraper for NYFF film lineup pages."""

    def __init__(self, cache_dir: str = "cache"):
        """Initialize the scraper with optional caching.

        Args:
            cache_dir: Directory to cache web requests
        """
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        self.cache_dir = cache_dir
        self.ensure_cache_dir()

    def ensure_cache_dir(self) -> None:
        """Create cache directory if it doesn't exist."""
        if not os.path.exists(self.cache_dir):
            os.makedirs(self.cache_dir)

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

    def scrape_nyff_lineup(self, url: str = None) -> List[Dict]:
        """Scrape NYFF lineup page for films and showtimes.

        Args:
            url: URL to scrape (defaults to NYFF 2025 lineup)

        Returns:
            List of film dictionaries
        """
        if url is None:
            url = "https://www.filmlinc.org/nyff/nyff63-lineup/"

        content = self.get_cached_or_fetch(url, "nyff_lineup.html")

        if not content:
            return []

        soup = BeautifulSoup(content, 'html.parser')
        films = []

        # Look for film containers based on NYFF structure
        film_containers = soup.select('div.py-8.lg\\:py-10.border-b.border-border')

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
            director = ""
            director_elem = title_link.find_next('p')
            if director_elem:
                director = director_elem.get_text(strip=True)

            # Extract description
            description = ""
            prose_section = element.select_one('.typography.prose p')
            if prose_section:
                description = prose_section.get_text(strip=True)

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

    def extract_metadata(self, element) -> tuple[str, str, str]:
        """Extract year, country, and runtime metadata.

        Args:
            element: BeautifulSoup element containing metadata

        Returns:
            Tuple of (year, country, runtime)
        """
        year = ""
        country = ""
        runtime = ""

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
                if len(parts) >= 3 and ('minute' in parts[2].lower() or parts[2].replace(' ', '').isdigit()):
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
        date_sections = showtime_section.select('div.flex.flex-col.gap-2.border-t.border-border.pt-2')

        for date_section in date_sections:
            # Extract date
            date_elem = date_section.select_one('p[data-typography-mobile="d-eyebrow-sm"]')
            date = ""
            if date_elem:
                date = date_elem.get_text(strip=True)

            # Extract time buttons
            time_buttons = date_section.select('button')
            for button in time_buttons:
                button_text = button.get_text(strip=True)

                # Extract time from button text
                time_match = re.search(r'\d{1,2}:\d{2}\s*(?:AM|PM)', button_text)
                time = ""
                if time_match:
                    time = time_match.group()

                # Check for special notes
                notes = []
                if 'Q&A' in button_text:
                    notes.append('Q&A')
                if 'Intro' in button_text:
                    notes.append('Intro')

                # Check availability
                is_available = not ('line-through' in button.get('class', []) or
                                  button.get('disabled') == '' or
                                  'cursor-not-allowed' in button.get('class', []))

                if time:
                    showtime_data = {
                        "date": date,
                        "time": time,
                        "venue": "TBA",
                        "notes": notes,
                        "available": is_available,
                        "raw_text": button_text
                    }
                    showtimes.append(showtime_data)

        return showtimes