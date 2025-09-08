"""
YouTube trailer enricher for adding trailer URLs to film data.
Uses Google search instead of scraping YouTube directly.

Author: Jack Murphy
"""

import requests
import re
import time
import string
import logging
from bs4 import BeautifulSoup
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)


class TrailerEnricher:
    """Enricher for adding YouTube trailer URLs to film data."""

    def __init__(self):
        """Initialize the trailer enricher."""
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })

    @staticmethod
    def normalize_text(text: str) -> str:
        """Lowercase, strip punctuation, collapse spaces."""
        translator = str.maketrans("", "", string.punctuation)
        return " ".join(text.lower().translate(translator).split())

    def search_youtube_trailer(
            self,
            title: str,
            year: str,
            director: str = "",
            is_restoration: bool = False) -> Optional[str]:
        """Search YouTube for a film trailer, validating against the film title."""
        try:
            query_parts = [title]
            if director:
                query_parts.append(director)
            if year:
                query_parts.append(year)
            query_parts.append("trailer")
            query = " ".join(query_parts)
            logger.info(f"Searching YouTube for: {query}")

            search_url = "https://www.youtube.com/results"
            params = {"search_query": query}
            response = self.session.get(search_url, params=params, timeout=10)
            response.raise_for_status()

            # Collect all candidate video IDs
            video_ids = re.findall(r'"videoId":"([^"]+)"', response.text)
            if not video_ids:
                logger.warning(f"No video IDs found for '{title}'")
                return ""

            film_norm = self.normalize_text(title)

            for vid in video_ids[:10]:  # check top 10 to avoid too many requests
                video_url = f"https://www.youtube.com/watch?v={vid}"
                try:
                    vresp = self.session.get(video_url, timeout=10)
                    vresp.raise_for_status()

                    # Extract <title> tag from the HTML
                    m = re.search(
                        r"<title>(.*?)</title>",
                        vresp.text,
                        re.IGNORECASE | re.DOTALL)
                    if not m:
                        continue

                    video_title = m.group(1)
                    video_title_norm = self.normalize_text(video_title)

                    # Check if most of the film title words appear in video
                    # title
                    film_words = set(film_norm.split())
                    overlap = sum(
                        1 for w in film_words if w in video_title_norm)

                    if film_words and overlap / len(film_words) >= 0.6:
                        logger.info(
                            f"Matched trailer for '{title}' -> {video_url} ({video_title.strip()})")
                        return video_url

                except Exception as e:
                    logger.debug(f"Error checking video {vid}: {e}")
                    continue

            logger.warning(
                f"No matching trailer found for '{title}' after checking candidates")
            return ""

        except Exception as e:
            logger.error(f"Error searching YouTube for '{title}': {e}")
            return ""

    def construct_youtube_search_url(
            self,
            title: str,
            year: str,
            director: str = "") -> str:
        """Construct a YouTube search URL for manual searching.

        Args:
            title: Film title
            year: Year of the film
            director: Director name to include in search

        Returns:
            YouTube search URL
        """
        query_parts = [title]
        if director:
            query_parts.append(director)
        if year:
            query_parts.append(year)
        query_parts.append("trailer")
        query = " ".join(query_parts).replace(" ", "+")
        return f"https://www.youtube.com/results?search_query={query}"

    def enrich_films(self, films: List[Dict], search_trailers: bool = True,
                     limit: int = None) -> List[Dict]:
        """Enrich films with YouTube trailer URLs.

        Args:
            films: List of film dictionaries to enrich
            search_trailers: Whether to actively search for trailers (vs just URLs)
            limit: Optional limit on number of films to process

        Returns:
            List of enriched film dictionaries
        """
        if limit:
            films = films[:limit]
            logger.info(f"Processing limited set of {len(films)} films")

        enriched_films = []

        for i, film in enumerate(films):
            logger.info(
                f"Processing film {i + 1}/{len(films)}: {film.get('title', 'Unknown')}")

            title = film.get('title', '')
            year = film.get('year', '')
            director = film.get('director', '')
            is_short_program = film.get('is_short_program', False)

            # Skip trailer search for shorts programs
            if is_short_program:
                logger.info(
                    f"Skipping trailer search for shorts program: {title}")
                film['trailer_url'] = ""
                film['youtube_search_url'] = ""
            elif search_trailers and title and year:
                # Active search for trailer
                trailer_url = self.search_youtube_trailer(
                    title, year, director)
                film['trailer_url'] = trailer_url
                # Be nice to Google - delay between searches
                time.sleep(2)

                # Always provide manual search URL
                film['youtube_search_url'] = self.construct_youtube_search_url(
                    title, year, director)
            else:
                # Just provide manual search URL
                film['trailer_url'] = ""
                if title and year:
                    film['youtube_search_url'] = self.construct_youtube_search_url(
                        title, year, director)
                else:
                    film['youtube_search_url'] = ""

            enriched_films.append(film)

        return enriched_films
