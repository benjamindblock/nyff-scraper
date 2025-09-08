"""
Letterboxd integration for generating film recommendations.

Still experimental!!!

This module scrapes Letterboxd user profiles and generates recommendations
for NYFF films based on similarity scoring with the user's watched films.

Author: Jack Murphy
"""

import requests
from bs4 import BeautifulSoup
import re
import time
import logging
from typing import List, Dict, Set, Optional, Tuple
from collections import Counter
from urllib.parse import urljoin

logger = logging.getLogger(__name__)


class LetterboxdScraper:
    """Scraper for Letterboxd user profiles and film data."""

    def __init__(self, cache_dir: str = "cache"):
        """Initialize the scraper.

        Args:
            cache_dir: Directory to cache web requests
        """
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        self.cache_dir = cache_dir

        # Common stopwords for text normalization
        self.stopwords = {
            'a', 'an', 'and', 'are', 'as', 'at', 'be', 'by', 'for', 'from',
            'has', 'he', 'in', 'is', 'it', 'its', 'of', 'on', 'that', 'the',
            'to', 'was', 'with', 'but', 'not', 'or', 'his', 'her', 'their',
            'this', 'these', 'they', 'we', 'you', 'your', 'all', 'any', 'can',
            'had', 'have', 'him', 'will', 'would'
        }

    def get_cached_or_fetch(self, url: str, filename: str) -> str:
        """Get content from cache or fetch from URL.

        Args:
            url: URL to fetch
            filename: Cache filename

        Returns:
            HTML content as string
        """
        import os
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
            os.makedirs(self.cache_dir, exist_ok=True)
            with open(cache_path, 'w', encoding='utf-8') as f:
                f.write(content)

            # Be nice to servers
            time.sleep(2)
            return content

        except requests.RequestException as e:
            logger.error(f"Error fetching {url}: {e}")
            return ""

    def normalize_text(self, text: str) -> Set[str]:
        """Normalize text by lowercasing, removing punctuation, and filtering stopwords.

        Args:
            text: Input text to normalize

        Returns:
            Set of normalized words
        """
        if not text:
            return set()

        # Convert to lowercase and remove punctuation
        text = re.sub(r'[^\w\s]', ' ', text.lower())

        # Split into words and remove stopwords
        words = text.split()
        normalized_words = {
            word for word in words if word and word not in self.stopwords}

        return normalized_words

    def scrape_user_films(self, username: str, max_pages: int = 5) -> Dict:
        """Scrape a user's Letterboxd films.

        Args:
            username: Letterboxd username
            max_pages: Maximum number of pages to scrape

        Returns:
            Dictionary containing user's film data
        """
        logger.info(f"Scraping Letterboxd profile for user: {username}")

        user_data = {
            'username': username,
            'films': [],
            'directors': Counter(),
            'countries': Counter(),
            'keywords': set(),
            'ratings': {}  # film_title -> rating (if available)
        }

        # Scrape multiple pages of the user's films
        for page in range(1, max_pages + 1):
            url = f"https://letterboxd.com/{username}/films/page/{page}/"
            filename = f"letterboxd_{username}_films_page_{page}.html"

            content = self.get_cached_or_fetch(url, filename)
            if not content:
                continue

            soup = BeautifulSoup(content, 'html.parser')

            # Extract films from this page
            page_films = self._extract_films_from_page(soup)
            if not page_films:
                logger.info(f"No more films found at page {page}, stopping")
                break

            user_data['films'].extend(page_films)
            logger.info(f"Found {len(page_films)} films on page {page}")

        # Process collected films to extract patterns
        self._process_user_films(user_data)

        logger.info(
            f"Scraped {len(user_data['films'])} total films for {username}")
        logger.info(f"Top directors: {user_data['directors'].most_common(5)}")
        logger.info(f"Top countries: {user_data['countries'].most_common(5)}")

        return user_data

    def _extract_films_from_page(self, soup: BeautifulSoup) -> List[Dict]:
        """Extract film data from a Letterboxd page.

        Args:
            soup: BeautifulSoup object of the page

        Returns:
            List of film dictionaries
        """
        films = []

        # Look for React components with film data
        react_components = soup.select('.react-component[data-item-name]')

        for component in react_components:
            try:
                film_data = {}

                # Extract film title and year from data attribute
                item_name = component.get('data-item-name', '').strip()
                if not item_name:
                    continue

                # Parse title and year from "Title (Year)" format
                year_match = re.search(r'\((\d{4})\)$', item_name)
                if year_match:
                    film_data['year'] = year_match.group(1)
                    film_data['title'] = item_name.replace(
                        f' ({year_match.group(1)})', '').strip()
                else:
                    film_data['title'] = item_name

                # Extract film link
                item_link = component.get('data-item-link', '')
                if item_link:
                    film_data['url'] = urljoin(
                        'https://letterboxd.com', item_link)

                # Extract slug for additional data if needed
                item_slug = component.get('data-item-slug', '')
                if item_slug:
                    film_data['slug'] = item_slug

                # Extract film ID
                film_id = component.get('data-film-id', '')
                if film_id:
                    film_data['film_id'] = film_id

                # For now, we don't extract ratings from the main films page
                # as they're not readily visible in this format

                if film_data.get('title'):
                    films.append(film_data)

            except Exception as e:
                logger.debug(
                    f"Error extracting film from React component: {e}")
                continue

        return films

    def _process_user_films(self, user_data: Dict) -> None:
        """Process scraped films to extract patterns and metadata.

        Args:
            user_data: User data dictionary to update
        """
        for film in user_data['films']:
            # For detailed film metadata, we'd need to scrape individual film pages
            # For now, we'll work with basic title and year data
            # In a full implementation, you might want to scrape film pages for
            # director/country

            # Store ratings if available
            if 'rating' in film and film.get('title'):
                user_data['ratings'][film['title']] = film['rating']

            # Extract keywords from titles (basic approach)
            if film.get('title'):
                title_keywords = self.normalize_text(film['title'])
                user_data['keywords'].update(title_keywords)


class LetterboxdRecommender:
    """Generate film recommendations based on Letterboxd user data."""

    def __init__(self):
        """Initialize the recommender."""
        pass

    def generate_recommendations(
            self,
            nyff_films: List[Dict],
            user_data: Dict,
            top_n: int = 5) -> List[Dict]:
        """Generate film recommendations for NYFF films.

        Args:
            nyff_films: List of NYFF film dictionaries
            user_data: Letterboxd user data
            top_n: Number of top recommendations to return

        Returns:
            List of recommended films with scores and reasoning
        """
        logger.info(
            f"Generating recommendations for {
                len(nyff_films)} NYFF films")

        scored_films = []

        for film in nyff_films:
            score, reasoning = self._score_film(film, user_data)
            if score > 0:  # Only include films with positive scores
                scored_films.append({
                    'film': film,
                    'score': score,
                    'reasoning': reasoning
                })

        # Sort by score descending and take top N
        scored_films.sort(key=lambda x: x['score'], reverse=True)
        recommendations = scored_films[:top_n]

        logger.info(f"Generated {len(recommendations)} recommendations")
        for i, rec in enumerate(recommendations, 1):
            logger.info(
                f"#{i}: {rec['film']['title']} (score: {rec['score']}) - {rec['reasoning']}")

        return recommendations

    def _score_film(self, film: Dict, user_data: Dict) -> Tuple[int, str]:
        """Score a single film based on user preferences.

        Args:
            film: NYFF film dictionary
            user_data: Letterboxd user data

        Returns:
            Tuple of (score, reasoning)
        """
        score = 0
        reasoning_parts = []

        # Director match (+5 points)
        film_director = film.get('director', '').strip()
        if film_director and film_director in user_data['directors']:
            score += 5
            count = user_data['directors'][film_director]
            reasoning_parts.append(
                f"Director {film_director} ({count} films watched)")

        # Country match (+3 points)
        film_country = film.get('country', '').strip()
        if film_country and film_country in user_data['countries']:
            score += 3
            count = user_data['countries'][film_country]
            reasoning_parts.append(
                f"Country {film_country} ({count} films watched)")

        # Keyword overlap from description
        film_description = film.get('description', '')
        if film_description:
            scraper = LetterboxdScraper()  # For normalize_text method
            film_keywords = scraper.normalize_text(film_description)
            keyword_overlap = len(film_keywords & user_data['keywords'])

            if keyword_overlap >= 5:
                score += 2
                reasoning_parts.append(
                    f"High keyword similarity ({keyword_overlap} matches)")
            elif keyword_overlap >= 3:
                score += 1
                reasoning_parts.append(
                    f"Moderate keyword similarity ({keyword_overlap} matches)")

        # Distribution bonus (+1 point for undistributed films)
        if not film.get('likely_theatrical', True):
            score += 1
            reasoning_parts.append("Rare/undistributed film")

        # Star ratings bonus (optional)
        # If user has rated films by this director highly, add bonus
        if film_director and user_data.get('ratings'):
            director_ratings = []
            for film_title, rating in user_data['ratings'].items():
                # This is a simplified check - in practice you'd need more
                # sophisticated matching
                if film_director.lower() in film_title.lower():
                    director_ratings.append(rating)

            if director_ratings:
                avg_rating = sum(director_ratings) / len(director_ratings)
                if avg_rating >= 4:  # High ratings (assuming 1-5 scale)
                    score += 1
                    reasoning_parts.append(
                        f"Highly rated director (avg: {avg_rating:.1f}/5)")

        reasoning = "; ".join(
            reasoning_parts) if reasoning_parts else "Basic compatibility"
        return score, reasoning


def get_letterboxd_recommendations(nyff_films: List[Dict],
                                   username: str,
                                   cache_dir: str = "cache",
                                   top_n: int = 5) -> Optional[List[Dict]]:
    """Main function to get Letterboxd recommendations.

    Args:
        nyff_films: List of NYFF film dictionaries
        username: Letterboxd username
        cache_dir: Cache directory for web requests
        top_n: Number of recommendations to return

    Returns:
        List of recommendations or None if error
    """
    try:
        # Scrape user's Letterboxd data
        scraper = LetterboxdScraper(cache_dir=cache_dir)
        user_data = scraper.scrape_user_films(username)

        if not user_data['films']:
            logger.warning(f"No films found for Letterboxd user: {username}")
            return None

        # Generate recommendations
        recommender = LetterboxdRecommender()
        recommendations = recommender.generate_recommendations(
            nyff_films, user_data, top_n)

        return recommendations

    except Exception as e:
        logger.error(f"Error generating Letterboxd recommendations: {e}")
        return None
