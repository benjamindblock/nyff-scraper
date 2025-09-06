"""
IMDb enricher for adding production company and distributor information.

Author: Jack Murphy
"""

import requests
from bs4 import BeautifulSoup
import re
import time
import logging
from typing import List, Dict, Optional
from urllib.parse import quote

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
        
    def search_imdb(self, film_title: str, year: str = "2025", director: str = "") -> Optional[str]:
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
        
        # Strategy 1: Use advanced search with title and year filter (most reliable)
        advanced_search_url = f"https://www.imdb.com/search/title/?title={quote(clean_title)}&release_date={year}-01-01,{year}-12-31"
        
        filename = f"imdb_advanced_search_{re.sub(r'[^\w]', '_', film_title)}.html"
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
                            logger.info(f"Found IMDb ID for '{film_title}' via advanced search: {imdb_id}")
                            return imdb_id
        
        # Strategy 2: Fall back to original find search if advanced search fails
        search_attempts = []
        
        if director:
            clean_director = normalize_title(director)
            search_attempts.append(f"{clean_title} {clean_director} {year}")
        
        search_attempts.append(f"{clean_title} {year}")
        search_attempts.append(clean_title)
        
        for attempt_num, search_query in enumerate(search_attempts, 1):
            search_url = f"https://www.imdb.com/find/?q={quote(search_query)}&s=tt&ttype=ft"
            
            filename = f"imdb_search_{re.sub(r'[^\w]', '_', film_title)}_attempt_{attempt_num}.html"
            content = self.get_cached_or_fetch(search_url, filename)
            
            if not content:
                continue
                
            soup = BeautifulSoup(content, 'html.parser')
            
            # Look for search results with improved selectors
            result_selectors = [
                '.findResult .result_text a',
                '.titleResult a', 
                '.find-title-result a',
                '.find-section .findResult h3.findResult-text a',
                'a[href*="/title/tt"]',
                '.ipc-metadata-list-summary-item__t',
                '.cli-title'
            ]
            
            for selector in result_selectors:
                links = soup.select(selector)
                for link in links:
                    href = link.get('href', '')
                    match = re.search(r'/title/(tt\d+)/', href)
                    if match:
                        imdb_id = match.group(1)
                        
                        if self._validate_search_result(link, film_title, year, director):
                            logger.info(f"Found IMDb ID for '{film_title}' (find search attempt {attempt_num}): {imdb_id}")
                            return imdb_id
                        
                        logger.info(f"Found IMDb ID for '{film_title}' (find search attempt {attempt_num}, validation unclear): {imdb_id}")
                        return imdb_id
            
            if attempt_num == 1 and director:
                logger.warning(f"No IMDb ID found for '{film_title}' with director '{director}', trying without director")
            elif attempt_num == 2:
                logger.warning(f"No IMDb ID found for '{film_title}' with year, trying title only")
        
        logger.warning(f"No IMDb ID found for '{film_title}' after all search attempts")
        return None
    
    def _is_title_match(self, found_title: str, expected_title: str, threshold: float = 0.7) -> bool:
        """Check if found title is similar enough to expected title."""
        try:
            from difflib import SequenceMatcher
            
            # Normalize both titles for comparison
            found_clean = re.sub(r'[^\w\s]', ' ', found_title.lower().strip())
            expected_clean = re.sub(r'[^\w\s]', ' ', expected_title.lower().strip())
            
            # Calculate similarity ratio
            similarity = SequenceMatcher(None, found_clean, expected_clean).ratio()
            
            # Also check if expected title is contained in found title (handles subtitles)
            contains_match = expected_clean in found_clean or found_clean in expected_clean
            
            return similarity >= threshold or contains_match
        except ImportError:
            # Fallback to simple string matching if difflib not available
            found_clean = re.sub(r'[^\w\s]', ' ', found_title.lower().strip())
            expected_clean = re.sub(r'[^\w\s]', ' ', expected_title.lower().strip())
            return expected_clean in found_clean or found_clean in expected_clean
    
    def _validate_advanced_result(self, link_element, expected_title: str, expected_year: str, expected_director: str = "") -> bool:
        """Validate advanced search results."""
        try:
            # Get the parent container which usually has year and director info
            parent = link_element.find_parent()
            while parent and parent.name not in ['li', 'div', 'article'] and parent.find_parent():
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
                director_found = any(word in parent_text.lower() for word in director_words if len(word) > 2)
            
            return bool(year_found) and director_found
        except Exception:
            return True  # If validation fails, assume it's okay

    def _validate_search_result(self, link_element, expected_title: str, expected_year: str, expected_director: str = "") -> bool:
        """Validate if a search result matches our expectations."""
        try:
            # Get the result text and surrounding context
            result_text = link_element.get_text(strip=True)
            parent_text = link_element.find_parent().get_text(strip=True) if link_element.find_parent() else ""
            
            # Check if year matches
            year_found = re.search(r'\b' + expected_year + r'\b', parent_text)
            
            # Check if director matches (if provided)
            director_found = True
            if expected_director:
                # Normalize director name for comparison
                normalized_director = re.sub(r'[^\w\s]', ' ', expected_director.lower())
                director_found = normalized_director in parent_text.lower()
            
            return year_found and director_found
        except Exception:
            return True  # If validation fails, assume it's okay
    
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
            'h4:contains("Release Date") + ul li'
        ]
        
        for selector in release_selectors:
            try:
                elements = soup.select(selector)
                for element in elements:
                    text = element.get_text(strip=True)
                    # Look for date patterns like "March 15, 2024", "15 March 2024", etc.
                    date_match = re.search(r'(\d{1,2}\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4})', text, re.I)
                    if date_match:
                        return date_match.group(1)
                    # Alternative format: Month Day, Year
                    date_match = re.search(r'((?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4})', text, re.I)
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
                    import json
                    data = json.loads(script.string)
                    if isinstance(data, dict) and 'datePublished' in data:
                        return data['datePublished']
                    elif isinstance(data, list):
                        for item in data:
                            if isinstance(item, dict) and 'datePublished' in item:
                                return item['datePublished']
                except (json.JSONDecodeError, AttributeError):
                    continue
        except Exception:
            pass
            
        logger.debug(f"No theatrical release date found for {imdb_id}")
        return None

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
        sections = soup.find_all(['h4', 'h3'], string=re.compile(r'production|distributor', re.I))
        
        for section in sections:
            section_text = section.get_text().lower()
            
            # Find the list of companies following this section
            next_element = section.find_next_sibling()
            companies = []
            
            while next_element and next_element.name not in ['h3', 'h4']:
                if next_element.name == 'ul':
                    companies.extend([li.get_text(strip=True) for li in next_element.find_all('li')])
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
                # Try to determine if it's production or distribution based on context
                context = link.find_parent().get_text().lower()
                
                if any(word in context for word in ['production', 'produced']):
                    production_companies.append(company_name)
                elif any(word in context for word in ['distribution', 'distributed']):
                    distributors.append(company_name)
                else:
                    # Default to production if unclear
                    production_companies.append(company_name)
        
        # Clean up duplicates and empty strings
        production_companies = list(set([pc for pc in production_companies if pc.strip()]))
        distributors = list(set([d for d in distributors if d.strip()]))
        
        logger.info(f"Found {len(production_companies)} production companies and {len(distributors)} distributors for {imdb_id}")
        
        return {
            "production_companies": production_companies,
            "distributors": distributors
        }
    
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
            logger.info(f"Processing limited set of {len(films)} films for testing")
        
        for i, film in enumerate(films):
            logger.info(f"Processing film {i+1}/{len(films)}: {film['title']}")
            
            # Search IMDb with director for better accuracy
            director = film.get('director', '')
            imdb_id = self.search_imdb(film['title'], film.get('year', '2025'), director)
            
            if imdb_id:
                # Get company credits
                credits = self.get_company_credits(imdb_id)
                film.update(credits)
                film['imdb_id'] = imdb_id
                
                # Get theatrical release date
                theatrical_release_date = self.get_theatrical_release_date(imdb_id)
                film['theatrical_release_date'] = theatrical_release_date
            else:
                film['production_companies'] = []
                film['distributors'] = []
                film['imdb_id'] = None
                film['theatrical_release_date'] = None
            
            # Keep legacy field for backwards compatibility
            # The new logic will be handled by MetadataEnricher
            production_count = len(film.get('production_companies', []))
            distributor_count = len(film.get('distributors', []))
            
            film['likely_theatrical'] = (production_count > 2 or distributor_count >= 1)
            
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
            with open(cache_path, 'w', encoding='utf-8') as f:
                f.write(content)
                
            # Be nice to servers
            time.sleep(1)
            return content
            
        except requests.RequestException as e:
            logger.error(f"Error fetching {url}: {e}")
            return ""