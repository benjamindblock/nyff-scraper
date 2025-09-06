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
        
    def search_imdb(self, film_title: str, year: str = "2025") -> Optional[str]:
        """Search IMDb for film and return IMDb ID.
        
        Args:
            film_title: Title of the film to search for
            year: Year to include in search (defaults to 2025)
            
        Returns:
            IMDb ID (e.g., "tt1234567") or None if not found
        """
        # Clean title for search
        clean_title = re.sub(r'[^\w\s]', '', film_title)
        search_query = quote(f"{clean_title} {year}")
        
        search_url = f"https://www.imdb.com/find/?q={search_query}&s=tt&ttype=ft"
        
        filename = f"imdb_search_{re.sub(r'[^\w]', '_', film_title)}.html"
        content = self.get_cached_or_fetch(search_url, filename)
        
        if not content:
            return None
            
        soup = BeautifulSoup(content, 'html.parser')
        
        # Look for search results
        result_selectors = [
            '.findResult .result_text a',
            '.titleResult a',
            '.find-title-result a',
            'a[href*="/title/tt"]'
        ]
        
        for selector in result_selectors:
            links = soup.select(selector)
            for link in links:
                href = link.get('href', '')
                # Extract IMDb ID from URL
                match = re.search(r'/title/(tt\d+)/', href)
                if match:
                    imdb_id = match.group(1)
                    logger.info(f"Found IMDb ID for '{film_title}': {imdb_id}")
                    return imdb_id
        
        logger.warning(f"No IMDb ID found for '{film_title}'")
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
            
            # Search IMDb
            imdb_id = self.search_imdb(film['title'], film.get('year', '2025'))
            
            if imdb_id:
                # Get company credits
                credits = self.get_company_credits(imdb_id)
                film.update(credits)
                film['imdb_id'] = imdb_id
            else:
                film['production_companies'] = []
                film['distributors'] = []
                film['imdb_id'] = None
            
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