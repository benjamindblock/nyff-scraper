"""
YouTube trailer enricher for adding trailer URLs to film data.

Author: Jack Murphy
"""

import requests
import re
import time
import logging
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
    
    def search_youtube_trailer(self, title: str, year: str, director: str = "", is_restoration: bool = False) -> Optional[str]:
        """Search for a film trailer on YouTube using direct HTTP requests.
        
        Args:
            title: Film title to search for
            year: Year of the film
            director: Director name to include in search for better accuracy
            is_restoration: Whether this is a restoration (uses original film year)
            
        Returns:
            YouTube URL of the first found trailer, or empty string if none found
        """
        try:
            # Create search query with director for better accuracy
            query_parts = [title]
            
            # Add director if provided
            if director:
                query_parts.append(director)
            
            # Add year
            if year:
                query_parts.append(year)
                
            # Add trailer keyword
            query_parts.append("trailer")
            
            query = " ".join(query_parts)
            logger.info(f"Searching YouTube for: {query}")
            
            # Use YouTube search URL
            search_url = "https://www.youtube.com/results"
            params = {"search_query": query}
            
            response = self.session.get(search_url, params=params, timeout=10)
            response.raise_for_status()
            
            # Look for video URLs in the response
            # YouTube embeds video info in JavaScript
            video_pattern = r'"videoId":"([^"]+)"'
            matches = re.findall(video_pattern, response.text)
            
            if matches:
                video_id = matches[0]
                video_url = f"https://www.youtube.com/watch?v={video_id}"
                logger.info(f"Found trailer for '{title}': {video_url}")
                return video_url
            
            logger.warning(f"No trailer found for '{title}'")
            return ""
            
        except Exception as e:
            logger.error(f"Error searching YouTube for '{title}': {e}")
            return ""
    
    def construct_youtube_search_url(self, title: str, year: str, director: str = "") -> str:
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
            logger.info(f"Processing film {i+1}/{len(films)}: {film.get('title', 'Unknown')}")
            
            title = film.get('title', '')
            year = film.get('year', '')
            director = film.get('director', '')
            is_short_program = film.get('is_short_program', False)
            is_restoration = film.get('is_restoration', False)
            
            # Skip trailer search for shorts programs
            if is_short_program:
                logger.info(f"Skipping trailer search for shorts program: {title}")
                film['trailer_url'] = ""
                film['youtube_search_url'] = ""
            elif search_trailers and title and year:
                # Active search for trailer
                trailer_url = self.search_youtube_trailer(title, year, director, is_restoration)
                film['trailer_url'] = trailer_url
                # Be nice to YouTube - delay between searches
                time.sleep(2)
                
                # Always provide search URL
                film['youtube_search_url'] = self.construct_youtube_search_url(title, year, director)
            else:
                # Just provide search URL for manual lookup
                film['trailer_url'] = ""
                if title and year:
                    film['youtube_search_url'] = self.construct_youtube_search_url(title, year, director)
                else:
                    film['youtube_search_url'] = ""
            
            enriched_films.append(film)
        
        return enriched_films