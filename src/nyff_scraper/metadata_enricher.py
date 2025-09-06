"""
Metadata enricher for adding classification fields to film data.

Author: Jack Murphy
"""

import re
import logging
from typing import List, Dict, Optional
from .distribution_scorer import DistributionLikelihoodScorer

logger = logging.getLogger(__name__)


class MetadataEnricher:
    """Enricher for adding metadata classification fields."""
    
    def __init__(self):
        """Initialize the metadata enricher."""
        self.distribution_scorer = DistributionLikelihoodScorer()
    
    def is_short_program(self, film: Dict) -> bool:
        """Detect if a film listing is a shorts program.
        
        Args:
            film: Film dictionary
            
        Returns:
            True if this appears to be a shorts program
        """
        title = film.get('title', '').lower()
        description = film.get('description', '').lower()
        director = film.get('director', '').lower()
        
        # Check title for shorts indicators
        shorts_title_indicators = [
            'shorts', 'short films', 'short program', 'anthology',
            'collection', 'omnibus', 'portmanteau'
        ]
        
        # Check for multiple directors (common in shorts programs)
        multiple_director_patterns = [
            r'(?:and|&|\+|,)\s*[A-Z]',  # "Director A and Director B"
            r',\s*[A-Z][a-z]+\s+[A-Z][a-z]+',  # Multiple full names
        ]
        
        # Title-based detection
        if any(indicator in title for indicator in shorts_title_indicators):
            return True
            
        # Description-based detection
        desc_indicators = [
            'short films', 'shorts program', 'anthology', 'collection of',
            'various directors', 'multiple filmmakers', 'several shorts'
        ]
        if any(indicator in description for indicator in desc_indicators):
            return True
            
        # Multiple directors pattern
        if any(re.search(pattern, director) for pattern in multiple_director_patterns):
            return True
        
        # Check for runtime patterns suggesting shorts
        runtime = film.get('runtime', '').lower()
        if runtime:
            # Look for patterns like "90 minutes (5 shorts)" or similar
            if any(word in runtime for word in ['shorts', 'films', 'segments']):
                return True
        
        return False
    
    def is_restoration(self, film: Dict) -> bool:
        """Detect if a film is a restoration/revival.
        
        Args:
            film: Film dictionary
            
        Returns:
            True if this appears to be a restoration or revival
        """
        title = film.get('title', '').lower()
        description = film.get('description', '').lower()
        year = film.get('year', '')
        
        # Check for restoration indicators in title/description
        restoration_indicators = [
            'restoration', '4k restoration', 'new restoration', 'restored',
            'remastered', 'revival', 'classic', 'retrospective',
            'newly restored', 'digital restoration'
        ]
        
        # Check title and description
        text_content = f"{title} {description}"
        if any(indicator in text_content for indicator in restoration_indicators):
            return True
        
        # Check if year suggests it's a classic (before 2020 for NYFF 2025)
        try:
            film_year = int(year) if year.isdigit() else None
            if film_year and film_year < 2020:
                return True
        except (ValueError, TypeError):
            pass
        
        return False
    
    def is_likely_to_be_distributed(self, film: Dict) -> bool:
        """Determine if film is likely to get theatrical distribution.
        
        This method is now deprecated in favor of the comprehensive
        distribution likelihood scoring system.
        
        Args:
            film: Film dictionary
            
        Returns:
            True if likely to get distribution (based on score >= 50)
        """
        # Use the new comprehensive scoring system
        score, is_likely, _ = self.distribution_scorer.calculate_distribution_likelihood_score(film)
        return is_likely
    
    def has_intro_or_qna(self, film: Dict) -> bool:
        """Detect if film has intro or Q&A based on existing notes.
        
        Args:
            film: Film dictionary
            
        Returns:
            True if there's an introduction, Q&A, or panel
        """
        # Check showtime notes
        showtimes = film.get('nyff_showtimes', [])
        for showtime in showtimes:
            notes = showtime.get('notes', [])
            for note in notes:
                note_lower = note.lower()
                if any(keyword in note_lower for keyword in ['q&a', 'intro', 'introduction', 'panel', 'discussion']):
                    return True
        
        # Check description for mentions
        description = film.get('description', '').lower()
        intro_qna_indicators = [
            'q&a', 'introduction', 'panel', 'discussion', 'filmmaker in attendance',
            'followed by', 'with director', 'with cast', 'film scholar', 'critic',
            'moderated', 'special guest'
        ]
        
        if any(indicator in description for indicator in intro_qna_indicators):
            return True
        
        return False
    
    def categorize_film(self, film: Dict) -> str:
        """Classify film into category.
        
        Args:
            film: Film dictionary with classification booleans set
            
        Returns:
            Category string: "shorts", "restoration", "spotlight", "feature", "other"
        """
        title = film.get('title', '').lower()
        description = film.get('description', '').lower()
        
        # Check if it's shorts program
        if film.get('is_short_program', False):
            return 'shorts'
        
        # Check if it's restoration
        if film.get('is_restoration', False):
            return 'restoration'
        
        # Check for spotlight indicators
        spotlight_indicators = [
            'spotlight', 'opening night', 'closing night', 'gala',
            'centerpiece', 'special screening', 'world premiere',
            'red carpet', 'festival highlight'
        ]
        
        text_content = f"{title} {description}"
        if any(indicator in text_content for indicator in spotlight_indicators):
            return 'spotlight'
        
        # Default to feature for narrative films
        runtime = film.get('runtime', '')
        if runtime:
            # Try to extract minutes
            runtime_match = re.search(r'(\d+)', runtime)
            if runtime_match:
                minutes = int(runtime_match.group(1))
                if minutes >= 40:  # Feature length
                    return 'feature'
        
        # If we can't determine, default to feature
        return 'feature'
    
    def clean_notes(self, notes: str) -> str:
        """Clean up notes to be plain text without emojis.
        
        Args:
            notes: Original notes string
            
        Returns:
            Cleaned notes string
        """
        if not notes:
            return ""
        
        # Remove emojis using regex
        emoji_pattern = re.compile(
            "["
            "\U0001F600-\U0001F64F"  # emoticons
            "\U0001F300-\U0001F5FF"  # symbols & pictographs
            "\U0001F680-\U0001F6FF"  # transport & map symbols
            "\U0001F1E0-\U0001F1FF"  # flags (iOS)
            "\U00002702-\U000027B0"
            "\U000024C2-\U0001F251"
            "]+", flags=re.UNICODE
        )
        
        cleaned = emoji_pattern.sub('', notes)
        
        # Clean up extra whitespace
        cleaned = ' '.join(cleaned.split())
        
        return cleaned
    
    def enrich_films(self, films: List[Dict]) -> List[Dict]:
        """Enrich films with metadata classification fields.
        
        Args:
            films: List of film dictionaries to enrich
            
        Returns:
            List of enriched film dictionaries
        """
        enriched_films = []
        
        logger.info(f"Adding metadata fields to {len(films)} films")
        
        for i, film in enumerate(films):
            logger.info(f"Processing metadata for film {i+1}/{len(films)}: {film.get('title', 'Unknown')}")
            
            # Add new boolean fields
            film['is_short_program'] = self.is_short_program(film)
            film['is_restoration'] = self.is_restoration(film)
            film['has_intro_or_qna'] = self.has_intro_or_qna(film)
            
            # Add category field
            film['category'] = self.categorize_film(film)
            
            # Use the new comprehensive distribution likelihood scoring
            enriched_film = self.distribution_scorer.enrich_film_with_distribution_score(film)
            
            # Clean up existing notes
            if 'notes' in enriched_film:
                enriched_film['notes'] = self.clean_notes(str(enriched_film['notes']))
            else:
                # Create structured notes from available data
                notes_parts = []
                
                if enriched_film['is_short_program']:
                    notes_parts.append("Shorts program")
                if enriched_film['is_restoration']:
                    notes_parts.append("Restoration/revival")
                if enriched_film['has_intro_or_qna']:
                    notes_parts.append("Includes intro/Q&A")
                if not enriched_film['is_likely_to_be_distributed']:
                    notes_parts.append("Limited distribution expected")
                
                enriched_film['notes'] = "; ".join(notes_parts) if notes_parts else ""
            
            enriched_films.append(enriched_film)
        
        # Log summary
        short_programs = len([f for f in enriched_films if f.get('is_short_program')])
        restorations = len([f for f in enriched_films if f.get('is_restoration')])
        with_intro_qna = len([f for f in enriched_films if f.get('has_intro_or_qna')])
        likely_distributed = len([f for f in enriched_films if f.get('is_likely_to_be_distributed')])
        
        logger.info(f"Metadata enrichment complete:")
        logger.info(f"  Short programs: {short_programs}")
        logger.info(f"  Restorations: {restorations}")
        logger.info(f"  With intro/Q&A: {with_intro_qna}")
        logger.info(f"  Likely distributed: {likely_distributed}")
        
        return enriched_films