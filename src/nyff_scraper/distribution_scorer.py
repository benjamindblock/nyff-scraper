"""
Distribution likelihood scoring module for NYFF films.

This module implements a comprehensive scoring system that evaluates the likelihood
of theatrical distribution based on multiple factors including festival section,
IMDb metadata, and production/distributor information.

Author: Jack Murphy
"""

import re
import logging
from typing import Dict, Optional, Tuple
from datetime import datetime

logger = logging.getLogger(__name__)


class DistributionLikelihoodScorer:
    """
    Comprehensive scoring system for distribution likelihood assessment.

    Scoring factors:
    - Festival section weighting (+30 to -50 points)
    - IMDb metadata (+40 to +10 points)
    - Base producers/distributors heuristic (+20 to +30 points)
    - Final score capped at 0-100
    - Boolean flag for scores >= 50
    """

    def __init__(self):
        """Initialize the distribution likelihood scorer."""
        # Festival section weights
        # Main Slate and Spotlight films are virtually guaranteed distribution
        # Restorations, shorts, and currents are least likely for theatrical release
        self.section_weights = {
            'main slate': 50,      # Guaranteed distribution
            'spotlight': 45,       # Very likely distribution
            'currents': -40,       # Experimental/art house - limited appeal, though not impossible.
            'restoration': -70,    # Classic films - very limited theatrical potential
            'revivals': -70,       # Same as restoration
            'shorts': -80          # Shorts programs - least likely for distribution
        }

    def extract_festival_section(self, film: Dict) -> str:
        """
        Determine the festival section from film data.

        Args:
            film: Film dictionary with title, description, and metadata

        Returns:
            Section name (lowercase) or 'unknown' if not determinable
        """
        # Check existing category field first
        category = film.get('category', '').lower()
        if category in ['shorts', 'restoration']:
            return category

        # Check for shorts program indicators
        if film.get('is_short_program', False):
            return 'shorts'

        # Check for restoration indicators
        if film.get('is_restoration', False):
            return 'restoration'

        # Check title and description for section indicators
        title = film.get('title', '').lower()
        description = film.get('description', '').lower()

        # Main Slate indicators
        main_slate_indicators = [
            'main slate', 'opening night', 'closing night', 'centerpiece',
            'gala screening', 'world premiere', 'north american premiere'
        ]

        # Spotlight indicators
        spotlight_indicators = [
            'spotlight', 'special presentation', 'red carpet',
            'festival highlight', 'special screening'
        ]

        # Currents indicators
        currents_indicators = [
            'currents', 'experimental', 'avant-garde', 'art house',
            'independent', 'emerging filmmaker'
        ]

        # Restoration/Revival indicators
        restoration_indicators = [
            'restoration', 'revival', 'retrospective', 'classic',
            'newly restored', '4k restoration', 'remastered'
        ]

        text_content = f"{title} {description}"

        for indicator in main_slate_indicators:
            if indicator in text_content:
                return 'main slate'

        for indicator in spotlight_indicators:
            if indicator in text_content:
                return 'spotlight'

        for indicator in currents_indicators:
            if indicator in text_content:
                return 'currents'

        for indicator in restoration_indicators:
            if indicator in text_content:
                return 'restoration'

        # Default to unknown if we can't determine
        return 'unknown'

    def extract_theatrical_release_date(self, imdb_content: str = None) -> Optional[str]:
        """
        Extract theatrical release date from IMDb content.

        Args:
            imdb_content: HTML content from IMDb page (optional for now)

        Returns:
            Release date string if found, None otherwise
        """
        # This would be implemented to parse IMDb release date information
        # For now, return None as a placeholder - this would require
        # additional IMDb page parsing in the IMDbEnricher
        return None

    def calculate_festival_section_score(self, festival_section: str) -> int:
        """
        Calculate score based on festival section.

        Args:
            festival_section: Section name (lowercase)

        Returns:
            Score points for the section
        """
        return self.section_weights.get(festival_section, 0)

    def calculate_imdb_score(self, film: Dict, theatrical_release_date: Optional[str] = None) -> int:
        """
        Calculate score based on IMDb metadata.

        Args:
            film: Film dictionary with IMDb data
            theatrical_release_date: Theatrical release date if available

        Returns:
            Total IMDb-based score
        """
        score = 0

        # +40 if at least one distributor is listed on IMDb
        distributors = film.get('distributors', [])
        if distributors:
            score += 40

        # +20 if more than 3 production companies are listed
        production_companies = film.get('production_companies', [])
        if len(production_companies) > 3:
            score += 20

        # +10 if a theatrical release date is already listed
        if theatrical_release_date:
            score += 10

        return score

    def calculate_legacy_producer_distributor_score(self, film: Dict) -> int:
        """
        Calculate score based on legacy producer/distributor heuristic.

        Args:
            film: Film dictionary with production data

        Returns:
            Score based on producer/distributor counts
        """
        score = 0

        # +20 if there are more than 2-3 producers
        production_companies = film.get('production_companies', [])
        if len(production_companies) > 2:
            score += 20

        # +30 if there is at least one distributor
        distributors = film.get('distributors', [])
        if distributors:
            score += 30

        return score

    def calculate_distribution_likelihood_score(self, film: Dict) -> Tuple[int, bool, Optional[str]]:
        """
        Calculate comprehensive distribution likelihood score.

        Args:
            film: Film dictionary with all available metadata

        Returns:
            Tuple of (score, is_likely_distributed, theatrical_release_date)
        """
        total_score = 0

        # 1. Festival section weighting
        festival_section = self.extract_festival_section(film)
        section_score = self.calculate_festival_section_score(festival_section)
        total_score += section_score

        logger.debug(f"Festival section '{festival_section}': {section_score} points")

        # 2. IMDb metadata scoring
        theatrical_release_date = film.get('theatrical_release_date')
        imdb_score = self.calculate_imdb_score(film, theatrical_release_date)
        total_score += imdb_score

        logger.debug(f"IMDb metadata: {imdb_score} points")

        # 3. Legacy producer/distributor heuristic
        legacy_score = self.calculate_legacy_producer_distributor_score(film)
        total_score += legacy_score

        logger.debug(f"Legacy producer/distributor: {legacy_score} points")

        # Cap score at 0-100
        final_score = max(0, min(100, total_score))

        # Boolean flag for likely distribution
        is_likely_distributed = final_score >= 50

        logger.info(f"Film '{film.get('title', 'Unknown')}': "
                   f"Section={festival_section}, Score={final_score}, "
                   f"Likely={is_likely_distributed}")

        return final_score, is_likely_distributed, theatrical_release_date

    def enrich_film_with_distribution_score(self, film: Dict) -> Dict:
        """
        Enrich a single film with distribution likelihood scoring.

        Args:
            film: Film dictionary to enrich

        Returns:
            Enriched film dictionary with new scoring fields
        """
        score, is_likely, release_date = self.calculate_distribution_likelihood_score(film)

        # Add new fields
        film['distribution_likelihood_score'] = score
        film['is_likely_to_be_distributed'] = is_likely
        film['theatrical_release_date'] = release_date

        # Keep legacy field for backward compatibility
        film['likely_theatrical'] = is_likely

        return film

    def enrich_films(self, films: list[Dict]) -> list[Dict]:
        """
        Enrich multiple films with distribution likelihood scoring.

        Args:
            films: List of film dictionaries to enrich

        Returns:
            List of enriched film dictionaries
        """
        logger.info(f"Calculating distribution likelihood scores for {len(films)} films")

        enriched_films = []
        score_distribution = {'0-25': 0, '26-50': 0, '51-75': 0, '76-100': 0}

        for i, film in enumerate(films):
            logger.debug(f"Processing film {i+1}/{len(films)}: {film.get('title', 'Unknown')}")

            enriched_film = self.enrich_film_with_distribution_score(film)
            enriched_films.append(enriched_film)

            # Track score distribution
            score = enriched_film['distribution_likelihood_score']
            if score <= 25:
                score_distribution['0-25'] += 1
            elif score <= 50:
                score_distribution['26-50'] += 1
            elif score <= 75:
                score_distribution['51-75'] += 1
            else:
                score_distribution['76-100'] += 1

        # Log summary statistics
        likely_distributed = len([f for f in enriched_films if f['is_likely_to_be_distributed']])
        logger.info(f"Distribution scoring complete:")
        logger.info(f"  Films likely to be distributed: {likely_distributed}/{len(films)} ({likely_distributed/len(films)*100:.1f}%)")
        logger.info(f"  Score distribution: {score_distribution}")

        return enriched_films