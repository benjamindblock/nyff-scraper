"""
Export modules for converting film data to various formats.

Author: Jack Murphy
"""

import json
import csv
import logging
from datetime import datetime
from typing import List, Dict

logger = logging.getLogger(__name__)


class JSONExporter:
    """Exporter for JSON format."""
    
    @staticmethod
    def export(films: List[Dict], filename: str = "nyff_films.json", recommendations: List[Dict] = None) -> None:
        """Export films data to JSON.
        
        Args:
            films: List of film dictionaries
            filename: Output filename
            recommendations: Optional Letterboxd recommendations
        """
        output_data = {
            "films": films,
            "total_films": len(films),
            "generated_at": datetime.now().isoformat()
        }
        
        if recommendations:
            output_data["recommendations"] = {
                "count": len(recommendations),
                "films": [
                    {
                        "rank": i + 1,
                        "title": rec["film"]["title"],
                        "director": rec["film"].get("director", ""),
                        "year": rec["film"].get("year", ""),
                        "score": rec["score"],
                        "reasoning": rec["reasoning"],
                        "film_data": rec["film"]
                    }
                    for i, rec in enumerate(recommendations)
                ]
            }
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False)
        
        rec_msg = f" with {len(recommendations)} recommendations" if recommendations else ""
        logger.info(f"Exported {len(films)} films{rec_msg} to {filename}")


class CSVExporter:
    """Exporter for CSV format."""
    
    @staticmethod
    def export(films: List[Dict], filename: str = "nyff_films.csv", recommendations: List[Dict] = None) -> None:
        """Export films data to CSV with one row per showtime.
        
        Args:
            films: List of film dictionaries
            filename: Output filename
            recommendations: Optional Letterboxd recommendations
        """
        csv_rows = []
        
        for film in films:
            # Get basic film info in requested field order
            title = film.get('title', '')
            director = film.get('director', '')
            year = film.get('year', '')
            country = film.get('country', '')
            runtime = film.get('runtime', '')
            description = film.get('description', '')
            category = film.get('category', 'feature')
            is_short_program = 'TRUE' if film.get('is_short_program', False) else 'FALSE'
            is_restoration = 'TRUE' if film.get('is_restoration', False) else 'FALSE'
            has_intro_or_qna = 'TRUE' if film.get('has_intro_or_qna', False) else 'FALSE'
            notes = film.get('notes', '')
            
            # Production and distribution info
            production_companies = '; '.join(film.get('production_companies', []))
            distributors = '; '.join(film.get('distributors', []))
            imdb_id = film.get('imdb_id', '')
            theatrical_release_date = film.get('theatrical_release_date', '')
            distribution_likelihood_score = film.get('distribution_likelihood_score', 0)
            is_likely_to_be_distributed = 'TRUE' if film.get('is_likely_to_be_distributed', False) else 'FALSE'
            likely_theatrical = 'TRUE' if film.get('likely_theatrical', False) else 'FALSE'
            trailer_url = film.get('trailer_url', '')
            youtube_search_url = film.get('youtube_search_url', '')
            
            # Check if this film is in recommendations
            recommendation_score = ''
            recommendation_reasoning = ''
            recommendation_rank = ''
            if recommendations:
                for i, rec in enumerate(recommendations):
                    if rec['film']['title'] == title:
                        recommendation_score = rec['score']
                        recommendation_reasoning = rec['reasoning']
                        recommendation_rank = i + 1
                        break
            
            # Handle showtimes - create one row per showtime
            showtimes = film.get('nyff_showtimes', [])
            
            if showtimes:
                for showtime in showtimes:
                    row = {
                        'Title': title,
                        'Director': director,
                        'Year': year,
                        'Country': country,
                        'Runtime': runtime,
                        'Description': description,
                        'Category': category,
                        'Is_Short_Program': is_short_program,
                        'Is_Restoration': is_restoration,
                        'Has_Intro_Or_QnA': has_intro_or_qna,
                        'Notes': notes,
                        'Date': showtime.get('date', ''),
                        'Time': showtime.get('time', ''),
                        'Venue': showtime.get('venue', ''),
                        'Showtime_Notes': '; '.join(showtime.get('notes', [])),
                        'Available': 'TRUE' if showtime.get('available', True) else 'FALSE',
                        'Production_Companies': production_companies,
                        'Distributors': distributors,
                        'IMDB_ID': imdb_id,
                        'Theatrical_Release_Date': theatrical_release_date,
                        'Distribution_Likelihood_Score': distribution_likelihood_score,
                        'Is_Likely_To_Be_Distributed': is_likely_to_be_distributed,
                        'Likely_Theatrical': likely_theatrical,
                        'Trailer_URL': trailer_url,
                        'YouTube_Search_URL': youtube_search_url,
                        'Recommendation_Rank': recommendation_rank,
                        'Recommendation_Score': recommendation_score,
                        'Recommendation_Reasoning': recommendation_reasoning
                    }
                    csv_rows.append(row)
            else:
                # No showtimes - create one row with empty showtime fields
                row = {
                    'Title': title,
                    'Director': director,
                    'Year': year,
                    'Country': country,
                    'Runtime': runtime,
                    'Description': description,
                    'Category': category,
                    'Is_Short_Program': is_short_program,
                    'Is_Restoration': is_restoration,
                    'Has_Intro_Or_QnA': has_intro_or_qna,
                    'Notes': notes,
                    'Date': '',
                    'Time': '',
                    'Venue': '',
                    'Showtime_Notes': '',
                    'Available': '',
                    'Production_Companies': production_companies,
                    'Distributors': distributors,
                    'IMDB_ID': imdb_id,
                    'Theatrical_Release_Date': theatrical_release_date,
                    'Distribution_Likelihood_Score': distribution_likelihood_score,
                    'Is_Likely_To_Be_Distributed': is_likely_to_be_distributed,
                    'Likely_Theatrical': likely_theatrical,
                    'Trailer_URL': trailer_url,
                    'YouTube_Search_URL': youtube_search_url,
                    'Recommendation_Rank': recommendation_rank,
                    'Recommendation_Score': recommendation_score,
                    'Recommendation_Reasoning': recommendation_reasoning
                }
                csv_rows.append(row)
        
        # Write to CSV with new field order
        fieldnames = [
            'Title', 'Director', 'Year', 'Country', 'Runtime', 'Description',
            'Category', 'Is_Short_Program', 'Is_Restoration', 'Has_Intro_Or_QnA', 'Notes',
            'Date', 'Time', 'Venue', 'Showtime_Notes', 'Available',
            'Production_Companies', 'Distributors', 'IMDB_ID',
            'Theatrical_Release_Date', 'Distribution_Likelihood_Score',
            'Is_Likely_To_Be_Distributed', 'Likely_Theatrical',
            'Trailer_URL', 'YouTube_Search_URL',
            'Recommendation_Rank', 'Recommendation_Score', 'Recommendation_Reasoning'
        ]
        
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(csv_rows)
        
        logger.info(f"Exported {len(csv_rows)} rows to {filename}")
        
        # Print summary
        films_with_trailers = len([row for row in csv_rows if row['Trailer_URL']])
        unique_films = len(set(row['Title'] for row in csv_rows))
        
        print(f"CSV export complete: {len(csv_rows)} rows, {unique_films} unique films")
        if films_with_trailers > 0:
            print(f"Found trailers for {films_with_trailers} films")
        print(f"Saved: {filename}")


class MarkdownExporter:
    """Exporter for Markdown format."""
    
    @staticmethod
    def export(films: List[Dict], filename: str = "nyff_films.md", recommendations: List[Dict] = None) -> None:
        """Export films data to Markdown.
        
        Args:
            films: List of film dictionaries
            filename: Output filename
            recommendations: Optional Letterboxd recommendations
        """
        with open(filename, 'w', encoding='utf-8') as f:
            f.write("# NYFF Film Lineup\n\n")
            f.write(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            f.write(f"Total films: {len(films)}\n\n")
            
            # Add recommendations section if available
            if recommendations:
                f.write("## 🎬 Letterboxd Recommendations\n\n")
                f.write(f"Based on your Letterboxd profile, here are our top {len(recommendations)} recommendations:\n\n")
                
                for i, rec in enumerate(recommendations, 1):
                    film = rec['film']
                    f.write(f"### {i}. {film['title']}")
                    if film.get('year'):
                        f.write(f" ({film['year']})")
                    f.write(f" - Score: {rec['score']}\n\n")
                    
                    if film.get('director'):
                        f.write(f"**Director:** {film['director']}\n\n")
                    
                    f.write(f"**Why recommended:** {rec['reasoning']}\n\n")
                    
                    if film.get('description'):
                        f.write(f"**Description:** {film['description']}\n\n")
                    
                    f.write("---\n\n")
                
                f.write("\n## 📽️ All Films\n\n")
            
            for film in films:
                f.write(f"## {film.get('title', 'Unknown Title')}\n\n")
                
                # Basic info
                if film.get('director'):
                    f.write(f"**Director:** {film['director']}\n\n")
                
                metadata = []
                if film.get('year'):
                    metadata.append(film['year'])
                if film.get('country'):
                    metadata.append(film['country'])
                if film.get('runtime'):
                    metadata.append(film['runtime'])
                
                if metadata:
                    f.write(f"**Details:** {' | '.join(metadata)}\n\n")
                
                if film.get('description'):
                    f.write(f"**Description:** {film['description']}\n\n")
                
                # Showtimes
                if film.get('nyff_showtimes'):
                    f.write("**Showtimes:**\n")
                    for showtime in film['nyff_showtimes']:
                        line = f"- {showtime.get('date', 'TBA')}"
                        if showtime.get('time'):
                            line += f" at {showtime['time']}"
                        if showtime.get('venue') and showtime['venue'] != 'TBA':
                            line += f" ({showtime['venue']})"
                        if showtime.get('notes'):
                            line += f" - {', '.join(showtime['notes'])}"
                        if not showtime.get('available', True):
                            line += " - **SOLD OUT**"
                        f.write(line + "\n")
                    f.write("\n")
                else:
                    f.write("**Showtimes:** TBA\n\n")
                
                # Production info
                if film.get('production_companies'):
                    f.write("**Production Companies:**\n")
                    for company in film['production_companies']:
                        f.write(f"- {company}\n")
                    f.write("\n")
                
                if film.get('distributors'):
                    f.write("**Distributors:**\n")
                    for distributor in film['distributors']:
                        f.write(f"- {distributor}\n")
                    f.write("\n")
                else:
                    f.write("**Distributors:** Not yet acquired\n\n")
                
                # Classification metadata
                category = film.get('category', 'feature')
                f.write(f"**Category:** {category.title()}\n\n")
                
                # Boolean flags
                flags = []
                if film.get('is_short_program', False):
                    flags.append("Shorts Program")
                if film.get('is_restoration', False):
                    flags.append("Restoration/Revival")
                if film.get('has_intro_or_qna', False):
                    flags.append("Includes Intro/Q&A")
                
                if flags:
                    f.write(f"**Special Notes:** {', '.join(flags)}\n\n")
                
                # Distribution likelihood with score
                distribution_score = film.get('distribution_likelihood_score', 0)
                likely_distributed = film.get('is_likely_to_be_distributed', False)
                f.write(f"**Distribution Likelihood:** Score {distribution_score}/100 ({'Yes' if likely_distributed else 'Limited'})\n\n")
                
                # Theatrical release date if available
                if film.get('theatrical_release_date'):
                    f.write(f"**Theatrical Release Date:** {film['theatrical_release_date']}\n\n")
                
                # Custom notes
                if film.get('notes'):
                    f.write(f"**Notes:** {film['notes']}\n\n")
                
                # Links
                links = []
                if film.get('imdb_id'):
                    links.append(f"[IMDb](https://www.imdb.com/title/{film['imdb_id']}/)")
                if film.get('trailer_url'):
                    links.append(f"[Trailer]({film['trailer_url']})")
                elif film.get('youtube_search_url'):
                    links.append(f"[Search for Trailer]({film['youtube_search_url']})")
                
                if links:
                    f.write(f"**Links:** {' | '.join(links)}\n\n")
                
                f.write("---\n\n")
        
        logger.info(f"Exported {len(films)} films to {filename}")
        
        # Summary stats
        with_imdb = len([f for f in films if f.get('imdb_id')])
        likely_theatrical = len([f for f in films if f.get('likely_theatrical')])
        with_trailers = len([f for f in films if f.get('trailer_url')])
        
        print(f"Markdown export complete: {len(films)} films")
        print(f"IMDb data: {with_imdb}, Likely theatrical: {likely_theatrical}")
        if with_trailers > 0:
            print(f"Found trailers for {with_trailers} films")
        print(f"Saved: {filename}")


def export_all_formats(films: List[Dict], base_name: str = "nyff_films", recommendations: List[Dict] = None) -> None:
    """Export films to all supported formats.
    
    Args:
        films: List of film dictionaries
        base_name: Base filename (without extension)
        recommendations: Optional Letterboxd recommendations
    """
    JSONExporter.export(films, f"{base_name}.json", recommendations=recommendations)
    CSVExporter.export(films, f"{base_name}.csv", recommendations=recommendations) 
    MarkdownExporter.export(films, f"{base_name}.md", recommendations=recommendations)