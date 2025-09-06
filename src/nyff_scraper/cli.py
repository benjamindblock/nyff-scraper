"""
Command-line interface for NYFF Scraper.

Author: Jack Murphy
"""

import argparse
import logging
import sys
from typing import Optional

from .scraper import NYFFScraper
from .imdb_enricher import IMDbEnricher
from .trailer_enricher import TrailerEnricher
from .exporters import export_all_formats

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def setup_argument_parser() -> argparse.ArgumentParser:
    """Set up command-line argument parser."""
    parser = argparse.ArgumentParser(
        description="Scrape NYFF film data and enrich with IMDb and trailer information",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  nyff-scraper                                          # Scrape default NYFF URL with all features
  nyff-scraper --url https://example.com/films         # Scrape custom URL
  nyff-scraper --only-scrape                           # Only scrape, no enrichment
  nyff-scraper --skip-trailers                         # Skip YouTube trailer search
  nyff-scraper --limit 10                              # Test with first 10 films only
  nyff-scraper --output-dir ./results                  # Save files to specific directory
        """
    )

    # Main arguments
    parser.add_argument(
        'url',
        nargs='?',
        default="https://www.filmlinc.org/nyff/nyff63-lineup/",
        help='URL to scrape (default: NYFF 2025 lineup)'
    )

    # Processing options
    processing_group = parser.add_argument_group('processing options')
    processing_group.add_argument(
        '--only-scrape',
        action='store_true',
        help='Only scrape film data, skip IMDb and trailer enrichment'
    )
    processing_group.add_argument(
        '--skip-imdb',
        action='store_true',
        help='Skip IMDb enrichment (production companies, distributors)'
    )
    processing_group.add_argument(
        '--skip-trailers',
        action='store_true',
        help='Skip YouTube trailer search'
    )
    processing_group.add_argument(
        '--limit',
        type=int,
        metavar='N',
        help='Limit processing to first N films (useful for testing)'
    )

    # Output options
    output_group = parser.add_argument_group('output options')
    output_group.add_argument(
        '--output-dir',
        default='.',
        help='Output directory for generated files (default: current directory)'
    )
    output_group.add_argument(
        '--output-name',
        default='nyff_films',
        help='Base name for output files (default: nyff_films)'
    )
    output_group.add_argument(
        '--cache-dir',
        default='cache',
        help='Directory for caching web requests (default: cache)'
    )

    # Export format options
    format_group = parser.add_argument_group('export format options')
    format_group.add_argument(
        '--json-only',
        action='store_true',
        help='Export only JSON format'
    )
    format_group.add_argument(
        '--csv-only',
        action='store_true',
        help='Export only CSV format'
    )
    format_group.add_argument(
        '--markdown-only',
        action='store_true',
        help='Export only Markdown format'
    )

    # Utility options
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging'
    )
    parser.add_argument(
        '--quiet', '-q',
        action='store_true',
        help='Suppress all output except errors'
    )

    return parser


def configure_logging(verbose: bool, quiet: bool) -> None:
    """Configure logging based on verbosity settings."""
    if quiet:
        logging.getLogger().setLevel(logging.ERROR)
    elif verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    else:
        logging.getLogger().setLevel(logging.INFO)


def validate_arguments(args) -> bool:
    """Validate command-line arguments."""
    # Check for conflicting format options
    format_options = sum([args.json_only, args.csv_only, args.markdown_only])
    if format_options > 1:
        logger.error("Cannot specify multiple --*-only format options")
        return False

    # Check for conflicting verbosity options
    if args.verbose and args.quiet:
        logger.error("Cannot specify both --verbose and --quiet")
        return False

    return True


def run_scraper_pipeline(args) -> int:
    """Run the complete scraper pipeline."""
    try:
        # Initialize components
        scraper = NYFFScraper(cache_dir=args.cache_dir)

        # Step 1: Scrape film data
        print("Scraping NYFF film lineup...")
        films = scraper.scrape_nyff_lineup(args.url)

        if not films:
            logger.error("No films found at the provided URL")
            return 1

        print(f"Found {len(films)} films")

        # Step 2: Enrich with IMDb data (unless skipped)
        if not args.only_scrape and not args.skip_imdb:
            print("Enriching with IMDb production company and distributor data...")
            imdb_enricher = IMDbEnricher(cache_dir=args.cache_dir)
            films = imdb_enricher.enrich_films(films, limit=args.limit)

            with_imdb = len([f for f in films if f.get('imdb_id')])
            print(f"Found IMDb data for {with_imdb}/{len(films)} films")

        # Step 3: Enrich with trailer data (unless skipped)
        if not args.only_scrape and not args.skip_trailers:
            print("Searching for YouTube trailers...")
            trailer_enricher = TrailerEnricher()
            films = trailer_enricher.enrich_films(films, search_trailers=True, limit=args.limit)

            with_trailers = len([f for f in films if f.get('trailer_url')])
            print(f"Found trailers for {with_trailers}/{len(films)} films")

        # Step 4: Export data
        print("Exporting data...")

        import os
        if args.output_dir != '.':
            os.makedirs(args.output_dir, exist_ok=True)

        base_path = os.path.join(args.output_dir, args.output_name)

        if args.json_only:
            from .exporters import JSONExporter
            JSONExporter.export(films, f"{base_path}.json")
        elif args.csv_only:
            from .exporters import CSVExporter
            CSVExporter.export(films, f"{base_path}.csv")
        elif args.markdown_only:
            from .exporters import MarkdownExporter
            MarkdownExporter.export(films, f"{base_path}.md")
        else:
            # Export all formats
            export_all_formats(films, base_path)

        # Final summary
        print(f"\nScraping completed successfully!")
        print(f"Processed {len(films)} films")

        if not args.only_scrape:
            if not args.skip_imdb:
                with_imdb = len([f for f in films if f.get('imdb_id')])
                likely_theatrical = len([f for f in films if f.get('likely_theatrical')])
                print(f"IMDb matches: {with_imdb}")
                print(f"Likely theatrical releases: {likely_theatrical}")

            if not args.skip_trailers:
                with_trailers = len([f for f in films if f.get('trailer_url')])
                print(f"Trailers found: {with_trailers}")

        return 0

    except KeyboardInterrupt:
        print("\nInterrupted by user")
        return 1
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=args.verbose)
        return 1


def main() -> int:
    """Main CLI entry point."""
    parser = setup_argument_parser()
    args = parser.parse_args()

    # Configure logging
    configure_logging(args.verbose, args.quiet)

    # Validate arguments
    if not validate_arguments(args):
        return 1

    # Run the scraper pipeline
    return run_scraper_pipeline(args)


if __name__ == "__main__":
    sys.exit(main())