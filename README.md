# NYFF Scraper

A Python tool for scraping the New York Film Festival (NYFF) website and enriching the data with IMDb information and YouTube trailers.

**Author:** Jack Murphy

## Features

- **Film Data Scraping**: Extract comprehensive film information from NYFF lineup pages
- **IMDb Integration**: Automatically find and retrieve production company and distributor data
- **Trailer Discovery**: Search for and link YouTube trailers
- **Multiple Export Formats**: Generate JSON, CSV, and Markdown outputs
- **Fast & Cacheable**: Built-in caching system for efficient re-runs

## Quick Start

### Installation

```bash
git clone <repository-url>
cd nyff-scraper
pip install -e .
```

### Basic Usage

```bash
# Scrape NYFF 2025 lineup with full enrichment
nyff-scraper

# Scrape a custom URL
nyff-scraper https://www.filmlinc.org/nyff/nyff63-lineup/

# Test with limited films
nyff-scraper --limit 10

# Skip trailer search (faster)
nyff-scraper --skip-trailers

# Export only specific formats
nyff-scraper --csv-only
```

## Output Formats

### JSON
Complete structured data with all film information, showtimes, and metadata. This is the most appropriate format for uploading to an LLM and asking it questions about what you might want to see at NYFF this year.

### CSV
Flattened data suitable for spreadsheet analysis with one row per showtime.

### Markdown
Human-readable format perfect for documentation and sharing.

## Installation Options

### Option 1: pip install (Recommended)

```bash
# Clone the repository
git clone <repository-url>
cd nyff-scraper

# Install in development mode
pip install -e .

# Or install from PyPI (when published)
pip install nyff-scraper
```

### Option 2: Manual setup

```bash
# Clone and install dependencies
git clone <repository-url>
cd nyff-scraper
pip install -r requirements.txt

# Run directly
python -m src.nyff_scraper.cli
```

## Usage Examples

### Command Line Interface

The `nyff-scraper` command provides a comprehensive CLI with many options:

```bash
# Full pipeline with all features
nyff-scraper

# Scrape only (no enrichment)
nyff-scraper --only-scrape

# Skip specific enrichment steps
nyff-scraper --skip-imdb --skip-trailers

# Custom output location
nyff-scraper --output-dir ./results --output-name my_films

# Test with limited data
nyff-scraper --limit 5 --verbose
```

### Python API

You can also use the components directly in Python:

```python
from nyff_scraper import NYFFScraper, IMDbEnricher, TrailerEnricher
from nyff_scraper.exporters import export_all_formats

# Initialize components
scraper = NYFFScraper()
imdb_enricher = IMDbEnricher()
trailer_enricher = TrailerEnricher()

# Scrape films
films = scraper.scrape_nyff_lineup()

# Enrich with IMDb data
films = imdb_enricher.enrich_films(films)

# Add trailers
films = trailer_enricher.enrich_films(films, search_trailers=True)

# Export to all formats
export_all_formats(films, "my_films")
```

## Command Line Options

```
positional arguments:
  url                   URL to scrape (default: NYFF 2025 lineup)

processing options:
  --only-scrape         Only scrape film data, skip IMDb and trailer enrichment
  --skip-imdb          Skip IMDb enrichment (production companies, distributors)
  --skip-trailers      Skip YouTube trailer search
  --limit N            Limit processing to first N films (useful for testing)

output options:
  --output-dir DIR     Output directory for generated files (default: current directory)
  --output-name NAME   Base name for output files (default: nyff_films)
  --cache-dir DIR      Directory for caching web requests (default: cache)

export format options:
  --json-only          Export only JSON format
  --csv-only           Export only CSV format
  --markdown-only      Export only Markdown format

utility options:
  --verbose, -v        Enable verbose logging
  --quiet, -q          Suppress all output except errors
  --help, -h           Show this help message and exit
```

## Project Structure

```
nyff-scraper/
├── src/
│   └── nyff_scraper/
│       ├── __init__.py          # Package initialization
│       ├── cli.py               # Command-line interface
│       ├── scraper.py           # Web scraping functionality
│       ├── imdb_enricher.py     # IMDb data enrichment
│       ├── trailer_enricher.py  # YouTube trailer search
│       └── exporters.py         # Data export modules
├── tests/                       # Test suite
├── scripts/                     # Additional utility scripts
├── pyproject.toml              # Project configuration
├── requirements.txt            # Core dependencies
├── requirements-dev.txt        # Development dependencies
├── README.md                   # This file
└── .gitignore                  # Git ignore patterns
```

## Development

### Setting up for development

```bash
# Clone the repository
git clone <repository-url>
cd nyff-scraper

# Install in development mode with dev dependencies
pip install -e ".[dev]"

# Or use requirements files
pip install -r requirements-dev.txt

# Set up pre-commit hooks
pre-commit install
```

### Running tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=nyff_scraper

# Run specific test file
pytest tests/test_scraper.py
```

### Code formatting and linting

```bash
# Format code
black src/ tests/

# Sort imports
isort src/ tests/

# Lint code
flake8 src/ tests/

# Type checking
mypy src/
```

## Architecture

The scraper is built with a modular architecture:

1. **NYFFScraper**: Handles web scraping of film lineup pages
2. **IMDbEnricher**: Searches IMDb and extracts production/distribution data
3. **TrailerEnricher**: Searches YouTube for film trailers
4. **Exporters**: Convert data to various output formats (JSON, CSV, Markdown)
5. **CLI**: Command-line interface tying everything together

Each module can be used independently, making it easy to customize the workflow or extend functionality.

## Extending for Other Festivals

The architecture is designed to be extensible. To adapt for other film festivals:

1. Create a new scraper class inheriting from a base scraper
2. Implement festival-specific parsing logic
3. Update the CLI to support the new festival
4. Add festival-specific configuration

## Dependencies

- **requests**: HTTP library for web scraping
- **beautifulsoup4**: HTML parsing and extraction
- **lxml**: Fast XML/HTML parser

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/new-feature`)
3. Make your changes
4. Add tests for new functionality
5. Run the test suite and linting
6. Commit your changes (`git commit -am 'Add new feature'`)
7. Push to the branch (`git push origin feature/new-feature`)
8. Create a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- New York Film Festival for providing accessible film data
- IMDb for production and distribution information
- YouTube for trailer hosting and search capabilities

## Troubleshooting

### Common Issues

**"No films found"**: Check that the URL is correct and the website structure hasn't changed.

**Rate limiting**: The scraper includes delays to be respectful to servers. For faster testing, use `--limit` option.

**Missing dependencies**: Ensure all requirements are installed with `pip install -r requirements.txt`.

**Permission errors**: Make sure you have write permissions in the output directory.

### Getting Help

- Check the [Issues](https://github.com/username/nyff-scraper/issues) page for known problems
- Create a new issue with detailed error information
- Use `--verbose` flag for detailed logging when reporting issues