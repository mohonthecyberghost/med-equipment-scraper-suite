# Medical Equipment Scraper Suite

A comprehensive web scraping solution for gathering medical equipment data from multiple sources for market research and competitor analysis.

## Supported Sources
- MedicalExpo (medicalexpo.com)
- Medline (medline.com)
- Alibaba Medical Equipment (alibaba.com)

## Features
- Dynamic content scraping using Playwright
- Structured data storage in MySQL
- Export capabilities (JSON and CSV)
- Configurable search parameters
- Anti-bot protection handling
- Pagination support

## Prerequisites
- Python 3.8+
- Node.js 16+
- MySQL 8.0+
- Playwright

## Installation

1. Clone the repository:
```bash
git clone [repository-url]
cd med-equipment-scraper-suite
```

2. Install Python dependencies:
```bash
pip install -r requirements.txt
```

3. Install Node.js dependencies:
```bash
npm install
```

4. Install Playwright browsers:
```bash
playwright install
```

5. Set up MySQL database:
```bash
mysql -u root -p < database/schema.sql
```

6. Configure environment variables:
```bash
cp .env.example .env
# Edit .env with your database credentials and other settings
```

## Usage

### Running Individual Scrapers

1. MedicalExpo Scraper:
```bash
python scrapers/medicalexpo.py --category "Diagnostic Equipment" --pages 5
```

2. Medline Scraper:
```bash
python scrapers/medline.py --keyword "surgical instruments" --max-items 100
```

3. Alibaba Scraper:
```bash
python scrapers/alibaba.py --category "Medical Equipment" --min-rating 4.5
```

### Exporting Data

Export to JSON:
```bash
python export.py --format json --source all
```

Export to CSV:
```bash
python export.py --format csv --source medicalexpo
```

## Project Structure
```
med-equipment-scraper-suite/
├── scrapers/              # Individual scraper modules
├── database/             # Database schema and migrations
├── utils/               # Shared utilities and helpers
├── config/             # Configuration files
├── exports/           # Export directory for data
└── tests/            # Test cases
```

## Configuration
- Edit `config/scraper_config.py` to customize scraping parameters
- Modify `config/database_config.py` for database settings
- Update `config/export_config.py` for export preferences

## Contributing
1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License
MIT License - see LICENSE file for details
