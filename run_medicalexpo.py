import asyncio
import logging
from scrapers.medicalexpo import MedicalExpoScraper

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

async def main():
    # Initialize the scraper
    scraper = MedicalExpoScraper()
    
    # Run the scraper with specific category and pages
    await scraper.scrape(
        category="dental-turbine-27517",  # Example category
        max_pages=1  # Start with 1 page for testing
    )

if __name__ == "__main__":
    asyncio.run(main()) 