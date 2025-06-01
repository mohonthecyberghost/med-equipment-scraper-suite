import asyncio
import logging
from typing import Dict, List, Optional
from urllib.parse import urljoin
from scrapers.base_scraper import BaseScraper

logger = logging.getLogger(__name__)

class MedicalExpoScraper(BaseScraper):
    def __init__(self):
        super().__init__('medicalexpo')
        self.base_url = 'https://www.medicalexpo.com'
        
    async def scrape(self, category: str = None, max_pages: int = 5, **kwargs):
        """
        Scrape products from MedicalExpo
        
        Args:
            category: Product category to scrape (e.g., 'dental-instruments-B')
            max_pages: Maximum number of pages to scrape
        """
        try:
            logger.info("Initializing browser....")
            await self.init_browser()
            
            # Navigate to category page
            category_url = f"{self.base_url}/cat/{category}" if category else self.base_url
            logger.info(f"Navigating to category URL: {category_url}")
            await self.page.goto(category_url)
            await self.handle_anti_bot()
            
            current_page = 1
            while current_page <= max_pages:
                logger.info(f"Scraping page {current_page}...")
                # Wait for product grid to load
                await self.page.wait_for_selector('.product-grid')
                
                # Get all product links on current page
                product_links = await self.page.query_selector_all('.product-grid a.product-link')
                logger.info(f"Found {len(product_links)} product links on page {current_page}")
                
                for link in product_links:
                    product_url = await link.get_attribute('href')
                    if product_url:
                        logger.info(f"Scraping product page: {product_url}")
                        await self.scrape_product_page(urljoin(self.base_url, product_url))
                
                # Check if next page exists
                next_button = await self.page.query_selector('a.next-page:not(.disabled)')
                if not next_button:
                    logger.info("No more pages to scrape.")
                    break
                    
                logger.info("Moving to the next page...")
                await next_button.click()
                
                await self.page.wait_for_load_state('networkidle')
                current_page += 1
                
        except Exception as e:
            logger.error(f"Error scraping MedicalExpo: {e}")
            raise
        finally:
            logger.info("Closing browser...")
            await self.close_browser()
    
    async def scrape_product_page(self, url: str):
        """Scrape individual product page"""
        try:
            logger.info(f"Navigating to product page: {url}")
            await self.page.goto(url)
            await self.handle_anti_bot()
            
            # Wait for product details to load
            await self.page.wait_for_selector('.product-details')
            
            # Extract product data
            product_data = {
                'source_id': url.split('/')[-1],
                'name': await self.get_text('.product-title'),
                'brand': await self.get_text('.brand-name'),
                'category': await self.get_text('.category-path'),
                'description': await self.get_text('.product-description'),
                'specifications': await self.get_specifications(),
                'images': await self.get_images(),
                'documents': await self.get_documents()
            }
            
            # Save to database
            logger.info(f"Saving product data for: {product_data['name']}")
            await self.save_product(product_data)
            logger.info(f"Successfully scraped product: {product_data['name']}")
            
        except Exception as e:
            logger.error(f"Error scraping product page {url}: {e}")
            raise
    
    async def get_text(self, selector: str) -> Optional[str]:
        """Get text content from element"""
        element = await self.page.query_selector(selector)
        return await element.text_content() if element else None
    
    async def get_specifications(self) -> Dict:
        """Extract product specifications"""
        specs = {}
        spec_rows = await self.page.query_selector_all('.specifications-table tr')
        
        for row in spec_rows:
            label = await row.query_selector('th')
            value = await row.query_selector('td')
            
            if label and value:
                label_text = await label.text_content()
                value_text = await value.text_content()
                specs[label_text.strip()] = value_text.strip()
        
        return specs
    
    async def get_images(self) -> List[Dict]:
        """Extract product images"""
        images = []
        image_elements = await self.page.query_selector_all('.product-gallery img')
        
        for idx, img in enumerate(image_elements):
            src = await img.get_attribute('src')
            if src:
                images.append({
                    'url': src,
                    'is_primary': idx == 0
                })
        
        return images
    
    async def get_documents(self) -> List[Dict]:
        """Extract product documents (PDFs, etc.)"""
        documents = []
        doc_links = await self.page.query_selector_all('.product-documents a')
        
        for link in doc_links:
            href = await link.get_attribute('href')
            if href and href.endswith('.pdf'):
                documents.append({
                    'url': href,
                    'document_type': 'pdf'
                })
        
        return documents

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Scrape MedicalExpo products')
    parser.add_argument('--category', help='Product category to scrape')
    parser.add_argument('--pages', type=int, default=5, help='Maximum number of pages to scrape')
    
    args = parser.parse_args()
    
    scraper = MedicalExpoScraper()
    asyncio.run(scraper.scrape(category=args.category, max_pages=args.pages)) 
