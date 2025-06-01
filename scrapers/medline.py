import asyncio
import logging
import re
from typing import Dict, List, Optional
from urllib.parse import urljoin, urlencode
from scrapers.base_scraper import BaseScraper

logger = logging.getLogger(__name__)

class MedlineScraper(BaseScraper):
    def __init__(self):
        super().__init__('medline')
        self.base_url = 'https://www.medline.com'
        self.search_url = f"{self.base_url}/search"
        
    async def scrape(self, keyword: str = None, max_items: int = 100, **kwargs):
        """
        Scrape products from Medline
        
        Args:
            keyword: Search keyword
            max_items: Maximum number of items to scrape
        """
        try:
            await self.init_browser()
            
            # Navigate to search page
            search_params = {'q': keyword} if keyword else {}
            search_url = f"{self.search_url}?{urlencode(search_params)}"
            await self.page.goto(search_url)
            await self.handle_anti_bot()
            
            items_scraped = 0
            while items_scraped < max_items:
                # Wait for product grid to load
                await self.page.wait_for_selector('.product-grid')
                
                # Get all product cards on current page
                product_cards = await self.page.query_selector_all('.product-card')
                
                for card in product_cards:
                    if items_scraped >= max_items:
                        break
                        
                    product_url = await card.query_selector('a.product-link')
                    if product_url:
                        url = await product_url.get_attribute('href')
                        if url:
                            await self.scrape_product_page(urljoin(self.base_url, url))
                            items_scraped += 1
                
                if items_scraped >= max_items:
                    break
                    
                # Check if next page exists
                next_button = await self.page.query_selector('button[aria-label="Next page"]:not([disabled])')
                if not next_button:
                    break
                    
                await next_button.click()
                await self.page.wait_for_load_state('networkidle')
                
        except Exception as e:
            logger.error(f"Error scraping Medline: {e}")
            raise
        finally:
            await self.close_browser()
    
    async def scrape_product_page(self, url: str):
        """Scrape individual product page"""
        try:
            await self.page.goto(url)
            await self.handle_anti_bot()
            
            # Wait for product details to load
            await self.page.wait_for_selector('.product-details')
            
            # Extract product data
            product_data = {
                'source_id': self.extract_sku(url),
                'name': await self.get_text('.product-title'),
                'brand': await self.get_text('.brand-name'),
                'category': await self.get_text('.breadcrumb-item:last-child'),
                'description': await self.get_text('.product-description'),
                'specifications': await self.get_specifications(),
                'pricing': await self.get_pricing(),
                'images': await self.get_images()
            }
            
            # Save to database
            await self.save_product(product_data)
            logger.info(f"Successfully scraped product: {product_data['name']}")
            
        except Exception as e:
            logger.error(f"Error scraping product page {url}: {e}")
            raise
    
    def extract_sku(self, url: str) -> str:
        """Extract SKU from URL"""
        match = re.search(r'/p/([^/]+)', url)
        return match.group(1) if match else url.split('/')[-1]
    
    async def get_text(self, selector: str) -> Optional[str]:
        """Get text content from element"""
        element = await self.page.query_selector(selector)
        return await element.text_content() if element else None
    
    async def get_specifications(self) -> Dict:
        """Extract product specifications"""
        specs = {}
        spec_sections = await self.page.query_selector_all('.specifications-section')
        
        for section in spec_sections:
            section_title = await section.query_selector('.section-title')
            if section_title:
                title_text = await section_title.text_content()
                specs[title_text.strip()] = {}
                
                spec_rows = await section.query_selector_all('.spec-row')
                for row in spec_rows:
                    label = await row.query_selector('.spec-label')
                    value = await row.query_selector('.spec-value')
                    
                    if label and value:
                        label_text = await label.text_content()
                        value_text = await value.text_content()
                        specs[title_text.strip()][label_text.strip()] = value_text.strip()
        
        return specs
    
    async def get_pricing(self) -> Dict:
        """Extract product pricing information"""
        pricing = {}
        
        # Get price
        price_element = await self.page.query_selector('.product-price')
        if price_element:
            price_text = await price_element.text_content()
            price_match = re.search(r'\$([\d,]+\.?\d*)', price_text)
            if price_match:
                pricing['min_price'] = float(price_match.group(1).replace(',', ''))
        
        # Get unit
        unit_element = await self.page.query_selector('.product-unit')
        if unit_element:
            pricing['unit'] = await unit_element.text_content()
        
        # Get minimum order quantity
        moq_element = await self.page.query_selector('.min-order-quantity')
        if moq_element:
            moq_text = await moq_element.text_content()
            moq_match = re.search(r'(\d+)', moq_text)
            if moq_match:
                pricing['min_order_quantity'] = int(moq_match.group(1))
        
        return pricing
    
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

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Scrape Medline products')
    parser.add_argument('--keyword', help='Search keyword')
    parser.add_argument('--max-items', type=int, default=100, help='Maximum number of items to scrape')
    
    args = parser.parse_args()
    
    scraper = MedlineScraper()
    asyncio.run(scraper.scrape(keyword=args.keyword, max_items=args.max_items)) 