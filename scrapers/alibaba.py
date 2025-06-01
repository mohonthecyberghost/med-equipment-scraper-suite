import asyncio
import logging
import re
from typing import Dict, List, Optional
from urllib.parse import urljoin, urlencode
from scrapers.base_scraper import BaseScraper

logger = logging.getLogger(__name__)

class AlibabaScraper(BaseScraper):
    def __init__(self):
        super().__init__('alibaba')
        self.base_url = 'https://www.alibaba.com'
        self.search_url = f"{self.base_url}/trade/search"
        
    async def scrape(self, category: str = None, min_rating: float = 0.0, max_items: int = 100, **kwargs):
        """
        Scrape products from Alibaba
        
        Args:
            category: Product category
            min_rating: Minimum seller rating
            max_items: Maximum number of items to scrape
        """
        try:
            await self.init_browser()
            
            # Navigate to category page
            search_params = {
                'SearchText': category,
                'catId': self.get_category_id(category),
                'minRating': min_rating
            }
            search_url = f"{self.search_url}?{urlencode(search_params)}"
            await self.page.goto(search_url)
            await self.handle_anti_bot()
            
            items_scraped = 0
            while items_scraped < max_items:
                # Wait for product grid to load
                await self.page.wait_for_selector('.product-card')
                
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
                next_button = await self.page.query_selector('a.next:not(.disabled)')
                if not next_button:
                    break
                    
                await next_button.click()
                await self.page.wait_for_load_state('networkidle')
                
        except Exception as e:
            logger.error(f"Error scraping Alibaba: {e}")
            raise
        finally:
            await self.close_browser()
    
    def get_category_id(self, category: str) -> str:
        """Map category name to Alibaba category ID"""
        # This is a simplified mapping - you may need to expand this
        category_map = {
            'medical equipment': '100003070',
            'surgical instruments': '100003071',
            'diagnostic equipment': '100003072',
            'medical supplies': '100003073'
        }
        return category_map.get(category.lower(), '')
    
    async def scrape_product_page(self, url: str):
        """Scrape individual product page"""
        try:
            await self.page.goto(url)
            await self.handle_anti_bot()
            
            # Wait for product details to load
            await self.page.wait_for_selector('.product-details')
            
            # Extract product data
            product_data = {
                'source_id': self.extract_product_id(url),
                'name': await self.get_text('.product-title'),
                'category': await self.get_text('.category-path'),
                'description': await self.get_text('.product-description'),
                'specifications': await self.get_specifications(),
                'pricing': await self.get_pricing(),
                'seller': await self.get_seller_info(),
                'images': await self.get_images()
            }
            
            # Save to database
            await self.save_product(product_data)
            logger.info(f"Successfully scraped product: {product_data['name']}")
            
        except Exception as e:
            logger.error(f"Error scraping product page {url}: {e}")
            raise
    
    def extract_product_id(self, url: str) -> str:
        """Extract product ID from URL"""
        match = re.search(r'/(\d+)\.html', url)
        return match.group(1) if match else url.split('/')[-1]
    
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
    
    async def get_pricing(self) -> Dict:
        """Extract product pricing information"""
        pricing = {}
        
        # Get price range
        price_element = await self.page.query_selector('.price-range')
        if price_element:
            price_text = await price_element.text_content()
            price_matches = re.findall(r'[\d,]+\.?\d*', price_text)
            if len(price_matches) >= 2:
                pricing['min_price'] = float(price_matches[0].replace(',', ''))
                pricing['max_price'] = float(price_matches[1].replace(',', ''))
        
        # Get minimum order quantity
        moq_element = await self.page.query_selector('.min-order-quantity')
        if moq_element:
            moq_text = await moq_element.text_content()
            moq_match = re.search(r'(\d+)', moq_text)
            if moq_match:
                pricing['min_order_quantity'] = int(moq_match.group(1))
        
        return pricing
    
    async def get_seller_info(self) -> Dict:
        """Extract seller information"""
        seller = {}
        
        # Get seller name
        name_element = await self.page.query_selector('.company-name')
        if name_element:
            seller['name'] = await name_element.text_content()
        
        # Get seller rating
        rating_element = await self.page.query_selector('.seller-rating')
        if rating_element:
            rating_text = await rating_element.text_content()
            rating_match = re.search(r'([\d.]+)', rating_text)
            if rating_match:
                seller['rating'] = float(rating_match.group(1))
        
        # Get seller location
        location_element = await self.page.query_selector('.company-location')
        if location_element:
            seller['location'] = await location_element.text_content()
        
        # Get seller website
        website_element = await self.page.query_selector('.company-website')
        if website_element:
            seller['website'] = await website_element.get_attribute('href')
        
        return seller
    
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
    
    parser = argparse.ArgumentParser(description='Scrape Alibaba products')
    parser.add_argument('--category', help='Product category')
    parser.add_argument('--min-rating', type=float, default=0.0, help='Minimum seller rating')
    parser.add_argument('--max-items', type=int, default=100, help='Maximum number of items to scrape')
    
    args = parser.parse_args()
    
    scraper = AlibabaScraper()
    asyncio.run(scraper.scrape(
        category=args.category,
        min_rating=args.min_rating,
        max_items=args.max_items
    )) 