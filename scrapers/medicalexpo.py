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
            category: Product category to scrape (e.g., 'dental-turbine-27517')
            max_pages: Maximum number of pages to scrape
        """
        try:
            logger.info("Initializing browser...")
            await self.init_browser()
            
            # Navigate to category page
            category_url = f"{self.base_url}/medical-manufacturer/{category}.html" if category else self.base_url
            logger.info(f"Navigating to category URL: {category_url}")
            await self.page.goto(category_url)
            await self.handle_anti_bot()
            
            current_page = 1
            while current_page <= max_pages:
                logger.info(f"Scraping page {current_page}...")
                
                # Wait for product list container to load
                await self.page.wait_for_selector('#result-list')
                
                # Get all product tiles on current page
                product_tiles = await self.page.query_selector_all('.product-tile')
                logger.info(f"Found {len(product_tiles)} products on page {current_page}")
                
                for tile in product_tiles:
                    try:
                        # Extract product ID and basic info
                        product_id = await tile.get_attribute('data-product-block')
                        logger.info(f"Processing product ID: {product_id}")
                        
                        # Extract product name and model
                        name_element = await tile.query_selector('.short-name')
                        product_name = await name_element.text_content() if name_element else None
                        
                        # Extract brand/model
                        brand_element = await tile.query_selector('.brand')
                        brand_model = await brand_element.text_content() if brand_element else None
                        
                        # Extract features
                        features = []
                        feature_elements = await tile.query_selector_all('.feature-values-container span')
                        for feature in feature_elements:
                            feature_text = await feature.text_content()
                            if feature_text:
                                features.append(feature_text.strip())
                        
                        # Extract specifications from description
                        specs = {}
                        desc_element = await tile.query_selector('.description-content p')
                        if desc_element:
                            desc_text = await desc_element.text_content()
                            # Parse specifications from description text
                            for line in desc_text.split('<br>'):
                                if ':' in line:
                                    key, value = line.split(':', 1)
                                    specs[key.strip()] = value.strip()
                        
                        # Extract product URL
                        link_element = await tile.query_selector('a[data-product-stand-link]')
                        if not link_element:
                            continue
                            
                        product_url = await link_element.get_attribute('href')
                        if product_url:
                            logger.info(f"Found product URL: {product_url}")
                            
                            # Extract manufacturer info
                            manufacturer_element = await tile.query_selector('.logo img')
                            manufacturer_name = await manufacturer_element.get_attribute('alt') if manufacturer_element else None
                            
                            # Extract image URL
                            img_element = await tile.query_selector('.inset-img img')
                            image_url = await img_element.get_attribute('src') if img_element else None
                            
                            # Create product data dictionary
                            product_data = {
                                'source_id': product_id,
                                'name': product_name,
                                'brand_model': brand_model,
                                'features': features,
                                'specifications': specs,
                                'manufacturer': manufacturer_name,
                                'image_url': image_url,
                                'url': product_url
                            }
                            
                            logger.info(f"Extracted product data: {product_data['name']} by {product_data['manufacturer']}")
                            await self.scrape_product_page(urljoin(self.base_url, product_url), product_data)
                    except Exception as e:
                        logger.error(f"Error processing product tile: {e}")
                        continue
                
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
    
    async def scrape_product_page(self, url: str, initial_data: dict = None):
        """Scrape individual product page"""
        try:
            logger.info(f"Navigating to product page: {url}")
            
            # Navigate to the page and wait for load
            try:
                await self.page.goto(url, wait_until='networkidle', timeout=30000)
                await self.page.wait_for_load_state('domcontentloaded')
            except Exception as e:
                logger.error(f"Failed to load page {url}: {e}")
                return
                
            # Handle anti-bot measures
            await self.handle_anti_bot()
            
            # Wait for product details to load with timeout
            try:
                await self.page.wait_for_selector('.sc-1w8z6ht-4', timeout=10000)
            except Exception as e:
                logger.warning(f"Timeout waiting for product details: {e}")
                return
            
            # Initialize product data with required fields
            product_data = {
                'source_id': None,
                'name': None,
                'brand_model': None,
                'features': [],
                'specifications': {},
                'manufacturer': None,
                'image_url': None,
                'url': url,
                'characteristics': {},
                'description': None,
                'video_url': None,
                'catalog_status': None
            }
            
            # Update with initial data if provided
            if initial_data:
                product_data.update({k: v for k, v in initial_data.items() if v is not None})
            
            try:
                # Extract characteristics
                try:
                    # Wait for characteristics to be available
                    await self.page.wait_for_selector('.sc-mgb5nu-0.gedvae', timeout=5000)
                    char_elements = await self.page.query_selector_all('.sc-mgb5nu-0.gedvae dt, .sc-mgb5nu-0.gedvae dd')
                    if char_elements:
                        for i in range(0, len(char_elements), 2):
                            if i + 1 < len(char_elements):
                                try:
                                    key = await char_elements[i].text_content()
                                    value = await char_elements[i + 1].text_content()
                                    if key and value:
                                        product_data['characteristics'][key.strip()] = value.strip()
                                except Exception as e:
                                    logger.warning(f"Error extracting characteristic: {e}")
                                    continue
                except Exception as e:
                    logger.warning(f"Error finding characteristics: {e}")
                
                # Extract description
                try:
                    # Wait for description to be available
                    await self.page.wait_for_selector('.sc-3fi1by-0.hlEuXW', timeout=5000)
                    desc_element = await self.page.query_selector('.sc-3fi1by-0.hlEuXW')
                    if desc_element:
                        product_data['description'] = await desc_element.text_content()
                except Exception as e:
                    logger.warning(f"Error extracting description: {e}")
                
                # Extract video URL if available
                try:
                    # Wait for video element to be available
                    await self.page.wait_for_selector('video source', timeout=5000)
                    video_element = await self.page.query_selector('video source')
                    if video_element:
                        product_data['video_url'] = await video_element.get_attribute('src')
                except Exception as e:
                    logger.warning(f"Error extracting video URL: {e}")
                
                # Extract catalog information
                try:
                    # Wait for catalog element to be available
                    await self.page.wait_for_selector('.sc-4ad8uc-9.kqzNkm', timeout=5000)
                    catalog_element = await self.page.query_selector('.sc-4ad8uc-9.kqzNkm')
                    if catalog_element:
                        product_data['catalog_status'] = await catalog_element.text_content()
                except Exception as e:
                    logger.warning(f"Error extracting catalog status: {e}")
                
                # Validate required fields before saving
                if not product_data.get('name'):
                    logger.warning("Product name not found, skipping save")
                    return
                
                # Clean up None values
                product_data = {k: v for k, v in product_data.items() if v is not None}
                
                # Save to database
                logger.info(f"Saving product data for: {product_data['name']}")
                await self.save_product(product_data)
                logger.info(f"Successfully scraped product: {product_data['name']}")
                
            except Exception as e:
                logger.error(f"Error processing product data: {e}")
                raise
            
        except Exception as e:
            logger.error(f"Error scraping product page {url}: {e}")
            raise
        finally:
            # Ensure we're not leaving any hanging contexts
            try:
                await self.page.wait_for_load_state('networkidle')
            except:
                pass
    
    async def handle_anti_bot(self):
        """Handle anti-bot measures"""
        try:
            # Wait for any potential anti-bot elements
            await self.page.wait_for_load_state('networkidle')
            await self.page.wait_for_timeout(2000)  # Add a small delay
            
            # Check for and handle any cookie consent or other popups
            try:
                consent_button = await self.page.query_selector('button[aria-label="Accept cookies"]')
                if consent_button:
                    await consent_button.click()
                    await self.page.wait_for_timeout(1000)
            except Exception as e:
                logger.debug(f"No cookie consent found or error handling it: {e}")
                
        except Exception as e:
            logger.warning(f"Error handling anti-bot measures: {e}")
    
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