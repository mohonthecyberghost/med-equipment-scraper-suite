import asyncio
import logging
from typing import Dict, List, Optional
from urllib.parse import urljoin
from scrapers.base_scraper import BaseScraper
import aiomysql
from dotenv import load_dotenv
import os
import json

logger = logging.getLogger(__name__)

class MedicalExpoScraper(BaseScraper):
    def __init__(self):
        super().__init__('medicalexpo')
        self.base_url = 'https://www.medicalexpo.com'
        self.db_pool = None
        
    async def init_database(self):
        """Initialize database connection pool"""
        try:
            # Load environment variables
            load_dotenv()
            
            # Get database configuration from environment variables
            db_config = {
                'host': os.getenv('DB_HOST', 'localhost'),
                'port': int(os.getenv('DB_PORT', 3306)),
                'user': os.getenv('DB_USER', 'root'),
                'password': os.getenv('DB_PASSWORD', ''),
                'db': os.getenv('DB_NAME', 'med_equipment'),
                'charset': 'utf8mb4',
                'autocommit': True
            }
            
            # Create connection pool
            self.db_pool = await aiomysql.create_pool(**db_config)
            logger.info("Database connection pool initialized")
            
        except Exception as e:
            logger.error(f"Error initializing database: {e}")
            raise
            
    async def close_database(self):
        """Close database connection pool"""
        if self.db_pool:
            self.db_pool.close()
            await self.db_pool.wait_closed()
            logger.info("Database connection pool closed")
            
    async def scrape(self, category: str = None, max_pages: int = 5, **kwargs):
        """
        Scrape products from MedicalExpo
        
        Args:
            category: Product category to scrape (e.g., 'dental-turbine-27517')
            max_pages: Maximum number of pages to scrape
        """
        try:
            # Initialize database
            await self.init_database()
            
            logger.info("Initializing browser....")
            await self.init_browser()
            
            # Navigate to category page
            category_url = f"{self.base_url}/medical-manufacturer/{category}.html" if category else self.base_url
            logger.info(f"Navigating to category URL: {category_url}")
            await self.page.goto(category_url, wait_until='networkidle')
            await self.handle_anti_bot()
            
            current_page = 1
            while current_page <= max_pages:
                logger.info(f"Scraping page {current_page}...")
                
                # Wait for product list container to load
                await self.page.wait_for_selector('#result-list', timeout=10000)
                
                # Get all product tiles on current page
                product_tiles = await self.page.query_selector_all('.result-tab-flex>.product-tile')
                logger.info(f"Found {len(product_tiles)} products on page {current_page}")
                
                # Store all product URLs first
                product_urls = []
                for tile in product_tiles:
                    try:
                        link_element = await tile.query_selector('a[data-product-stand-link]')
                        if link_element:
                            product_url = await link_element.get_attribute('href')
                            if product_url:
                                product_urls.append(urljoin(self.base_url, product_url))
                    except Exception as e:
                        logger.warning(f"Error extracting product URL: {e}")
                        continue
                
                # Process each product URL
                for product_url in product_urls:
                    try:
                        # Extract source_id from URL
                        source_id = None
                        try:
                            # Extract the product ID from the URL (e.g., product-94091-1057479)
                            import re
                            match = re.search(r'product-(\d+-\d+)', product_url)
                            if match:
                                source_id = match.group(1)
                        except Exception as e:
                            logger.warning(f"Error extracting source_id from URL: {e}")
                        
                        # Navigate to product page
                        logger.info(f"Navigating to product page: {product_url}")
                        response = await self.page.goto(product_url, wait_until='networkidle', timeout=30000)
                        if not response or not response.ok:
                            logger.error(f"Failed to load page {product_url}: {response.status if response else 'No response'}")
                            continue
                            
                        await self.page.wait_for_load_state('domcontentloaded')
                        
                        # Handle anti-bot measures
                        await self.handle_anti_bot()
                        
                        # Wait for product details to load
                        try:
                            await self.page.wait_for_selector('.sc-1w8z6ht-4', timeout=10000)
                        except Exception as e:
                            logger.warning(f"Timeout waiting for product details: {e}")
                            continue
                        
                        # Initialize product data
                        product_data = {
                            'source': 'medicalexpo',
                            'source_id': source_id,
                            'name': None,
                            'manufacturer': None,
                            'category': category,
                            'description': None,
                            'specifications': {},
                            'url': product_url,
                            'video_url': None
                        }
                        
                        # Extract product information
                        try:
                            # Extract name
                            name_element = await self.page.query_selector('.sc-1w8z6ht-4 h1')
                            if name_element:
                                product_data['name'] = await name_element.text_content()
                            
                            # Extract manufacturer
                            try:
                                # Wait for manufacturer element to be available
                                await self.page.wait_for_selector('.supplierDetails__Name-sc-1u0qos1-6', timeout=5000)
                                manufacturer_element = await self.page.query_selector('.supplierDetails__Name-sc-1u0qos1-6')
                                if manufacturer_element:
                                    product_data['manufacturer'] = await manufacturer_element.text_content()
                                    logger.info(f"Found manufacturer: {product_data['manufacturer']}")
                                else:
                                    logger.warning("Manufacturer element not found")
                            except Exception as e:
                                logger.warning(f"Error extracting manufacturer: {e}")
                            
                            # Extract characteristics and add to specifications
                            try:
                                char_elements = await self.page.query_selector_all('.sc-mgb5nu-0.gedvae dt, .sc-mgb5nu-0.gedvae dd')
                                if char_elements:
                                    for i in range(0, len(char_elements), 2):
                                        if i + 1 < len(char_elements):
                                            try:
                                                key = await char_elements[i].text_content()
                                                value = await char_elements[i + 1].text_content()
                                                if key and value:
                                                    product_data['specifications'][key.strip()] = value.strip()
                                            except Exception as e:
                                                logger.warning(f"Error extracting characteristic: {e}")
                                                continue
                            except Exception as e:
                                logger.warning(f"Error finding characteristics: {e}")
                            
                            # Extract description
                            try:
                                desc_element = await self.page.query_selector('.sc-3fi1by-0.hlEuXW')
                                if desc_element:
                                    product_data['description'] = await desc_element.text_content()
                            except Exception as e:
                                logger.warning(f"Error extracting description: {e}")
                            
                            # Extract video URL if available
                            try:
                                video_element = await self.page.query_selector('video source')
                                if video_element:
                                    product_data['video_url'] = await video_element.get_attribute('src')
                            except Exception as e:
                                logger.warning(f"Error extracting video URL: {e}")
                            
                            # Clean up None values
                            product_data = {k: v for k, v in product_data.items() if v is not None}
                            
                            # Save to database if we have required fields
                            if product_data.get('name') and product_data.get('source_id'):
                                logger.info(f"Saving product data for: {product_data['name']}")
                                await self.save_product(product_data)
                                logger.info(f"Successfully scraped product: {product_data['name']}")
                            else:
                                logger.warning("Required fields (name or source_id) not found, skipping save")
                            
                        except Exception as e:
                            logger.error(f"Error processing product data: {e}")
                            continue
                            
                    except Exception as e:
                        logger.error(f"Error processing product page {product_url}: {e}")
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
            await self.close_database()
    
    async def scrape_product_page(self, url: str, initial_data: dict = None):
        """Scrape individual product page"""
        try:
            logger.info(f"Navigating to product page: {url}")
            
            # Navigate to the page and wait for load
            try:
                response = await self.page.goto(url, wait_until='networkidle', timeout=30000)
                if not response or not response.ok:
                    logger.error(f"Failed to load page {url}: {response.status if response else 'No response'}")
                    return
                    
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

    async def save_product(self, product_data: dict):
        """Save or update product data in the database"""
        if not self.db_pool:
            raise Exception("Database pool not initialized")
            
        try:
            # Check if product already exists
            check_query = "SELECT id FROM products WHERE source_id = %s"
            async with self.db_pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(check_query, (product_data['source_id'],))
                    result = await cursor.fetchone()
                    
                    if result:
                        # Product exists, update it
                        logger.info(f"Product {product_data['source_id']} exists, updating...")
                        update_fields = []
                        update_values = []
                        
                        # Build update query dynamically based on available fields
                        for key, value in product_data.items():
                            if key != 'source_id':  # Don't update the source_id
                                update_fields.append(f"{key} = %s")
                                # Convert dictionary to JSON string if value is a dict
                                if isinstance(value, dict):
                                    value = json.dumps(value)
                                update_values.append(value)
                        
                        # Add source_id for WHERE clause
                        update_values.append(product_data['source_id'])
                        
                        update_query = f"""
                            UPDATE products 
                            SET {', '.join(update_fields)}, 
                                updated_at = CURRENT_TIMESTAMP
                            WHERE source_id = %s
                        """
                        
                        await cursor.execute(update_query, tuple(update_values))
                        await conn.commit()
                        logger.info(f"Successfully updated product {product_data['source_id']}")
                    else:
                        # Product doesn't exist, insert new
                        logger.info(f"Product {product_data['source_id']} not found, inserting new...")
                        fields = list(product_data.keys())
                        values = []
                        
                        # Convert dictionary values to JSON strings
                        for value in product_data.values():
                            if isinstance(value, dict):
                                value = json.dumps(value)
                            values.append(value)
                        
                        placeholders = ['%s'] * len(fields)
                        
                        insert_query = f"""
                            INSERT INTO products 
                            ({', '.join(fields)}, created_at, updated_at)
                            VALUES ({', '.join(placeholders)}, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                        """
                        
                        await cursor.execute(insert_query, tuple(values))
                        await conn.commit()
                        logger.info(f"Successfully inserted new product {product_data['source_id']}")
                        
        except Exception as e:
            logger.error(f"Error saving product to database: {e}")
            raise

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Scrape MedicalExpo products')
    parser.add_argument('--category', help='Product category to scrape')
    parser.add_argument('--pages', type=int, default=5, help='Maximum number of pages to scrape')
    
    args = parser.parse_args()
    
    scraper = MedicalExpoScraper()
    asyncio.run(scraper.scrape(category=args.category, max_pages=args.pages)) 
