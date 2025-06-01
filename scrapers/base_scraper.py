import os
import json
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, List, Optional, Any
from playwright.async_api import async_playwright, Browser, Page
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
import random

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class BaseScraper(ABC):
    def __init__(self, source_name: str):
        load_dotenv()
        self.source_name = source_name
        self.db_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'user': os.getenv('DB_USER', 'root'),
            'password': os.getenv('DB_PASSWORD', ''),
            'database': os.getenv('DB_NAME', 'med_equipment_db')
        }
        self.browser = None
        self.page = None

    async def init_browser(self):
        """Initialize Playwright browser"""
        playwright = await async_playwright().start()
        self.browser = await playwright.chromium.launch(
            headless=True,
            args=['--no-sandbox', '--disable-setuid-sandbox']
        )
        self.page = await self.browser.new_page()
        
        # Set default timeout
        self.page.set_default_timeout(30000)
        
        # Add common headers
        await self.page.set_extra_http_headers({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })

    async def close_browser(self):
        """Close browser and cleanup"""
        if self.browser:
            await self.browser.close()

    def get_db_connection(self):
        """Create database connection"""
        try:
            connection = mysql.connector.connect(**self.db_config)
            return connection
        except Error as e:
            logger.error(f"Error connecting to MySQL: {e}")
            raise

    async def save_product(self, product_data: Dict[str, Any]) -> int:
        """Save product data to database"""
        connection = self.get_db_connection()
        cursor = connection.cursor()
        
        try:
            # Insert product
            product_query = """
                INSERT INTO products (source, source_id, name, brand, category, description, specifications)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                name = VALUES(name),
                brand = VALUES(brand),
                category = VALUES(category),
                description = VALUES(description),
                specifications = VALUES(specifications)
            """
            
            cursor.execute(product_query, (
                self.source_name,
                product_data['source_id'],
                product_data['name'],
                product_data.get('brand'),
                product_data.get('category'),
                product_data.get('description'),
                json.dumps(product_data.get('specifications', {}))
            ))
            
            product_id = cursor.lastrowid or cursor.fetchone()[0]
            
            # Save images
            if 'images' in product_data:
                for image in product_data['images']:
                    image_query = """
                        INSERT INTO images (product_id, url, is_primary)
                        VALUES (%s, %s, %s)
                    """
                    cursor.execute(image_query, (
                        product_id,
                        image['url'],
                        image.get('is_primary', False)
                    ))
            
            # Save pricing
            if 'pricing' in product_data:
                pricing_query = """
                    INSERT INTO pricing (product_id, currency, min_price, max_price, unit, min_order_quantity)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """
                cursor.execute(pricing_query, (
                    product_id,
                    product_data['pricing'].get('currency', 'USD'),
                    product_data['pricing'].get('min_price'),
                    product_data['pricing'].get('max_price'),
                    product_data['pricing'].get('unit'),
                    product_data['pricing'].get('min_order_quantity')
                ))
            
            # Save seller info
            if 'seller' in product_data:
                seller_query = """
                    INSERT INTO sellers (product_id, name, rating, location, website)
                    VALUES (%s, %s, %s, %s, %s)
                """
                cursor.execute(seller_query, (
                    product_id,
                    product_data['seller'].get('name'),
                    product_data['seller'].get('rating'),
                    product_data['seller'].get('location'),
                    product_data['seller'].get('website')
                ))
            
            connection.commit()
            return product_id
            
        except Error as e:
            connection.rollback()
            logger.error(f"Error saving product data: {e}")
            raise
        finally:
            cursor.close()
            connection.close()

    @abstractmethod
    async def scrape(self, **kwargs):
        """Main scraping method to be implemented by child classes"""
        pass

    async def handle_anti_bot(self):
        """Handle common anti-bot measures"""
        # Add random delay
        await self.page.wait_for_timeout(2000 + (1000 * random.random()))
        
        # Check for common anti-bot elements
        if await self.page.query_selector('input[name="captcha"]'):
            logger.warning("Captcha detected! Manual intervention may be required.")
            # Implement captcha handling logic here

    async def retry_on_failure(self, func, max_retries=3, delay=2000):
        """Retry function on failure with exponential backoff"""
        for attempt in range(max_retries):
            try:
                return await func()
            except Exception as e:
                if attempt == max_retries - 1:
                    raise
                logger.warning(f"Attempt {attempt + 1} failed: {e}")
                await self.page.wait_for_timeout(delay * (2 ** attempt)) 