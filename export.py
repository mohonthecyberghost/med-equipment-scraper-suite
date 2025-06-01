import os
import json
import logging
import argparse
from typing import List, Dict, Any
import pandas as pd
from datetime import datetime
from mysql.connector import Error
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataExporter:
    def __init__(self):
        load_dotenv()
        self.db_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'user': os.getenv('DB_USER', 'root'),
            'password': os.getenv('DB_PASSWORD', ''),
            'database': os.getenv('DB_NAME', 'med_equipment_db')
        }
        self.export_dir = 'exports'
        os.makedirs(self.export_dir, exist_ok=True)
    
    def get_db_connection(self):
        """Create database connection"""
        try:
            connection = mysql.connector.connect(**self.db_config)
            return connection
        except Error as e:
            logger.error(f"Error connecting to MySQL: {e}")
            raise
    
    def fetch_data(self, source: str = None) -> List[Dict[str, Any]]:
        """Fetch data from database"""
        connection = self.get_db_connection()
        cursor = connection.cursor(dictionary=True)
        
        try:
            # Base query
            query = """
                SELECT 
                    p.*,
                    GROUP_CONCAT(DISTINCT i.url) as image_urls,
                    GROUP_CONCAT(DISTINCT d.url) as document_urls,
                    pr.min_price,
                    pr.max_price,
                    pr.unit,
                    pr.min_order_quantity,
                    s.name as seller_name,
                    s.rating as seller_rating,
                    s.location as seller_location,
                    s.website as seller_website
                FROM products p
                LEFT JOIN images i ON p.id = i.product_id
                LEFT JOIN documents d ON p.id = d.product_id
                LEFT JOIN pricing pr ON p.id = pr.product_id
                LEFT JOIN sellers s ON p.id = s.product_id
            """
            
            # Add source filter if specified
            if source and source.lower() != 'all':
                query += " WHERE p.source = %s"
                cursor.execute(query, (source,))
            else:
                cursor.execute(query)
            
            # Group results by product
            results = cursor.fetchall()
            
            # Process results
            products = []
            for row in results:
                product = {
                    'id': row['id'],
                    'source': row['source'],
                    'source_id': row['source_id'],
                    'name': row['name'],
                    'brand': row['brand'],
                    'category': row['category'],
                    'description': row['description'],
                    'specifications': json.loads(row['specifications']) if row['specifications'] else {},
                    'images': row['image_urls'].split(',') if row['image_urls'] else [],
                    'documents': row['document_urls'].split(',') if row['document_urls'] else [],
                    'pricing': {
                        'min_price': row['min_price'],
                        'max_price': row['max_price'],
                        'unit': row['unit'],
                        'min_order_quantity': row['min_order_quantity']
                    },
                    'seller': {
                        'name': row['seller_name'],
                        'rating': row['seller_rating'],
                        'location': row['seller_location'],
                        'website': row['seller_website']
                    },
                    'created_at': row['created_at'].isoformat() if row['created_at'] else None,
                    'updated_at': row['updated_at'].isoformat() if row['updated_at'] else None
                }
                products.append(product)
            
            return products
            
        except Error as e:
            logger.error(f"Error fetching data: {e}")
            raise
        finally:
            cursor.close()
            connection.close()
    
    def export_json(self, data: List[Dict[str, Any]], source: str = None):
        """Export data to JSON file"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{self.export_dir}/products_{source or 'all'}_{timestamp}.json"
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            logger.info(f"Successfully exported data to {filename}")
        except Exception as e:
            logger.error(f"Error exporting to JSON: {e}")
            raise
    
    def export_csv(self, data: List[Dict[str, Any]], source: str = None):
        """Export data to CSV file"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{self.export_dir}/products_{source or 'all'}_{timestamp}.csv"
        
        try:
            # Flatten nested structures for CSV
            flattened_data = []
            for product in data:
                flat_product = {
                    'id': product['id'],
                    'source': product['source'],
                    'source_id': product['source_id'],
                    'name': product['name'],
                    'brand': product['brand'],
                    'category': product['category'],
                    'description': product['description'],
                    'specifications': json.dumps(product['specifications']),
                    'images': '|'.join(product['images']),
                    'documents': '|'.join(product['documents']),
                    'min_price': product['pricing']['min_price'],
                    'max_price': product['pricing']['max_price'],
                    'unit': product['pricing']['unit'],
                    'min_order_quantity': product['pricing']['min_order_quantity'],
                    'seller_name': product['seller']['name'],
                    'seller_rating': product['seller']['rating'],
                    'seller_location': product['seller']['location'],
                    'seller_website': product['seller']['website'],
                    'created_at': product['created_at'],
                    'updated_at': product['updated_at']
                }
                flattened_data.append(flat_product)
            
            # Convert to DataFrame and export
            df = pd.DataFrame(flattened_data)
            df.to_csv(filename, index=False, encoding='utf-8')
            logger.info(f"Successfully exported data to {filename}")
        except Exception as e:
            logger.error(f"Error exporting to CSV: {e}")
            raise

def main():
    parser = argparse.ArgumentParser(description='Export scraped product data')
    parser.add_argument('--format', choices=['json', 'csv'], required=True, help='Export format')
    parser.add_argument('--source', default='all', help='Data source to export (default: all)')
    
    args = parser.parse_args()
    
    exporter = DataExporter()
    data = exporter.fetch_data(args.source)
    
    if args.format == 'json':
        exporter.export_json(data, args.source)
    else:
        exporter.export_csv(data, args.source)

if __name__ == '__main__':
    main() 