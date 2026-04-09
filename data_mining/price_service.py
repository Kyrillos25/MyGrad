"""
Price Service Module
Handles fetching product prices from SQL Server Cleaned_data table.
"""

import pyodbc
from config import Config

class PriceService:
    def __init__(self, sql_server_config=None):
        """Initialize price service with SQL Server configuration."""
        self.sql_server_config = sql_server_config or Config.SQL_SERVER_CONFIG
        self.conn = None
        self.connected = False
    
    def connect_to_sql_server(self):
        """Establish connection to SQL Server using SQL Server Authentication."""
        try:
            # Use SQL Server Authentication
            conn_str = (
                    "DRIVER={ODBC Driver 17 for SQL Server};"
                    "SERVER=localhost;"
                    "DATABASE=TestDB;"
                    "Trusted_Connection=yes;"
                )
            
            
            self.conn = pyodbc.connect(conn_str)
            self.connected = True
            return True

        except Exception as e:
            print(f"SQL Server connection failed: {e}")
            self.connected = False
            return False
    
    def get_product_price(self, product_name):
        """Get the average UnitPrice for a product from Cleaned_data table."""
        if not self.connected:
            if not self.connect_to_sql_server():
                return None
        
        try:
            cursor = self.conn.cursor()
            
            # Get average UnitPrice for the product
            cursor.execute("""
                SELECT AVG(CAST(UnitPrice AS FLOAT)) as avg_price
                FROM Cleaned_data 
                WHERE Description = ? AND UnitPrice > 0
            """, (product_name,))
            
            result = cursor.fetchone()
            if result and result[0]:
                return float(result[0])
            else:
                return None
                
        except Exception as e:
            print(f"Error fetching price for {product_name}: {e}")
            return None
    
    def get_product_prices_batch(self, product_names):
        """Get prices for multiple products in a single query."""
        if not self.connected:
            if not self.connect_to_sql_server():
                return {}
        
        if not product_names:
            return {}
        
        try:
            cursor = self.conn.cursor()
            
            # Create parameter placeholders for IN clause
            placeholders = ','.join(['?' for _ in product_names])
            
            # Get average UnitPrice for all products
            cursor.execute(f"""
                SELECT Description, AVG(CAST(UnitPrice AS FLOAT)) as avg_price
                FROM Cleaned_data 
                WHERE Description IN ({placeholders}) AND UnitPrice > 0
                GROUP BY Description
            """, product_names)
            
            results = cursor.fetchall()
            prices = {}
            for row in results:
                prices[row[0]] = float(row[1])
            
            return prices
                
        except Exception as e:
            print(f"Error fetching batch prices: {e}")
            return {}
    
    def get_all_product_prices(self):
        """Get prices for all products in Cleaned_data table."""
        if not self.connected:
            if not self.connect_to_sql_server():
                return {}
        
        try:
            cursor = self.conn.cursor()
            
            # Get average UnitPrice for all products
            cursor.execute("""
                SELECT Description, AVG(CAST(UnitPrice AS FLOAT)) as avg_price
                FROM Cleaned_data 
                WHERE UnitPrice > 0
                GROUP BY Description
            """)
            
            results = cursor.fetchall()
            prices = {}
            for row in results:
                prices[row[0]] = float(row[1])
            
            return prices
                
        except Exception as e:
            print(f"Error fetching all prices: {e}")
            return {}
    
    def is_connected(self):
        """Check if connected to SQL Server."""
        return self.connected
    
    def __del__(self):
        """Clean up connection."""
        if self.conn:
            self.conn.close()