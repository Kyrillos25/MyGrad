"""
Data Pipeline Module
Handles data import, cleaning, and preprocessing for market basket analysis.
Refactored from your existing partitional_load() and Cleaning() functions.
Supports both SQL Server and SQLite databases.
"""

import os
import ast
import pandas as pd
import pyodbc
from pyspark.sql import SparkSession
from collections import defaultdict
from datetime import datetime
import json
from .market_basket_preparation import MarketBasketDataPreparator

class DataPipeline:
    def __init__(self, sql_server_config=None, sqlite_db_path=None):
        """Initialize data pipeline with SQL Server or SQLite configuration."""
        self.sql_server_config = sql_server_config or {
            'driver': '{ODBC Driver 17 for SQL Server}',
            'server': 'localhost',
            'database': 'TestDB',
            'trusted_connection': 'yes'
        }
        self.sqlite_db_path = sqlite_db_path
        self.conn = None
        self.spark = None
        self.use_sqlite = False
        
    def connect_to_sql_server(self):
        """Establish connection to SQL Server."""
        try:
            conn_str = (
                f"DRIVER={self.sql_server_config['driver']};"
                f"SERVER={self.sql_server_config['server']};"
                f"DATABASE={self.sql_server_config['database']};"
                f"{self.sql_server_config['trusted_connection']}"
            )
            self.conn = pyodbc.connect(conn_str)
            print("SQL Server connection successful")
            self.use_sqlite = False
            return True
        except Exception as e:
            print(f"SQL Server connection failed: {e}")
            return False
    
    def connect_to_sqlite(self, db_path=None):
        """Establish connection to SQLite database."""
        import sqlite3
        if db_path:
            self.sqlite_db_path = db_path
        if not self.sqlite_db_path:
            self.sqlite_db_path = 'app.db'
        try:
            self.conn = sqlite3.connect(self.sqlite_db_path)
            print(f"SQLite connection successful to {self.sqlite_db_path}")
            self.use_sqlite = True
            return True
        except Exception as e:
            print(f"SQLite connection failed: {e}")
            return False
    
    def load_csv_with_spark(self, csv_path, num_partitions=4):
        """Load CSV file using PySpark with specified partitions."""
        try:
            self.spark = SparkSession.builder \
                .appName("DataPipeline") \
                .master("local[*]") \
                .getOrCreate()
            
            # Handle BOM (Byte Order Mark) in CSV files
            df = self.spark.read.csv(csv_path, header=True, inferSchema=True)
            
            # Clean column names (remove BOM from first column name)
            columns = df.columns
            if columns and columns[0].startswith('\ufeff'):
                df = df.withColumnRenamed(columns[0], columns[0][1:])  # Remove BOM
            
            # Print schema for debugging
            print(f"CSV Schema:")
            df.printSchema()
            
            df = df.repartition(num_partitions)
            
            print(f"Loaded CSV with {df.count()} rows and {len(df.columns)} columns")
            print(f"Column names: {df.columns}")
            print(f"Number of partitions: {df.rdd.getNumPartitions()}")
            
            return df
        except Exception as e:
            print(f"Error loading CSV: {e}")
            return None
    
    def load_csv_with_pandas(self, csv_path):
        """Load CSV file using pandas (fallback method)."""
        try:
            # Try reading with utf-8-sig to handle BOM
            df = pd.read_csv(csv_path, encoding='utf-8-sig')
            print(f"Loaded CSV with pandas: {len(df)} rows, {len(df.columns)} columns")
            print(f"Column names: {list(df.columns)}")
            print(f"Sample data:\n{df.head()}")
            return df
        except Exception as e:
            print(f"Error loading CSV with pandas: {e}")
            # Try with different encoding
            try:
                df = pd.read_csv(csv_path, encoding='latin-1')
                print(f"Loaded CSV with pandas (latin-1 encoding): {len(df)} rows")
                return df
            except Exception as e2:
                print(f"Error loading CSV with latin-1: {e2}")
                return None
    
    def insert_dataframe_to_sql(self, df, table_name, columns):
        """Insert Spark DataFrame into SQL Server table."""
        if not self.conn:
            if not self.connect_to_sql_server():
                return 0
        
        try:
            cursor = self.conn.cursor()
            
            # Truncate table first
            cursor.execute(f"TRUNCATE TABLE {table_name}")
            self.conn.commit()
            
            # Insert data
            rows_inserted = 0
            for row in df.collect():
                try:
                    values = [getattr(row, col, None) for col in columns]
                    placeholders = ','.join(['?'] * len(columns))
                    cursor.execute(
                        f"INSERT INTO {table_name} ({','.join(columns)}) VALUES ({placeholders})",
                        values
                    )
                    rows_inserted += 1
                except Exception as e:
                    print(f"Error inserting row: {e}")
                    continue
            
            self.conn.commit()
            print(f"Inserted {rows_inserted} rows into {table_name}")
            return rows_inserted
            
        except Exception as e:
            print(f"Error in batch insert: {e}")
            return 0
    
    def insert_dataframe_to_sqlite(self, df, table_name, columns):
        """Insert pandas DataFrame into SQLite table."""
        import sqlite3
        if not self.conn:
            if not self.connect_to_sqlite():
                return 0
        
        try:
            cursor = self.conn.cursor()
            
            # Clear table first
            cursor.execute(f"DELETE FROM {table_name}")
            self.conn.commit()
            
            # Insert data
            rows_inserted = 0
            for _, row in df.iterrows():
                try:
                    values = [row.get(col, None) for col in columns]
                    placeholders = ','.join(['?'] * len(columns))
                    cursor.execute(
                        f"INSERT INTO {table_name} ({','.join(columns)}) VALUES ({placeholders})",
                        values
                    )
                    rows_inserted += 1
                except Exception as e:
                    print(f"Error inserting row: {e}")
                    continue
            
            self.conn.commit()
            print(f"Inserted {rows_inserted} rows into {table_name}")
            return rows_inserted
            
        except Exception as e:
            print(f"Error in batch insert: {e}")
            return 0
    
    def clean_data_sql_server(self):
        """Clean transaction data using SQL Server."""
        if not self.conn:
            self.connect_to_sql_server()
        
        try:
            cursor = self.conn.cursor()
            
            # Clean data
            cursor.execute("""
                truncate table Cleaned_data;
                insert into Cleaned_data select * from Row_data;
                delete from Cleaned_data where InvoiceNo like '%C%';
                delete from Cleaned_data where CustomerID is null;
                delete from Cleaned_data where Quantity like '%-%';
                delete from Cleaned_data where UnitPrice = '0' or UnitPrice like '%-%';
                delete from Cleaned_data where Description is null or Description like '%?%';
                delete from Cleaned_data where StockCode = 'POST';
            """)
            
            # Clear previous results
            cursor.execute("TRUNCATE TABLE tableed")
            self.conn.commit()
            
            # Aggregate into baskets
            cursor.execute("""
                insert into tableed
                select InvoiceNo, STRING_AGG(CAST(description AS VARCHAR(MAX)), ', ') AS products
                FROM Cleaned_data
                GROUP BY InvoiceNo;
            """)
            
            self.conn.commit()
            print("Data cleaning completed successfully")
            return True
            
        except Exception as e:
            print(f"Error cleaning data: {e}")
            return False
    
    def clean_data_sqlite(self):
        """Clean transaction data using SQLite."""
        if not self.conn:
            self.connect_to_sqlite()
        
        try:
            cursor = self.conn.cursor()
            
            # Clean data - SQLite version
            cursor.execute("""
                DELETE FROM Cleaned_data;
                INSERT INTO Cleaned_data 
                SELECT * FROM Row_data 
                WHERE InvoiceNo NOT LIKE '%C%'
                AND CustomerID IS NOT NULL
                AND Quantity NOT LIKE '%-%'
                AND UnitPrice != '0'
                AND UnitPrice NOT LIKE '%-%'
                AND Description IS NOT NULL
                AND Description NOT LIKE '%?%'
                AND StockCode != 'POST';
            """)
            
            # Clear previous results
            cursor.execute("DELETE FROM tableed")
            self.conn.commit()
            
            # Aggregate into baskets using GROUP_CONCAT
            cursor.execute("""
                INSERT INTO tableed (InvoiceNo, products)
                SELECT InvoiceNo, GROUP_CONCAT(description, ', ') AS products
                FROM Cleaned_data
                GROUP BY InvoiceNo;
            """)
            
            self.conn.commit()
            print("Data cleaning completed successfully (SQLite)")
            return True
            
        except Exception as e:
            print(f"Error cleaning data: {e}")
            return False
    
    def clean_data(self):
        """Clean transaction data - auto-detects database type."""
        if self.use_sqlite:
            return self.clean_data_sqlite()
        else:
            return self.clean_data_sql_server()
    
    def load_transactions_from_db(self):
        """Load transactions from database for mining."""
        if not self.conn:
            if self.use_sqlite:
                self.connect_to_sqlite()
            else:
                self.connect_to_sql_server()
        
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT products FROM tableed ORDER BY InvoiceNo")
            rows = cursor.fetchall()
            
            transactions = []
            for row in rows:
                try:
                    products_str = row[0] if isinstance(row, tuple) else row
                    if products_str:
                        # Split by comma and strip whitespace
                        items = [item.strip() for item in products_str.split(',')]
                        items = list(set(items))  # Remove duplicates
                        if items:
                            transactions.append(items)
                except Exception as e:
                    print(f"Error parsing row: {e}")
                    continue
            
            print(f"Loaded {len(transactions)} transactions from database")
            return transactions
            
        except Exception as e:
            print(f"Error loading transactions: {e}")
            return []
    
    def process_csv_file(self, csv_path, sql_columns=None, use_sqlite=True):
        """Complete pipeline: Load CSV -> Insert to DB -> Clean -> Prepare for mining.
        
        Args:
            csv_path: Path to the CSV file
            sql_columns: List of column names to insert
            use_sqlite: If True, use SQLite; if False, use SQL Server
        """
        if sql_columns is None:
            sql_columns = [
                'InvoiceNo', 'StockCode', 'Description', 'Quantity', 
                'InvoiceDate', 'UnitPrice', 'CustomerID', 'Country'
            ]
        
        print(f"Processing CSV file: {csv_path}")
        
        # Step 1: Try loading with Spark first, fallback to pandas
        df_spark = self.load_csv_with_spark(csv_path)
        df_pandas = None
        
        if df_spark is None:
            print("Spark loading failed, trying pandas...")
            df_pandas = self.load_csv_with_pandas(csv_path)
            if df_pandas is None:
                return False
        
        # Step 2: Choose database and insert data
        if use_sqlite:
            self.connect_to_sqlite()
            if df_pandas is None and df_spark is not None:
                # Convert Spark DataFrame to pandas
                df_pandas = df_spark.toPandas()
            
            if self.insert_dataframe_to_sqlite(df_pandas, 'Row_data', sql_columns):
                if self.clean_data_sqlite():
                    print("CSV processing completed successfully (SQLite)")
                    return True
        else:
            if df_spark is not None:
                if self.insert_dataframe_to_sql(df_spark, 'Row_data', sql_columns):
                    if self.clean_data_sql_server():
                        print("CSV processing completed successfully (SQL Server)")
                        return True
        
        return False
    
    def get_transaction_stats(self):
        """Get statistics about transactions in database."""
        if not self.conn:
            if self.use_sqlite:
                self.connect_to_sqlite()
            else:
                self.connect_to_sql_server()
        
        try:
            cursor = self.conn.cursor()
            
            # Get transaction count
            cursor.execute("SELECT COUNT(*) FROM tableed")
            transaction_count = cursor.fetchone()[0]
            
            if self.use_sqlite:
                # SQLite version
                cursor.execute("SELECT COUNT(*) FROM tableed")
                transaction_count = cursor.fetchone()[0]
                
                # Get unique products
                cursor.execute("""
                    SELECT COUNT(DISTINCT TRIM(value)) 
                    FROM tableed, 
                    LATERAL (SELECT value FROM string_split(products, ',')) 
                """)
                # SQLite doesn't have STRING_SPLIT, use a different approach
                cursor.execute("SELECT products FROM tableed")
                all_products = set()
                for row in cursor.fetchall():
                    if row[0]:
                        products = [p.strip() for p in row[0].split(',')]
                        all_products.update(products)
                product_count = len(all_products)
                
                # Get average basket size
                cursor.execute("SELECT products FROM tableed")
                basket_sizes = []
                for row in cursor.fetchall():
                    if row[0]:
                        basket_sizes.append(len(row[0].split(',')))
                avg_basket_size = sum(basket_sizes) / len(basket_sizes) if basket_sizes else 0
            else:
                # SQL Server version
                cursor.execute("""
                    SELECT COUNT(DISTINCT LTRIM(RTRIM(value))) 
                    FROM tableed 
                    CROSS APPLY STRING_SPLIT(products, ',')
                """)
                product_count = cursor.fetchone()[0]
                
                # Get average basket size
                cursor.execute("""
                    SELECT AVG(LEN(products) - LEN(REPLACE(products, ',', '')) + 1) 
                    FROM tableed
                """)
                avg_basket_size = cursor.fetchone()[0]
            
            return {
                'transaction_count': transaction_count,
                'unique_products': product_count,
                'avg_basket_size': round(avg_basket_size, 2) if avg_basket_size else 0
            }
            
        except Exception as e:
            print(f"Error getting stats: {e}")
            return {}
    
    def process_csv_direct(self, csv_path, min_quantity=1):
        """Process CSV file directly without database - returns transactions list.
        
        This is a simpler method that doesn't require SQL Server or SQLite.
        """
        try:
            # Load with pandas
            df = pd.read_csv(csv_path, encoding='utf-8-sig')
            print(f"Loaded {len(df)} rows from CSV")
            
            # Clean data
            # Remove cancelled orders (InvoiceNo contains 'C')
            df = df[~df['InvoiceNo'].astype(str).str.contains('C', na=False)]
            
            # Remove rows with no CustomerID
            df = df.dropna(subset=['CustomerID'])
            
            # Remove negative quantities
            df = df[df['Quantity'] > 0]
            
            # Remove zero or negative prices
            df = df[df['UnitPrice'] > 0]
            
            # Remove rows with no description
            df = df.dropna(subset=['Description'])
            df = df[df['Description'].astype(str).str.strip() != '']
            
            # Remove POST stock code
            df = df[df['StockCode'] != 'POST']
            
            print(f"After cleaning: {len(df)} rows")
            
            # Group by InvoiceNo and create baskets
            transactions = []
            for invoice, group in df.groupby('InvoiceNo'):
                items = group['Description'].str.strip().tolist()
                items = list(set(items))  # Remove duplicates
                if items:
                    transactions.append(items)
            
            print(f"Created {len(transactions)} transaction baskets")
            return transactions
            
        except Exception as e:
            print(f"Error processing CSV directly: {e}")
            return []
    
    def get_transactions_from_sql_server(self):
        """Get transactions directly from SQL Server Cleaned_data table using MarketBasketDataPreparator.
        
        This method uses the new MarketBasketDataPreparator class to fetch and prepare
        transactions from the Cleaned_data table in SQL Server.
        """
        try:
            # Create a MarketBasketDataPreparator instance
            preparator = MarketBasketDataPreparator(self.sql_server_config)
            
            # Get transactions from SQL Server
            df_transactions = preparator.get_transactions_from_sql()
            
            if df_transactions.empty:
                print("No transactions found in SQL Server")
                return []
            
            # Convert to list of lists format for Apriori/FP-Growth
            transactions = preparator.prepare_for_apriori(df_transactions)
            
            # Get statistics
            stats = preparator.get_transaction_statistics(df_transactions)
            print(f"Loaded {stats['total_transactions']} transactions from SQL Server")
            print(f"Average items per transaction: {stats['avg_items_per_transaction']:.1f}")
            
            preparator.close_connection()
            return transactions
            
        except Exception as e:
            print(f"Error getting transactions from SQL Server: {e}")
            return []
    
    def get_transactions_from_pandas(self, df_raw=None):
        """Get transactions using pandas operations with MarketBasketDataPreparator.
        
        This method uses the MarketBasketDataPreparator class to process
        data using pandas operations.
        """
        try:
            # Create a MarketBasketDataPreparator instance
            preparator = MarketBasketDataPreparator(self.sql_server_config)
            
            # Get transactions using pandas
            df_transactions = preparator.get_transactions_from_pandas(df_raw)
            
            if df_transactions.empty:
                print("No transactions found")
                return []
            
            # Convert to list of lists format for Apriori/FP-Growth
            transactions = preparator.prepare_for_apriori(df_transactions)
            
            # Get statistics
            stats = preparator.get_transaction_statistics(df_transactions)
            print(f"Processed {stats['total_transactions']} transactions")
            print(f"Average items per transaction: {stats['avg_items_per_transaction']:.1f}")
            
            preparator.close_connection()
            return transactions
            
        except Exception as e:
            print(f"Error getting transactions with pandas: {e}")
            return []
    
    def __del__(self):
        """Clean up connections."""
        if self.conn:
            self.conn.close()
        if self.spark:
            self.spark.stop()