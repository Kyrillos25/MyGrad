"""
Distributed Data Pipeline for Market Basket Analysis
Handles CSV upload, data cleaning, and distributed processing using PySpark
"""

import os
import pandas as pd
import pyodbc
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from config import Config
import logging
from datetime import datetime
import json

class DistributedDataPipeline:
    """Complete distributed data pipeline for market basket analysis"""
    
    def __init__(self, sql_server_config=None, spark_config=None):
        """Initialize pipeline with SQL Server and Spark configurations"""
        self.sql_server_config = sql_server_config or Config.SQL_SERVER_CONFIG
        self.spark_config = spark_config or {
            'app_name': 'MarketBasketAnalysis',
            'master': 'local[*]',
            'memory': '4g',
            'cores': '4'
        }
        self.conn = None
        self.spark = None
        self.logger = self._setup_logging()
        
    def _setup_logging(self):
        """Setup logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        return logging.getLogger(__name__)
    
    def connect_to_sql_server(self):
        """Establish connection to SQL Server"""
        try:
            conn_str = (
                f"DRIVER={{{self.sql_server_config['driver']}}};"
                f"SERVER={self.sql_server_config['server']};"
                f"DATABASE={self.sql_server_config['database']};"
                f"UID={self.sql_server_config.get('username', 'sa')};"
                f"PWD={self.sql_server_config.get('password', '')};"
            )
            
            self.conn = pyodbc.connect(conn_str)
            self.logger.info("SQL Server connection successful")
            return True
            
        except Exception as e:
            self.logger.error(f"SQL Server connection failed: {e}")
            return False
    
    def initialize_spark(self):
        """Initialize PySpark session with optimized configuration"""
        try:
            self.spark = SparkSession.builder \
                .appName(self.spark_config['app_name']) \
                .master(self.spark_config['master']) \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.sql.adaptive.skewJoin.enabled", "true") \
                .config("spark.executor.memory", self.spark_config['memory']) \
                .config("spark.default.parallelism", self.spark_config['cores']) \
                .config("spark.sql.shuffle.partitions", "200") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .getOrCreate()
            
            self.logger.info("PySpark session initialized successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"PySpark initialization failed: {e}")
            return False
    
    def truncate_tables(self):
        """Truncate both Row_data and Cleaned_data tables"""
        if not self.conn:
            if not self.connect_to_sql_server():
                return False
        
        try:
            cursor = self.conn.cursor()
            
            # Truncate tables
            cursor.execute("TRUNCATE TABLE Cleaned_data")
            cursor.execute("TRUNCATE TABLE Row_data")
            
            self.conn.commit()
            self.logger.info("Tables truncated successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Error truncating tables: {e}")
            return False
    
    def load_csv_to_spark(self, csv_path, num_partitions=8):
        """Load CSV file into PySpark DataFrame with optimized partitioning"""
        try:
            # Handle BOM and encoding issues
            df = self.spark.read.csv(
                csv_path, 
                header=True, 
                inferSchema=True,
                encoding='utf-8-sig'
            )
            
            # Clean column names (remove BOM)
            columns = df.columns
            for col in columns:
                if col.startswith('\ufeff'):
                    df = df.withColumnRenamed(col, col[1:])
            
            # Repartition for better parallel processing
            df = df.repartition(num_partitions)
            
            self.logger.info(f"Loaded CSV with {df.count()} rows and {len(df.columns)} columns")
            self.logger.info(f"Number of partitions: {df.rdd.getNumPartitions()}")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error loading CSV to Spark: {e}")
            return None
    
    def insert_to_sql_server(self, df, table_name):
        """Insert Spark DataFrame to SQL Server using JDBC"""
        try:
            jdbc_url = f"jdbc:sqlserver://{self.sql_server_config['server']};databaseName={self.sql_server_config['database']}"
            properties = {
                "user": self.sql_server_config.get('username', 'sa'),
                "password": self.sql_server_config.get('password', ''),
                "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
                "batchsize": "1000",
                "truncate": "true"
            }
            
            # Write to SQL Server
            df.write \
                .mode("overwrite") \
                .jdbc(url=jdbc_url, table=table_name, properties=properties)
            
            self.logger.info(f"Successfully inserted data into {table_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error inserting to SQL Server: {e}")
            return False
    
    def apply_data_cleaning(self):
        """Apply data cleaning rules to Cleaned_data table"""
        if not self.conn:
            if not self.connect_to_sql_server():
                return False
        
        try:
            cursor = self.conn.cursor()
            
            # Apply cleaning rules
            cleaning_rules = [
                "DELETE FROM Cleaned_data WHERE InvoiceNo LIKE '%C%'",
                "DELETE FROM Cleaned_data WHERE CustomerID IS NULL",
                "DELETE FROM Cleaned_data WHERE Quantity LIKE '%-%'",
                "DELETE FROM Cleaned_data WHERE UnitPrice = 0 OR UnitPrice LIKE '%-%'",
                "DELETE FROM Cleaned_data WHERE Description IS NULL OR Description LIKE '%?%'",
                "DELETE FROM Cleaned_data WHERE StockCode = 'POST'"
            ]
            
            for rule in cleaning_rules:
                cursor.execute(rule)
                self.conn.commit()
            
            self.logger.info("Data cleaning completed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Error applying data cleaning: {e}")
            return False
    
    def get_cleaned_data_from_spark(self):
        """Load cleaned data from SQL Server into Spark DataFrame"""
        try:
            jdbc_url = f"jdbc:sqlserver://{self.sql_server_config['server']};databaseName={self.sql_server_config['database']}"
            properties = {
                "user": self.sql_server_config.get('username', 'sa'),
                "password": self.sql_server_config.get('password', ''),
                "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
            }
            
            # Read from SQL Server
            df = self.spark.read.jdbc(
                url=jdbc_url, 
                table="Cleaned_data", 
                properties=properties
            )
            
            # Cache the DataFrame for better performance
            df.cache()
            
            self.logger.info(f"Loaded {df.count()} cleaned records from SQL Server")
            return df
            
        except Exception as e:
            self.logger.error(f"Error loading cleaned data: {e}")
            return None
    
    def prepare_transactions_distributed(self, df):
        """Prepare transactional data using distributed processing"""
        try:
            # Remove duplicates within same transaction
            df_unique = df.dropDuplicates(['InvoiceNo', 'Description'])
            
            # Filter out invalid descriptions
            df_clean = df_unique.filter(
                (col("Description").isNotNull()) &
                (trim(col("Description")) != "") &
                (trim(col("Description")) != "?")
            )
            
            # Group by InvoiceNo and aggregate items
            transactions_df = df_clean.groupBy("InvoiceNo") \
                .agg(
                    collect_list("Description").alias("items"),
                    count("Description").alias("item_count")
                ) \
                .filter(col("item_count") > 0)
            
            # Cache the result
            transactions_df.cache()
            
            self.logger.info(f"Prepared {transactions_df.count()} transactions")
            return transactions_df
            
        except Exception as e:
            self.logger.error(f"Error preparing transactions: {e}")
            return None
    
    def save_transactions_to_sql(self, transactions_df):
        """Save processed transactions to SQL Server"""
        try:
            # Convert to format suitable for SQL Server
            result_df = transactions_df.select(
                col("InvoiceNo"),
                concat_ws(", ", col("items")).alias("products"),
                col("item_count")
            )
            
            jdbc_url = f"jdbc:sqlserver://{self.sql_server_config['server']};databaseName={self.sql_server_config['database']}"
            properties = {
                "user": self.sql_server_config.get('username', 'sa'),
                "password": self.sql_server_config.get('password', ''),
                "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
            }
            
            # Write to SQL Server (create tableed table if it doesn't exist)
            result_df.write \
                .mode("overwrite") \
                .jdbc(url=jdbc_url, table="tableed", properties=properties)
            
            self.logger.info("Transactions saved to tableed table")
            return True
            
        except Exception as e:
            self.logger.error(f"Error saving transactions: {e}")
            return False
    
    def get_transaction_stats(self, transactions_df):
        """Get comprehensive statistics about transactions"""
        try:
            stats = {}
            
            # Basic statistics
            total_transactions = transactions_df.count()
            avg_items_per_transaction = transactions_df.agg(avg("item_count")).collect()[0][0]
            min_items = transactions_df.agg(min("item_count")).collect()[0][0]
            max_items = transactions_df.agg(max("item_count")).collect()[0][0]
            
            # Get unique products
            unique_products_df = transactions_df.select(explode("items").alias("product")) \
                .distinct()
            unique_products = unique_products_df.count()
            
            # Calculate distribution
            distribution = transactions_df.groupBy("item_count") \
                .count() \
                .orderBy("item_count") \
                .collect()
            
            stats = {
                'total_transactions': total_transactions,
                'unique_products': unique_products,
                'avg_items_per_transaction': round(avg_items_per_transaction, 2),
                'min_items_per_transaction': min_items,
                'max_items_per_transaction': max_items,
                'transaction_distribution': {str(row['item_count']): row['count'] for row in distribution}
            }
            
            self.logger.info(f"Transaction statistics: {stats}")
            return stats
            
        except Exception as e:
            self.logger.error(f"Error calculating statistics: {e}")
            return {}
    
    def process_csv_pipeline(self, csv_path):
        """Complete pipeline: CSV → SQL Server → Cleaning → Transactions"""
        try:
            self.logger.info(f"Starting pipeline for: {csv_path}")
            
            # Step 1: Initialize connections
            if not self.connect_to_sql_server():
                return False
            
            if not self.initialize_spark():
                return False
            
            # Step 2: Truncate tables
            if not self.truncate_tables():
                return False
            
            # Step 3: Load CSV and insert to Row_data
            df_raw = self.load_csv_to_spark(csv_path)
            if df_raw is None:
                return False
            
            if not self.insert_to_sql_server(df_raw, "Row_data"):
                return False
            
            # Step 4: Copy to Cleaned_data and apply cleaning
            # Copy data from Row_data to Cleaned_data
            cursor = self.conn.cursor()
            cursor.execute("INSERT INTO Cleaned_data SELECT * FROM Row_data")
            self.conn.commit()
            
            if not self.apply_data_cleaning():
                return False
            
            # Step 5: Load cleaned data and prepare transactions
            df_cleaned = self.get_cleaned_data_from_spark()
            if df_cleaned is None:
                return False
            
            transactions_df = self.prepare_transactions_distributed(df_cleaned)
            if transactions_df is None:
                return False
            
            # Step 6: Save transactions and get statistics
            if not self.save_transactions_to_sql(transactions_df):
                return False
            
            stats = self.get_transaction_stats(transactions_df)
            
            self.logger.info("Pipeline completed successfully")
            return stats
            
        except Exception as e:
            self.logger.error(f"Pipeline failed: {e}")
            return False
    
    def get_transactions_for_mining(self):
        """Get transactions in format suitable for mining algorithms"""
        try:
            transactions_df = self.get_cleaned_data_from_spark()
            if transactions_df is None:
                return []
            
            transactions_prepared = self.prepare_transactions_distributed(transactions_df)
            if transactions_prepared is None:
                return []
            
            # Convert to list of lists format
            transactions = transactions_prepared.select("items").rdd.map(lambda row: row[0]).collect()
            
            self.logger.info(f"Retrieved {len(transactions)} transactions for mining")
            return transactions
            
        except Exception as e:
            self.logger.error(f"Error getting transactions for mining: {e}")
            return []
    
    def __del__(self):
        """Clean up resources"""
        if self.conn:
            self.conn.close()
        if self.spark:
            self.spark.stop()


class DataQualityChecker:
    """Data quality validation for the pipeline"""
    
    @staticmethod
    def validate_csv_structure(df):
        """Validate CSV structure and data quality"""
        required_columns = ['InvoiceNo', 'StockCode', 'Description', 'Quantity', 
                          'InvoiceDate', 'UnitPrice', 'CustomerID', 'Country']
        
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        # Check for empty DataFrame
        if df.count() == 0:
            raise ValueError("CSV file is empty")
        
        return True
    
    @staticmethod
    def validate_cleaned_data(df):
        """Validate cleaned data quality"""
        # Check for remaining invalid records
        invalid_records = df.filter(
            (col("InvoiceNo").like("%C%")) |
            (col("CustomerID").isNull()) |
            (col("Quantity").like("%-%")) |
            (col("UnitPrice") <= 0) |
            (col("Description").isNull()) |
            (col("StockCode") == "POST")
        ).count()
        
        if invalid_records > 0:
            raise ValueError(f"Found {invalid_records} invalid records after cleaning")
        
        return True


if __name__ == "__main__":
    # Example usage
    pipeline = DistributedDataPipeline()
    
    # Process a CSV file
    csv_path = "OnlineRetail.csv"
    result = pipeline.process_csv_pipeline(csv_path)
    
    if result:
        print("Pipeline completed successfully!")
        print(f"Statistics: {result}")
    else:
        print("Pipeline failed!")