"""
Market Basket Data Preparation Module
Prepares data from Cleaned_data table for Apriori/FP-Growth algorithms.
"""

import pandas as pd
import pyodbc
from config import Config
from typing import List, Tuple


class MarketBasketDataPreparator:
    """Handles data preparation for market basket analysis."""
    
    def __init__(self, sql_server_config=None):
        """Initialize with SQL Server configuration."""
        self.sql_server_config = sql_server_config or Config.SQL_SERVER_CONFIG
        self.conn = None
        self.connected = False
    
    def connect_to_sql_server(self):
        """Establish connection to SQL Server using SQL Server Authentication."""
        try:
            conn_str = (
                f"DRIVER={{{self.sql_server_config['driver']}}};"
                f"SERVER={self.sql_server_config['server']};"
                f"DATABASE={self.sql_server_config['database']};"
                f"UID={self.sql_server_config.get('username', 'sa')};"
                f"PWD={self.sql_server_config.get('password', '')};"
            )
            
            self.conn = pyodbc.connect(conn_str)
            self.connected = True
            return True
            
        except Exception as e:
            print(f"SQL Server connection failed: {e}")
            self.connected = False
            return False
    
    def get_transactions_from_sql(self) -> pd.DataFrame:
        """
        SQL Solution: Group transactions using STRING_AGG.
        Returns DataFrame with InvoiceNo and aggregated Items list.
        """
        if not self.connected:
            if not self.connect_to_sql_server():
                return pd.DataFrame()
        
        try:
            # SQL query using STRING_AGG to group items by InvoiceNo
            query = """
            SELECT 
                InvoiceNo,
                STRING_AGG(DISTINCT Description, ', ') AS Items
            FROM Cleaned_data 
            WHERE Description IS NOT NULL 
                AND LTRIM(RTRIM(Description)) <> ''
            GROUP BY InvoiceNo
            ORDER BY InvoiceNo
            """
            
            df = pd.read_sql(query, self.conn)
            
            # Convert comma-separated string to list
            df['Items'] = df['Items'].apply(lambda x: x.split(', ') if pd.notna(x) else [])
            
            return df
            
        except Exception as e:
            print(f"Error executing SQL query: {e}")
            return pd.DataFrame()
    
    def get_transactions_from_pandas(self, df_raw: pd.DataFrame = None) -> pd.DataFrame:
        """
        Python/Pandas Solution: Group transactions using pandas operations.
        If df_raw is not provided, fetches data from SQL Server.
        """
        if df_raw is None:
            if not self.connected:
                if not self.connect_to_sql_server():
                    return pd.DataFrame()
            
            # Fetch raw data from SQL Server
            query = "SELECT InvoiceNo, Description FROM Cleaned_data WHERE Description IS NOT NULL AND LTRIM(RTRIM(Description)) <> ''"
            df_raw = pd.read_sql(query, self.conn)
        
        # Remove duplicates within same transaction
        df_clean = df_raw.drop_duplicates(subset=['InvoiceNo', 'Description'])
        
        # Group by InvoiceNo and aggregate descriptions into list
        df_transactions = df_clean.groupby('InvoiceNo')['Description'].agg(list).reset_index()
        df_transactions.columns = ['InvoiceNo', 'Items']
        
        return df_transactions
    
    def get_transactions_pyspark(self, spark, table_name: str = 'Cleaned_data') -> 'pyspark.sql.DataFrame':
        """
        PySpark Solution: Group transactions using PySpark operations.
        Requires a SparkSession to be provided.
        """
        # Read data from SQL Server
        jdbc_url = f"jdbc:sqlserver://{self.sql_server_config['server']};databaseName={self.sql_server_config['database']}"
        properties = {
            "user": self.sql_server_config.get('username', 'sa'),
            "password": self.sql_server_config.get('password', ''),
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        }
        
        # Load data from SQL Server
        df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=properties)
        
        # Filter out NULL and empty descriptions
        from pyspark.sql import functions as F
        df_filtered = df.filter(
            (F.col("Description").isNotNull()) & 
            (F.trim(F.col("Description")) != "")
        )
        
        # Remove duplicates and group by InvoiceNo
        df_transactions = df_filtered.select("InvoiceNo", "Description") \
            .distinct() \
            .groupBy("InvoiceNo") \
            .agg(F.collect_list("Description").alias("Items")) \
            .orderBy("InvoiceNo")
        
        return df_transactions
    
    def prepare_for_apriori(self, df_transactions: pd.DataFrame) -> List[List[str]]:
        """
        Convert transaction DataFrame to list of lists format for Apriori.
        Input: DataFrame with columns [InvoiceNo, Items] where Items is a list
        Output: List of lists, where each inner list is a transaction
        """
        return df_transactions['Items'].tolist()
    
    def prepare_for_fp_growth(self, df_transactions: pd.DataFrame) -> List[List[str]]:
        """
        Convert transaction DataFrame to list of lists format for FP-Growth.
        Same format as Apriori - both algorithms accept the same input format.
        """
        return self.prepare_for_apriori(df_transactions)
    
    def get_sample_transactions(self, n: int = 5) -> pd.DataFrame:
        """Get a sample of transactions for testing."""
        df = self.get_transactions_from_sql()
        return df.head(n)
    
    def get_transaction_statistics(self, df_transactions: pd.DataFrame) -> dict:
        """Calculate statistics about the transactions."""
        if df_transactions.empty:
            return {}
        
        stats = {
            'total_transactions': len(df_transactions),
            'total_unique_items': len(set(item for items in df_transactions['Items'] for item in items)),
            'avg_items_per_transaction': df_transactions['Items'].apply(len).mean(),
            'min_items_per_transaction': df_transactions['Items'].apply(len).min(),
            'max_items_per_transaction': df_transactions['Items'].apply(len).max(),
            'median_items_per_transaction': df_transactions['Items'].apply(len).median()
        }
        
        return stats
    
    def close_connection(self):
        """Close SQL Server connection."""
        if self.conn:
            self.conn.close()
            self.connected = False


def sql_solution_example():
    """
    SQL Solution for Market Basket Data Preparation
    
    This function demonstrates the SQL approach using STRING_AGG.
    """
    sql_query = """
    -- SQL Solution: Group transactions using STRING_AGG
    SELECT 
        InvoiceNo,
        STRING_AGG( cast(Description as varchar(max)) , ', ' ) AS Items
    FROM Cleaned_data 
    WHERE Description IS NOT NULL 
        AND LTRIM(RTRIM(Description)) <> ''
    GROUP BY InvoiceNo
    ORDER BY InvoiceNo;
    
    -- Alternative for older SQL Server versions (pre-2017)
    -- SELECT 
    --     InvoiceNo,
    --     STUFF((
    --         SELECT ', ' + DISTINCT Description
    --         FROM Cleaned_data cd2
    --         WHERE cd2.InvoiceNo = cd1.InvoiceNo
    --             AND Description IS NOT NULL
    --             AND LTRIM(RTRIM(Description)) <> ''
    --         FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)')
    --     , 1, 2, '') AS Items
    -- FROM Cleaned_data cd1
    -- GROUP BY InvoiceNo
    -- ORDER BY InvoiceNo;
    """
    return sql_query


def pandas_solution_example():
    """
    Python/Pandas Solution for Market Basket Data Preparation
    
    This function demonstrates the pandas approach.
    """
    code_example = """
import pandas as pd
import pyodbc
from config import Config

# Connect to SQL Server
conn_str = (
    f"DRIVER={{{Config.SQL_SERVER_CONFIG['driver']}}};"
    f"SERVER={Config.SQL_SERVER_CONFIG['server']};"
    f"DATABASE={Config.SQL_SERVER_CONFIG['database']};"
    f"UID={Config.SQL_SERVER_CONFIG.get('username', 'sa')};"
    f"PWD={Config.SQL_SERVER_CONFIG.get('password', '')};"
)
conn = pyodbc.connect(conn_str)

# Fetch raw data
query = '''
SELECT InvoiceNo, Description 
FROM Cleaned_data 
WHERE Description IS NOT NULL 
    AND LTRIM(RTRIM(Description)) <> ''
'''
df_raw = pd.read_sql(query, conn)

# Remove duplicates within same transaction
df_clean = df_raw.drop_duplicates(subset=['InvoiceNo', 'Description'])

# Group by InvoiceNo and aggregate descriptions into list
df_transactions = df_clean.groupby('InvoiceNo')['Description'].agg(list).reset_index()
df_transactions.columns = ['InvoiceNo', 'Items']

# Now df_transactions is ready for Apriori/FP-Growth
# Example: Convert to list of lists for mlxtend or other libraries
transactions = df_transactions['Items'].tolist()

# Print sample
print(f"Total transactions: {len(df_transactions)}")
print(f"Sample transaction: {df_transactions.iloc[0]['Items'][:5]}")
"""
    return code_example


def pyspark_solution_example():
    """
    PySpark Solution for Market Basket Data Preparation
    
    This function demonstrates the PySpark approach.
    """
    code_example = """
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from config import Config

# Initialize Spark session
spark = SparkSession.builder \
    .appName("MarketBasketAnalysis") \
    .master("local[*]") \
    .config("spark.jars.packages", "com.microsoft.sqlserver:mssql-jdbc:9.4.1.jre8") \
    .getOrCreate()

# SQL Server connection details
sql_config = Config.SQL_SERVER_CONFIG
jdbc_url = f"jdbc:sqlserver://{sql_config['server']};databaseName={sql_config['database']}"
properties = {
    "user": sql_config.get('username', 'sa'),
    "password": sql_config.get('password', ''),
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# Load data from SQL Server
df = spark.read.jdbc(url=jdbc_url, table="Cleaned_data", properties=properties)

# Filter out NULL and empty descriptions
df_filtered = df.filter(
    (F.col("Description").isNotNull()) & 
    (F.trim(F.col("Description")) != "")
)

# Remove duplicates and group by InvoiceNo
df_transactions = df_filtered.select("InvoiceNo", "Description") \
    .distinct() \
    .groupBy("InvoiceNo") \
    .agg(F.collect_list("Description").alias("Items")) \
    .orderBy("InvoiceNo")

# Show sample
df_transactions.show(5, truncate=False)

# Convert to pandas for use with mlxtend or other libraries
pandas_df = df_transactions.toPandas()
transactions = pandas_df['Items'].tolist()

spark.stop()
"""
    return code_example


if __name__ == "__main__":
    # Example usage
    print("=== Market Basket Data Preparation ===\n")
    
    # SQL Solution
    print("1. SQL Solution:")
    print(sql_solution_example())
    print("\n" + "="*50 + "\n")
    
    # Pandas Solution
    print("2. Python/Pandas Solution:")
    print(pandas_solution_example())
    print("\n" + "="*50 + "\n")
    
    # PySpark Solution
    print("3. PySpark Solution:")
    print(pyspark_solution_example())
    
    # Test with actual data (if SQL Server is available)
    print("\n" + "="*50)
    print("Testing with actual data...")
    
    try:
        preparator = MarketBasketDataPreparator()
        if preparator.connect_to_sql_server():
            # Get sample transactions
            sample = preparator.get_sample_transactions(3)
            print("\nSample transactions:")
            for idx, row in sample.iterrows():
                print(f"\nInvoiceNo: {row['InvoiceNo']}")
                print(f"Items: {row['Items'][:5]}...")  # Show first 5 items
            
            # Get full transactions
            df_transactions = preparator.get_transactions_from_sql()
            stats = preparator.get_transaction_statistics(df_transactions)
            
            print(f"\nTransaction Statistics:")
            for key, value in stats.items():
                print(f"{key}: {value}")
            
            # Prepare for Apriori
            transactions_list = preparator.prepare_for_apriori(df_transactions)
            print(f"\nReady for Apriori/FP-Growth: {len(transactions_list)} transactions")
            
            preparator.close_connection()
        else:
            print("Could not connect to SQL Server. Please check your configuration.")
    except Exception as e:
        print(f"Error during testing: {e}")