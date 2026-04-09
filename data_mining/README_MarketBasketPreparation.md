# Market Basket Data Preparation Guide

This guide provides comprehensive solutions for preparing data from the `Cleaned_data` table for Market Basket Analysis using Apriori and FP-Growth algorithms.

## Table Schema

```sql
CREATE TABLE Cleaned_data (
    InvoiceNo VARCHAR(100), 
    StockCode VARCHAR(100), 
    Description VARCHAR(1000), 
    Quantity VARCHAR(1000), 
    InvoiceDate VARCHAR(100),
    UnitPrice DECIMAL(28,4), 
    CustomerID VARCHAR(100), 
    Country VARCHAR(1000)
);
```

## Objective

Transform the transactional data into a format suitable for Market Basket Analysis:
- **Input**: Multiple rows per `InvoiceNo` (transaction)
- **Output**: One row per `InvoiceNo` with aggregated items list

## Example Transformation

**Before:**
```
InvoiceNo | Description
536365    | WHITE HANGING HEART T-LIGHT HOLDER
536365    | WHITE METAL LANTERN
536365    | CREAM CUPID HEARTS COAT HANGER
536366    | JUMBO BAG RED RETROSPOT
536366    | ASSORTED COLOUR BIRD ORNAMENT
```

**After:**
```
InvoiceNo | Items
536365    | ['WHITE HANGING HEART T-LIGHT HOLDER', 'WHITE METAL LANTERN', 'CREAM CUPID HEARTS COAT HANGER']
536366    | ['JUMBO BAG RED RETROSPOT', 'ASSORTED COLOUR BIRD ORNAMENT']
```

## Solutions Provided

### 1. SQL Solutions

#### Solution 1: SQL Server 2017+ (Recommended)
```sql
SELECT 
    InvoiceNo,
    STRING_AGG(DISTINCT Description, ', ') AS Items
FROM Cleaned_data 
WHERE Description IS NOT NULL 
    AND LTRIM(RTRIM(Description)) <> ''
GROUP BY InvoiceNo
ORDER BY InvoiceNo;
```

#### Solution 2: SQL Server 2016 and Earlier
```sql
SELECT 
    cd1.InvoiceNo,
    STUFF((
        SELECT ', ' + cd2.Description
        FROM Cleaned_data cd2
        WHERE cd2.InvoiceNo = cd1.InvoiceNo
            AND cd2.Description IS NOT NULL
            AND LTRIM(RTRIM(cd2.Description)) <> ''
        GROUP BY cd2.Description
        FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)')
    , 1, 2, '') AS Items
FROM Cleaned_data cd1
WHERE cd1.Description IS NOT NULL 
    AND LTRIM(RTRIM(cd1.Description)) <> ''
GROUP BY cd1.InvoiceNo
ORDER BY cd1.InvoiceNo;
```

#### Solution 3: Create a View
```sql
CREATE VIEW vw_MarketBasketTransactions AS
SELECT 
    InvoiceNo,
    STRING_AGG(DISTINCT Description, ', ') AS Items
FROM Cleaned_data 
WHERE Description IS NOT NULL 
    AND LTRIM(RTRIM(Description)) <> ''
GROUP BY InvoiceNo;
```

### 2. Python/Pandas Solution

```python
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

# Fetch and process data
query = '''
SELECT InvoiceNo, Description 
FROM Cleaned_data 
WHERE Description IS NOT NULL 
    AND LTRIM(RTRIM(Description)) <> ''
'''
df_raw = pd.read_sql(query, conn)

# Remove duplicates and group
df_clean = df_raw.drop_duplicates(subset=['InvoiceNo', 'Description'])
df_transactions = df_clean.groupby('InvoiceNo')['Description'].agg(list).reset_index()
df_transactions.columns = ['InvoiceNo', 'Items']

# Convert to format for Apriori/FP-Growth
transactions = df_transactions['Items'].tolist()
```

### 3. PySpark Solution

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from config import Config

# Initialize Spark
spark = SparkSession.builder \
    .appName("MarketBasketAnalysis") \
    .master("local[*]") \
    .config("spark.jars.packages", "com.microsoft.sqlserver:mssql-jdbc:9.4.1.jre8") \
    .getOrCreate()

# SQL Server connection
sql_config = Config.SQL_SERVER_CONFIG
jdbc_url = f"jdbc:sqlserver://{sql_config['server']};databaseName={sql_config['database']}"
properties = {
    "user": sql_config.get('username', 'sa'),
    "password": sql_config.get('password', ''),
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# Load and process data
df = spark.read.jdbc(url=jdbc_url, table="Cleaned_data", properties=properties)
df_filtered = df.filter(
    (F.col("Description").isNotNull()) & 
    (F.trim(F.col("Description")) != "")
)

df_transactions = df_filtered.select("InvoiceNo", "Description") \
    .distinct() \
    .groupBy("InvoiceNo") \
    .agg(F.collect_list("Description").alias("Items")) \
    .orderBy("InvoiceNo")

# Convert to pandas for use with mlxtend
pandas_df = df_transactions.toPandas()
transactions = pandas_df['Items'].tolist()
```

## Key Features

### Data Quality Handling
- **NULL filtering**: Excludes rows where `Description` is NULL
- **Empty string filtering**: Excludes rows where `Description` is empty or whitespace
- **Duplicate removal**: Ensures no duplicate items within the same transaction
- **Case sensitivity**: Uses exact string matching (consider using UPPER() if needed)

### Performance Optimizations
- **Indexed views**: For large datasets, create indexed views for better performance
- **Batch processing**: Process data in chunks for memory efficiency
- **Caching**: Cache frequently accessed data in memory

### Output Formats

#### 1. DataFrame Format
```python
# Columns: InvoiceNo, Items (list)
df_transactions = pd.DataFrame({
    'InvoiceNo': ['536365', '536366'],
    'Items': [['WHITE HANGING HEART T-LIGHT HOLDER', 'WHITE METAL LANTERN'],
              ['JUMBO BAG RED RETROSPOT', 'ASSORTED COLOUR BIRD ORNAMENT']]
})
```

#### 2. List of Lists Format
```python
# Direct input for Apriori/FP-Growth
transactions = [
    ['WHITE HANGING HEART T-LIGHT HOLDER', 'WHITE METAL LANTERN'],
    ['JUMBO BAG RED RETROSPOT', 'ASSORTED COLOUR BIRD ORNAMENT']
]
```

#### 3. JSON Format
```json
[
    {"InvoiceNo": "536365", "Items": ["WHITE HANGING HEART T-LIGHT HOLDER", "WHITE METAL LANTERN"]},
    {"InvoiceNo": "536366", "Items": ["JUMBO BAG RED RETROSPOT", "ASSORTED COLOUR BIRD ORNAMENT"]}
]
```

## Usage with Popular Libraries

### mlxtend (Apriori)
```python
from mlxtend.frequent_patterns import apriori, association_rules

# Get transactions
transactions = df_transactions['Items'].tolist()

# Apply Apriori
frequent_itemsets = apriori(transactions, min_support=0.01, use_colnames=True)

# Generate rules
rules = association_rules(frequent_itemsets, metric="confidence", min_threshold=0.5)
```

### PySpark MLlib (FP-Growth)
```python
from pyspark.ml.fpm import FPGrowth

# Create FPGrowth model
fp_growth = FPGrowth(itemsCol="Items", minSupport=0.01, minConfidence=0.5)
model = fp_growth.fit(df_transactions)

# Display frequent itemsets
model.freqItemsets.show()

# Display association rules
model.associationRules.show()
```

## Statistics and Analysis

### Transaction Statistics
```python
stats = {
    'total_transactions': len(df_transactions),
    'total_unique_items': len(set(item for items in df_transactions['Items'] for item in items)),
    'avg_items_per_transaction': df_transactions['Items'].apply(len).mean(),
    'min_items_per_transaction': df_transactions['Items'].apply(len).min(),
    'max_items_per_transaction': df_transactions['Items'].apply(len).max(),
    'median_items_per_transaction': df_transactions['Items'].apply(len).median()
}
```

### Item Frequency Analysis
```python
from collections import Counter

# Count item frequencies
all_items = [item for items in df_transactions['Items'] for item in items]
item_counts = Counter(all_items)

# Get most frequent items
most_common = item_counts.most_common(10)
```

## Best Practices

1. **Data Validation**: Always check for data quality issues before processing
2. **Memory Management**: Use appropriate data types and consider chunking for large datasets
3. **Performance**: Create indexes on frequently queried columns
4. **Testing**: Validate results with sample data before processing the full dataset
5. **Documentation**: Keep track of data transformations and assumptions

## Files Included

- `market_basket_preparation.py`: Complete Python implementation with all solutions
- `sql_solutions/market_basket_preparation.sql`: Comprehensive SQL solutions
- `README_MarketBasketPreparation.md`: This documentation file

## Dependencies

### Python
```python
pandas>=1.0.0
pyodbc>=4.0.0
mlxtend>=0.18.0  # For Apriori
pyspark>=3.0.0   # For PySpark solutions
```

### SQL Server
- SQL Server 2016+ (for STRING_AGG and other modern functions)
- Appropriate permissions for reading the Cleaned_data table

## Troubleshooting

### Common Issues

1. **Connection Problems**: Check SQL Server credentials and network connectivity
2. **Memory Issues**: Process data in chunks for large datasets
3. **Performance**: Create indexes on InvoiceNo and Description columns
4. **Data Quality**: Handle NULL values and duplicates appropriately

### Performance Tips

1. **Indexing**: Create indexes on InvoiceNo and Description
2. **Partitioning**: Consider partitioning large tables by date ranges
3. **Caching**: Cache frequently accessed data in memory
4. **Parallel Processing**: Use parallel processing for large datasets

## Next Steps

1. **Data Preparation**: Use the provided solutions to prepare your data
2. **Algorithm Selection**: Choose between Apriori and FP-Growth based on your needs
3. **Parameter Tuning**: Experiment with support and confidence thresholds
4. **Result Analysis**: Analyze and interpret the association rules
5. **Business Application**: Apply insights to improve business processes