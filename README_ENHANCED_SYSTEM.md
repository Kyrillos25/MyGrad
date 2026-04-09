# Market Basket Analysis - Complete Distributed System

## 📋 Table of Contents

1. [System Overview](#system-overview)
2. [Architecture](#architecture)
3. [Installation & Setup](#installation--setup)
4. [Data Pipeline](#data-pipeline)
5. [Mining Algorithms](#mining-algorithms)
6. [Recommendation Engine](#recommendation-engine)
7. [API Reference](#api-reference)
8. [Usage Examples](#usage-examples)
9. [Performance Optimization](#performance-optimization)
10. [Troubleshooting](#troubleshooting)

---

## 🎯 System Overview

This is a complete distributed market basket analysis system built with:
- **SQL Server** for data storage
- **PySpark** for distributed processing
- **Flask** for web application
- **Apriori & FP-Growth** for association rule mining
- **Advanced Recommendation Engine** for product recommendations

### Key Features

✅ **Complete Data Pipeline**
- CSV upload and validation
- Automatic data cleaning
- Distributed processing with PySpark
- SQL Server integration

✅ **Distributed Mining Algorithms**
- Apriori (SON algorithm for distributed processing)
- FP-Growth (PySpark MLlib implementation)
- Frequent itemset generation
- Association rule mining

✅ **Advanced Recommendation Engine**
- Hybrid recommendation method
- Real-time recommendations
- Cart-based recommendations
- Product page recommendations

✅ **Production-Ready**
- RESTful API endpoints
- Error handling and logging
- Performance optimization
- Scalable architecture

---

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Flask Web     │    │   Data Mining   │    │   SQL Server    │
│   Application   │◄──►│   Components    │◄──►│   Database      │
└─────────────────┘    └─────────────────┘    └─────────────────┘
        │                       │                       │
        ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Templates     │    │   PySpark       │    │   Row_data      │
│   (HTML/CSS)    │    │   Processing    │    │   Cleaned_data  │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

### Component Structure

```
data_mining/
├── distributed_pipeline.py      # Complete data pipeline
├── distributed_apriori.py       # Distributed Apriori algorithm
├── enhanced_fpgrowth.py         # Enhanced FP-Growth implementation
├── recommendation_engine.py     # Advanced recommendation engine
├── market_basket_preparation.py # Data preparation utilities
└── sql_solutions/
    └── market_basket_preparation.sql  # SQL solutions
```

---

## 🚀 Installation & Setup

### Prerequisites

- Python 3.8+
- SQL Server 2016+
- PySpark 3.0+
- Java 8+

### Installation Steps

1. **Install Dependencies**
```bash
pip install -r requirements.txt
```

2. **Configure SQL Server**
```bash
# Edit .env file
cp .env.example .env
# Update with your SQL Server credentials
```

3. **Initialize Database**
```bash
python init_db.py
```

4. **Run Application**
```bash
python run.py
```

### Configuration

```python
# config.py
SQL_SERVER_CONFIG = {
    'driver': 'ODBC Driver 17 for SQL Server',
    'server': 'localhost',
    'database': 'TestDB',
    'username': 'sa',
    'password': 'your_password'
}
```

---

## 🔄 Data Pipeline

### Pipeline Flow

```
CSV Upload → Validate → Truncate Tables → Insert to Row_data → 
Copy to Cleaned_data → Apply Cleaning Rules → Prepare Transactions → 
Save to tableed → Run Mining
```

### Data Cleaning Rules

```python
# Remove cancelled transactions
DELETE FROM Cleaned_data WHERE InvoiceNo LIKE '%C%'

# Remove anonymous users
DELETE FROM Cleaned_data WHERE CustomerID IS NULL

# Remove negative quantities
DELETE FROM Cleaned_data WHERE Quantity LIKE '%-%'

# Remove zero/negative prices
DELETE FROM Cleaned_data WHERE UnitPrice = 0 OR UnitPrice LIKE '%-%'

# Remove invalid descriptions
DELETE FROM Cleaned_data WHERE Description IS NULL OR Description LIKE '%?%'

# Remove POST stock codes
DELETE FROM Cleaned_data WHERE StockCode = 'POST'
```

### Usage Example

```python
from data_mining.distributed_pipeline import DistributedDataPipeline

# Initialize pipeline
pipeline = DistributedDataPipeline()

# Process CSV file
result = pipeline.process_csv_pipeline('data/OnlineRetail.csv')

if result:
    print(f"Total transactions: {result['total_transactions']}")
    print(f"Unique products: {result['unique_products']}")
```

---

## ⛏️ Mining Algorithms

### Apriori Algorithm (Distributed)

Uses the SON (Savasere, Omiecinski, Navathe) algorithm for distributed frequent itemset mining.

```python
from data_mining.distributed_apriori import DistributedApriori

# Initialize
apriori = DistributedApriori(
    spark_session=spark,
    min_support=0.01,
    min_confidence=0.5
)

# Load transactions
apriori.load_transactions_from_spark(transactions_df)

# Run SON algorithm
frequent_itemsets = apriori.get_frequent_itemsets_son(transactions_df)

# Generate rules
rules = apriori.generate_association_rules()
```

### FP-Growth Algorithm (Enhanced)

Uses PySpark MLlib for distributed FP-Growth with additional optimizations.

```python
from data_mining.enhanced_fpgrowth import EnhancedFPGrowth

# Initialize
fp_growth = EnhancedFPGrowth(
    spark_session=spark,
    min_support=0.01,
    min_confidence=0.5
)

# Load and run
fp_growth.load_transactions_from_spark(transactions_df)
fp_growth.run_fpgrowth()

# Get results
frequent_itemsets = fp_growth.get_frequent_itemsets_by_size(2)
top_rules = fp_growth.get_top_rules(10)
```

### Algorithm Comparison

| Feature | Apriori | FP-Growth |
|---------|---------|-----------|
| **Speed** | Slower (candidate generation) | Faster (tree structure) |
| **Memory** | Higher | Lower |
| **Scalability** | Good | Excellent |
| **Distributed** | Yes (SON) | Yes (MLlib) |

---

## 🎯 Recommendation Engine

### Recommendation Methods

1. **Association Rules**: Based on mined association rules
2. **Similarity**: Based on product co-occurrence
3. **Hybrid**: Combines both methods (recommended)

### Usage Example

```python
from data_mining.recommendation_engine import RecommendationEngine

# Initialize with rules
engine = RecommendationEngine(
    apriori_rules=apriori_rules,
    fp_growth_rules=fp_growth_rules
)

# Get recommendations
recommendations = engine.get_recommendations(
    products=['Product A', 'Product B'],
    method='hybrid',
    top_n=5
)

for rec in recommendations:
    print(f"{rec.product}: confidence={rec.confidence:.2f}, lift={rec.lift:.2f}")
```

### Real-Time Recommendations

```python
from data_mining.recommendation_engine import RealTimeRecommender

recommender = RealTimeRecommender(engine)

# Cart recommendations
cart_recs = recommender.get_cart_recommendations(['Item1', 'Item2'])

# Product page recommendations
product_recs = recommender.get_product_page_recommendations('Product A')
```

---

## 🔌 API Reference

### Recommendation APIs

#### POST /enhanced_manager/api/recommend

Get product recommendations.

**Request:**
```json
{
  "products": ["Product A", "Product B"],
  "method": "hybrid",
  "top_n": 5
}
```

**Response:**
```json
{
  "recommendations": [
    {
      "product": "Product C",
      "confidence": 0.85,
      "lift": 2.5,
      "score": 0.78,
      "reason": "Customers who bought Product A, Product B also bought Product C"
    }
  ]
}
```

#### POST /enhanced_manager/api/cart_recommend

Get cart recommendations.

**Request:**
```json
{
  "cart_items": ["Item1", "Item2"]
}
```

#### GET /enhanced_manager/api/product_recommend/<product>

Get product page recommendations.

### System APIs

#### GET /enhanced_manager/system_status

Get detailed system status.

#### GET /enhanced_manager/export_results

Export mining results as JSON.

---

## 📖 Usage Examples

### Complete Workflow

```python
# 1. Initialize pipeline
from data_mining.distributed_pipeline import DistributedDataPipeline
from data_mining.enhanced_fpgrowth import EnhancedFPGrowth
from data_mining.recommendation_engine import RecommendationEngine

# 2. Process data
pipeline = DistributedDataPipeline()
stats = pipeline.process_csv_pipeline('data/OnlineRetail.csv')

# 3. Run mining
transactions = pipeline.get_transactions_for_mining()
fp_growth = EnhancedFPGrowth(pipeline.spark)
fp_growth.load_transactions_from_list(transactions)
fp_growth.run_fpgrowth()

# 4. Generate recommendations
rules = fp_growth.association_rules_df.collect()
engine = RecommendationEngine(fp_growth_rules=rules)

# 5. Get recommendations
recs = engine.get_recommendations(['WHITE HANGING HEART T-LIGHT HOLDER'], top_n=5)
```

### Web Interface

1. **Dashboard**: `/enhanced_manager/enhanced_dashboard`
2. **Upload**: `/enhanced_manager/enhanced_upload`
3. **Mining Results**: `/enhanced_manager/mining_results`
4. **Recommendations**: `/enhanced_manager/recommendations`

---

## ⚡ Performance Optimization

### PySpark Configuration

```python
spark_config = {
    'app_name': 'MarketBasketAnalysis',
    'master': 'local[*]',
    'memory': '4g',
    'cores': '4'
}
```

### Optimization Techniques

1. **Data Partitioning**: Optimize partition count based on data size
2. **Caching**: Cache frequently used DataFrames
3. **Broadcast Joins**: Use for small lookup tables
4. **Adaptive Query Execution**: Enable AQE for dynamic optimization

### Performance Tips

- Use appropriate `min_support` threshold (0.001-0.05 recommended)
- Limit rule generation with `min_confidence` and `min_lift`
- Cache intermediate results for iterative algorithms
- Monitor Spark UI for performance bottlenecks

---

## 🐛 Troubleshooting

### Common Issues

#### 1. SQL Server Connection Failed

**Solution:** Check credentials in `.env` file and ensure SQL Server is running.

#### 2. PySpark Initialization Failed

**Solution:** Verify Java installation and JAVA_HOME environment variable.

#### 3. Memory Issues

**Solution:** Increase Spark memory configuration:
```python
spark_config['memory'] = '8g'
```

#### 4. No Recommendations Found

**Solution:** Lower `min_support` and `min_confidence` thresholds.

### Debug Mode

Enable debug logging:
```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

### Monitoring

- **Spark UI**: http://localhost:4040
- **System Status**: `/enhanced_manager/system_status`
- **Logs**: Check application logs for detailed information

---

## 📊 Expected Output

### 1. Clean Transactional Dataset
- Grouped by InvoiceNo
- Items aggregated per transaction
- Duplicates removed
- Invalid entries filtered

### 2. Frequent Itemsets
- 1-itemsets, 2-itemsets, 3-itemsets, etc.
- Support values for each itemset
- Sorted by frequency

### 3. Association Rules
- Antecedent → Consequent
- Confidence, Lift, Support metrics
- Filtered by thresholds

### 4. Product Recommendations
- Based on association rules
- Ranked by score
- Real-time generation

---

## 🎓 Best Practices

1. **Data Quality**: Ensure clean, well-formatted CSV files
2. **Parameter Tuning**: Experiment with support/confidence thresholds
3. **Performance**: Use distributed processing for large datasets
4. **Validation**: Split data into train/test for recommendation evaluation
5. **Monitoring**: Regularly check system performance and accuracy

---

## 📞 Support

For issues and questions:
- Check the troubleshooting section
- Review application logs
- Monitor Spark UI for performance issues

---

## 📄 License

This project is for educational and demonstration purposes.