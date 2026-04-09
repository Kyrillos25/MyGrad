"""
Enhanced FP-Growth Algorithm Implementation
Uses PySpark MLlib for distributed FP-Growth with additional optimizations and features
"""

from pyspark.ml.fpm import FPGrowth
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
import logging
from typing import List, Dict, Tuple, Set
import time
import json


class EnhancedFPGrowth:
    """Enhanced FP-Growth implementation with additional features and optimizations"""
    
    def __init__(self, spark_session: SparkSession, min_support: float = 0.01, min_confidence: float = 0.5, 
                 num_partitions: int = 8):
        """
        Initialize Enhanced FP-Growth
        
        Args:
            spark_session: PySpark session
            min_support: Minimum support threshold (0.0 to 1.0)
            min_confidence: Minimum confidence threshold (0.0 to 1.0)
            num_partitions: Number of partitions for distributed processing
        """
        self.spark = spark_session
        self.min_support = min_support
        self.min_confidence = min_confidence
        self.num_partitions = num_partitions
        self.logger = logging.getLogger(__name__)
        
        # Results storage
        self.frequent_itemsets_df = None
        self.association_rules_df = None
        self.fp_growth_model = None
        self.transaction_count = 0
        
    def load_transactions_from_spark(self, transactions_df):
        """Load transactions from Spark DataFrame"""
        try:
            # Ensure proper format
            if 'items' not in transactions_df.columns:
                raise ValueError("DataFrame must have 'items' column")
            
            # Cache the DataFrame for better performance
            self.transactions_df = transactions_df.select("items").cache()
            self.transaction_count = self.transactions_df.count()
            
            self.logger.info(f"Loaded {self.transaction_count} transactions")
            return True
            
        except Exception as e:
            self.logger.error(f"Error loading transactions: {e}")
            return False
    
    def load_transactions_from_list(self, transactions: List[List[str]]):
        """Load transactions from Python list"""
        try:
            # Convert to Spark DataFrame
            data = [{'items': items} for items in transactions]
            self.transactions_df = self.spark.createDataFrame(data).cache()
            self.transaction_count = len(transactions)
            
            self.logger.info(f"Loaded {self.transaction_count} transactions from list")
            return True
            
        except Exception as e:
            self.logger.error(f"Error loading transactions from list: {e}")
            return False
    
    def run_fpgrowth(self, min_confidence: float = None) -> bool:
        """
        Run FP-Growth algorithm with optimizations
        
        Args:
            min_confidence: Override default min_confidence
        """
        try:
            self.logger.info("Starting Enhanced FP-Growth algorithm...")
            start_time = time.time()
            
            # Create FP-Growth model with optimizations
            fp_growth = FPGrowth(
                itemsCol="items",
                minSupport=self.min_support,
                minConfidence=min_confidence or self.min_confidence,
                numPartitions=self.num_partitions
            )
            
            # Fit the model
            self.fp_growth_model = fp_growth.fit(self.transactions_df)
            
            # Get frequent itemsets
            self.frequent_itemsets_df = self.fp_growth_model.freqItemsets.cache()
            
            # Get association rules
            self.association_rules_df = self.fp_growth_model.associationRules.cache()
            
            # Filter rules by confidence
            if min_confidence:
                self.association_rules_df = self.association_rules_df.filter(
                    col("confidence") >= min_confidence
                )
            
            # Count results
            frequent_count = self.frequent_itemsets_df.count()
            rules_count = self.association_rules_df.count()
            
            end_time = time.time()
            processing_time = end_time - start_time
            
            self.logger.info(f"FP-Growth completed in {processing_time:.2f} seconds")
            self.logger.info(f"Found {frequent_count} frequent itemsets")
            self.logger.info(f"Generated {rules_count} association rules")
            
            return True
            
        except Exception as e:
            self.logger.error(f"FP-Growth failed: {e}")
            return False
    
    def get_frequent_itemsets_by_size(self, size: int) -> pd.DataFrame:
        """Get frequent itemsets of specific size"""
        try:
            if self.frequent_itemsets_df is None:
                raise ValueError("FP-Growth model not fitted yet")
            
            result = self.frequent_itemsets_df.filter(size(col("items")) == size) \
                .orderBy(desc("freq")) \
                .toPandas()
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error getting itemsets by size: {e}")
            return pd.DataFrame()
    
    def get_top_rules(self, n: int = 10, sort_by: str = 'confidence') -> pd.DataFrame:
        """Get top N association rules sorted by specified metric"""
        try:
            if self.association_rules_df is None:
                raise ValueError("FP-Growth model not fitted yet")
            
            # PySpark FP-Growth returns columns: antecedent, consequent, confidence, lift
            # Check if sort_by column exists
            columns = self.association_rules_df.columns
            if sort_by not in columns:
                sort_by = 'confidence'
            
            result = self.association_rules_df.orderBy(desc(sort_by)) \
                .limit(n) \
                .toPandas()
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error getting top rules: {e}")
            return pd.DataFrame()
    
    def get_itemset_statistics(self) -> Dict:
        """Get comprehensive statistics about frequent itemsets"""
        try:
            if self.frequent_itemsets_df is None:
                return {}
            
            # Get itemset size distribution
            size_distribution = self.frequent_itemsets_df \
                .groupBy(size(col("items")).alias("itemset_size")) \
                .count() \
                .orderBy("itemset_size") \
                .toPandas()
            
            # Get support distribution
            support_stats = self.frequent_itemsets_df \
                .agg(
                    min("freq").alias("min_support"),
                    max("freq").alias("max_support"),
                    avg("freq").alias("avg_support"),
                    stddev("freq").alias("std_support")
                ) \
                .collect()[0]
            
            # Get total unique items
            unique_items = self.frequent_itemsets_df \
                .select(explode("items").alias("item")) \
                .distinct() \
                .count()
            
            stats = {
                'total_frequent_itemsets': self.frequent_itemsets_df.count(),
                'unique_items': unique_items,
                'itemset_size_distribution': size_distribution.to_dict('records'),
                'support_statistics': {
                    'min': float(support_stats['min_support']),
                    'max': float(support_stats['max_support']),
                    'avg': float(support_stats['avg_support']),
                    'std': float(support_stats['std_support'])
                }
            }
            
            return stats
            
        except Exception as e:
            self.logger.error(f"Error getting itemset statistics: {e}")
            return {}
    
    def get_rule_statistics(self) -> Dict:
        """Get comprehensive statistics about association rules"""
        try:
            if self.association_rules_df is None:
                return {}
            
            # Get confidence distribution
            confidence_stats = self.association_rules_df \
                .agg(
                    min("confidence").alias("min_confidence"),
                    max("confidence").alias("max_confidence"),
                    avg("confidence").alias("avg_confidence"),
                    stddev("confidence").alias("std_confidence")
                ) \
                .collect()[0]
            
            # Get lift distribution
            lift_stats = self.association_rules_df \
                .agg(
                    min("lift").alias("min_lift"),
                    max("lift").alias("max_lift"),
                    avg("lift").alias("avg_lift"),
                    stddev("lift").alias("std_lift")
                ) \
                .collect()[0]
            
            # Get rule size distribution
            rule_size_distribution = self.association_rules_df \
                .groupBy(
                    size(col("antecedent")).alias("antecedent_size"),
                    size(col("consequent")).alias("consequent_size")
                ) \
                .count() \
                .orderBy("antecedent_size", "consequent_size") \
                .toPandas()
            
            stats = {
                'total_rules': self.association_rules_df.count(),
                'confidence_statistics': {
                    'min': float(confidence_stats['min_confidence']),
                    'max': float(confidence_stats['max_confidence']),
                    'avg': float(confidence_stats['avg_confidence']),
                    'std': float(confidence_stats['std_confidence'])
                },
                'lift_statistics': {
                    'min': float(lift_stats['min_lift']),
                    'max': float(lift_stats['max_lift']),
                    'avg': float(lift_stats['avg_lift']),
                    'std': float(lift_stats['std_lift'])
                },
                'rule_size_distribution': rule_size_distribution.to_dict('records')
            }
            
            return stats
            
        except Exception as e:
            self.logger.error(f"Error getting rule statistics: {e}")
            return {}
    
    def find_rules_for_item(self, item: str, sort_by: str = 'confidence', limit: int = 20) -> pd.DataFrame:
        """Find association rules that involve a specific item"""
        try:
            if self.association_rules_df is None:
                raise ValueError("FP-Growth model not fitted yet")
            
            # Find rules where item is in antecedent or consequent
            result = self.association_rules_df \
                .filter(
                    array_contains(col("antecedent"), item) |
                    array_contains(col("consequent"), item)
                ) \
                .orderBy(desc(sort_by)) \
                .limit(limit) \
                .toPandas()
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error finding rules for item '{item}': {e}")
            return pd.DataFrame()
    
    def get_recommendations(self, items: List[str], top_n: int = 5) -> List[Dict]:
        """Get recommendations based on a list of items"""
        try:
            if self.association_rules_df is None:
                raise ValueError("FP-Growth model not fitted yet")
            
            # Find rules where the given items are in the antecedent
            recommendations = []
            
            # Convert to set for faster lookup
            item_set = set(items)
            
            # Get relevant rules
            relevant_rules = self.association_rules_df \
                .filter(array_contains(col("antecedent"), items[0])) \
                .collect()
            
            for rule in relevant_rules:
                antecedent = set(rule.antecedent)
                consequent = set(rule.consequent)
                
                # Check if all given items are in antecedent
                if item_set.issubset(antecedent):
                    for item in consequent:
                        if item not in item_set:  # Don't recommend items already in the list
                            recommendations.append({
                                'item': item,
                                'confidence': float(rule.confidence),
                                'lift': float(rule.lift),
                                'antecedent': list(antecedent),
                                'support': float(rule.support)
                            })
            
            # Sort by confidence and lift
            recommendations.sort(key=lambda x: (x['confidence'], x['lift']), reverse=True)
            
            # Return top N
            return recommendations[:top_n]
            
        except Exception as e:
            self.logger.error(f"Error getting recommendations: {e}")
            return []
    
    def print_results(self):
        """Print comprehensive results"""
        print("\n" + "="*60)
        print("ENHANCED FP-GROWTH RESULTS")
        print("="*60)
        
        # Print frequent itemsets summary
        if self.frequent_itemsets_df is not None:
            print(f"\nFREQUENT ITEMSETS (min_support = {self.min_support}):")
            print("-" * 50)
            
            itemset_stats = self.get_itemset_statistics()
            print(f"Total frequent itemsets: {itemset_stats['total_frequent_itemsets']}")
            print(f"Unique items: {itemset_stats['unique_items']}")
            print(f"Average support: {itemset_stats['support_statistics']['avg']:.4f}")
            
            # Show itemsets by size
            for size in range(1, 6):  # Show up to 5-itemsets
                itemsets = self.get_frequent_itemsets_by_size(size)
                if len(itemsets) > 0:
                    print(f"\n{size}-itemsets ({len(itemsets)} found):")
                    for _, row in itemsets.head(5).iterrows():
                        items = sorted(row['items'])
                        print(f"  {items} (support: {row['freq']})")
        
        # Print association rules summary
        if self.association_rules_df is not None:
            print(f"\nASSOCIATION RULES (min_confidence = {self.min_confidence}):")
            print("-" * 50)
            
            rule_stats = self.get_rule_statistics()
            print(f"Total rules: {rule_stats['total_rules']}")
            print(f"Average confidence: {rule_stats['confidence_statistics']['avg']:.4f}")
            print(f"Average lift: {rule_stats['lift_statistics']['avg']:.4f}")
            
            # Show top rules
            top_rules = self.get_top_rules(10)
            if len(top_rules) > 0:
                print(f"\nTop 10 rules by confidence:")
                for i, (_, rule) in enumerate(top_rules.iterrows(), 1):
                    antecedent = sorted(rule['antecedent'])
                    consequent = sorted(rule['consequent'])
                    print(f"\n{i}. {antecedent} → {consequent}")
                    print(f"   Confidence: {rule['confidence']:.4f}")
                    print(f"   Lift: {rule['lift']:.4f}")
                    print(f"   Support: {rule['support']:.4f}")
    
    def save_results(self, output_path: str):
        """Save comprehensive results to JSON file"""
        try:
            results = {
                'parameters': {
                    'min_support': self.min_support,
                    'min_confidence': self.min_confidence,
                    'num_partitions': self.num_partitions,
                    'transaction_count': self.transaction_count
                },
                'frequent_itemsets': {
                    'count': self.frequent_itemsets_df.count() if self.frequent_itemsets_df else 0,
                    'statistics': self.get_itemset_statistics(),
                    'data': self.frequent_itemsets_df.toPandas().to_dict('records') 
                           if self.frequent_itemsets_df else []
                },
                'association_rules': {
                    'count': self.association_rules_df.count() if self.association_rules_df else 0,
                    'statistics': self.get_rule_statistics(),
                    'data': self.association_rules_df.toPandas().to_dict('records') 
                           if self.association_rules_df else []
                }
            }
            
            with open(output_path, 'w') as f:
                json.dump(results, f, indent=2, default=str)
            
            self.logger.info(f"Results saved to {output_path}")
            
        except Exception as e:
            self.logger.error(f"Error saving results: {e}")
    
    def optimize_parameters(self, support_range: List[float] = None, 
                          confidence_range: List[float] = None) -> Dict:
        """
        Optimize parameters by testing different combinations
        
        Args:
            support_range: List of support values to test
            confidence_range: List of confidence values to test
        """
        if support_range is None:
            support_range = [0.001, 0.005, 0.01, 0.02, 0.05]
        
        if confidence_range is None:
            confidence_range = [0.1, 0.3, 0.5, 0.7, 0.9]
        
        best_params = {'support': 0, 'confidence': 0, 'score': 0}
        results = []
        
        for support in support_range:
            for confidence in confidence_range:
                try:
                    # Run FP-Growth with current parameters
                    fp_growth = FPGrowth(
                        itemsCol="items",
                        minSupport=support,
                        minConfidence=confidence,
                        numPartitions=self.num_partitions
                    )
                    
                    model = fp_growth.fit(self.transactions_df)
                    rules_count = model.associationRules.count()
                    itemsets_count = model.freqItemsets.count()
                    
                    # Calculate score (balance between coverage and quality)
                    score = rules_count * confidence * (1 - support)
                    
                    result = {
                        'support': support,
                        'confidence': confidence,
                        'rules_count': rules_count,
                        'itemsets_count': itemsets_count,
                        'score': score
                    }
                    results.append(result)
                    
                    if score > best_params['score']:
                        best_params = {
                            'support': support,
                            'confidence': confidence,
                            'score': score,
                            'rules_count': rules_count,
                            'itemsets_count': itemsets_count
                        }
                    
                except Exception as e:
                    self.logger.warning(f"Failed with support={support}, confidence={confidence}: {e}")
        
        self.logger.info(f"Best parameters: {best_params}")
        return {'best_params': best_params, 'all_results': results}


class FPGrowthComparison:
    """Compare FP-Growth with other algorithms"""
    
    @staticmethod
    def compare_algorithms(spark, transactions_df, min_support=0.01, min_confidence=0.5):
        """Compare FP-Growth with traditional Apriori"""
        from .distributed_apriori import TraditionalApriori
        
        results = {}
        
        # FP-Growth
        fp_growth = EnhancedFPGrowth(spark, min_support, min_confidence)
        fp_growth.load_transactions_from_spark(transactions_df)
        fp_growth.run_fpgrowth()
        
        results['fp_growth'] = {
            'frequent_itemsets': fp_growth.frequent_itemsets_df.count(),
            'association_rules': fp_growth.association_rules_df.count(),
            'time': 'measured separately'
        }
        
        # Traditional Apriori (for comparison on smaller datasets)
        try:
            # Convert to list for traditional algorithm
            transactions_list = transactions_df.select("items").rdd.map(lambda row: row[0]).collect()
            
            trad_apriori = TraditionalApriori(min_support, min_confidence)
            trad_apriori.load_transactions(transactions_list)
            frequent_itemsets = trad_apriori.find_frequent_itemsets()
            
            results['traditional_apriori'] = {
                'frequent_itemsets': len(frequent_itemsets),
                'association_rules': 'not implemented in traditional version',
                'time': 'measured separately'
            }
            
        except Exception as e:
            results['traditional_apriori'] = {'error': str(e)}
        
        return results


if __name__ == "__main__":
    # Example usage
    logging.basicConfig(level=logging.INFO)
    
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("EnhancedFPGrowth") \
        .master("local[*]") \
        .getOrCreate()
    
    # Create sample data
    sample_transactions = [
        ['A', 'B', 'C'],
        ['A', 'B'],
        ['A', 'C'],
        ['B', 'C'],
        ['A', 'B', 'C', 'D'],
        ['A', 'D'],
        ['B', 'D'],
        ['C', 'D']
    ]
    
    # Test Enhanced FP-Growth
    fp_growth = EnhancedFPGrowth(spark, min_support=0.25, min_confidence=0.6)
    fp_growth.load_transactions_from_list(sample_transactions)
    fp_growth.run_fpgrowth()
    
    # Print results
    fp_growth.print_results()
    
    # Get recommendations
    recommendations = fp_growth.get_recommendations(['A', 'B'])
    print(f"\nRecommendations for ['A', 'B']: {recommendations}")
    
    spark.stop()