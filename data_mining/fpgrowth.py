"""
FP-Growth Algorithm Module
Distributed FP-Growth implementation using PySpark.
Refactored from your existing fp_growth() and parallel_fpgrowth() functions.
"""

import os
import json
from collections import defaultdict
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.ml.fpm import FPGrowth as SparkFPGrowth

class FPGrowthMiner:
    def __init__(self, min_support=0.01, min_confidence=0.5):
        """Initialize FP-Growth miner with parameters."""
        self.min_support = min_support
        self.min_confidence = min_confidence
        self.spark = None
    
    def _get_spark_session(self):
        """Get or create Spark session."""
        if self.spark is None:
            self.spark = SparkSession.builder \
                .appName("FPGrowthMining") \
                .master("local[*]") \
                .config("spark.driver.memory", "4g") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .getOrCreate()
        return self.spark
    
    
    def run_distributed(self, transactions):
        """Run FP-Growth using PySpark ML (distributed)."""
        spark = self._get_spark_session()
        
        # Convert transactions to DataFrame
        rows = [Row(items=list(t)) for t in transactions]
        df = spark.createDataFrame(rows)
        
        # Run FP-Growth
        fp = SparkFPGrowth(
            itemsCol="items",
            minSupport=self.min_support,
            minConfidence=self.min_confidence
        )
        
        model = fp.fit(df)
        
        # Get frequent itemsets
        frequent_itemsets = model.freqItemsets.collect()
        
        # Get association rules
        association_rules = model.associationRules.collect()
        
        # Format results
        results = {
            'frequent_itemsets': [],
            'association_rules': []
        }
        
        for itemset in frequent_itemsets:
            results['frequent_itemsets'].append({
                'items': itemset['items'],
                'freq': itemset['freq']
            })
        
        for rule in association_rules:
            results['association_rules'].append({
                'antecedents': rule['antecedents'],
                'consequents': rule['consequents'],
                'confidence': rule['confidence'],
                'lift': rule['lift']
            })
        
        return results
    
    def run_distributed_filtered(self, transactions, min_itemset_size=2):
        """Run FP-Growth and filter by itemset size."""
        spark = self._get_spark_session()
        
        # Convert transactions to DataFrame
        rows = [Row(items=list(t)) for t in transactions]
        df = spark.createDataFrame(rows)
        
        # Run FP-Growth
        fp = SparkFPGrowth(
            itemsCol="items",
            minSupport=self.min_support,
            minConfidence=self.min_confidence
        )
        
        model = fp.fit(df)
        
        # Filter itemsets by size
        filtered_itemsets = model.freqItemsets.filter(f"size(items) >= {min_itemset_size}").collect()
        
        # Format results
        results = []
        for itemset in filtered_itemsets:
            results.append({
                'items': itemset['items'],
                'freq': itemset['freq'],
                'support': itemset['freq'] / len(transactions) if transactions else 0
            })
        
        return results
    
    def generate_recommendations(self, frequent_itemsets, top_n=5):
        """Generate product recommendations from frequent itemsets."""
        recommendations = {}
        
        for itemset_info in frequent_itemsets:
            items = itemset_info['items']
            support = itemset_info.get('support', 0)
            
            # For each item in the itemset, recommend other items
            for item in items:
                if item not in recommendations:
                    recommendations[item] = []
                
                other_items = [i for i in items if i != item]
                for other_item in other_items:
                    recommendations[item].append({
                        'recommended_with': other_item,
                        'support': support,
                        'items_together': items
                    })
        
        # Sort by support and limit
        for item in recommendations:
            recommendations[item].sort(key=lambda x: x['support'], reverse=True)
            recommendations[item] = recommendations[item][:top_n]
        
        return recommendations
    
    def __del__(self):
        """Clean up Spark session."""
        if self.spark:
            self.spark.stop()