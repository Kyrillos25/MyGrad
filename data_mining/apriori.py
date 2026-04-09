"""
Apriori Algorithm Module
Distributed Apriori implementation using PySpark (SON Algorithm).
Refactored from your existing apriori_algorithm() and son_apriori() functions.
"""

import math
from collections import defaultdict
from itertools import combinations
from pyspark.sql import SparkSession

class AprioriMiner:
    def __init__(self, min_support=0.01, min_confidence=0.5):
        """Initialize Apriori miner with parameters."""
        self.min_support = min_support
        self.min_confidence = min_confidence
        self.spark = None
    
    def _get_spark_session(self):
        """Get or create Spark session."""
        if self.spark is None:
            self.spark = SparkSession.builder \
                .appName("AprioriMining") \
                .master("local[*]") \
                .config("spark.driver.memory", "2g") \
                .getOrCreate()
        return self.spark
    
    
    def _apriori_partition(self, partition, support_threshold, total_count):
        """Local Apriori for a single partition (used in SON algorithm)."""
        partition = list(partition)
        partition_size = len(partition)
        
        # Adjusted local support
        local_support = math.ceil(support_threshold * partition_size / total_count)
        
        # Count 1-itemsets
        item_counts = {}
        for txn in partition:
            for item in txn:
                item_counts[item] = item_counts.get(item, 0) + 1
        
        freq_items = set(
            [frozenset([item]) for item, count in item_counts.items() if count >= local_support]
        )
        
        current_freq = freq_items
        all_freq = set(freq_items)
        k = 2
        
        while current_freq:
            candidates = set()
            freq_list = list(current_freq)
            
            # Generate candidates
            for i in range(len(freq_list)):
                for j in range(i + 1, len(freq_list)):
                    union = freq_list[i].union(freq_list[j])
                    if len(union) == k:
                        candidates.add(union)
            
            # Count candidates
            candidate_counts = {}
            for txn in partition:
                txn_set = set(txn)
                for candidate in candidates:
                    if candidate.issubset(txn_set):
                        candidate_counts[candidate] = candidate_counts.get(candidate, 0) + 1
            
            # Filter by local support
            current_freq = set(
                [c for c, count in candidate_counts.items() if count >= local_support]
            )
            
            all_freq.update(current_freq)
            k += 1
        
        return iter(all_freq)
    
    def run_distributed_son(self, transactions):
        """Run SON (Savasere-Omiecinski-Navathe) Apriori algorithm using PySpark."""
        spark = self._get_spark_session()
        sc = spark.sparkContext
        
        # Parallelize transactions
        rdd = sc.parallelize(transactions)
        total_count = rdd.count()
        
        if total_count == 0:
            return []
        
        # Phase 1: Local frequent itemsets (candidates)
        candidates_rdd = rdd.mapPartitions(
            lambda part: self._apriori_partition(part, self.min_support, total_count)
        )
        candidates = candidates_rdd.distinct().collect()
        
        # Convert to frozensets
        candidates = [frozenset(c) for c in candidates]
        
        # Broadcast candidates
        bc_candidates = sc.broadcast(candidates)
        
        # Phase 2: Global counting
        candidate_counts = rdd.flatMap(lambda txn: [
            (candidate, 1)
            for candidate in bc_candidates.value
            if candidate.issubset(set(txn))
        ]).reduceByKey(lambda a, b: a + b)
        
        # Filter by global support
        frequent_itemsets = candidate_counts \
            .filter(lambda x: x[1] / total_count >= self.min_support) \
            .map(lambda x: x[0]) \
            .collect()
        
        # Format results
        results = []
        for itemset in frequent_itemsets:
            results.append({
                'items': list(itemset),
                'support': sum(1 for txn in transactions if itemset.issubset(set(txn))) / total_count
            })
        
        return results
    
    def run_distributed(self, transactions):
        """Run distributed Apriori using PySpark."""
        return self.run_distributed_son(transactions)
    
    def generate_association_rules(self, frequent_itemsets, transactions, min_confidence=None):
        """Generate association rules from frequent itemsets."""
        if min_confidence is None:
            min_confidence = self.min_confidence
        
        rules = []
        total_transactions = len(transactions)
        transaction_sets = [set(txn) for txn in transactions]
        
        for itemset_info in frequent_itemsets:
            itemset = set(itemset_info['items'])
            itemset_count = itemset_info.get('support', 0) * total_transactions
            
            if len(itemset) < 2:
                continue
            
            # Generate all possible rules
            for i in range(1, len(itemset)):
                for antecedent in combinations(itemset, i):
                    antecedent = set(antecedent)
                    consequent = itemset - antecedent
                    
                    # Calculate confidence
                    antecedent_count = sum(1 for txn in transaction_sets if antecedent.issubset(txn))
                    if antecedent_count == 0:
                        continue
                    
                    confidence = itemset_count / antecedent_count
                    
                    if confidence >= min_confidence:
                        # Calculate lift
                        consequent_count = sum(1 for txn in transaction_sets if consequent.issubset(txn))
                        lift = (confidence * total_transactions) / consequent_count if consequent_count > 0 else 0
                        
                        rules.append({
                            'antecedents': list(antecedent),
                            'consequents': list(consequent),
                            'confidence': confidence,
                            'support': itemset_count / total_transactions,
                            'lift': lift
                        })
        
        # Sort by lift descending
        rules.sort(key=lambda x: x['lift'], reverse=True)
        
        return rules
    
    def __del__(self):
        """Clean up Spark session."""
        if self.spark:
            self.spark.stop()