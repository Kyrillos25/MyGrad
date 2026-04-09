"""
Distributed Apriori Algorithm Implementation
Implements the SON (Savasere, Omiecinski, Navathe) algorithm for distributed frequent itemset mining
"""

import itertools
from collections import defaultdict, Counter
from typing import List, Set, Tuple, Dict, Generator
import time
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd

class DistributedApriori:
    """Distributed Apriori implementation using SON algorithm"""
    
    def __init__(self, spark_session: SparkSession, min_support: float = 0.01, min_confidence: float = 0.5):
        """
        Initialize Distributed Apriori
        
        Args:
            spark_session: PySpark session
            min_support: Minimum support threshold (0.0 to 1.0)
            min_confidence: Minimum confidence threshold (0.0 to 1.0)
        """
        self.spark = spark_session
        self.min_support = min_support
        self.min_confidence = min_confidence
        self.logger = logging.getLogger(__name__)
        
        # Results storage
        self.frequent_itemsets = []
        self.association_rules = []
        self.transaction_count = 0
        
    def load_transactions_from_spark(self, transactions_df):
        """Load transactions from Spark DataFrame"""
        try:
            # Convert Spark DataFrame to list of itemsets
            transactions_rdd = transactions_df.select("items").rdd.map(lambda row: set(row[0]))
            self.transactions = transactions_rdd.collect()
            self.transaction_count = len(self.transactions)
            
            self.logger.info(f"Loaded {self.transaction_count} transactions")
            return True
            
        except Exception as e:
            self.logger.error(f"Error loading transactions: {e}")
            return False
    
    def load_transactions_from_list(self, transactions: List[List[str]]):
        """Load transactions from Python list"""
        self.transactions = [set(transaction) for transaction in transactions]
        self.transaction_count = len(self.transactions)
        
        self.logger.info(f"Loaded {self.transaction_count} transactions from list")
        return True
    
    def calculate_support(self, itemset: Set[str]) -> float:
        """Calculate support for an itemset"""
        count = sum(1 for transaction in self.transactions if itemset.issubset(transaction))
        return count / self.transaction_count
    
    def get_frequent_itemsets_son(self, transactions_df, num_partitions: int = 8) -> List[Tuple[Set[str], float]]:
        """
        SON Algorithm for distributed frequent itemset mining
        
        Phase 1: Find local frequent itemsets in each partition
        Phase 2: Combine candidates and find global frequent itemsets
        """
        try:
            self.logger.info("Starting SON Algorithm...")
            
            # Phase 1: Find local frequent itemsets
            local_frequent_itemsets = self._phase1_local_mining(transactions_df, num_partitions)
            
            # Phase 2: Combine candidates and find global frequent itemsets
            global_frequent_itemsets = self._phase2_global_mining(local_frequent_itemsets)
            
            self.frequent_itemsets = global_frequent_itemsets
            self.logger.info(f"Found {len(global_frequent_itemsets)} frequent itemsets")
            
            return global_frequent_itemsets
            
        except Exception as e:
            self.logger.error(f"SON Algorithm failed: {e}")
            return []
    
    def _phase1_local_mining(self, transactions_df, num_partitions: int) -> List[Set[str]]:
        """Phase 1: Find local frequent itemsets in each partition"""
        try:
            # Repartition data
            partitioned_df = transactions_df.repartition(num_partitions)
            
            # Find local frequent itemsets using map-reduce
            local_candidates = partitioned_df.rdd \
                .mapPartitions(self._find_local_frequent_itemsets_partition) \
                .flatMap(lambda x: x) \
                .distinct() \
                .collect()
            
            self.logger.info(f"Phase 1 completed. Found {len(local_candidates)} local candidates")
            return local_candidates
            
        except Exception as e:
            self.logger.error(f"Phase 1 failed: {e}")
            return []
    
    def _find_local_frequent_itemsets_partition(self, partition) -> Generator[Set[str], None, None]:
        """Find frequent itemsets in a single partition"""
        local_transactions = [set(row.items) for row in partition]
        local_transaction_count = len(local_transactions)
        
        if local_transaction_count == 0:
            return
        
        # Calculate local support threshold
        local_min_support = self.min_support * local_transaction_count
        
        # Phase 1a: Find frequent 1-itemsets
        item_counts = defaultdict(int)
        for transaction in local_transactions:
            for item in transaction:
                item_counts[frozenset([item])] += 1
        
        frequent_1_itemsets = {itemset for itemset, count in item_counts.items() 
                              if count >= local_min_support}
        
        if not frequent_1_itemsets:
            return
        
        # Phase 1b: Generate and test larger itemsets
        current_frequent = frequent_1_itemsets
        k = 2
        
        while current_frequent:
            # Generate candidates of size k
            candidates = self._generate_candidates(current_frequent, k)
            
            if not candidates:
                break
            
            # Count candidates
            candidate_counts = defaultdict(int)
            for transaction in local_transactions:
                for candidate in candidates:
                    if candidate.issubset(transaction):
                        candidate_counts[candidate] += 1
            
            # Filter frequent candidates
            new_frequent = {candidate for candidate, count in candidate_counts.items() 
                           if count >= local_min_support}
            
            # Yield frequent itemsets
            for itemset in new_frequent:
                yield itemset
            
            current_frequent = new_frequent
            k += 1
    
    def _generate_candidates(self, frequent_itemsets: Set[frozenset], k: int) -> Set[frozenset]:
        """Generate candidate itemsets of size k"""
        candidates = set()
        
        for itemset1 in frequent_itemsets:
            for itemset2 in frequent_itemsets:
                # Check if we can join these itemsets
                union = itemset1 | itemset2
                if len(union) == k:
                    # Check if all subsets of size k-1 are frequent
                    valid = True
                    for subset in itertools.combinations(union, k-1):
                        if frozenset(subset) not in frequent_itemsets:
                            valid = False
                            break
                    
                    if valid:
                        candidates.add(frozenset(union))
        
        return candidates
    
    def _phase2_global_mining(self, local_candidates: List[Set[str]]) -> List[Tuple[Set[str], float]]:
        """Phase 2: Count global support for candidates"""
        try:
            if not local_candidates:
                return []
            
            # Convert to RDD for distributed counting
            candidates_rdd = self.spark.sparkContext.parallelize(local_candidates)
            
            # Count global support
            global_supports = candidates_rdd \
                .map(lambda candidate: (candidate, self._count_global_support(candidate))) \
                .filter(lambda x: x[1] >= self.min_support) \
                .collect()
            
            return global_supports
            
        except Exception as e:
            self.logger.error(f"Phase 2 failed: {e}")
            return []
    
    def _count_global_support(self, candidate: Set[str]) -> float:
        """Count global support for a candidate itemset"""
        count = sum(1 for transaction in self.transactions if candidate.issubset(transaction))
        return count / self.transaction_count
    
    def generate_association_rules(self) -> List[Dict]:
        """Generate association rules from frequent itemsets"""
        try:
            rules = []
            
            for itemset, support in self.frequent_itemsets:
                if len(itemset) < 2:
                    continue
                
                # Generate all possible non-empty subsets
                for i in range(1, len(itemset)):
                    for antecedent in itertools.combinations(itemset, i):
                        antecedent = frozenset(antecedent)
                        consequent = itemset - antecedent
                        
                        if len(consequent) == 0:
                            continue
                        
                        # Calculate confidence
                        antecedent_support = self._get_itemset_support(antecedent)
                        if antecedent_support > 0:
                            confidence = support / antecedent_support
                            
                            # Calculate lift
                            consequent_support = self._get_itemset_support(consequent)
                            if consequent_support > 0:
                                lift = confidence / consequent_support
                            else:
                                lift = float('inf')
                            
                            # Filter by confidence threshold
                            if confidence >= self.min_confidence:
                                rule = {
                                    'antecedent': list(antecedent),
                                    'consequent': list(consequent),
                                    'support': support,
                                    'confidence': confidence,
                                    'lift': lift,
                                    'antecedent_support': antecedent_support,
                                    'consequent_support': consequent_support
                                }
                                rules.append(rule)
            
            # Sort rules by confidence and lift
            rules.sort(key=lambda x: (x['confidence'], x['lift']), reverse=True)
            self.association_rules = rules
            
            self.logger.info(f"Generated {len(rules)} association rules")
            return rules
            
        except Exception as e:
            self.logger.error(f"Rule generation failed: {e}")
            return []
    
    def _get_itemset_support(self, itemset: Set[str]) -> float:
        """Get support for an itemset from stored frequent itemsets"""
        for frequent_itemset, support in self.frequent_itemsets:
            if frequent_itemset == itemset:
                return support
        return self.calculate_support(itemset)
    
    def get_top_rules(self, n: int = 10) -> List[Dict]:
        """Get top N association rules by confidence and lift"""
        if not self.association_rules:
            self.generate_association_rules()
        
        return self.association_rules[:n]
    
    def get_frequent_itemsets_by_size(self, size: int) -> List[Tuple[Set[str], float]]:
        """Get frequent itemsets of specific size"""
        return [(itemset, support) for itemset, support in self.frequent_itemsets 
                if len(itemset) == size]
    
    def print_results(self):
        """Print frequent itemsets and association rules"""
        print("\n" + "="*60)
        print("DISTRIBUTED APRIORI RESULTS")
        print("="*60)
        
        # Print frequent itemsets
        print(f"\nFREQUENT ITEMSETS (min_support = {self.min_support}):")
        print("-" * 50)
        
        itemsets_by_size = {}
        for itemset, support in self.frequent_itemsets:
            size = len(itemset)
            if size not in itemsets_by_size:
                itemsets_by_size[size] = []
            itemsets_by_size[size].append((itemset, support))
        
        for size in sorted(itemsets_by_size.keys()):
            print(f"\n{size}-itemsets ({len(itemsets_by_size[size])} found):")
            for itemset, support in sorted(itemsets_by_size[size], key=lambda x: x[1], reverse=True)[:10]:
                items = sorted(list(itemset))
                print(f"  {items} (support: {support:.4f})")
        
        # Print association rules
        print(f"\nASSOCIATION RULES (min_confidence = {self.min_confidence}):")
        print("-" * 50)
        
        if self.association_rules:
            for i, rule in enumerate(self.association_rules[:10], 1):
                antecedent = sorted(rule['antecedent'])
                consequent = sorted(rule['consequent'])
                print(f"\n{i}. {antecedent} → {consequent}")
                print(f"   Support: {rule['support']:.4f}")
                print(f"   Confidence: {rule['confidence']:.4f}")
                print(f"   Lift: {rule['lift']:.4f}")
        else:
            print("No association rules found with current thresholds.")
    
    def save_results(self, output_path: str):
        """Save results to JSON file"""
        results = {
            'parameters': {
                'min_support': self.min_support,
                'min_confidence': self.min_confidence,
                'transaction_count': self.transaction_count
            },
            'frequent_itemsets': [
                {
                    'itemset': sorted(list(itemset)),
                    'support': support
                } for itemset, support in self.frequent_itemsets
            ],
            'association_rules': self.association_rules
        }
        
        import json
        with open(output_path, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        self.logger.info(f"Results saved to {output_path}")


class TraditionalApriori:
    """Traditional Apriori algorithm for comparison"""
    
    def __init__(self, min_support: float = 0.01, min_confidence: float = 0.5):
        self.min_support = min_support
        self.min_confidence = min_confidence
        self.logger = logging.getLogger(__name__)
        
    def load_transactions(self, transactions: List[List[str]]):
        """Load transactions"""
        self.transactions = [set(transaction) for transaction in transactions]
        self.transaction_count = len(self.transactions)
        
    def find_frequent_itemsets(self) -> List[Tuple[Set[str], float]]:
        """Traditional Apriori algorithm"""
        frequent_itemsets = []
        
        # Generate 1-itemsets
        item_counts = Counter()
        for transaction in self.transactions:
            for item in transaction:
                item_counts[frozenset([item])] += 1
        
        # Filter frequent 1-itemsets
        min_count = self.min_support * self.transaction_count
        current_frequent = {itemset for itemset, count in item_counts.items() 
                           if count >= min_count}
        
        # Add to results
        for itemset in current_frequent:
            support = item_counts[itemset] / self.transaction_count
            frequent_itemsets.append((set(itemset), support))
        
        k = 2
        while current_frequent:
            # Generate candidates
            candidates = self._generate_candidates_traditional(current_frequent, k)
            
            if not candidates:
                break
            
            # Count candidates
            candidate_counts = Counter()
            for transaction in self.transactions:
                for candidate in candidates:
                    if candidate.issubset(transaction):
                        candidate_counts[candidate] += 1
            
            # Filter frequent candidates
            new_frequent = {candidate for candidate, count in candidate_counts.items() 
                           if count >= min_count}
            
            # Add to results
            for itemset in new_frequent:
                support = candidate_counts[itemset] / self.transaction_count
                frequent_itemsets.append((set(itemset), support))
            
            current_frequent = new_frequent
            k += 1
        
        return frequent_itemsets
    
    def _generate_candidates_traditional(self, frequent_itemsets: Set[frozenset], k: int) -> Set[frozenset]:
        """Generate candidates for traditional Apriori"""
        candidates = set()
        frequent_list = list(frequent_itemsets)
        
        for i in range(len(frequent_list)):
            for j in range(i + 1, len(frequent_list)):
                itemset1, itemset2 = frequent_list[i], frequent_list[j]
                union = itemset1 | itemset2
                
                if len(union) == k:
                    # Check if all subsets are frequent
                    valid = True
                    for subset in itertools.combinations(union, k-1):
                        if frozenset(subset) not in frequent_itemsets:
                            valid = False
                            break
                    
                    if valid:
                        candidates.add(frozenset(union))
        
        return candidates


if __name__ == "__main__":
    # Example usage
    logging.basicConfig(level=logging.INFO)
    
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("DistributedApriori") \
        .master("local[*]") \
        .getOrCreate()
    
    # Create sample data
    sample_transactions = [
        ['A', 'B', 'C'],
        ['A', 'B'],
        ['A', 'C'],
        ['B', 'C'],
        ['A', 'B', 'C', 'D']
    ]
    
    # Test distributed Apriori
    dist_apriori = DistributedApriori(spark, min_support=0.4, min_confidence=0.6)
    dist_apriori.load_transactions_from_list(sample_transactions)
    
    # For distributed version, you would use:
    # transactions_df = spark.createDataFrame([{'items': items} for items in sample_transactions])
    # dist_apriori.get_frequent_itemsets_son(transactions_df)
    
    # Test traditional Apriori
    trad_apriori = TraditionalApriori(min_support=0.4, min_confidence=0.6)
    trad_apriori.load_transactions(sample_transactions)
    frequent_itemsets = trad_apriori.find_frequent_itemsets()
    
    print("Traditional Apriori Results:")
    for itemset, support in frequent_itemsets:
        print(f"{sorted(list(itemset))}: {support:.2f}")
    
    spark.stop()