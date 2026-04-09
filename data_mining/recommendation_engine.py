"""
Advanced Recommendation Engine for Market Basket Analysis
Provides product recommendations based on association rules from Apriori and FP-Growth
"""

import pandas as pd
import numpy as np
from typing import List, Dict, Tuple, Set, Union
from collections import defaultdict, Counter
import logging
from dataclasses import dataclass
import json
from datetime import datetime


@dataclass
class Recommendation:
    """Data class for recommendations"""
    product: str
    confidence: float
    lift: float
    support: float
    rule_antecedent: List[str]
    rule_consequent: List[str]
    algorithm: str  # 'apriori' or 'fp_growth'
    score: float = 0.0  # Combined score for ranking


class RecommendationEngine:
    """Advanced recommendation engine based on association rules"""
    
    def __init__(self, apriori_rules: List[Dict] = None, fp_growth_rules: List[Dict] = None):
        """
        Initialize recommendation engine
        
        Args:
            apriori_rules: List of association rules from Apriori
            fp_growth_rules: List of association rules from FP-Growth
        """
        self.apriori_rules = apriori_rules or []
        self.fp_growth_rules = fp_growth_rules or []
        self.logger = logging.getLogger(__name__)
        
        # Build recommendation indexes for fast lookup
        self._build_indexes()
        
    def _build_indexes(self):
        """Build indexes for fast recommendation lookup"""
        self.antecedent_index = defaultdict(list)
        self.consequent_index = defaultdict(list)
        self.product_similarity = defaultdict(dict)
        
        # Index Apriori rules
        for rule in self.apriori_rules:
            antecedent_key = tuple(sorted(rule['antecedent']))
            self.antecedent_index[antecedent_key].append(rule)
            
            for item in rule['consequent']:
                self.consequent_index[item].append(rule)
        
        # Index FP-Growth rules
        for rule in self.fp_growth_rules:
            antecedent_key = tuple(sorted(rule['antecedent']))
            self.antecedent_index[antecedent_key].append(rule)
            
            for item in rule['consequent']:
                self.consequent_index[item].append(rule)
        
        # Build product similarity matrix
        self._build_similarity_matrix()
        
        self.logger.info(f"Built indexes with {len(self.antecedent_index)} antecedent patterns")
    
    def _build_similarity_matrix(self):
        """Build product similarity matrix based on co-occurrence"""
        # Count co-occurrences
        cooccurrence = defaultdict(lambda: defaultdict(int))
        total_transactions = 0
        
        # Process Apriori rules
        for rule in self.apriori_rules:
            antecedent = set(rule['antecedent'])
            consequent = set(rule['consequent'])
            
            # Count antecedent items
            for item1 in antecedent:
                for item2 in antecedent:
                    if item1 != item2:
                        cooccurrence[item1][item2] += rule['support']
            
            # Count consequent items
            for item1 in consequent:
                for item2 in consequent:
                    if item1 != item2:
                        cooccurrence[item1][item2] += rule['support']
            
            # Count cross items
            for item1 in antecedent:
                for item2 in consequent:
                    cooccurrence[item1][item2] += rule['support']
                    cooccurrence[item2][item1] += rule['support']
        
        # Process FP-Growth rules
        for rule in self.fp_growth_rules:
            antecedent = set(rule['antecedent'])
            consequent = set(rule['consequent'])
            
            # Count antecedent items
            for item1 in antecedent:
                for item2 in antecedent:
                    if item1 != item2:
                        cooccurrence[item1][item2] += rule['support']
            
            # Count consequent items
            for item1 in consequent:
                for item2 in consequent:
                    if item1 != item2:
                        cooccurrence[item1][item2] += rule['support']
            
            # Count cross items
            for item1 in antecedent:
                for item2 in consequent:
                    cooccurrence[item1][item2] += rule['support']
                    cooccurrence[item2][item1] += rule['support']
        
        # Calculate similarity scores
        all_products = set(cooccurrence.keys())
        for product1 in all_products:
            for product2 in all_products:
                if product1 != product2:
                    # Jaccard similarity with support weighting
                    similarity = cooccurrence[product1][product2] / (
                        cooccurrence[product1][product1] + cooccurrence[product2][product2] - 
                        cooccurrence[product1][product2]
                    )
                    self.product_similarity[product1][product2] = similarity
        
        self.logger.info(f"Built similarity matrix for {len(all_products)} products")
    
    def get_recommendations(self, products: List[str], 
                          method: str = 'association_rules',
                          top_n: int = 10,
                          min_confidence: float = 0.1,
                          min_lift: float = 1.0) -> List[Recommendation]:
        """
        Get product recommendations based on input products
        
        Args:
            products: List of products in the cart
            method: Recommendation method ('association_rules', 'similarity', 'hybrid')
            top_n: Number of recommendations to return
            min_confidence: Minimum confidence threshold
            min_lift: Minimum lift threshold
        """
        if not products:
            return []
        
        recommendations = []
        
        if method == 'association_rules':
            recommendations = self._get_association_recommendations(products, min_confidence, min_lift)
        elif method == 'similarity':
            recommendations = self._get_similarity_recommendations(products, top_n)
        elif method == 'hybrid':
            recommendations = self._get_hybrid_recommendations(products, min_confidence, min_lift, top_n)
        else:
            raise ValueError(f"Unknown method: {method}")
        
        # Sort by score and return top N
        recommendations.sort(key=lambda x: x.score, reverse=True)
        return recommendations[:top_n]
    
    def _get_association_recommendations(self, products: List[str], 
                                       min_confidence: float, min_lift: float) -> List[Recommendation]:
        """Get recommendations based on association rules"""
        recommendations = []
        product_set = set(products)
        
        # Look for exact matches
        antecedent_key = tuple(sorted(products))
        if antecedent_key in self.antecedent_index:
            for rule in self.antecedent_index[antecedent_key]:
                if rule['confidence'] >= min_confidence and rule['lift'] >= min_lift:
                    for item in rule['consequent']:
                        if item not in product_set:
                            score = self._calculate_rule_score(rule)
                            recommendations.append(Recommendation(
                                product=item,
                                confidence=rule['confidence'],
                                lift=rule['lift'],
                                support=rule['support'],
                                rule_antecedent=rule['antecedent'],
                                rule_consequent=rule['consequent'],
                                algorithm=rule.get('algorithm', 'unknown'),
                                score=score
                            ))
        
        # Look for subset matches (any subset of products)
        from itertools import combinations
        for r in range(1, len(products) + 1):
            for subset in combinations(products, r):
                subset_key = tuple(sorted(subset))
                if subset_key in self.antecedent_index:
                    for rule in self.antecedent_index[subset_key]:
                        if rule['confidence'] >= min_confidence and rule['lift'] >= min_lift:
                            for item in rule['consequent']:
                                if item not in product_set:
                                    score = self._calculate_rule_score(rule)
                                    recommendations.append(Recommendation(
                                        product=item,
                                        confidence=rule['confidence'],
                                        lift=rule['lift'],
                                        support=rule['support'],
                                        rule_antecedent=rule['antecedent'],
                                        rule_consequent=rule['consequent'],
                                        algorithm=rule.get('algorithm', 'unknown'),
                                        score=score
                                    ))
        
        # Remove duplicates and keep best score
        unique_recs = {}
        for rec in recommendations:
            if rec.product not in unique_recs or rec.score > unique_recs[rec.product].score:
                unique_recs[rec.product] = rec
        
        return list(unique_recs.values())
    
    def _get_similarity_recommendations(self, products: List[str], top_n: int) -> List[Recommendation]:
        """Get recommendations based on product similarity"""
        recommendations = []
        product_set = set(products)
        
        # Calculate similarity scores for all other products
        similarity_scores = defaultdict(float)
        
        for product in products:
            if product in self.product_similarity:
                for similar_product, similarity in self.product_similarity[product].items():
                    if similar_product not in product_set:
                        similarity_scores[similar_product] += similarity
        
        # Create recommendations
        for product, score in similarity_scores.items():
            recommendations.append(Recommendation(
                product=product,
                confidence=score,
                lift=1.0,  # Not applicable for similarity
                support=score,
                rule_antecedent=products,
                rule_consequent=[product],
                algorithm='similarity',
                score=score
            ))
        
        return recommendations
    
    def _get_hybrid_recommendations(self, products: List[str], 
                                  min_confidence: float, min_lift: float, 
                                  top_n: int) -> List[Recommendation]:
        """Get hybrid recommendations combining association rules and similarity"""
        # Get association-based recommendations
        assoc_recs = self._get_association_recommendations(products, min_confidence, min_lift)
        
        # Get similarity-based recommendations
        sim_recs = self._get_similarity_recommendations(products, top_n)
        
        # Combine and weight them
        combined_recs = {}
        
        # Add association recommendations with higher weight
        for rec in assoc_recs:
            combined_recs[rec.product] = rec
            # Boost score for association-based recommendations
            combined_recs[rec.product].score *= 1.5
        
        # Add similarity recommendations
        for rec in sim_recs:
            if rec.product not in combined_recs:
                combined_recs[rec.product] = rec
            else:
                # Blend scores
                combined_recs[rec.product].score = (
                    combined_recs[rec.product].score * 0.7 + 
                    rec.score * 0.3
                )
        
        return list(combined_recs.values())
    
    def _calculate_rule_score(self, rule: Dict) -> float:
        """Calculate combined score for a rule"""
        # Weighted combination of confidence, lift, and support
        confidence_weight = 0.4
        lift_weight = 0.4
        support_weight = 0.2
        
        score = (
            rule['confidence'] * confidence_weight +
            min(rule['lift'] / 10, 1.0) * lift_weight +  # Normalize lift
            rule['support'] * support_weight
        )
        
        return score
    
    def get_basket_recommendations(self, basket: List[str], 
                                 strategy: str = 'complementary') -> List[Recommendation]:
        """
        Get recommendations for a shopping basket
        
        Args:
            basket: Current items in basket
            strategy: Recommendation strategy ('complementary', 'alternative', 'upsell')
        """
        if strategy == 'complementary':
            return self.get_recommendations(basket, method='hybrid', top_n=5)
        elif strategy == 'alternative':
            return self._get_alternative_recommendations(basket)
        elif strategy == 'upsell':
            return self._get_upsell_recommendations(basket)
        else:
            return self.get_recommendations(basket, method='hybrid', top_n=5)
    
    def _get_alternative_recommendations(self, products: List[str]) -> List[Recommendation]:
        """Get alternative product recommendations (substitutes)"""
        alternatives = []
        
        for product in products:
            if product in self.product_similarity:
                # Get most similar products
                similar_products = sorted(
                    self.product_similarity[product].items(),
                    key=lambda x: x[1],
                    reverse=True
                )[:5]
                
                for alt_product, similarity in similar_products:
                    alternatives.append(Recommendation(
                        product=alt_product,
                        confidence=similarity,
                        lift=1.0,
                        support=similarity,
                        rule_antecedent=[product],
                        rule_consequent=[alt_product],
                        algorithm='similarity',
                        score=similarity
                    ))
        
        return alternatives
    
    def _get_upsell_recommendations(self, products: List[str]) -> List[Recommendation]:
        """Get upsell recommendations (premium versions)"""
        # This would typically require product hierarchy data
        # For now, return high-value association rules
        recommendations = self.get_recommendations(products, method='association_rules', top_n=3)
        
        # Filter for high-value items (this would need price data)
        # For demonstration, we'll just return the top recommendations
        return recommendations
    
    def get_personalized_recommendations(self, user_id: str, 
                                       user_history: List[List[str]],
                                       current_basket: List[str]) -> List[Recommendation]:
        """Get personalized recommendations based on user history"""
        # Combine user history patterns with current basket
        all_products = set(current_basket)
        for transaction in user_history:
            all_products.update(transaction)
        
        # Get recommendations based on combined history
        recommendations = self.get_recommendations(list(all_products), method='hybrid', top_n=10)
        
        # Filter out items already in current basket
        current_set = set(current_basket)
        recommendations = [rec for rec in recommendations if rec.product not in current_set]
        
        return recommendations
    
    def get_trending_recommendations(self, time_window_days: int = 30) -> List[Recommendation]:
        """Get trending recommendations based on recent rules"""
        # This would require temporal data
        # For now, return rules with highest confidence
        all_rules = self.apriori_rules + self.fp_growth_rules
        trending_rules = sorted(all_rules, key=lambda x: x['confidence'], reverse=True)[:10]
        
        recommendations = []
        for rule in trending_rules:
            for item in rule['consequent']:
                recommendations.append(Recommendation(
                    product=item,
                    confidence=rule['confidence'],
                    lift=rule['lift'],
                    support=rule['support'],
                    rule_antecedent=rule['antecedent'],
                    rule_consequent=rule['consequent'],
                    algorithm=rule.get('algorithm', 'unknown'),
                    score=rule['confidence']
                ))
        
        return recommendations
    
    def evaluate_recommendations(self, test_transactions: List[List[str]], 
                               top_n: int = 5) -> Dict:
        """Evaluate recommendation quality using test data"""
        correct_predictions = 0
        total_predictions = 0
        total_possible = 0
        
        for transaction in test_transactions:
            if len(transaction) < 2:
                continue
            
            # Use first half as input, second half as test
            split_point = len(transaction) // 2
            input_products = transaction[:split_point]
            test_products = transaction[split_point:]
            
            # Get recommendations
            recommendations = self.get_recommendations(input_products, top_n=top_n)
            recommended_products = [rec.product for rec in recommendations]
            
            # Count correct predictions
            for test_product in test_products:
                if test_product in recommended_products:
                    correct_predictions += 1
            
            total_predictions += len(recommended_products)
            total_possible += len(test_products)
        
        # Calculate metrics
        precision = correct_predictions / total_predictions if total_predictions > 0 else 0
        recall = correct_predictions / total_possible if total_possible > 0 else 0
        f1_score = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0
        
        return {
            'precision': precision,
            'recall': recall,
            'f1_score': f1_score,
            'correct_predictions': correct_predictions,
            'total_predictions': total_predictions,
            'total_possible': total_possible
        }
    
    def save_model(self, filepath: str):
        """Save the recommendation model"""
        model_data = {
            'apriori_rules': self.apriori_rules,
            'fp_growth_rules': self.fp_growth_rules,
            'product_similarity': dict(self.product_similarity),
            'created_at': datetime.now().isoformat()
        }
        
        with open(filepath, 'w') as f:
            json.dump(model_data, f, indent=2, default=str)
        
        self.logger.info(f"Model saved to {filepath}")
    
    def load_model(self, filepath: str):
        """Load a saved recommendation model"""
        with open(filepath, 'r') as f:
            model_data = json.load(f)
        
        self.apriori_rules = model_data.get('apriori_rules', [])
        self.fp_growth_rules = model_data.get('fp_growth_rules', [])
        self.product_similarity = defaultdict(dict, model_data.get('product_similarity', {}))
        
        # Rebuild indexes
        self._build_indexes()
        
        self.logger.info(f"Model loaded from {filepath}")


class RealTimeRecommender:
    """Real-time recommendation system for web applications"""
    
    def __init__(self, recommendation_engine: RecommendationEngine):
        self.engine = recommendation_engine
        self.logger = logging.getLogger(__name__)
    
    def get_cart_recommendations(self, cart_items: List[str]) -> Dict:
        """Get real-time recommendations for shopping cart"""
        recommendations = self.engine.get_recommendations(
            cart_items, 
            method='hybrid', 
            top_n=5,
            min_confidence=0.3,
            min_lift=1.5
        )
        
        return {
            'recommendations': [
                {
                    'product': rec.product,
                    'confidence': rec.confidence,
                    'lift': rec.lift,
                    'score': rec.score,
                    'reason': f"Customers who bought {', '.join(rec.rule_antecedent)} also bought {rec.product}"
                } for rec in recommendations
            ],
            'timestamp': datetime.now().isoformat()
        }
    
    def get_product_page_recommendations(self, product: str) -> Dict:
        """Get recommendations for a product page"""
        recommendations = self.engine.get_recommendations(
            [product], 
            method='association_rules', 
            top_n=4,
            min_confidence=0.2
        )
        
        return {
            'product': product,
            'recommendations': [
                {
                    'product': rec.product,
                    'confidence': rec.confidence,
                    'lift': rec.lift,
                    'score': rec.score,
                    'reason': f"Often purchased with {product}"
                } for rec in recommendations
            ],
            'timestamp': datetime.now().isoformat()
        }


if __name__ == "__main__":
    # Example usage
    logging.basicConfig(level=logging.INFO)
    
    # Sample association rules
    sample_apriori_rules = [
        {
            'antecedent': ['A', 'B'],
            'consequent': ['C'],
            'confidence': 0.8,
            'lift': 2.5,
            'support': 0.15,
            'algorithm': 'apriori'
        },
        {
            'antecedent': ['A'],
            'consequent': ['D'],
            'confidence': 0.6,
            'lift': 1.8,
            'support': 0.2,
            'algorithm': 'apriori'
        }
    ]
    
    sample_fp_growth_rules = [
        {
            'antecedent': ['B', 'C'],
            'consequent': ['E'],
            'confidence': 0.7,
            'lift': 3.0,
            'support': 0.12,
            'algorithm': 'fp_growth'
        }
    ]
    
    # Create recommendation engine
    engine = RecommendationEngine(sample_apriori_rules, sample_fp_growth_rules)
    
    # Get recommendations
    products = ['A', 'B']
    recommendations = engine.get_recommendations(products, method='hybrid', top_n=3)
    
    print("Recommendations for products", products, ":")
    for rec in recommendations:
        print(f"  {rec.product}: confidence={rec.confidence:.2f}, lift={rec.lift:.2f}, score={rec.score:.2f}")
    
    # Test real-time recommender
    real_time = RealTimeRecommender(engine)
    cart_recs = real_time.get_cart_recommendations(['A', 'B'])
    print("\nReal-time cart recommendations:")
    print(json.dumps(cart_recs, indent=2))