"""
Recommendation Engine Module
Generates product recommendations from mining results.
"""

from collections import defaultdict
import json

class RecommendationEngine:
    def __init__(self):
        """Initialize recommendation engine."""
        self.recommendations = {}
        self.frequent_itemsets = []
        self.association_rules = []
    
    def load_mining_results(self, frequent_itemsets, association_rules=None):
        """Load mining results from FP-Growth or Apriori."""
        self.frequent_itemsets = frequent_itemsets
        self.association_rules = association_rules or []
        self._generate_recommendations()
    
    def _generate_recommendations(self):
        """Generate recommendations from frequent itemsets."""
        self.recommendations = {}
        
        # Build recommendations from frequent itemsets
        for itemset_info in self.frequent_itemsets:
            items = itemset_info.get('items', [])
            support = itemset_info.get('support', 0)
            freq = itemset_info.get('freq', 0)
            
            if len(items) < 2:
                continue
            
            # For each item, recommend other items in the same itemset
            for item in items:
                if item not in self.recommendations:
                    self.recommendations[item] = []
                
                other_items = [i for i in items if i != item]
                for other_item in other_items:
                    # Check if recommendation already exists
                    exists = False
                    for rec in self.recommendations[item]:
                        if rec['product'] == other_item:
                            # Update if higher support
                            if support > rec.get('support', 0):
                                rec['support'] = support
                                rec['freq'] = freq
                                rec['items_together'] = items
                            exists = True
                            break
                    
                    if not exists:
                        self.recommendations[item].append({
                            'product': other_item,
                            'support': support,
                            'freq': freq,
                            'items_together': items
                        })
        
        # Sort recommendations by support
        for item in self.recommendations:
            self.recommendations[item].sort(key=lambda x: x['support'], reverse=True)
    
    def get_recommendations_for_product(self, product_name, top_n=5):
        """Get top N recommendations for a specific product."""
        if product_name in self.recommendations:
            return self.recommendations[product_name][:top_n]
        return []
    
    def get_frequently_bought_together(self, product_name, top_n=3):
        """Get 'frequently bought together' recommendations."""
        return self.get_recommendations_for_product(product_name, top_n)
    
    def get_similar_products(self, product_name, top_n=5):
        """Get similar products based on co-occurrence."""
        recommendations = self.get_recommendations_for_product(product_name, top_n)
        return [rec['product'] for rec in recommendations]
    
    def get_association_rules_for_product(self, product_name, top_n=5):
        """Get association rules involving a specific product."""
        rules = []
        for rule in self.association_rules:
            antecedents = rule.get('antecedents', [])
            consequents = rule.get('consequents', [])
            
            if product_name in antecedents or product_name in consequents:
                rules.append(rule)
        
        # Sort by lift
        rules.sort(key=lambda x: x.get('lift', 0), reverse=True)
        return rules[:top_n]
    
    def get_bundle_suggestions(self, min_items=2, max_items=5, top_n=10):
        """Get product bundle suggestions for promotions."""
        bundles = []
        
        for itemset_info in self.frequent_itemsets:
            items = itemset_info.get('items', [])
            support = itemset_info.get('support', 0)
            freq = itemset_info.get('freq', 0)
            
            if min_items <= len(items) <= max_items:
                bundles.append({
                    'items': items,
                    'support': support,
                    'freq': freq
                })
        
        # Sort by support
        bundles.sort(key=lambda x: x['support'], reverse=True)
        return bundles[:top_n]
    
    def get_cross_sell_recommendations(self, cart_items, top_n=5):
        """Get cross-sell recommendations based on cart items."""
        recommendations = defaultdict(float)
        
        for item in cart_items:
            item_recs = self.get_recommendations_for_product(item, top_n=10)
            for rec in item_recs:
                rec_product = rec['product']
                if rec_product not in cart_items:
                    recommendations[rec_product] += rec['support']
        
        # Sort by aggregated support
        sorted_recs = sorted(recommendations.items(), key=lambda x: x[1], reverse=True)
        
        return [
            {'product': product, 'score': score}
            for product, score in sorted_recs[:top_n]
        ]
    
    def get_checkout_impulse_items(self, top_n=5):
        """Get impulse buy items for checkout display."""
        # Get items that frequently appear with small basket sizes
        impulse_candidates = []
        
        for itemset_info in self.frequent_itemsets:
            items = itemset_info.get('items', [])
            support = itemset_info.get('support', 0)
            
            # Items in small baskets (2-3 items) are good impulse candidates
            if 2 <= len(items) <= 3:
                for item in items:
                    impulse_candidates.append({
                        'product': item,
                        'support': support
                    })
        
        # Aggregate and sort
        product_scores = defaultdict(float)
        for candidate in impulse_candidates:
            product_scores[candidate['product']] += candidate['support']
        
        sorted_products = sorted(product_scores.items(), key=lambda x: x[1], reverse=True)
        return [
            {'product': product, 'score': score}
            for product, score in sorted_products[:top_n]
        ]
    
    def to_dict(self):
        """Convert recommendations to dictionary format."""
        return {
            'recommendations': self.recommendations,
            'frequent_itemsets': self.frequent_itemsets,
            'association_rules': self.association_rules
        }
    
    def to_json(self):
        """Convert recommendations to JSON string."""
        return json.dumps(self.to_dict(), indent=2)
    
    def load_from_json(self, json_str):
        """Load recommendations from JSON string."""
        data = json.loads(json_str)
        self.recommendations = data.get('recommendations', {})
        self.frequent_itemsets = data.get('frequent_itemsets', [])
        self.association_rules = data.get('association_rules', [])