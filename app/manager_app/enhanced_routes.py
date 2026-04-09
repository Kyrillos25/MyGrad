"""
Enhanced Manager Routes for Complete Data Pipeline and Mining
Integrates distributed data pipeline, Apriori, FP-Growth, and recommendation engine
"""

from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from werkzeug.utils import secure_filename
import os
import pandas as pd
from datetime import datetime
import logging
from config import Config
from ..models import Product, db
from ..data_mining.distributed_pipeline import DistributedDataPipeline
from ..data_mining.distributed_apriori import DistributedApriori, TraditionalApriori
from ..data_mining.enhanced_fpgrowth import EnhancedFPGrowth
from ..data_mining.recommendation_engine import RecommendationEngine, RealTimeRecommender
from ..data_mining.market_basket_preparation import MarketBasketDataPreparator
import json

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create blueprint
enhanced_manager_bp = Blueprint('enhanced_manager', __name__, template_folder='../../templates/manager_app')

# Global instances for the enhanced system
data_pipeline = None
apriori_engine = None
fp_growth_engine = None
recommendation_engine = None
real_time_recommender = None


def initialize_engines():
    """Initialize all mining engines"""
    global data_pipeline, apriori_engine, fp_growth_engine, recommendation_engine, real_time_recommender
    
    if data_pipeline is None:
        data_pipeline = DistributedDataPipeline()
        data_pipeline.initialize_spark()
    
    if apriori_engine is None:
        apriori_engine = DistributedApriori(data_pipeline.spark)
    
    if fp_growth_engine is None:
        fp_growth_engine = EnhancedFPGrowth(data_pipeline.spark)
    
    if recommendation_engine is None:
        recommendation_engine = RecommendationEngine()
        real_time_recommender = RealTimeRecommender(recommendation_engine)


@enhanced_manager_bp.route('/enhanced_dashboard')
def enhanced_dashboard():
    """Enhanced dashboard showing complete system status"""
    initialize_engines()
    
    # Get system status
    status = {
        'data_pipeline': 'Connected' if data_pipeline and data_pipeline.spark else 'Disconnected',
        'apriori': 'Ready' if apriori_engine else 'Not Initialized',
        'fp_growth': 'Ready' if fp_growth_engine else 'Not Initialized',
        'recommendations': 'Ready' if recommendation_engine else 'Not Initialized'
    }
    
    # Get transaction statistics
    stats = {}
    try:
        transactions_df = data_pipeline.get_cleaned_data_from_spark()
        if transactions_df:
            stats = data_pipeline.get_transaction_stats(transactions_df)
    except Exception as e:
        logger.error(f"Error getting stats: {e}")
    
    return render_template('manager_app/enhanced_dashboard.html', 
                         status=status, stats=stats)


@enhanced_manager_bp.route('/enhanced_upload', methods=['GET', 'POST'])
def enhanced_upload():
    """Enhanced CSV upload with complete pipeline processing"""
    if request.method == 'POST':
        if 'file' not in request.files:
            flash('No file selected')
            return redirect(request.url)
        
        file = request.files['file']
        if file.filename == '':
            flash('No file selected')
            return redirect(request.url)
        
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            filepath = os.path.join(Config.UPLOAD_FOLDER, filename)
            
            # Save file
            file.save(filepath)
            
            try:
                # Process with complete pipeline
                initialize_engines()
                
                # Run complete pipeline
                result = data_pipeline.process_csv_pipeline(filepath)
                
                if result:
                    flash(f'File processed successfully! {result["total_transactions"]} transactions found.')
                    
                    # Get transactions for mining
                    transactions = data_pipeline.get_transactions_for_mining()
                    
                    # Run both algorithms
                    run_mining_algorithms(transactions)
                    
                    return redirect(url_for('enhanced_manager.enhanced_dashboard'))
                else:
                    flash('Pipeline processing failed')
                    
            except Exception as e:
                logger.error(f"Upload error: {e}")
                flash(f'Error processing file: {str(e)}')
    
    return render_template('manager_app/enhanced_upload.html')


@enhanced_manager_bp.route('/run_mining', methods=['POST'])
def run_mining():
    """Run mining algorithms on current data"""
    try:
        initialize_engines()
        
        # Get transactions
        transactions = data_pipeline.get_transactions_for_mining()
        
        if not transactions:
            flash('No transactions found. Please upload data first.')
            return redirect(url_for('enhanced_manager.enhanced_dashboard'))
        
        # Run mining
        run_mining_algorithms(transactions)
        
        flash('Mining completed successfully!')
        return redirect(url_for('enhanced_manager.mining_results'))
        
    except Exception as e:
        logger.error(f"Mining error: {e}")
        flash(f'Mining failed: {str(e)}')
        return redirect(url_for('enhanced_manager.enhanced_dashboard'))


def run_mining_algorithms(transactions):
    """Run both Apriori and FP-Growth algorithms"""
    global apriori_engine, fp_growth_engine, recommendation_engine
    
    # Convert transactions to Spark DataFrame
    transactions_data = [{'items': items} for items in transactions]
    transactions_df = data_pipeline.spark.createDataFrame(transactions_data)
    
    # Run FP-Growth
    logger.info("Running FP-Growth...")
    fp_growth_engine.load_transactions_from_spark(transactions_df)
    fp_growth_engine.run_fpgrowth()
    
    # Run Apriori (traditional version for comparison)
    logger.info("Running Apriori...")
    trad_apriori = TraditionalApriori(min_support=0.01, min_confidence=0.5)
    trad_apriori.load_transactions(transactions)
    frequent_itemsets = trad_apriori.find_frequent_itemsets()
    
    # Generate association rules for Apriori (simplified)
    apriori_rules = []
    for itemset, support in frequent_itemsets:
        if len(itemset) >= 2:
            # Generate simple rules
            for i in range(1, len(itemset)):
                for antecedent in combinations(itemset, i):
                    antecedent = list(antecedent)
                    consequent = list(itemset - set(antecedent))
                    apriori_rules.append({
                        'antecedent': antecedent,
                        'consequent': consequent,
                        'confidence': 0.5,  # Simplified
                        'lift': 1.5,  # Simplified
                        'support': support,
                        'algorithm': 'apriori'
                    })
    
    # Update recommendation engine
    fp_growth_rules = []
    if fp_growth_engine.association_rules_df:
        fp_growth_rules = fp_growth_engine.association_rules_df.collect()
        fp_growth_rules = [
            {
                'antecedent': row.antecedent,
                'consequent': row.consequent,
                'confidence': float(row.confidence),
                'lift': float(row.lift),
                'support': float(row.support),
                'algorithm': 'fp_growth'
            } for row in fp_growth_rules
        ]
    
    recommendation_engine = RecommendationEngine(apriori_rules, fp_growth_rules)
    real_time_recommender = RealTimeRecommender(recommendation_engine)
    
    logger.info(f"Generated {len(apriori_rules)} Apriori rules and {len(fp_growth_rules)} FP-Growth rules")


@enhanced_manager_bp.route('/mining_results')
def mining_results():
    """Display mining results from both algorithms"""
    initialize_engines()
    
    results = {
        'apriori': {},
        'fp_growth': {},
        'recommendations': []
    }
    
    # Get FP-Growth results
    if fp_growth_engine and fp_growth_engine.frequent_itemsets_df:
        # Get frequent itemsets by size
        for size in range(1, 6):
            itemsets = fp_growth_engine.get_frequent_itemsets_by_size(size)
            if len(itemsets) > 0:
                results['fp_growth'][f'{size}_itemsets'] = itemsets.to_dict('records')[:10]
        
        # Get top rules
        top_rules = fp_growth_engine.get_top_rules(10)
        results['fp_growth']['top_rules'] = top_rules.to_dict('records')
        
        # Get statistics
        results['fp_growth']['statistics'] = fp_growth_engine.get_rule_statistics()
    
    # Get recommendation examples
    if recommendation_engine:
        sample_recommendations = recommendation_engine.get_recommendations(['A'], top_n=5)
        results['recommendations'] = [
            {
                'product': rec.product,
                'confidence': rec.confidence,
                'lift': rec.lift,
                'score': rec.score
            } for rec in sample_recommendations
        ]
    
    return render_template('manager_app/mining_results.html', results=results)


@enhanced_manager_bp.route('/recommendations')
def recommendations_page():
    """Display recommendation system interface"""
    initialize_engines()
    
    # Sample products for demo
    sample_products = ['WHITE HANGING HEART T-LIGHT HOLDER', 'WHITE METAL LANTERN', 'JUMBO BAG RED RETROSPOT']
    
    recommendations = []
    if recommendation_engine:
        recommendations = recommendation_engine.get_recommendations(sample_products, top_n=5)
    
    return render_template('manager_app/recommendations.html', 
                         recommendations=recommendations,
                         sample_products=sample_products)


@enhanced_manager_bp.route('/api/recommend', methods=['POST'])
def api_recommend():
    """API endpoint for getting recommendations"""
    try:
        data = request.get_json()
        products = data.get('products', [])
        method = data.get('method', 'hybrid')
        top_n = data.get('top_n', 5)
        
        initialize_engines()
        
        if not recommendation_engine:
            return jsonify({'error': 'Recommendation engine not initialized'}), 400
        
        recommendations = recommendation_engine.get_recommendations(
            products, method=method, top_n=top_n
        )
        
        result = [
            {
                'product': rec.product,
                'confidence': rec.confidence,
                'lift': rec.lift,
                'score': rec.score,
                'reason': f"Customers who bought {', '.join(rec.rule_antecedent)} also bought {rec.product}"
            } for rec in recommendations
        ]
        
        return jsonify({'recommendations': result})
        
    except Exception as e:
        logger.error(f"API recommendation error: {e}")
        return jsonify({'error': str(e)}), 500


@enhanced_manager_bp.route('/api/cart_recommend', methods=['POST'])
def api_cart_recommend():
    """API endpoint for cart recommendations"""
    try:
        data = request.get_json()
        cart_items = data.get('cart_items', [])
        
        initialize_engines()
        
        if not real_time_recommender:
            return jsonify({'error': 'Real-time recommender not initialized'}), 400
        
        recommendations = real_time_recommender.get_cart_recommendations(cart_items)
        
        return jsonify(recommendations)
        
    except Exception as e:
        logger.error(f"API cart recommendation error: {e}")
        return jsonify({'error': str(e)}), 500


@enhanced_manager_bp.route('/api/product_recommend/<product>')
def api_product_recommend(product):
    """API endpoint for product page recommendations"""
    try:
        initialize_engines()
        
        if not real_time_recommender:
            return jsonify({'error': 'Real-time recommender not initialized'}), 400
        
        recommendations = real_time_recommender.get_product_page_recommendations(product)
        
        return jsonify(recommendations)
        
    except Exception as e:
        logger.error(f"API product recommendation error: {e}")
        return jsonify({'error': str(e)}), 500


@enhanced_manager_bp.route('/system_status')
def system_status():
    """Get detailed system status"""
    initialize_engines()
    
    status = {
        'timestamp': datetime.now().isoformat(),
        'components': {
            'data_pipeline': {
                'status': 'Connected' if data_pipeline and data_pipeline.spark else 'Disconnected',
                'spark_session': str(data_pipeline.spark) if data_pipeline and data_pipeline.spark else None
            },
            'apriori_engine': {
                'status': 'Ready' if apriori_engine else 'Not Initialized',
                'min_support': apriori_engine.min_support if apriori_engine else None,
                'min_confidence': apriori_engine.min_confidence if apriori_engine else None
            },
            'fp_growth_engine': {
                'status': 'Ready' if fp_growth_engine else 'Not Initialized',
                'min_support': fp_growth_engine.min_support if fp_growth_engine else None,
                'min_confidence': fp_growth_engine.min_confidence if fp_growth_engine else None
            },
            'recommendation_engine': {
                'status': 'Ready' if recommendation_engine else 'Not Initialized',
                'apriori_rules': len(recommendation_engine.apriori_rules) if recommendation_engine else 0,
                'fp_growth_rules': len(recommendation_engine.fp_growth_rules) if recommendation_engine else 0
            }
        }
    }
    
    return jsonify(status)


@enhanced_manager_bp.route('/export_results')
def export_results():
    """Export mining results"""
    initialize_engines()
    
    export_data = {
        'timestamp': datetime.now().isoformat(),
        'data_pipeline_stats': {},
        'fp_growth_results': {},
        'recommendations': []
    }
    
    # Export FP-Growth results
    if fp_growth_engine:
        export_data['fp_growth_results'] = {
            'frequent_itemsets_count': fp_growth_engine.frequent_itemsets_df.count() if fp_growth_engine.frequent_itemsets_df else 0,
            'association_rules_count': fp_growth_engine.association_rules_df.count() if fp_growth_engine.association_rules_df else 0,
            'statistics': fp_growth_engine.get_rule_statistics()
        }
    
    # Export recommendations
    if recommendation_engine:
        sample_recs = recommendation_engine.get_recommendations(['A'], top_n=10)
        export_data['recommendations'] = [
            {
                'product': rec.product,
                'confidence': rec.confidence,
                'lift': rec.lift,
                'score': rec.score,
                'algorithm': rec.algorithm
            } for rec in sample_recs
        ]
    
    return jsonify(export_data)


def allowed_file(filename):
    """Check if file extension is allowed"""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in Config.ALLOWED_EXTENSIONS


# Import required for combinations
from itertools import combinations