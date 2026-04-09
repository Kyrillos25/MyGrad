from flask import Blueprint, jsonify
from app import db
from app.models import Product, Recommendation, MiningResult

api_bp = Blueprint('api', __name__)

@api_bp.route('/products', methods=['GET'])
def get_products():
    """API endpoint to get all products."""
    products = Product.query.all()
    return jsonify([{
        'id': p.id,
        'name': p.name,
        'category': p.category,
        'price': p.price,
        'description': p.description
    } for p in products])

@api_bp.route('/products/<int:product_id>', methods=['GET'])
def get_product(product_id):
    """API endpoint to get a single product."""
    product = Product.query.get_or_404(product_id)
    return jsonify({
        'id': product.id,
        'name': product.name,
        'category': product.category,
        'price': product.price,
        'description': product.description
    })

@api_bp.route('/products/<int:product_id>/recommendations', methods=['GET'])
def get_product_recommendations(product_id):
    """API endpoint to get recommendations for a product."""
    recommendations = Recommendation.query.filter_by(product_id=product_id).all()
    return jsonify([{
        'product_id': rec.recommended_with_id,
        'product_name': rec.recommended_with.name,
        'support': rec.support,
        'confidence': rec.confidence
    } for rec in recommendations])

@api_bp.route('/mining/results', methods=['GET'])
def get_mining_results():
    """API endpoint to get recent mining results."""
    results = MiningResult.query.order_by(MiningResult.created_at.desc()).limit(10).all()
    return jsonify([{
        'id': r.id,
        'algorithm': r.algorithm,
        'created_at': r.created_at.isoformat(),
        'created_by': r.created_by
    } for r in results])