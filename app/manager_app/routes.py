from flask import Blueprint, render_template, request, redirect, url_for, flash, session, jsonify
from flask_login import login_user, logout_user, login_required, current_user
from werkzeug.utils import secure_filename
from app import db
from app.models import User, Product, MiningResult, Recommendation
from config import Config
import os
import json
from datetime import datetime

manager_bp = Blueprint('manager_app', __name__)

# Allowed file extensions for upload
ALLOWED_EXTENSIONS = {'csv', 'txt'}

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@manager_bp.route('/login', methods=['GET', 'POST'])
def login():
    """Admin login page."""
    if current_user.is_authenticated:
        return redirect(url_for('manager_app.dashboard'))
    
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        
        user = User.query.filter_by(username=username).first()
        
        if user and user.check_password(password):
            login_user(user)
            return redirect(url_for('manager_app.dashboard'))
        
        flash('Invalid username or password', 'error')
    
    return render_template('manager_app/login.html')

@manager_bp.route('/logout')
@login_required
def logout():
    """Admin logout."""
    logout_user()
    return redirect(url_for('manager_app.login'))

@manager_bp.route('/')
@manager_bp.route('/dashboard')
@login_required
def dashboard():
    """Admin dashboard."""
    # Get statistics
    product_count = Product.query.count()
    recommendation_count = Recommendation.query.count()
    recent_results = MiningResult.query.order_by(MiningResult.created_at.desc()).limit(5).all()
    
    return render_template('manager_app/dashboard.html',
                         product_count=product_count,
                         recommendation_count=recommendation_count,
                         recent_results=recent_results)

@manager_bp.route('/upload', methods=['GET', 'POST'])
@login_required
def upload():
    """Upload transaction data."""
    if request.method == 'POST':
        # Check if file is present
        if 'file' not in request.files:
            flash('No file selected', 'error')
            return redirect(request.url)
        
        file = request.files['file']
        
        if file.filename == '':
            flash('No file selected', 'error')
            return redirect(request.url)
        
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            
            # Create upload directory if it doesn't exist
            upload_dir = Config.UPLOAD_FOLDER
            os.makedirs(upload_dir, exist_ok=True)
            
            filepath = os.path.join(upload_dir, filename)
            file.save(filepath)
            
            flash(f'File {filename} uploaded successfully!', 'success')
            
            # Store file path in session for processing
            session['uploaded_file'] = filepath
            
            return redirect(url_for('manager_app.process_upload'))
        
        flash('File type not allowed', 'error')
    
    return render_template('manager_app/upload.html')

@manager_bp.route('/process_upload', methods=['GET', 'POST'])
@login_required
def process_upload():
    """Process uploaded file."""
    filepath = session.get('uploaded_file')
    
    if not filepath or not os.path.exists(filepath):
        flash('No file to process', 'error')
        return redirect(url_for('manager_app.upload'))
    
    if request.method == 'POST':
        try:
            # Import data pipeline
            from data_mining.data_pipeline import DataPipeline
            
            # Use direct CSV processing instead of database
            pipeline = DataPipeline()
            transactions = pipeline.process_csv_direct(filepath)
            
            if transactions:
                # Extract unique products from transactions
                all_products = set()
                for transaction in transactions:
                    all_products.update(transaction)
                
                # Clear existing products and add new ones from CSV
                Product.query.delete()
                db.session.commit()
                
                # Add products from CSV data
                for i, product_name in enumerate(all_products):
                    # Create a simple product entry
                    product = Product(
                        name=product_name,
                        category='General',  # Default category
                        price=10.0,  # Default price
                        description=f'Product from uploaded data',
                        stock_code=f'CSV-{i+1:05d}',
                        image_url=''
                    )
                    db.session.add(product)
                
                db.session.commit()
                
                flash(f'Data processed successfully! Created {len(all_products)} products from CSV.', 'success')
                session.pop('uploaded_file', None)
            else:
                flash('No valid transactions found in the CSV file.', 'error')
        except Exception as e:
            flash(f'Error processing data: {str(e)}', 'error')
        
        return redirect(url_for('manager_app.dashboard'))
    
    return render_template('manager_app/process_upload.html', filepath=filepath)

@manager_bp.route('/mining', methods=['GET', 'POST'])
@login_required
def mining():
    """Run data mining algorithms."""
    if request.method == 'POST':
        algorithm = request.form.get('algorithm', 'fpgrowth')
        min_support = float(request.form.get('min_support', 0.01))
        min_confidence = float(request.form.get('min_confidence', 0.5))
        
        # Import mining modules
        from data_mining.data_pipeline import DataPipeline
        from data_mining.fpgrowth import FPGrowthMiner
        from data_mining.apriori import AprioriMiner
        from data_mining.recommendations import RecommendationEngine
        
        try:
            # Check if we have an uploaded file to process
            filepath = session.get('uploaded_file')
            
            # If no file in session, check if we have products from CSV processing
            if not filepath or not os.path.exists(filepath):
                # Check if we have products in the database (indicating CSV was processed)
                product_count = Product.query.count()
                if product_count == 0:
                    flash('No data available. Please upload a CSV file first.', 'warning')
                    return redirect(url_for('manager_app.upload'))
                
                # Try to find the original uploaded file
                upload_dir = Config.UPLOAD_FOLDER
                if os.path.exists(upload_dir):
                    csv_files = [f for f in os.listdir(upload_dir) if f.endswith('.csv')]
                    if csv_files:
                        filepath = os.path.join(upload_dir, csv_files[0])
                    else:
                        flash('CSV file not found. Please re-upload your data.', 'warning')
                        return redirect(url_for('manager_app.upload'))
                else:
                    flash('Upload directory not found. Please re-upload your data.', 'warning')
                    return redirect(url_for('manager_app.upload'))
            
            # Load transactions directly from CSV
            pipeline = DataPipeline()
            transactions = pipeline.process_csv_direct(filepath)
            
            if not transactions:
                flash('No valid transactions found in the CSV file.', 'warning')
                return redirect(url_for('manager_app.upload'))
            
            # Run selected algorithm with proper Spark session management
            if algorithm == 'fpgrowth':
                miner = FPGrowthMiner(min_support=min_support, min_confidence=min_confidence)
                try:
                    # Try distributed first, fallback to local if it fails
                    results = miner.run_distributed(transactions)
                except Exception as e:
                    flash(f'FP-Growth mining failed: {str(e)}', 'error')
                    return redirect(url_for('manager_app.mining'))
            elif algorithm == 'apriori':
                miner = AprioriMiner(min_support=min_support, min_confidence=min_confidence)
                try:
                    results = miner.run_distributed(transactions)
                except Exception as e:
                    flash(f'Apriori mining failed: {str(e)}', 'error')
                    return redirect(url_for('manager_app.mining'))
            else:
                flash('Invalid algorithm selected', 'error')
                return redirect(url_for('manager_app.mining'))
            
            # Save results to database
            mining_result = MiningResult(
                algorithm=algorithm,
                parameters=json.dumps({
                    'min_support': min_support,
                    'min_confidence': min_confidence
                }),
                results=json.dumps(results),
                created_by=current_user.username
            )
            db.session.add(mining_result)
            db.session.commit()
            
            # Generate recommendations
            if results.get('frequent_itemsets'):
                rec_engine = RecommendationEngine()
                rec_engine.load_mining_results(
                    results['frequent_itemsets'],
                    results.get('association_rules', [])
                )
                
                # Save recommendations to database
                for product_name, recs in rec_engine.recommendations.items():
                    for rec in recs:
                        # Find product IDs
                        product = Product.query.filter_by(name=product_name).first()
                        rec_product = Product.query.filter_by(name=rec['product']).first()
                        
                        if product and rec_product:
                            recommendation = Recommendation(
                                product_id=product.id,
                                recommended_with_id=rec_product.id,
                                confidence=rec.get('support', 0),
                                support=rec.get('support', 0),
                                lift=1.0  # Could be calculated from rules
                            )
                            db.session.add(recommendation)
                
                db.session.commit()
            
            flash(f'{algorithm.upper()} mining completed successfully!', 'success')
            return redirect(url_for('manager_app.results', result_id=mining_result.id))
            
        except Exception as e:
            flash(f'Error running mining: {str(e)}', 'error')
    
    return render_template('manager_app/mining.html',
                         min_support_options=Config.MIN_SUPPORT_OPTIONS,
                         min_confidence_options=Config.MIN_CONFIDENCE_OPTIONS)

@manager_bp.route('/results')
@login_required
def results():
    """View mining results."""
    result_id = request.args.get('result_id')
    
    if result_id:
        result = MiningResult.query.get_or_404(result_id)
        return render_template('manager_app/results.html', result=result)
    
    # Show all results
    results = MiningResult.query.order_by(MiningResult.created_at.desc()).all()
    return render_template('manager_app/results.html', results=results)

@manager_bp.route('/results/<int:result_id>/export')
@login_required
def export_results(result_id):
    """Export mining results."""
    result = MiningResult.query.get_or_404(result_id)
    format_type = request.args.get('format', 'json')
    
    if format_type == 'json':
        response_data = {
            'algorithm': result.algorithm,
            'parameters': json.loads(result.parameters),
            'results': json.loads(result.results),
            'created_at': result.created_at.isoformat()
        }
        
        return jsonify(response_data)
    
    elif format_type == 'csv':
        # Generate CSV
        results_data = json.loads(result.results)
        csv_content = "items,freq,support\n"
        
        for itemset in results_data.get('frequent_itemsets', []):
            items = ','.join(itemset['items'])
            freq = itemset['freq']
            support = itemset.get('support', 0)
            csv_content += f'"{items}",{freq},{support}\n'
        
        return csv_content, 200, {
            'Content-Type': 'text/csv',
            'Content-Disposition': f'attachment; filename=mining_results_{result_id}.csv'
        }
    
    flash('Invalid export format', 'error')
    return redirect(url_for('manager_app.results'))

@manager_bp.route('/products')
@login_required
def products():
    """Product catalog management."""
    products = Product.query.all()
    return render_template('manager_app/products.html', products=products)

@manager_bp.route('/products/add', methods=['GET', 'POST'])
@login_required
def add_product():
    """Add new product."""
    if request.method == 'POST':
        product = Product(
            name=request.form.get('name'),
            category=request.form.get('category'),
            price=float(request.form.get('price')),
            description=request.form.get('description'),
            stock_code=request.form.get('stock_code'),
            image_url=request.form.get('image_url', '')
        )
        
        db.session.add(product)
        db.session.commit()
        
        flash('Product added successfully!', 'success')
        return redirect(url_for('manager_app.products'))
    
    return render_template('manager_app/product_form.html')

@manager_bp.route('/products/<int:product_id>/edit', methods=['GET', 'POST'])
@login_required
def edit_product(product_id):
    """Edit product."""
    product = Product.query.get_or_404(product_id)
    
    if request.method == 'POST':
        product.name = request.form.get('name')
        product.category = request.form.get('category')
        product.price = float(request.form.get('price'))
        product.description = request.form.get('description')
        product.stock_code = request.form.get('stock_code')
        product.image_url = request.form.get('image_url', product.image_url)
        
        db.session.commit()
        
        flash('Product updated successfully!', 'success')
        return redirect(url_for('manager_app.products'))
    
    return render_template('manager_app/product_form.html', product=product)

@manager_bp.route('/products/<int:product_id>/delete', methods=['POST'])
@login_required
def delete_product(product_id):
    """Delete product."""
    product = Product.query.get_or_404(product_id)
    db.session.delete(product)
    db.session.commit()
    
    flash('Product deleted successfully!', 'success')
    return redirect(url_for('manager_app.products'))

@manager_bp.route('/recommendations')
@login_required
def recommendations():
    """View recommendations."""
    recommendations = Recommendation.query.all()
    return render_template('manager_app/recommendations.html', recommendations=recommendations)