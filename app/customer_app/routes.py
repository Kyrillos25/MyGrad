from flask import Blueprint, render_template, request, session, redirect, url_for, flash
from app import db
from app.models import Product, Order, OrderItem, Recommendation
from datetime import datetime
import uuid

customer_bp = Blueprint('customer_app', __name__)

# Cache for SQL Server prices to avoid repeated queries
_price_cache = {}
_price_cache_timestamp = None
_sql_server_available = False

# Helper function to get cart from session
def get_cart():
    return session.get('cart', {})

def get_product_price_from_sql(product):
    """Get product price from SQL Server Cleaned_data table, with fallback to product.price."""
    global _sql_server_available
    
    try:
        from data_mining.price_service import PriceService
        
        # Use cached prices if available (cache for 5 minutes)
        global _price_cache, _price_cache_timestamp
        from datetime import datetime as dt
        current_time = dt.now()
        
        if not _price_cache or not _price_cache_timestamp or (current_time - _price_cache_timestamp).total_seconds() > 300:
            # Refresh cache
            price_service = PriceService()
            _price_cache = price_service.get_all_product_prices()
            _price_cache_timestamp = current_time
            _sql_server_available = price_service.is_connected()
        
        # Try to get price from SQL Server
        product_name = product.name
        if product_name in _price_cache:
            return _price_cache[product_name]
        
        # Fallback to product's stored price
        return product.price
        
    except Exception as e:
        # Fallback to product's stored price if SQL Server fails
        _sql_server_available = False
        return product.price

def get_sql_server_status():
    """Get SQL Server connection status."""
    return _sql_server_available

def get_cart_total():
    cart = get_cart()
    total = 0
    for product_id, quantity in cart.items():
        product = Product.query.get(product_id)
        if product:
            price = get_product_price_from_sql(product)
            total += price * quantity
    return total

def get_cart_count():
    cart = get_cart()
    return sum(cart.values())

# Make cart available to all templates
@customer_bp.context_processor
def inject_cart():
    return dict(cart_count=get_cart_count(), cart_total=get_cart_total)

@customer_bp.route('/')
@customer_bp.route('/index')
def index():
    """Home page - product catalog."""
    products = Product.query.all()
    
    # Get prices from SQL Server for all products
    products_with_prices = []
    for product in products:
        price = get_product_price_from_sql(product)
        products_with_prices.append({
            'product': product,
            'price': price
        })
    
    return render_template('customer_app/index.html', 
                         products_with_prices=products_with_prices,
                         sql_server_available=get_sql_server_status())

@customer_bp.route('/product/<int:product_id>')
def product_detail(product_id):
    """Product detail page with recommendations."""
    product = Product.query.get_or_404(product_id)
    
    # Get price from SQL Server
    product_price = get_product_price_from_sql(product)
    
    # Get recommendations for this product
    recommendations = Recommendation.query.filter_by(product_id=product_id).all()
    recommended_products_with_prices = []
    for rec in recommendations[:4]:
        rec_price = get_product_price_from_sql(rec.recommended_with)
        recommended_products_with_prices.append({
            'product': rec.recommended_with,
            'price': rec_price
        })
    
    # Get similar products (no longer by category, just other products)
    similar_products_with_prices = []
    other_products = Product.query.filter(Product.id != product_id).limit(4).all()
    for sim_product in other_products:
        sim_price = get_product_price_from_sql(sim_product)
        similar_products_with_prices.append({
            'product': sim_product,
            'price': sim_price
        })
    
    return render_template('customer_app/product.html',
                         product=product,
                         product_price=product_price,
                         recommended_products=recommended_products_with_prices,
                         similar_products=similar_products_with_prices,
                         sql_server_available=get_sql_server_status())

@customer_bp.route('/cart')
def cart():
    """Shopping cart page."""
    cart = get_cart()
    cart_items = []
    
    for product_id, quantity in cart.items():
        product = Product.query.get(product_id)
        if product:
            price = get_product_price_from_sql(product)
            cart_items.append({
                'product': product,
                'quantity': quantity,
                'price': price,
                'subtotal': price * quantity
            })
    
    # Get cross-sell recommendations based on cart items
    cart_product_ids = list(cart.keys())
    cross_sell_products_with_prices = []
    if cart_product_ids:
        # Get products that are frequently bought with cart items
        cross_sell_products = Product.query.filter(
            Product.id.notin_(cart_product_ids)
        ).limit(4).all()
        for product in cross_sell_products:
            price = get_product_price_from_sql(product)
            cross_sell_products_with_prices.append({
                'product': product,
                'price': price
            })
    
    total = sum(item['subtotal'] for item in cart_items)
    
    return render_template('customer_app/cart.html',
                         cart_items=cart_items,
                         total=total,
                         cross_sell_products=cross_sell_products_with_prices,
                         sql_server_available=get_sql_server_status())

@customer_bp.route('/cart/add/<int:product_id>', methods=['POST'])
def add_to_cart(product_id):
    """Add product to cart."""
    product = Product.query.get_or_404(product_id)
    
    if 'cart' not in session:
        session['cart'] = {}
    
    cart = session['cart']
    product_id_str = str(product_id)
    
    if product_id_str in cart:
        cart[product_id_str] += 1
    else:
        cart[product_id_str] = 1
    
    session['cart'] = cart
    session.modified = True
    
    flash(f'{product.name} added to cart!', 'success')
    return redirect(request.referrer or url_for('customer_app.index'))

@customer_bp.route('/cart/remove/<int:product_id>', methods=['POST'])
def remove_from_cart(product_id):
    """Remove product from cart."""
    if 'cart' in session:
        cart = session['cart']
        product_id_str = str(product_id)
        
        if product_id_str in cart:
            del cart[product_id_str]
            session['cart'] = cart
            session.modified = True
            flash('Product removed from cart', 'info')
    
    return redirect(url_for('customer_app.cart'))

@customer_bp.route('/cart/update/<int:product_id>', methods=['POST'])
def update_cart(product_id):
    """Update product quantity in cart."""
    quantity = int(request.form.get('quantity', 1))
    
    if 'cart' in session:
        cart = session['cart']
        product_id_str = str(product_id)
        
        if product_id_str in cart:
            if quantity <= 0:
                del cart[product_id_str]
            else:
                cart[product_id_str] = quantity
            
            session['cart'] = cart
            session.modified = True
    
    return redirect(url_for('customer_app.cart'))

@customer_bp.route('/checkout', methods=['GET', 'POST'])
def checkout():
    """Checkout page."""
    cart = get_cart()
    
    if not cart:
        flash('Your cart is empty', 'warning')
        return redirect(url_for('customer_app.index'))
    
    if request.method == 'POST':
        # Process checkout
        customer_name = request.form.get('customer_name')
        customer_email = request.form.get('customer_email')
        
        if not customer_name:
            flash('Please enter your name', 'error')
            return render_template('customer_app/checkout.html')
        
        # Create order
        order = Order(
            order_number=f"ORD-{uuid.uuid4().hex[:8].upper()}",
            customer_name=customer_name,
            customer_email=customer_email,
            total_amount=get_cart_total(),
            status='completed'
        )
        
        db.session.add(order)
        db.session.commit()
        
        # Add order items with prices from SQL Server
        for product_id, quantity in cart.items():
            product = Product.query.get(product_id)
            if product:
                price = get_product_price_from_sql(product)
                order_item = OrderItem(
                    order_id=order.id,
                    product_id=product.id,
                    quantity=quantity,
                    price=price
                )
                db.session.add(order_item)
        
        db.session.commit()
        
        # Clear cart
        session.pop('cart', None)
        
        flash(f'Order {order.order_number} placed successfully!', 'success')
        return redirect(url_for('customer_app.order_confirmation', order_id=order.id))
    
    return render_template('customer_app/checkout.html')

@customer_bp.route('/order/<int:order_id>/confirmation')
def order_confirmation(order_id):
    """Order confirmation page."""
    order = Order.query.get_or_404(order_id)
    return render_template('customer_app/confirmation.html', order=order)

@customer_bp.route('/search')
def search():
    """Search products."""
    query = request.args.get('q', '')
    
    if query:
        products = Product.query.filter(
            (Product.name.ilike(f'%{query}%')) | 
            (Product.description.ilike(f'%{query}%'))
        ).all()
    else:
        products = Product.query.all()
    
    # Get prices from SQL Server
    products_with_prices = []
    for product in products:
        price = get_product_price_from_sql(product)
        products_with_prices.append({
            'product': product,
            'price': price
        })
    
    return render_template('customer_app/search.html', 
                         products_with_prices=products_with_prices, 
                         query=query,
                         sql_server_available=get_sql_server_status())
