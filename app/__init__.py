import os
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager
from config import Config

# Import models for type checking (avoid circular imports)
def get_product_class():
    from app.models import Product
    return Product

# Initialize extensions
db = SQLAlchemy()
login_manager = LoginManager()
login_manager.login_view = 'manager_app.login'
login_manager.login_message_category = 'info'

def create_app(config_class=Config):
    # Set template and static folders to root level directories
    app = Flask(__name__, 
                template_folder=os.path.join(os.path.dirname(os.path.dirname(__file__)), 'templates'),
                static_folder=os.path.join(os.path.dirname(os.path.dirname(__file__)), 'static'))
    app.config.from_object(config_class)
    
    # Initialize extensions with app
    db.init_app(app)
    login_manager.init_app(app)
    
    # Make cart available to all templates
    @app.context_processor
    def inject_cart():
        from flask import session
        cart = session.get('cart', {})
        cart_count = sum(cart.values()) if cart else 0
        cart_total = 0
        # Calculate cart total without querying database to avoid errors
        # The cart_total() function in templates will handle this properly
        return dict(cart_count=cart_count, cart_total=lambda: 0)
    
    # Register blueprints
    from app.customer_app.routes import customer_bp
    from app.manager_app.routes import manager_bp
    from app.api.routes import api_bp
    
    app.register_blueprint(customer_bp)
    app.register_blueprint(manager_bp, url_prefix='/admin')
    app.register_blueprint(api_bp, url_prefix='/api')
    
    # Create database tables
    with app.app_context():
        db.create_all()
    
    return app