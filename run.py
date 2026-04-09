import os
from app import create_app, db
from app.models import User, Product, MiningResult, Order, OrderItem

# Set up environment variables for PySpark
os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jdk-17"
os.environ["PYSPARK_PYTHON"] = "python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python"

app = create_app()

@app.shell_context_processor
def make_shell_context():
    return {
        'db': db,
        'User': User,
        'Product': Product,
        'MiningResult': MiningResult,
        'Order': Order,
        'OrderItem': OrderItem
    }

@app.cli.command()
def init_db():
    """Initialize the database and create admin user."""
    with app.app_context():
        db.create_all()
        User.create_admin()
        print('Database initialized. Admin user created.')
        print('Login credentials: username=admin, password=admin123')

@app.cli.command()
def add_sample_products():
    """Add sample products to the catalog."""
    from app.models import Product
    
    sample_products = [
        Product(name="Bread", category="Bakery", price=2.50, stock_code="BRD001",
                description="Fresh white bread loaf"),
        Product(name="Jam", category="Condiments", price=3.00, stock_code="JAM001",
                description="Strawberry jam jar"),
        Product(name="Pasta", category="Dry Goods", price=1.50, stock_code="PST001",
                description="Spaghetti pasta 500g"),
        Product(name="Pasta Sauce", category="Condiments", price=2.75, stock_code="SAC001",
                description="Tomato pasta sauce"),
        Product(name="Candy", category="Snacks", price=1.00, stock_code="CND001",
                description="Assorted candy pack"),
        Product(name="Gum", category="Snacks", price=0.75, stock_code="GUM001",
                description="Chewing gum pack"),
        Product(name="Honey", category="Condiments", price=4.50, stock_code="HON001",
                description="Pure honey jar"),
        Product(name="BBQ Sauce", category="Condiments", price=3.25, stock_code="BBQ001",
                description="Barbecue sauce bottle"),
        Product(name="Soda", category="Beverages", price=1.25, stock_code="SOD001",
                description="Cola soda 500ml"),
        Product(name="Olive Oil", category="Oils", price=6.00, stock_code="OIL001",
                description="Extra virgin olive oil"),
        Product(name="Butter", category="Dairy", price=3.50, stock_code="BUT001",
                description="Salted butter 250g"),
        Product(name="Milk", category="Dairy", price=2.00, stock_code="MLK001",
                description="Whole milk 1L"),
        Product(name="Eggs", category="Dairy", price=4.00, stock_code="EGG001",
                description="Dozen eggs"),
        Product(name="Cheese", category="Dairy", price=5.50, stock_code="CHS001",
                description="Cheddar cheese block"),
        Product(name="Coffee", category="Beverages", price=8.00, stock_code="COF001",
                description="Ground coffee 250g"),
    ]
    
    with app.app_context():
        for product in sample_products:
            existing = Product.query.filter_by(stock_code=product.stock_code).first()
            if not existing:
                db.session.add(product)
        
        db.session.commit()
        print(f'Added {len(sample_products)} sample products.')

if __name__ == '__main__':
    # Initialize database and create admin user on first run
    with app.app_context():
        db.create_all()
        User.create_admin()
    
    # Run the application
    app.run(debug=True, host='0.0.0.0', port=5000)