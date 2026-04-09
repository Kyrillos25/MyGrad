import os
from datetime import timedelta

basedir = os.path.abspath(os.path.dirname(__file__))

class Config:
    # Secret key for session management and CSRF protection
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'dev-secret-key-change-in-production'
    
    # SQL Server database configuration
    SQLALCHEMY_DATABASE_URI = os.environ.get('DATABASE_URL') or \
        'mssql+pyodbc://localhost/TestDB?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes'
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    
    # File upload configuration
    UPLOAD_FOLDER = os.path.join(basedir, 'data', 'raw')
    MAX_CONTENT_LENGTH = 128 * 1024 * 1024  # 128MB max file size (increased for large CSV files)
    ALLOWED_EXTENSIONS = {'csv', 'txt'}
    
    # PySpark Configuration
    SPARK_APP_NAME = "MarketBasketAnalysis"
    SPARK_MASTER = "local[*]"
    
    # Session Configuration
    PERMANENT_SESSION_LIFETIME = timedelta(minutes=30)
    
    # SQL Server Configuration (for your existing data)
    # Using SQL Server Authentication
    SQL_SERVER_CONFIG = {
        'driver': '{ODBC Driver 17 for SQL Server}',
        'server': os.environ.get('SQL_SERVER', 'localhost'),
        'database': os.environ.get('SQL_DATABASE', 'TestDB'),
        'username': os.environ.get('SQL_USERNAME', 'sa'),
        'password': os.environ.get('SQL_PASSWORD', ''),
        'trusted_connection': 'no'  # Use SQL Server Authentication
    }
    
    # Data Mining Parameters
    DEFAULT_MIN_SUPPORT = 0.01
    DEFAULT_MIN_CONFIDENCE = 0.5
    MIN_SUPPORT_OPTIONS = [0.001, 0.005, 0.01, 0.02, 0.05, 0.1]
    MIN_CONFIDENCE_OPTIONS = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8]