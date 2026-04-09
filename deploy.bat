@echo off
echo ========================================
echo Market Basket Analysis Web Application
echo Deployment Script for Windows
echo ========================================
echo.

echo Step 1: Creating project structure...
if not exist "data\raw" mkdir "data\raw"
if not exist "data\processed" mkdir "data\processed"
if not exist "data\sample" mkdir "data\sample"
if not exist "static\css" mkdir "static\css"
if not exist "static\js" mkdir "static\js"
if not exist "static\images" mkdir "static\images"
echo Done!
echo.

echo Step 2: Creating virtual environment...
python -m venv venv
if errorlevel 1 (
    echo ERROR: Failed to create virtual environment
    echo Make sure Python is installed and in PATH
    pause
    exit /b 1
)
echo Done!
echo.

echo Step 3: Activating virtual environment...
call venv\Scripts\activate
echo Done!
echo.

echo Step 4: Installing dependencies...
pip install -r requirements.txt
if errorlevel 1 (
    echo ERROR: Failed to install dependencies
    pause
    exit /b 1
)
echo Done!
echo.

echo Step 5: Setting up environment variables...
(
    echo FLASK_APP=run.py
    echo FLASK_ENV=development
    echo SECRET_KEY=dev-secret-key-change-in-production
    echo DATABASE_URL=sqlite:///app.db
    echo JAVA_HOME=C:\Program Files\Java\jdk-17
) > .env
echo Done!
echo.

echo Step 6: Initializing database...
python -c "from run import app, db; app.app_context().push(); db.create_all()"
echo Done!
echo.

echo Step 7: Creating admin user...
python -c "from run import app, db; from app.models import User; app.app_context().push(); User.create_admin()"
echo Done!
echo.

echo Step 8: Adding sample products...
python -c "from run import app, db; app.app_context().push(); from app.models import Product; db.create_all(); from run import add_sample_products; add_sample_products()"
echo Done!
echo.

echo ========================================
echo Deployment Complete!
echo ========================================
echo.
echo To start the application:
echo   1. Activate virtual environment: venv\Scripts\activate
echo   2. Run the application: python run.py
echo   3. Open browser: http://localhost:5000/
echo.
echo Admin Login:
echo   Username: admin
echo   Password: admin123
echo.
echo Manager Dashboard: http://localhost:5000/admin/
echo.
pause