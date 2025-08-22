@echo off
echo ============================================================
echo   Customer Purchase Behavior Analysis - Quick Start
echo ============================================================
echo.

echo 🚀 Welcome to the Customer Purchase Behavior Analysis project!
echo.
echo This script will help you get started quickly.
echo.

:menu
echo Please choose an option:
echo.
echo 1. Setup environment and install dependencies
echo 2. Run complete analysis (download data + analyze + visualize)
echo 3. Run data ingestion only
echo 4. Run analysis on existing data
echo 5. Open Jupyter notebook
echo 6. View project structure
echo 7. Exit
echo.
set /p choice="Enter your choice (1-7): "

if "%choice%"=="1" (
    echo.
    echo 📦 Setting up environment...
    python setup.py
    pause
    goto menu
)

if "%choice%"=="2" (
    echo.
    echo 🔍 Running complete analysis...
    python run_analysis.py
    pause
    goto menu
)

if "%choice%"=="3" (
    echo.
    echo 📥 Running data ingestion...
    python src/data_ingestion.py
    pause
    goto menu
)

if "%choice%"=="4" (
    echo.
    echo 📊 Running analysis on existing data...
    python src/data_analysis.py
    pause
    goto menu
)

if "%choice%"=="5" (
    echo.
    echo 📓 Opening Jupyter notebook...
    echo Navigate to: notebooks/analysis.ipynb
    jupyter notebook notebooks/analysis.ipynb
    pause
    goto menu
)

if "%choice%"=="6" (
    echo.
    echo 📁 Project structure:
    echo.
    tree /F
    echo.
    pause
    goto menu
)

if "%choice%"=="7" (
    echo.
    echo 👋 Thank you for using the Customer Purchase Behavior Analysis project!
    echo.
    exit /b 0
)

echo.
echo ❌ Invalid choice. Please select 1-7.
echo.
goto menu
