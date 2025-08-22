"""
Setup Script for Customer Purchase Behavior Analysis Project
Installs dependencies and sets up the environment
"""

import subprocess
import sys
import os
from pathlib import Path

def run_command(command, description):
    """Run a command and handle errors"""
    print(f"\n🔧 {description}...")
    try:
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        print(f"✅ {description} completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ {description} failed:")
        print(f"Error: {e.stderr}")
        return False

def check_python_version():
    """Check if Python version is compatible"""
    version = sys.version_info
    if version.major < 3 or (version.major == 3 and version.minor < 7):
        print("❌ Python 3.7+ is required for this project")
        print(f"Current version: {version.major}.{version.minor}")
        return False
    else:
        print(f"✅ Python version {version.major}.{version.minor} is compatible")
        return True

def install_java():
    """Check for Java installation (required for PySpark)"""
    try:
        result = subprocess.run("java -version", shell=True, capture_output=True, text=True)
        if result.returncode == 0:
            print("✅ Java is already installed")
            return True
        else:
            print("❌ Java not found. PySpark requires Java 8 or 11")
            print("Please install Java from: https://adoptopenjdk.net/")
            return False
    except:
        print("❌ Java not found. PySpark requires Java 8 or 11")
        print("Please install Java from: https://adoptopenjdk.net/")
        return False

def main():
    """Main setup function"""
    print("🚀 Setting up Customer Purchase Behavior Analysis Project")
    print("=" * 60)
    
    # Check Python version
    if not check_python_version():
        return 1
    
    # Check Java installation
    if not install_java():
        print("\n⚠️  Java installation required for PySpark")
        print("Please install Java and run this setup script again")
        return 1
    
    # Upgrade pip
    if not run_command(f"{sys.executable} -m pip install --upgrade pip", "Upgrading pip"):
        return 1
    
    # Install requirements
    requirements_file = Path(__file__).parent / "requirements.txt"
    if not run_command(f"{sys.executable} -m pip install -r {requirements_file}", 
                      "Installing Python dependencies"):
        return 1
    
    # Verify PySpark installation
    if not run_command(f"{sys.executable} -c \"import pyspark; print('PySpark version:', pyspark.__version__)\"",
                      "Verifying PySpark installation"):
        print("\n❌ PySpark installation failed. This might be due to Java compatibility issues.")
        print("Please ensure Java 8 or 11 is installed and try again.")
        return 1
    
    # Create directories (they should already exist, but just in case)
    directories = [
        "data/raw",
        "data/processed", 
        "outputs/reports",
        "outputs/charts"
    ]
    
    for directory in directories:
        dir_path = Path(__file__).parent / directory
        dir_path.mkdir(parents=True, exist_ok=True)
    
    print("\n🎉 Setup completed successfully!")
    print("\nNext steps:")
    print("1. Run data ingestion: python src/data_ingestion.py")
    print("2. Run full analysis: python run_analysis.py")
    print("3. Open Jupyter notebook: jupyter notebook notebooks/analysis.ipynb")
    
    print("\n📊 Project is ready for analysis!")
    
    return 0

if __name__ == "__main__":
    exit_code = main()
    input("\nPress Enter to continue...")
    sys.exit(exit_code)
