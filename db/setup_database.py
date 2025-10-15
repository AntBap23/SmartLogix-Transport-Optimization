#!/usr/bin/env python3
"""
Database Setup Script for SmartLogix Transport Optimization
This script sets up the PostgreSQL database and loads initial data.
"""

import os
import sys
import subprocess
import time
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def run_command(command: str, description: str) -> bool:
    """Run a shell command and return success status."""
    try:
        logger.info(f"Running: {description}")
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        logger.info(f"Success: {description}")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed: {description}")
        logger.error(f"Error: {e.stderr}")
        return False

def check_docker_installed() -> bool:
    """Check if Docker is installed and running."""
    try:
        result = subprocess.run("docker --version", shell=True, capture_output=True, text=True)
        if result.returncode == 0:
            logger.info("Docker is installed")
            return True
        else:
            logger.error("Docker is not installed or not running")
            return False
    except Exception as e:
        logger.error(f"Error checking Docker: {e}")
        return False

def check_docker_compose_installed() -> bool:
    """Check if Docker Compose is installed."""
    try:
        result = subprocess.run("docker-compose --version", shell=True, capture_output=True, text=True)
        if result.returncode == 0:
            logger.info("Docker Compose is installed")
            return True
        else:
            logger.error("Docker Compose is not installed")
            return False
    except Exception as e:
        logger.error(f"Error checking Docker Compose: {e}")
        return False

def setup_database():
    """Set up the database using Docker Compose."""
    logger.info("Setting up SmartLogix Transport Optimization Database")
    
    # Check prerequisites
    if not check_docker_installed():
        logger.error("Docker is required but not installed. Please install Docker first.")
        return False
    
    if not check_docker_compose_installed():
        logger.error("Docker Compose is required but not installed. Please install Docker Compose first.")
        return False
    
    # Get the project root directory
    project_root = Path(__file__).parent.parent
    os.chdir(project_root)
    
    # Stop any existing containers
    logger.info("Stopping any existing containers...")
    run_command("docker-compose down", "Stop existing containers")
    
    # Build and start the database
    logger.info("Starting PostgreSQL database...")
    if not run_command("docker-compose up -d postgres", "Start PostgreSQL container"):
        return False
    
    # Wait for database to be ready
    logger.info("Waiting for database to be ready...")
    time.sleep(10)
    
    # Check if database is running
    if not run_command("docker-compose ps", "Check container status"):
        return False
    
    # Start PgAdmin (optional)
    logger.info("Starting PgAdmin...")
    run_command("docker-compose up -d pgadmin", "Start PgAdmin container")
    
    logger.info("Database setup completed successfully!")
    logger.info("Database connection details:")
    logger.info("  Host: localhost")
    logger.info("  Port: 5432")
    logger.info("  Database: smartlogix_transport")
    logger.info("  Username: smartlogix_user")
    logger.info("  Password: smartlogix_password")
    logger.info("")
    logger.info("PgAdmin access:")
    logger.info("  URL: http://localhost:8080")
    logger.info("  Email: admin@smartlogix.com")
    logger.info("  Password: admin123")
    
    return True

def load_initial_data():
    """Load initial data into the database."""
    logger.info("Loading initial data...")
    
    # Install Python dependencies
    logger.info("Installing Python dependencies...")
    if not run_command("pip install -r requirements.txt", "Install Python dependencies"):
        logger.warning("Failed to install dependencies. Please install manually: pip install -r requirements.txt")
    
    # Run data loading script
    logger.info("Loading data from CSV files...")
    if not run_command("python db/load_data.py", "Load data from CSV files"):
        logger.warning("Failed to load data. You can run 'python db/load_data.py' manually later.")
    
    return True

def main():
    """Main setup function."""
    logger.info("SmartLogix Transport Optimization Database Setup")
    logger.info("=" * 50)
    
    # Setup database
    if not setup_database():
        logger.error("Database setup failed!")
        return False
    
    # Load initial data
    if not load_initial_data():
        logger.warning("Data loading had issues, but database is ready.")
    
    logger.info("Setup completed!")
    logger.info("")
    logger.info("Next steps:")
    logger.info("1. Access PgAdmin at http://localhost:8080")
    logger.info("2. Connect to database using the credentials above")
    logger.info("3. Run 'python db/load_data.py' to load your data")
    logger.info("4. Check the database tables and views")
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
