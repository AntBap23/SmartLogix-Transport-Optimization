#!/bin/bash
# Script to run the Streamlit dashboard

# Navigate to project root
cd "$(dirname "$0")/../.."

# Activate virtual environment if it exists
if [ -d ".venv" ]; then
    source .venv/bin/activate
fi

# Run Streamlit app
streamlit run app/dashboard/app.py --server.port 8501

