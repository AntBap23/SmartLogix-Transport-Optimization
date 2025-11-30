"""
Helper script to get Google Drive file IDs from a folder.
This script helps you extract file IDs for the data_loader.py configuration.
"""

import requests
import re
from typing import Dict

# Your Google Drive folder ID
FOLDER_ID = "1v1MXZs4jVVqsHAqCxGCJDH6mHNPNaVk4"
FOLDER_URL = f"https://drive.google.com/drive/folders/{FOLDER_ID}"

# Files we're looking for
FILES_TO_FIND = [
    "delhivery_cleaned.csv",
    "distance_cleaned.csv",
    "inventory_cleaned.csv",
    "merged_transport_data.csv",
    "orders_cleaned.csv",
    "supply_chain_cleaned.csv",
    "weather_cleaned.csv",
    "optimizer_orders.csv",
    "optimizer_distances.csv",
]


def extract_file_id_from_url(url: str) -> str:
    """Extract file ID from Google Drive URL."""
    # Pattern: /file/d/FILE_ID/ or /folders/FILE_ID/
    match = re.search(r'/file/d/([a-zA-Z0-9_-]+)', url)
    if match:
        return match.group(1)
    match = re.search(r'/folders/([a-zA-Z0-9_-]+)', url)
    if match:
        return match.group(1)
    return None


def get_file_ids_manual():
    """
    Manual method: Instructions for getting file IDs.
    This is the most reliable method.
    """
    print("=" * 70)
    print("📋 MANUAL METHOD: Get File IDs from Google Drive")
    print("=" * 70)
    print()
    print("For each file in your Google Drive folder:")
    print()
    print("1. Open your folder:")
    print(f"   {FOLDER_URL}")
    print()
    print("2. Click on each file to open it")
    print()
    print("3. Look at the URL in your browser:")
    print("   Example: https://drive.google.com/file/d/ABC123XYZ456/view")
    print("                              ^^^^^^^^^^^^")
    print("                              This is the FILE ID")
    print()
    print("4. Copy the file ID (the part between /d/ and /view)")
    print()
    print("5. Add to app/data_loader.py in the GOOGLE_DRIVE_FILE_IDS dictionary:")
    print()
    print("   GOOGLE_DRIVE_FILE_IDS = {")
    for file in FILES_TO_FIND:
        print(f'       "{file}": "YOUR_FILE_ID_HERE",')
    print("   }")
    print()
    print("=" * 70)
    print()


def create_file_id_template():
    """Create a template for file IDs."""
    template = """# Google Drive File IDs
# Add the file IDs you get from Google Drive URLs here

GOOGLE_DRIVE_FILE_IDS = {
"""
    for file in FILES_TO_FIND:
        template += f'    "{file}": "",  # Get ID from: Open file in Google Drive → Copy ID from URL\n'
    template += "}\n"
    return template


if __name__ == "__main__":
    print("\n" + "=" * 70)
    print("🔍 Google Drive File ID Extractor")
    print("=" * 70)
    print()
    print(f"📁 Folder: {FOLDER_URL}")
    print()
    print("📋 Files to find:")
    for i, file in enumerate(FILES_TO_FIND, 1):
        print(f"   {i}. {file}")
    print()
    print("=" * 70)
    print()
    
    # Show manual method (most reliable)
    get_file_ids_manual()
    
    # Create template
    print("=" * 70)
    print("📝 Template for app/data_loader.py:")
    print("=" * 70)
    print()
    print(create_file_id_template())
    print()
    print("=" * 70)
    print()
    print("💡 Quick Steps:")
    print("   1. Open each file in Google Drive")
    print("   2. Copy the file ID from the URL")
    print("   3. Paste into the template above")
    print("   4. Save to app/data_loader.py")
    print()
    print("✅ Once configured, your app can download files from Google Drive!")

