"""
Data Loader with Google Drive Support
Downloads large data files from Google Drive if not available locally.
"""

import os
import requests
from pathlib import Path
import pandas as pd
from typing import Optional

# Streamlit import - only import if available (for optional features)
try:
    import streamlit as st
    STREAMLIT_AVAILABLE = True
except ImportError:
    STREAMLIT_AVAILABLE = False
    st = None

# Google Drive folder ID from the user's link
GOOGLE_DRIVE_FOLDER_ID = "1v1MXZs4jVVqsHAqCxGCJDH6mHNPNaVk4"

# File mappings: local_path -> Google Drive file name
DATA_FILE_MAPPINGS = {
    "data/coherent/merged_transport_data.csv": "merged_transport_data.csv",
    "data/coherent/delhivery_cleaned.csv": "delhivery_cleaned.csv",
    "data/coherent/orders_cleaned.csv": "orders_cleaned.csv",
    "data/coherent/distance_cleaned.csv": "distance_cleaned.csv",
    "data/coherent/weather_cleaned.csv": "weather_cleaned.csv",
    "data/coherent/inventory_cleaned.csv": "inventory_cleaned.csv",
    "data/coherent/supply_chain_cleaned.csv": "supply_chain_cleaned.csv",
    "data/optimizer/optimizer_orders.csv": "optimizer_orders.csv",
    "data/optimizer/optimizer_distances.csv": "optimizer_distances.csv",
}

# Add your file IDs here after getting them from Google Drive
# To get file IDs:
# 1. Open your Google Drive folder: https://drive.google.com/drive/folders/1v1MXZs4jVVqsHAqCxGCJDH6mHNPNaVk4
# 2. Click on each file to open it
# 3. Copy the file ID from the URL (the part between /d/ and /view)
# 4. Paste it below
# 
# Example URL: https://drive.google.com/file/d/ABC123XYZ456/view
#                                    ^^^^^^^^^^^^
#                                    This is the FILE ID

GOOGLE_DRIVE_FILE_IDS = {
    # Large files (too big for GitHub)
    "merged_transport_data.csv": "",  # 1.95 GB - Get ID from Google Drive
    "delhivery_cleaned.csv": "",      # 198.1 MB - Get ID from Google Drive
    
    # Medium files (can be in GitHub, but can also use Google Drive)
    "orders_cleaned.csv": "",         # 3.5 MB - Get ID from Google Drive
    
    # Small files (usually in GitHub, optional Google Drive)
    "distance_cleaned.csv": "",       # 14 KB
    "inventory_cleaned.csv": "",      # 15 KB
    "supply_chain_cleaned.csv": "",    # 10 KB
    "weather_cleaned.csv": "",         # 80 KB
    
    # Optimizer files
    "optimizer_orders.csv": "",       # If in Google Drive
    "optimizer_distances.csv": "",    # If in Google Drive
}


def get_google_drive_download_url(file_id: str) -> str:
    """
    Convert Google Drive file ID to direct download URL.
    
    Args:
        file_id: Google Drive file ID
    
    Returns:
        Direct download URL
    """
    return f"https://drive.google.com/uc?export=download&id={file_id}"


def download_from_google_drive(file_id: str, output_path: Path, show_progress: bool = False):
    """
    Download a file from Google Drive.
    
    Args:
        file_id: Google Drive file ID
        output_path: Local path to save the file
        show_progress: Whether to show download progress (for Streamlit)
    
    Returns:
        Path to downloaded file
    """
    url = get_google_drive_download_url(file_id)
    
    # Create output directory if it doesn't exist
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        # For large files, Google Drive may redirect to a confirmation page
        session = requests.Session()
        response = session.get(url, stream=True, timeout=30)
        
        # Handle large file downloads (Google Drive shows confirmation page)
        if response.status_code == 200:
            # Check if we got HTML (confirmation page) instead of file
            content_type = response.headers.get('Content-Type', '')
            if 'text/html' in content_type:
                # Extract download link from confirmation page
                # For now, try direct download with confirm parameter
                confirm_url = f"{url}&confirm=t"
                response = session.get(confirm_url, stream=True, timeout=30)
        
        # Get file size if available
        total_size = int(response.headers.get('content-length', 0))
        
        # Download file
        downloaded = 0
        with open(output_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    
                    if show_progress and total_size > 0 and STREAMLIT_AVAILABLE:
                        progress = min(downloaded / total_size, 1.0)
                        if st is not None:
                            st.progress(progress)
        
        return output_path
    
    except Exception as e:
        raise Exception(f"Failed to download from Google Drive: {e}")


def ensure_data_file(local_path: str, google_drive_file_id: Optional[str] = None, 
                     use_cache: bool = True, show_progress: bool = False) -> Path:
    """
    Ensure a data file exists locally, downloading from Google Drive if needed.
    
    Args:
        local_path: Local file path
        google_drive_file_id: Google Drive file ID (if known)
        use_cache: If True, use existing file if it exists
        show_progress: Show download progress (for Streamlit)
    
    Returns:
        Path to the local file
    """
    local_path = Path(local_path)
    
    # Check if file already exists
    if use_cache and local_path.exists():
        return local_path
    
    # Try to download from Google Drive
    if google_drive_file_id:
        try:
            if show_progress and STREAMLIT_AVAILABLE and st is not None:
                with st.spinner(f"Downloading {local_path.name} from Google Drive..."):
                    download_from_google_drive(google_drive_file_id, local_path, show_progress=show_progress)
            else:
                print(f"Downloading {local_path.name} from Google Drive...")
                download_from_google_drive(google_drive_file_id, local_path, show_progress=False)
            return local_path
        except Exception as e:
            print(f"Warning: Could not download from Google Drive: {e}")
            if show_progress and STREAMLIT_AVAILABLE and st is not None:
                st.warning(f"Could not download from Google Drive: {e}")
    
    # If download failed and file doesn't exist, raise error
    if not local_path.exists():
        raise FileNotFoundError(
            f"Data file not found: {local_path}\n"
            f"Please ensure the file exists locally or provide a Google Drive file ID.\n"
            f"See GOOGLE_DRIVE_SETUP.md for instructions."
        )
    
    return local_path


def load_csv_with_fallback(csv_path: str, google_drive_file_id: Optional[str] = None,
                           use_cache: bool = True, show_progress: bool = False) -> pd.DataFrame:
    """
    Load a CSV file, downloading from Google Drive if not available locally.
    
    Args:
        csv_path: Local CSV file path
        google_drive_file_id: Google Drive file ID (optional)
        use_cache: Use cached file if exists
        show_progress: Show download progress
    
    Returns:
        DataFrame
    """
    local_path = ensure_data_file(csv_path, google_drive_file_id, use_cache=use_cache, show_progress=show_progress)
    return pd.read_csv(local_path)


def get_file_id_from_name(file_name: str) -> Optional[str]:
    """
    Get Google Drive file ID from file name using the mapping.
    
    Args:
        file_name: Name of the file
    
    Returns:
        File ID if found, None otherwise
    """
    return GOOGLE_DRIVE_FILE_IDS.get(file_name)


# For Streamlit integration
def load_orders_for_forecasting(use_google_drive: bool = False):
    """
    Load orders data for forecasting, with optional Google Drive fallback.
    Note: If using in Streamlit, wrap with @st.cache_data decorator in the calling code.
    """
    """
    Load orders data for forecasting, with optional Google Drive fallback.
    
    Args:
        use_google_drive: Whether to attempt download from Google Drive if local file missing
    
    Returns:
        DataFrame with orders
    """
    # Try local files first
    possible_paths = [
        Path(__file__).parent.parent / "data" / "coherent" / "orders_cleaned_sample.csv",
        Path(__file__).parent.parent / "data" / "coherent" / "orders_cleaned.csv",
        Path(__file__).parent.parent / "data" / "optimizer" / "optimizer_orders.csv",
    ]
    
    for path in possible_paths:
        if path.exists():
            return pd.read_csv(path)
    
    # If not found and Google Drive enabled, try to download
    if use_google_drive:
        file_id = get_file_id_from_name("orders_cleaned.csv")
        if file_id:
            try:
                return load_csv_with_fallback(
                    str(possible_paths[1]),  # orders_cleaned.csv
                    google_drive_file_id=file_id,
                    show_progress=True
                )
            except Exception as e:
                if STREAMLIT_AVAILABLE and st is not None:
                    st.warning(f"Could not download from Google Drive: {e}")
        else:
            if STREAMLIT_AVAILABLE and st is not None:
                st.warning("Google Drive file ID not configured. See GOOGLE_DRIVE_SETUP.md")
    
    raise FileNotFoundError("Could not find orders CSV file locally or on Google Drive.")


if __name__ == "__main__":
    # Test the data loader
    print("Data Loader with Google Drive Support")
    print("=" * 50)
    print(f"Google Drive Folder ID: {GOOGLE_DRIVE_FOLDER_ID}")
    print(f"Folder Link: https://drive.google.com/drive/folders/{GOOGLE_DRIVE_FOLDER_ID}")
    print(f"\nSupported files: {list(DATA_FILE_MAPPINGS.keys())}")
    print("\n📝 Next Steps:")
    print("1. Make the Google Drive folder publicly accessible")
    print("2. Get file IDs for each CSV file (see GOOGLE_DRIVE_SETUP.md)")
    print("3. Add file IDs to GOOGLE_DRIVE_FILE_IDS dictionary in this file")
    print("4. Test download with: ensure_data_file('path/to/file.csv', 'YOUR_FILE_ID')")
