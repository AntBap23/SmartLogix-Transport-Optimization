# 📁 Google Drive Data Setup Guide

This guide helps you set up Google Drive as a data source for large files that can't be uploaded to GitHub.

## 🔗 Your Google Drive Folder

**Folder Link**: https://drive.google.com/drive/folders/1v1MXZs4jVVqsHAqCxGCJDH6mHNPNaVk4?usp=drive_link

**Folder ID**: `1v1MXZs4jVVqsHAqCxGCJDH6mHNPNaVk4`

---

## 📋 Step-by-Step Setup

### Step 1: Make Folder Publicly Accessible

1. Open your [Google Drive folder](https://drive.google.com/drive/folders/1v1MXZs4jVVqsHAqCxGCJDH6mHNPNaVk4?usp=drive_link)
2. Click the **Share** button (top right)
3. Click **Change to anyone with the link**
4. Set permission to **Viewer**
5. Click **Done**

### Step 2: Get File IDs

For each CSV file you want to download programmatically:

1. **Open the file** in Google Drive
2. **Copy the file ID** from the URL:
   ```
   URL: https://drive.google.com/file/d/FILE_ID_HERE/view?usp=sharing
                              ^^^^^^^^^^^^
                              This is your file ID
   ```

3. **Add to `app/data_loader.py`**:

Open `app/data_loader.py` and find the `GOOGLE_DRIVE_FILE_IDS` dictionary:

```python
GOOGLE_DRIVE_FILE_IDS = {
    "merged_transport_data.csv": "YOUR_FILE_ID_1",
    "delhivery_cleaned.csv": "YOUR_FILE_ID_2",
    "orders_cleaned.csv": "YOUR_FILE_ID_3",
    # Add more as needed
}
```

Replace `YOUR_FILE_ID_1`, etc. with the actual file IDs you copied.

### Step 3: Test Download

Test downloading a file:

```python
from app.data_loader import download_from_google_drive
from pathlib import Path

# Download a file
file_id = "YOUR_FILE_ID"
output_path = Path("data/coherent/test_file.csv")

download_from_google_drive(file_id, output_path)
print(f"✅ Downloaded to {output_path}")
```

### Step 4: Update App Code (Optional)

If you want the app to automatically download from Google Drive when files are missing, update your code:

```python
# In app/forecast_orders.py or app/optimizer.py
from app.data_loader import load_csv_with_fallback, get_file_id_from_name

# Instead of:
# df = pd.read_csv("data/coherent/orders_cleaned.csv")

# Use:
file_id = get_file_id_from_name("orders_cleaned.csv")
df = load_csv_with_fallback(
    "data/coherent/orders_cleaned.csv",
    google_drive_file_id=file_id
)
```

---

## 🔧 Alternative: Direct Download URLs

If you prefer direct download links:

1. **Get shareable link** for each file
2. **Convert to download URL**:
   ```
   Shareable: https://drive.google.com/file/d/FILE_ID/view?usp=sharing
   Download:  https://drive.google.com/uc?export=download&id=FILE_ID
   ```

3. **Use in code**:
```python
import requests

def download_file(file_id, output_path):
    url = f"https://drive.google.com/uc?export=download&id={file_id}"
    response = requests.get(url, stream=True)
    with open(output_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
```

---

## 📊 File Size Reference

Based on your current files:

| File | Size | Status |
|------|------|--------|
| `merged_transport_data.csv` | 1.9GB | ❌ Too large for GitHub → Use Google Drive |
| `delhivery_cleaned.csv` | 198MB | ❌ Too large for GitHub → Use Google Drive |
| `orders_cleaned.csv` | 3.5MB | ✅ OK for GitHub |
| `optimizer_orders.csv` | 3.3MB | ✅ OK for GitHub |
| `optimizer_distances.csv` | 32KB | ✅ OK for GitHub |

---

## 🚀 For Streamlit Cloud Deployment

1. **Keep small files in GitHub** (already done via `.gitignore`)
2. **Store large files on Google Drive** (your folder)
3. **Add file IDs to `app/data_loader.py`**
4. **Deploy to Streamlit Cloud** - files will download on first use

The app will automatically try to download from Google Drive if local files are missing.

---

## ⚠️ Important Notes

1. **File IDs are required** - The current implementation needs individual file IDs
2. **Public access** - Folder/files must be publicly accessible (view-only is fine)
3. **First download** - Files download on first use (cached after that)
4. **Streamlit Cloud** - Works on Streamlit Cloud, but first download may be slow for large files

---

## 🔐 Security Considerations

- **Public access** means anyone with the link can download
- For sensitive data, use **Google Drive API with authentication** instead
- See `DEPLOYMENT.md` for API-based approaches

---

## ✅ Quick Checklist

- [ ] Google Drive folder is publicly accessible
- [ ] File IDs collected for each large CSV
- [ ] File IDs added to `app/data_loader.py` → `GOOGLE_DRIVE_FILE_IDS`
- [ ] Test download works locally
- [ ] App code updated to use data loader (optional)
- [ ] Tested on Streamlit Cloud (if deploying)

---

## 🆘 Troubleshooting

**Issue**: "File not found" error  
**Solution**: Check file ID is correct and folder is public

**Issue**: Download is slow  
**Solution**: Large files take time. Consider using sample data for demos.

**Issue**: "Access denied"  
**Solution**: Ensure folder/file is set to "Anyone with the link"

**Issue**: "Confirmation page" error  
**Solution**: The code handles this automatically, but if issues persist, try the direct download URL method

---

## 📝 Example: Getting File IDs

Based on your [Google Drive folder](https://drive.google.com/drive/folders/1v1MXZs4jVVqsHAqCxGCJDH6mHNPNaVk4?usp=sharing), here are the files:

| File | Size | Priority |
|------|------|----------|
| `merged_transport_data.csv` | 1.95 GB | ⚠️ **Required** (too large for GitHub) |
| `delhivery_cleaned.csv` | 198.1 MB | ⚠️ **Required** (too large for GitHub) |
| `orders_cleaned.csv` | 3.5 MB | Optional (can be in GitHub) |
| `distance_cleaned.csv` | 14 KB | Optional (small, can be in GitHub) |
| `inventory_cleaned.csv` | 15 KB | Optional (small, can be in GitHub) |
| `supply_chain_cleaned.csv` | 10 KB | Optional (small, can be in GitHub) |
| `weather_cleaned.csv` | 80 KB | Optional (small, can be in GitHub) |

### Steps to Get File IDs:

1. Go to your [Google Drive folder](https://drive.google.com/drive/folders/1v1MXZs4jVVqsHAqCxGCJDH6mHNPNaVk4?usp=sharing)
2. Click on a file (e.g., `merged_transport_data.csv`)
3. Look at the URL in your browser: `https://drive.google.com/file/d/ABC123XYZ456/view`
4. Copy `ABC123XYZ456` (the part between `/d/` and `/view`)
5. Add to `GOOGLE_DRIVE_FILE_IDS` in `app/data_loader.py`

### Quick Helper Script:

Run this to see instructions:
```bash
python scripts/get_google_drive_file_ids.py
```

---

*For more deployment options, see [DEPLOYMENT.md](DEPLOYMENT.md)*
