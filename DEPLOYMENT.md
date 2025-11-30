# 🚀 Deployment Guide for SmartLogix Transport Optimization

This guide covers deploying the Streamlit app when some data files are too large for GitHub.

---

## 📊 File Size Overview

- **Small files (will be in repo)**: `orders_cleaned.csv` (3.5MB), `optimizer_orders.csv` (3.3MB), `optimizer_distances.csv` (32KB)
- **Large files (excluded)**: `merged_transport_data.csv` (1.9GB), `delhivery_cleaned.csv` (198MB)

---

## 🌐 Option 1: Streamlit Cloud (Recommended - Free)

**Best for**: Quick deployment, free hosting, automatic updates from GitHub

### Steps:

1. **Prepare your repository**:
   ```bash
   # Ensure .gitignore excludes large files
   # Commit and push only small files
   git add .
   git commit -m "Ready for deployment"
   git push origin main
   ```

2. **Deploy on Streamlit Cloud**:
   - Go to [share.streamlit.io](https://share.streamlit.io)
   - Sign in with GitHub
   - Click "New app"
   - Select your repository
   - Set main file path: `app/dashboard/app.py`
   - Click "Deploy"

3. **For large files** (if needed):
   - Use **Streamlit Secrets** to store file URLs
   - Or use external storage (see Option 3)

**Pros**: Free, automatic updates, easy setup  
**Cons**: Large files need external storage

---

## ☁️ Option 2: External Data Storage (Recommended for Large Files)

Store large files externally and load them at runtime.

### A. Google Drive (Your Setup) ✅

**Your Google Drive Folder**: [View Folder](https://drive.google.com/drive/folders/1v1MXZs4jVVqsHAqCxGCJDH6mHNPNaVk4?usp=drive_link)

#### Option 1: Public Folder Access (Easiest)

1. **Make folder publicly accessible**:
   - Right-click folder → Share → Change to "Anyone with the link"
   - Copy the folder ID: `1v1MXZs4jVVqsHAqCxGCJDH6mHNPNaVk4`

2. **Get individual file IDs**:
   - Open each file in Google Drive
   - Copy the file ID from the URL: `https://drive.google.com/file/d/FILE_ID/view`
   - Add to `app/data_loader.py`:

```python
GOOGLE_DRIVE_FILES = {
    "merged_transport_data.csv": "YOUR_FILE_ID_HERE",
    "delhivery_cleaned.csv": "YOUR_FILE_ID_HERE",
    # ... etc
}
```

3. **Use the data loader**:
```python
from app.data_loader import load_csv_with_fallback

# Load with Google Drive fallback
df = load_csv_with_fallback(
    "data/coherent/orders_cleaned.csv",
    google_drive_file_id="YOUR_FILE_ID"
)
```

#### Option 2: Direct Download Links

1. **Get shareable links** for each file:
   - Right-click file → Get link → Change to "Anyone with the link"
   - Copy link

2. **Convert to direct download URL**:
   ```
   Original: https://drive.google.com/file/d/FILE_ID/view?usp=sharing
   Download: https://drive.google.com/uc?export=download&id=FILE_ID
   ```

3. **Use in code**:
```python
import requests
from pathlib import Path

def download_from_gdrive(file_id, output_path):
    url = f"https://drive.google.com/uc?export=download&id={file_id}"
    response = requests.get(url, stream=True)
    with open(output_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
```

### B. AWS S3 / Google Cloud Storage

1. Upload files to S3/GCS bucket
2. Use boto3/gcs libraries to load:

```python
import boto3
import pandas as pd

def load_from_s3(bucket_name, file_key):
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket_name, Key=file_key)
    return pd.read_csv(obj['Body'])
```

---

## 🐳 Option 3: Docker Deployment

Deploy with Docker to include all files.

### Create Dockerfile:

```dockerfile
FROM python:3.9-slim

WORKDIR /app

# Copy requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY app/ ./app/
COPY data/ ./data/

# Expose Streamlit port
EXPOSE 8501

# Run Streamlit
CMD ["streamlit", "run", "app/dashboard/app.py", "--server.port=8501", "--server.address=0.0.0.0"]
```

### Deploy to:
- **Heroku**: `heroku container:push web`
- **AWS ECS/Fargate**: Use AWS CLI
- **Google Cloud Run**: `gcloud run deploy`
- **Azure Container Instances**: Use Azure CLI

---

## 📦 Option 4: Git LFS (Git Large File Storage)

For files between 100MB-2GB that you want in the repo.

### Setup:

```bash
# Install Git LFS
git lfs install

# Track large files
git lfs track "data/coherent/merged_transport_data.csv"
git lfs track "data/coherent/delhivery_cleaned.csv"

# Add .gitattributes
git add .gitattributes

# Commit and push
git add data/coherent/*.csv
git commit -m "Add large files with Git LFS"
git push origin main
```

**Note**: Git LFS has storage limits on free GitHub accounts (1GB storage, 1GB bandwidth/month)

---

## 🎯 Option 5: Sample Data Approach (Best for Portfolio)

Use smaller sample datasets for deployment.

### Steps:

1. **Create sample data**:
   ```python
   # scripts/create_sample_data.py
   import pandas as pd
   
   # Load full data
   df = pd.read_csv('data/coherent/orders_cleaned.csv')
   
   # Take sample (e.g., last 30 days)
   df['available_time'] = pd.to_datetime(df['available_time'])
   sample = df[df['available_time'] >= df['available_time'].max() - pd.Timedelta(days=30)]
   
   # Save sample
   sample.to_csv('data/coherent/orders_cleaned_sample.csv', index=False)
   ```

2. **Update app to use sample data**:
   ```python
   # In forecast_orders.py
   if csv_path is None:
       # Try sample first, then full
       possible_paths = [
           Path(__file__).parent.parent / "data" / "coherent" / "orders_cleaned_sample.csv",
           Path(__file__).parent.parent / "data" / "coherent" / "orders_cleaned.csv",
       ]
   ```

3. **Include sample in repo**, keep full data locally

---

## 🚀 Recommended Deployment Strategy

### For Portfolio/Demo (Recommended):
1. **Use sample data** (Option 5) - smaller files, faster loading
2. **Deploy to Streamlit Cloud** (Option 1) - free, easy
3. **Google Drive for large files** - Store large files on Google Drive, download on-demand
4. **Add note in README** about full dataset availability on Google Drive

### For Production:
1. **Use external storage** (Option 2) - S3/GCS for large files
2. **Docker deployment** (Option 3) - full control
3. **Load data on-demand** - don't include in container

### Quick Setup with Your Google Drive:

1. **Make folder public** (or get file IDs):
   - Folder: `1v1MXZs4jVVqsHAqCxGCJDH6mHNPNaVk4`
   - Right-click → Share → "Anyone with the link"

2. **Get file IDs** for each CSV:
   - Open each file → Copy ID from URL
   - Add to `app/data_loader.py`

3. **Update app to use data loader**:
   ```python
   from app.data_loader import load_csv_with_fallback
   
   # In your forecasting/optimizer code
   df = load_csv_with_fallback(
       "data/coherent/orders_cleaned.csv",
       google_drive_file_id="YOUR_FILE_ID"
   )
   ```

---

## 📝 Quick Start: Streamlit Cloud Deployment

1. **Ensure your repo is clean**:
   ```bash
   git status
   git add .
   git commit -m "Ready for Streamlit Cloud"
   git push origin main
   ```

2. **Go to [share.streamlit.io](https://share.streamlit.io)**

3. **Deploy**:
   - Click "New app"
   - Repository: `AntBap23/SmartLogix-Transport-Optimization`
   - Branch: `main`
   - Main file path: `app/dashboard/app.py`
   - Click "Deploy"

4. **Your app will be live at**: `https://your-app-name.streamlit.app`

---

## 🔧 Environment Variables (if needed)

If using external storage, add secrets in Streamlit Cloud:
- Go to app settings → Secrets
- Add:
  ```toml
  [secrets]
  AWS_ACCESS_KEY_ID = "your-key"
  AWS_SECRET_ACCESS_KEY = "your-secret"
  S3_BUCKET = "your-bucket"
  ```

---

## ✅ Checklist Before Deployment

- [ ] Large files excluded in `.gitignore`
- [ ] Small data files included (needed for app)
- [ ] `requirements.txt` is up to date
- [ ] App works locally (`streamlit run app/dashboard/app.py`)
- [ ] README has deployment instructions
- [ ] No hardcoded paths (use relative paths)
- [ ] Environment variables documented

---

## 🆘 Troubleshooting

**Issue**: App fails to load data  
**Solution**: Check file paths are relative, not absolute

**Issue**: Large files cause deployment to fail  
**Solution**: Use external storage or sample data

**Issue**: Slow loading times  
**Solution**: Use data caching in Streamlit (`@st.cache_data`)

---

*For questions, check the [Streamlit Cloud documentation](https://docs.streamlit.io/streamlit-community-cloud)*

