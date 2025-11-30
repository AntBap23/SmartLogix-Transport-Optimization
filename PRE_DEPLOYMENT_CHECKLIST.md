# ✅ Pre-Deployment Checklist

Use this checklist before pushing to GitHub and deploying.

## 🧪 Local Testing

- [x] Streamlit app runs locally (`streamlit run app/dashboard/app.py`)
- [ ] All tabs load without errors
- [ ] Project Overview tab displays correctly
- [ ] Analytics Dashboard shows Tableau dashboard
- [ ] Route Optimization tab works
- [ ] Order Forecasting tab generates forecasts
- [ ] No console errors

## 📁 Files to Commit

### Core Application
- [x] `app/dashboard/app.py` - Main dashboard with all tabs
- [x] `app/optimizer.py` - Route optimization engine
- [x] `app/visualize_routes.py` - Route visualization
- [x] `app/forecast_orders.py` - ML forecasting model
- [x] `app/data_loader.py` - Google Drive data loader

### Configuration
- [x] `requirements.txt` - All dependencies
- [x] `.gitignore` - Excludes large files
- [x] `README.md` - Updated documentation

### Documentation
- [x] `DEPLOYMENT.md` - Deployment guide
- [x] `GOOGLE_DRIVE_SETUP.md` - Google Drive setup
- [x] `scripts/get_google_drive_file_ids.py` - Helper script

### Data Files (Small ones only)
- [x] `data/optimizer/optimizer_orders.csv` (3.3MB) ✅
- [x] `data/optimizer/optimizer_distances.csv` (32KB) ✅
- [x] `data/coherent/orders_cleaned.csv` (3.5MB) ✅
- [x] Other small CSV files ✅

## ❌ Files Excluded (Too Large)

- [x] `data/coherent/merged_transport_data.csv` (1.9GB) - In `.gitignore`
- [x] `data/coherent/delhivery_cleaned.csv` (198MB) - In `.gitignore`

## 🔧 Before Pushing to GitHub

1. **Test locally**:
   ```bash
   streamlit run app/dashboard/app.py
   ```
   Verify all tabs work.

2. **Check .gitignore**:
   ```bash
   git status
   ```
   Ensure large files are not listed.

3. **Review changes**:
   ```bash
   git diff
   ```
   Make sure only intended files are changed.

4. **Commit**:
   ```bash
   git add .
   git commit -m "Add order forecasting ML model and Google Drive integration"
   ```

5. **Push**:
   ```bash
   git push origin main
   ```

## 🚀 Deployment Steps

### Streamlit Cloud

1. Go to [share.streamlit.io](https://share.streamlit.io)
2. Sign in with GitHub
3. Click "New app"
4. Select repository: `AntBap23/SmartLogix-Transport-Optimization`
5. Branch: `main`
6. Main file: `app/dashboard/app.py`
7. Click "Deploy"

### After Deployment

- [ ] App loads successfully
- [ ] All tabs accessible
- [ ] Order Forecasting works (may need Google Drive file IDs)
- [ ] Route Optimization works
- [ ] No errors in logs

## 📝 Notes

- **Google Drive files**: Large files are on Google Drive. Add file IDs to `app/data_loader.py` if you want automatic downloads.
- **Sample data**: The app works with the included small CSV files for forecasting.
- **Route visuals**: Pre-generated visuals are in `app/dashboard/route_visuals/`.

## 🆘 Troubleshooting

**Issue**: App won't start on Streamlit Cloud  
**Solution**: Check `requirements.txt` has all dependencies

**Issue**: Forecasting tab shows error  
**Solution**: Ensure `orders_cleaned.csv` is in the repo (it is - 3.5MB)

**Issue**: Large files needed  
**Solution**: Add Google Drive file IDs to `app/data_loader.py` (see `GOOGLE_DRIVE_SETUP.md`)

---

✅ **Ready to deploy when all items above are checked!**

