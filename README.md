
# 🚚 SmartLogix Transport Optimization

SmartLogix is an intelligent routing and logistics optimization platform built with **Python**, **Databricks**, and **Streamlit**, leveraging **Google OR-Tools** for route solving and **machine learning models** for delay prediction. The platform is optimized for **Databricks** cloud computing environment with **Apache Spark** for big data processing.

---

## 🧠 Project Overview

- **Goal**: Optimize last-mile delivery routes and proactively predict delays in transportation logistics.
- **Optimization Engine**: Google OR-Tools with customizable constraints (e.g., time windows, capacity, delivery zones)
- **Predictive Layer**: Machine learning models to forecast late deliveries
- **Interface**: Streamlit dashboard for visualizing routing output and key logistics KPIs

---

## 📁 Repository Structure

```

smartlogix-transport-optimization/
│
├── data/              # Raw geospatial & delivery datasets (CSV/GeoJSON)
├── db/                # SQL scripts for PostgreSQL schema and seed data
├── notebooks/         # Jupyter notebooks for EDA, ML experiments, and forecasting
├── src/               # Core Python modules for routing, modeling, and utilities
├── tests/             # Unit tests for core logic and services
│
├── .env               # Environment variables for DB credentials and config
├── LICENSE            # License for open use
├── README.md          # Project documentation
├── docker-compose.yml # Orchestration of app + PostgreSQL container
├── requirements.txt   # Python dependencies

````

---

## ⚙️ Core Technologies

| Component          | Stack/Tools                                      |
|--------------------|--------------------------------------------------|
| Optimization       | Python, Google OR-Tools                          |
| Geospatial Routing | Custom distance matrix, time windows             |
| Prediction Model   | scikit-learn, pandas, delay classification       |
| Dashboard          | Streamlit for route/KPI visualization            |
| Storage            | Databricks Delta Tables, Parquet files           |
| Processing         | Apache Spark, PySpark for big data               |
| Deployment         | Databricks Workspace                             |

---

## 🚀 Getting Started

### 1. Clone the Repository

```bash
git clone https://github.com/AntBap23/SmartLogix-Transport-Optimization.git
cd SmartLogix-Transport-Optimization
````

### 2. Setup Databricks Environment

#### Option A: Databricks Workspace
1. Create a new Databricks workspace
2. Create a cluster with Python 3.9+ and Spark 3.4+
3. Install required libraries from `requirements.txt`

#### Option B: Local Development
```bash
pip install -r requirements.txt
```

### 3. Load Data

```bash
# Load data into Databricks tables
python db/load_data.py
```

This will:
* Load CSV data into Databricks Delta tables
* Process and clean the data using Spark
* Create optimized parquet files for analysis

---

## ▶️ How to Use

* Upload delivery data or use provided datasets from `data/`
* Configure routing parameters (number of vehicles, time windows, capacity)
* Run optimization algorithms using Databricks notebooks
* Launch the Streamlit app and visualize:

  * Optimized routes on a map
  * KPIs like total route distance and predicted delays
* Use the delay prediction module to flag high-risk deliveries
* Leverage Spark for large-scale data processing and ML model training

---

## 📊 Key Features

* ✅ **Constraint-based Routing**: Distance minimization with support for:

  * Time windows
  * Vehicle capacity
  * Zone-based delivery grouping
* 🤖 **Delay Forecasting**:

  * Predict delays based on historical delivery patterns
  * Classify risk using custom-trained models
* 📍 **Interactive Dashboard**:

  * Visual route maps
  * Fleet performance KPIs
  * Delay likelihood indicators

---

## 📈 Sample KPIs

| Metric            | Description                              |
| ----------------- | ---------------------------------------- |
| Route Distance    | Total travel distance after optimization |
| Stops per Vehicle | Efficiency of vehicle assignment         |
| Delay Probability | Model-predicted risk of shipment delay   |
| On-Time Rate      | % of deliveries predicted to be on-time  |

---

## 🧪 Testing

To run unit tests:

```bash
pytest tests/
```

---

## 📓 Databricks Notebooks

The project includes ready-to-use Databricks notebooks:

* **`notebooks/01_data_loading.py`** - Load and process data into Delta tables
* **`notebooks/02_route_optimization.py`** - Implement route optimization with OR-Tools

### Running Notebooks in Databricks

1. Upload notebooks to your Databricks workspace
2. Create a cluster with Python 3.9+ and Spark 3.4+
3. Install required libraries from `requirements.txt`
4. Upload data files to DBFS or use external storage
5. Run notebooks sequentially for complete setup

## 🔮 Future Roadmap

* Add traffic-aware routing using OSRM or Google Maps API
* Export routes in JSON/GeoJSON for external mapping tools
* Support clustering-based delivery zoning
* Add REST API to serve model predictions externally
* Implement MLflow for model versioning and deployment
* Add real-time streaming data processing

---

## 🛡 License

This project is released under the [MIT License](./LICENSE) for academic and portfolio use.

---

## 👤 Author

**Anthony Baptiste**
[LinkedIn](https://www.linkedin.com/in/anthony-baptiste00)
[Portfolio](https://antbap23.github.io/portfolio)





