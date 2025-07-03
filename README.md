
# 🚚 SmartLogix Transport Optimization

SmartLogix is an intelligent routing and logistics optimization platform built with **Python**, **PostgreSQL**, and **Streamlit**, leveraging **Google OR-Tools** for route solving and **machine learning models** for delay prediction. The platform is fully containerized with **Docker** and orchestrated using **docker-compose**.

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
| Storage            | PostgreSQL for route and delivery data           |
| Deployment         | Docker + docker-compose                          |

---

## 🚀 Getting Started

### 1. Clone the Repository

```bash
git clone https://github.com/AntBap23/SmartLogix-Transport-Optimization.git
cd SmartLogix-Transport-Optimization
````

### 2. Setup Environment Variables

Create a `.env` file in the root directory:

```env
POSTGRES_USER=smartlogix
POSTGRES_PASSWORD=your_password
POSTGRES_DB=logistics
```

### 3. Start Docker Services

```bash
docker-compose up --build
```

This spins up:

* PostgreSQL container (preloaded with schema + delivery data)
* Streamlit container for the dashboard interface

---

## ▶️ How to Use

* Upload delivery data or use provided datasets from `data/`
* Configure routing parameters (number of vehicles, time windows, capacity)
* Launch the Streamlit app and visualize:

  * Optimized routes on a map
  * KPIs like total route distance and predicted delays
* Use the delay prediction module to flag high-risk deliveries

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

## 🔮 Future Roadmap

* Add traffic-aware routing using OSRM or Google Maps API
* Export routes in JSON/GeoJSON for external mapping tools
* Support clustering-based delivery zoning
* Add REST API to serve model predictions externally

---

## 🛡 License

This project is released under the [MIT License](./LICENSE) for academic and portfolio use.

---

## 👤 Author

**Anthony Baptiste**
[LinkedIn](https://www.linkedin.com/in/anthony-baptiste00)
[Portfolio](https://antbap23.github.io/portfolio)





