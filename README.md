# 🌦 Weather Data Pipeline on GCP  
**Composer → BigQuery → Colab → Looker Studio**

## 📌 Objective  
This project builds a fully automated data pipeline to **fetch and visualize live weather data** (temperature, humidity, conditions) for major Indian cities using the **Google Cloud Platform (GCP)** ecosystem.

The pipeline:  
- ⏱ Calls **OpenWeather API** hourly  
- ⚙️ Orchestrates ingestion with **Cloud Composer (Airflow)**  
- 🗄 Stores curated records in **BigQuery** (partitioned & clustered for cost efficiency)  
- ☁ Archives raw JSON into **Cloud Storage** (optional, for audits/backfills)  
- 📊 Explores insights in **Google Colab**  
- 📈 Visualizes results in **Looker Studio** dashboards  

---

## 🏗 Architecture  

![Weather Data Pipeline Architecture](<img width="1326" height="619" alt="image" src="https://github.com/user-attachments/assets/b411a0ad-c23c-4e04-bc91-b157bd1dd65a" />
)

---

## 🔧 Components  
- 🌐 **OpenWeather API** → Source of live weather data (15 Indian cities)  
- 🔑 **Secret Manager / Env Vars** → Secure API key storage  
- ☁ **Cloud Composer (Airflow)** → Orchestration of ingestion + checks  
- 🗄 **Cloud Storage (GCS)** → Raw JSON archive (`gs://weather-raw-archive/`)  
- 📊 **BigQuery** → Analytical warehouse  
  - Partitioned by `DATE(ts_utc)`  
  - Clustered by `city`  
  - Unique key → `(city, source_dt_utc)`  
- 📈 **Looker Studio** → Interactive visualization dashboards  
- 🐍 **Google Colab** → Exploratory analysis + insights generation  

---

## 🚀 Setup Steps  

### 1. OpenWeather API  
- Sign up: [OpenWeather API](https://openweathermap.org/api)  
- Generate and copy your API key  

### 2. Cloud Composer  
- Create a Composer environment in the same region as BigQuery dataset  
- Add environment variables:  
  ```bash
  OPENWEATHER_API_KEY=<your_api_key>
  RAW_BUCKET=weather-raw-archive
  ```

### 3. BigQuery  
- Create dataset: `airflow_bq_looker_project`  
- The DAG will create table **`openweather_15_cities_v2`**  
  - Partitioned + clustered by `city`  

### 4. Cloud Storage  
- Create bucket: `weather-raw-archive`  
- Raw JSON payloads stored as:  
  ```
  gs://weather-raw-archive/openweather/YYYY/MM/DD/HH/payload.json
  ```

### 5. Deploy DAG  
- Save DAG as `openweather_to_bq.py`  
- Upload to Composer’s DAGs bucket  
- Trigger DAG from Airflow UI  

### 6. Validate in BigQuery  
Run test query:  
```sql
SELECT city, temp, ts_utc
FROM `airflowbigqueryproject.airflow_bq_looker_project.openweather_15_cities_v2`
ORDER BY ts_utc DESC
LIMIT 20;
```

### 7. Looker Studio  
- Connect Looker Studio to BigQuery  
- Build dashboards for:  
  - Temperature trends per city  
  - Humidity & pressure comparisons  
  - Latest snapshot across all cities  

### 8. Google Colab  
- Connect Colab to BigQuery  
- Generate exploratory insights  

---

## 🔒 Reliability & Security  
- **Secrets** → API key stored in Env Vars  
- **Idempotency** → Stage + MERGE pattern prevents duplicates  
- **Partitioning & Clustering** → Optimizes query cost & performance  
- **Monitoring** →  
  - Row count check (≥ 80% of cities)  
  - Temperature sanity check (`-20°C < temp < 55°C`)  

---

## 🌱 Future Enhancements  
- Add a **dbt / Python transformation layer**  
- Enrich with other APIs (AQI, rainfall, weather alerts)  

---

## 📂 Project Structure  
```
├── dags/
│   └── openweather_to_bq.py    # Airflow DAG for ingestion
├── documentation/              # Notion Documentation
├── colab/                      # Colab notebooks for analysis
└── README.md                   # Project documentation
```
