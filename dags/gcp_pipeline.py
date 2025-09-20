from __future__ import annotations
import os, json, time, math, pytz, requests
from datetime import datetime, timezone
from typing import List, Dict

import pandas as pd
from google.cloud import bigquery, storage 

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable
import pendulum

# ---------------- CONFIGURATION ----------------
PROJECT_ID   = "airflowbigqueryproject"
DATASET      = "airflow_bq_looker_project"
TABLE        = "openweather_15_cities_v2"
RAW_BUCKET   = os.getenv("RAW_BUCKET", "weather-raw-archive") 
UNITS        = "metric"
IST          = pytz.timezone("Asia/Kolkata")
TZ           = pendulum.timezone("Asia/Kolkata")

CITIES = [
    "Mumbai,IN", "Delhi,IN", "Bengaluru,IN", "Hyderabad,IN", "Ahmedabad,IN",
    "Chennai,IN", "Kolkata,IN", "Surat,IN", "Pune,IN", "Jaipur,IN",
    "Lucknow,IN", "Kanpur,IN", "Nagpur,IN", "Indore,IN", "Bhopal,IN"
]

# Rate limiting: OpenWeather free tier commonly allows 60 calls/min.
MAX_CALLS_PER_MIN = 55
SLEEP_BETWEEN_CALLS_SEC = max(0.0, 60.0 / MAX_CALLS_PER_MIN)


# fetching environment variable
def _get_api_key() -> str:
    key = os.environ.get("OPENWEATHER_API_KEY")
    if not key:
        raise RuntimeError("Set OPENWEATHER_API_KEY via Composer env vars or Secret Manager")
    return key

# Checking if table exists in bq
def ensure_table():
    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.{DATASET}.{TABLE}"

    schema = [
        bigquery.SchemaField("city", "STRING"),
        bigquery.SchemaField("country", "STRING"),
        bigquery.SchemaField("coord_lat", "FLOAT"),
        bigquery.SchemaField("coord_lon", "FLOAT"),
        bigquery.SchemaField("temp", "FLOAT"),
        bigquery.SchemaField("feels_like", "FLOAT"),
        bigquery.SchemaField("humidity", "INTEGER"),
        bigquery.SchemaField("pressure", "INTEGER"),
        bigquery.SchemaField("weather_main", "STRING"),
        bigquery.SchemaField("weather_desc", "STRING"),
        bigquery.SchemaField("wind_speed", "FLOAT"),
        bigquery.SchemaField("wind_deg", "INTEGER"),
        bigquery.SchemaField("ts_ist", "DATETIME"),
        bigquery.SchemaField("ts_utc", "TIMESTAMP"),
        bigquery.SchemaField("source_dt_utc", "TIMESTAMP"),
    ]

    try:
        client.get_table(table_id)
        return
    except Exception:
        pass

    table = bigquery.Table(table_id, schema=schema)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="ts_utc"
    )
    table.clustering_fields = ["city"]
    client.create_table(table)
    print(f"Created table {table_id} with partitioning on ts_utc and clustering on city")

def fetch_and_archive_raw(**context) -> List[Dict]:
    """Fetch per-city JSON; optionally drop raw payloads to GCS; return parsed list."""
    api_key = _get_api_key()
    now_ist = datetime.now(IST)
    now_utc = datetime.now(timezone.utc)

    session = requests.Session()
    rows = []
    raw_objects = []

    for idx, city in enumerate(CITIES):
        try:
            resp = session.get(
                "https://api.openweathermap.org/data/2.5/weather",
                params={"q": city, "appid": api_key, "units": UNITS},
                timeout=30,
            )
            resp.raise_for_status()
            j = resp.json()

            # Collect parsed row
            rows.append({
                "city": j.get("name"),
                "country": (j.get("sys") or {}).get("country"),
                "coord_lat": (j.get("coord") or {}).get("lat"),
                "coord_lon": (j.get("coord") or {}).get("lon"),
                "temp": (j.get("main") or {}).get("temp"),
                "feels_like": (j.get("main") or {}).get("feels_like"),
                "humidity": (j.get("main") or {}).get("humidity"),
                "pressure": (j.get("main") or {}).get("pressure"),
                "weather_main": (j.get("weather") or [{}])[0].get("main"),
                "weather_desc": (j.get("weather") or [{}])[0].get("description"),
                "wind_speed": (j.get("wind") or {}).get("speed"),
                "wind_deg": (j.get("wind") or {}).get("deg"),
                "ts_ist": now_ist.replace(tzinfo=None),
                "ts_utc": now_utc,
                "source_dt_utc": datetime.fromtimestamp(j.get("dt", 0), tz=timezone.utc),
            })

            # Keep raw (optional)
            raw_objects.append({"city": city, "fetched_utc": now_utc.isoformat(), "payload": j})

        except Exception as e:
            print(f"Failed to fetch {city}: {e}")

        time.sleep(SLEEP_BETWEEN_CALLS_SEC)

    if not rows:
        raise RuntimeError("No rows fetched from OpenWeather")

    # Archive raw JSON list to GCS 
    if RAW_BUCKET:
        try:
            gcs = storage.Client(project=PROJECT_ID)
            bucket = gcs.bucket(RAW_BUCKET)
            ymdh = datetime.now(timezone.utc).strftime("%Y/%m/%d/%H")
            blob = bucket.blob(f"openweather/{ymdh}/payload.json")
            blob.upload_from_string(json.dumps(raw_objects, ensure_ascii=False), content_type="application/json")
            print(f"Archived raw JSON to gs://{RAW_BUCKET}/openweather/{ymdh}/payload.json")
        except Exception as e:
            print(f"Raw archive failed (non-blocking): {e}")

    # Push to XCom for next task
    return rows

def load_idempotent(**context):
    rows = context["ti"].xcom_pull(task_ids="fetch_and_archive_raw")
    df = pd.DataFrame(rows)

    client = bigquery.Client(project=PROJECT_ID)
    dataset_table = f"{PROJECT_ID}.{DATASET}.{TABLE}"
    stage_table   = f"{PROJECT_ID}.{DATASET}.{TABLE}_stage"

    # Stage current batch
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",  # overwrite stage per run
        schema=[
            bigquery.SchemaField("city", "STRING"),
            bigquery.SchemaField("country", "STRING"),
            bigquery.SchemaField("coord_lat", "FLOAT"),
            bigquery.SchemaField("coord_lon", "FLOAT"),
            bigquery.SchemaField("temp", "FLOAT"),
            bigquery.SchemaField("feels_like", "FLOAT"),
            bigquery.SchemaField("humidity", "INTEGER"),
            bigquery.SchemaField("pressure", "INTEGER"),
            bigquery.SchemaField("weather_main", "STRING"),
            bigquery.SchemaField("weather_desc", "STRING"),
            bigquery.SchemaField("wind_speed", "FLOAT"),
            bigquery.SchemaField("wind_deg", "INTEGER"),
            bigquery.SchemaField("ts_ist", "DATETIME"),
            bigquery.SchemaField("ts_utc", "TIMESTAMP"),
            bigquery.SchemaField("source_dt_utc", "TIMESTAMP"),
        ],
    )
    client.load_table_from_dataframe(df, stage_table, job_config=job_config).result()

    # Idempotent MERGE (no dupes for same city + source_dt_utc)
    merge_sql = f"""
    CREATE TABLE IF NOT EXISTS `{dataset_table}` PARTITION BY DATE(ts_utc) CLUSTER BY city AS
    SELECT * FROM `{stage_table}` WHERE 1=0;

    MERGE `{dataset_table}` T
    USING `{stage_table}` S
      ON T.city = S.city AND T.source_dt_utc = S.source_dt_utc
    WHEN NOT MATCHED THEN
      INSERT ROW;
    """
    client.query(merge_sql).result()

def dq_check(**context):
    """Basic quality: row_count == len(CITIES), sane temp range, non-null keys in latest hour."""
    client = bigquery.Client(project=PROJECT_ID)
    table = f"`{PROJECT_ID}.{DATASET}.{TABLE}`"
    expected = len(CITIES)

    sql = f"""
    WITH recent AS (
      SELECT *
      FROM {table}
      WHERE ts_utc >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR)
    )
    SELECT
      COUNT(1) AS row_cnt,
      COUNTIF(temp < -20 OR temp > 55) AS bad_temp,
      COUNTIF(city IS NULL OR source_dt_utc IS NULL) AS bad_keys
    FROM recent
    """
    res = list(client.query(sql).result())[0]
    row_cnt  = res["row_cnt"]
    bad_temp = res["bad_temp"]
    bad_keys = res["bad_keys"]

    errors = []
    if row_cnt < expected * 0.8:  
        errors.append(f"Row count too low: {row_cnt} < {math.floor(expected*0.8)}")
    if bad_temp > 0:
        errors.append(f"Out-of-range temps: {bad_temp}")
    if bad_keys > 0:
        errors.append(f"Null key fields: {bad_keys}")

    if errors:
        raise RuntimeError("DQ Check failed: " + "; ".join(errors))

# --- DAG ----- #
with DAG(
    dag_id="openweather_to_bigquery_direct",
    start_date=pendulum.datetime(2025, 1, 1, tz=TZ),
    schedule_interval="15 * * * *",  # run at minute 15 of every hour (IST)
    catchup=False,
    tags=["api", "weather", "india", "bq"],
    default_args={
        "retries": 3,
        "retry_delay": pendulum.duration(minutes=2),   # task-level retries w/ backoff
    },
) as dag:

    t_ensure = PythonOperator(
        task_id="ensure_table",
        python_callable=ensure_table,
    )

    t_fetch = PythonOperator(
        task_id="fetch_and_archive_raw",
        python_callable=fetch_and_archive_raw,
    )

    t_load = PythonOperator(
        task_id="load_idempotent",
        python_callable=load_idempotent,
    )

    t_dq = PythonOperator(
        task_id="dq_check",
        python_callable=dq_check,
    )

    t_ensure >> t_fetch >> t_load >> t_dq