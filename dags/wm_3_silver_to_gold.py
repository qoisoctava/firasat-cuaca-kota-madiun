"""
DAG 3 — wm_3_silver_to_gold
Schedule: triggered otomatis oleh DAG 2 via Airflow Dataset

Tugas:
  Menjalankan seluruh dbt project menggunakan Astronomer Cosmos.
  Setiap model dbt menjadi satu task terpisah di Airflow UI,
  dengan dependency antar task ditentukan otomatis oleh Cosmos
  berdasarkan ref() di setiap model.

Flow task yang dihasilkan Cosmos:
  int_weather_forecast
        │
        ├── dim_date
        ├── dim_time
        ├── dim_weather_condition
        ├── dim_location
        │
        ├── fact_comfort_safety
        ├── fact_energy_proxy
        └── fact_agriculture_proxy

Upstream : wm_2_bronze_to_silver (trigger via Dataset)
"""

import logging
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.empty import EmptyOperator
from cosmos import DbtTaskGroup, ExecutionConfig, ProjectConfig, RenderConfig
from cosmos.config import ProfileConfig
from cosmos.constants import ExecutionMode

logger = logging.getLogger(__name__)

# ─── Constants ──────────────────────────────────────────────────────────────
DBT_PROJECT_PATH = Path("/opt/airflow/dbt")
DBT_PROFILES_PATH = Path("/opt/airflow/dbt")
DBT_EXECUTABLE   = "/home/airflow/.local/bin/dbt"

SILVER_DATASET = Dataset("s3://weather/silver/")

# ─── Default args ───────────────────────────────────────────────────────────
default_args = {
    "owner":            "wm-pipeline",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
}

# ─── Cosmos Config ──────────────────────────────────────────────────────────

# ProjectConfig: tunjukkan lokasi dbt project
project_config = ProjectConfig(
    dbt_project_path = DBT_PROJECT_PATH,
)

# ProfileConfig: tunjukkan lokasi profiles.yml
profile_config = ProfileConfig(
    profile_name          = "weather_madiun",
    target_name           = "dev",
    profiles_yml_filepath = DBT_PROFILES_PATH / "profiles.yml",
)

# ExecutionConfig: jalankan dbt secara lokal di dalam container
# ExecutionMode.LOCAL = Cosmos panggil dbt CLI langsung,
# bukan via subprocess atau virtual environment terpisah
execution_config = ExecutionConfig(
    execution_mode    = ExecutionMode.LOCAL,
    dbt_executable_path = DBT_EXECUTABLE,
)

# RenderConfig: konfigurasi tampilan di Airflow UI
render_config = RenderConfig(
    select = ["path:models/"],   # jalankan semua model di folder models/
)


# ─── DAG Definition ─────────────────────────────────────────────────────────

with DAG(
    dag_id          = "wm_3_silver_to_gold",
    default_args    = default_args,
    description     = "Jalankan dbt models via Cosmos: Silver → Gold layer",
    schedule        = [SILVER_DATASET],   # triggered otomatis setelah DAG 2
    start_date      = datetime(2026, 1, 1),
    catchup         = False,
    max_active_runs = 1,
    tags            = ["transform", "gold", "dbt", "cosmos"],
) as dag:

    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end")

    # DbtTaskGroup: Cosmos parse dbt project dan buat task per model
    # operator_args: konfigurasi tambahan untuk setiap task yang dibuat Cosmos
    dbt_group = DbtTaskGroup(
        group_id         = "dbt_models",
        project_config   = project_config,
        profile_config   = profile_config,
        execution_config = execution_config,
        render_config    = render_config,
        operator_args    = {
            "pool": "duckdb_pool",   # serialisasi akses DuckDB
        },
    )

    # ── Flow ────────────────────────────────────────────────────────────────
    start >> dbt_group >> end
