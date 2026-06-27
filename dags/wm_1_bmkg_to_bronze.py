"""
DAG 1 — wm_1_bmkg_to_bronze
Schedule: 2x sehari — 06:00 WIB (23:00 UTC) dan 18:00 WIB (11:00 UTC)

Tugas:
  [1] fetch_bmkg       → fetch data cuaca dari API BMKG untuk 27 kelurahan Kota Madiun
  [2] validate_fetch   → pastikan data yang diterima masuk akal sebelum ditulis
  [3] upload_parquet   → konversi ke Parquet dan upload ke RustFS Bronze layer
  [4] write_manifest   → tulis manifest JSON sebagai tanda selesai + trigger DAG 2

Output RustFS:
  weather/
  └── bronze/
      └── YYYY/
          └── MM/
              └── DD/
                  └── HH/
                      └── bmkg_YYYYMMDD_HH.parquet

Downstream: wm_2_bronze_to_silver (trigger via Airflow Dataset)
"""

import json
import logging
import os
import sys
from datetime import datetime, timedelta
from io import BytesIO

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

# Tambahkan path ingestion agar api_bmkg bisa diimport
sys.path.insert(0, "/opt/airflow/ingestion")
from api_bmkg import fetch_all

logger = logging.getLogger(__name__)

# ─── Constants ──────────────────────────────────────────────────────────────
RUSTFS_BUCKET = "weather"
MIN_RECORDS   = 400    # minimum record yang dianggap wajar (27 kelurahan × ~20)

BRONZE_DATASET = Dataset("s3://weather/bronze/")

# ─── Default args ───────────────────────────────────────────────────────────
default_args = {
    "owner":            "wm-pipeline",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          3,
    "retry_delay":      timedelta(minutes=5),
}

# ─── Schema Parquet ─────────────────────────────────────────────────────────
# Mendefinisikan tipe data eksplisit agar konsisten setiap run
PARQUET_SCHEMA = pa.schema([
    pa.field("location_id",     pa.string()),
    pa.field("utc_datetime",    pa.string()),
    pa.field("local_datetime",  pa.string()),
    pa.field("t",               pa.float64()),
    pa.field("hu",              pa.float64()),
    pa.field("weather_code",    pa.int64()),
    pa.field("weather_desc",    pa.string()),
    pa.field("weather_desc_en", pa.string()),
    pa.field("ws",              pa.float64()),
    pa.field("wd",              pa.string()),
    pa.field("wd_deg",          pa.int64()),
    pa.field("wd_to",           pa.string()),
    pa.field("tcc",             pa.float64()),
    pa.field("vs",              pa.int64()),
    pa.field("vs_text",         pa.string()),
    pa.field("tp",              pa.float64()),
    pa.field("analysis_date",   pa.string()),
    pa.field("time_index",      pa.string()),
    pa.field("image_url",       pa.string()),
])


# ─── Helper ─────────────────────────────────────────────────────────────────

def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url          = os.environ["RUSTFS_ENDPOINT"],
        aws_access_key_id     = os.environ["RUSTFS_ACCESS_KEY"],
        aws_secret_access_key = os.environ["RUSTFS_SECRET_KEY"],
        region_name           = "us-east-1",
    )

def get_run_hour(execution_date: datetime) -> str:
    """
    Ambil jam run dalam format 2 digit (00 atau 12).
    Dipakai untuk partisi path dan nama file.
    """
    return execution_date.strftime("%H")


# ─── Task Functions ─────────────────────────────────────────────────────────

def task_fetch_bmkg(**ctx):
    """
    [1] Fetch data cuaca dari API BMKG untuk semua adm4 Kota Madiun.
    Push list of dict ke XCom.
    """
    batch_date = ctx["ds"]          # format: YYYY-MM-DD
    records    = fetch_all()

    if not records:
        raise RuntimeError("[FETCH] Tidak ada data yang berhasil di-fetch dari BMKG.")

    # Tambahkan _batch_date ke setiap record
    for r in records:
        r["_batch_date"] = batch_date

    ctx["ti"].xcom_push(key="records",   value=records)
    ctx["ti"].xcom_push(key="row_count", value=len(records))

    logger.info(f"[FETCH] {len(records)} record berhasil di-fetch untuk {batch_date}.")


def task_validate_fetch(**ctx):
    """
    [2] Validasi data dari XCom sebelum ditulis ke RustFS.
    DAG berhenti di sini jika data tidak masuk akal.

    Pengecekan:
    - Records tidak kosong
    - Jumlah record >= MIN_RECORDS (400)
    - Semua record punya field 'location_id' dan 'utc_datetime'
    """
    ti        = ctx["ti"]
    records   = ti.xcom_pull(task_ids="fetch_bmkg", key="records")
    row_count = ti.xcom_pull(task_ids="fetch_bmkg", key="row_count")

    # Cek 1: tidak kosong
    if not records:
        raise ValueError("[VALIDATE] Records kosong — fetch mungkin gagal.")

    # Cek 2: jumlah masuk akal
    if row_count < MIN_RECORDS:
        raise ValueError(
            f"[VALIDATE] Hanya {row_count} record — minimum {MIN_RECORDS}. "
            f"Kemungkinan sebagian besar adm4 gagal di-fetch."
        )

    # Cek 3: field wajib ada di setiap record
    missing = [
        i for i, r in enumerate(records)
        if not r.get("location_id") or not r.get("utc_datetime")
    ]
    if missing:
        raise ValueError(
            f"[VALIDATE] {len(missing)} record tidak punya location_id atau utc_datetime."
        )

    logger.info(f"[VALIDATE] OK — {row_count} record lolos validasi.")


def task_upload_parquet(**ctx):
    """
    [3] Konversi records ke Parquet dan upload ke RustFS.
    Path: weather/bronze/YYYY/MM/DD/HH/bmkg_YYYYMMDD_HH.parquet
    """
    ti             = ctx["ti"]
    batch_date     = ctx["ds"]
    execution_date = ctx["execution_date"]
    run_hour       = get_run_hour(execution_date)
    date_nodash    = batch_date.replace("-", "")

    records = ti.xcom_pull(task_ids="fetch_bmkg", key="records")

    # Konversi list of dict → PyArrow Table → Parquet bytes
    table       = pa.Table.from_pylist(records, schema=PARQUET_SCHEMA)
    buffer      = BytesIO()
    pq.write_table(table, buffer, compression="snappy")
    parquet_bytes = buffer.getvalue()

    # Susun path RustFS
    year, month, day = batch_date.split("-")
    object_key = (
        f"bronze/{year}/{month}/{day}/{run_hour}/"
        f"bmkg_{date_nodash}_{run_hour}.parquet"
    )

    # Upload ke RustFS
    client = get_s3_client()
    client.put_object(
        Bucket       = RUSTFS_BUCKET,
        Key          = object_key,
        Body         = parquet_bytes,
        ContentType  = "application/octet-stream",
    )

    ti.xcom_push(key="object_key",  value=object_key)
    ti.xcom_push(key="size_bytes",  value=len(parquet_bytes))

    logger.info(
        f"[UPLOAD] s3://{RUSTFS_BUCKET}/{object_key} "
        f"({len(parquet_bytes):,} bytes, {len(records)} records)"
    )


def task_write_manifest(**ctx):
    """
    [4] Tulis manifest JSON ke RustFS sebagai tanda pipeline selesai.
    Manifest dipakai oleh DAG 2 untuk mengetahui file mana yang perlu diproses.
    Outlets ke Dataset → trigger DAG 2 secara otomatis.

    Path: weather/bronze/_manifests/YYYYMMDD_HH.json
    """
    ti             = ctx["ti"]
    batch_date     = ctx["ds"]
    execution_date = ctx["execution_date"]
    run_hour       = get_run_hour(execution_date)
    date_nodash    = batch_date.replace("-", "")

    object_key = ti.xcom_pull(task_ids="upload_parquet", key="object_key")
    size_bytes = ti.xcom_pull(task_ids="upload_parquet", key="size_bytes")
    row_count  = ti.xcom_pull(task_ids="fetch_bmkg",     key="row_count")

    manifest = {
        "dag_id":     "wm_1_bmkg_to_bronze",
        "run_id":     ctx["run_id"],
        "batch_date": batch_date,
        "run_hour":   run_hour,
        "created_at": datetime.utcnow().isoformat() + "Z",
        "source":     "bmkg",
        "bucket":     RUSTFS_BUCKET,
        "object_key": object_key,
        "row_count":  row_count,
        "size_bytes": size_bytes,
        "status":     "completed",
    }

    manifest_key   = f"bronze/_manifests/{date_nodash}_{run_hour}.json"
    manifest_bytes = json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8")

    client = get_s3_client()
    client.put_object(
        Bucket      = RUSTFS_BUCKET,
        Key         = manifest_key,
        Body        = manifest_bytes,
        ContentType = "application/json",
    )

    logger.info(f"[MANIFEST] s3://{RUSTFS_BUCKET}/{manifest_key}")


# ─── DAG Definition ─────────────────────────────────────────────────────────

with DAG(
    dag_id          = "wm_1_bmkg_to_bronze",
    default_args    = default_args,
    description     = "Fetch cuaca BMKG → Parquet → RustFS Bronze layer",
    schedule        = "0 23,11 * * *",   # 06:00 WIB (23 UTC) & 18:00 WIB (11 UTC)
    start_date      = datetime(2026, 1, 1),
    catchup         = False,
    max_active_runs = 1,
    tags            = ["ingest", "bmkg", "bronze"],
) as dag:

    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end")

    fetch_data = PythonOperator(
        task_id         = "fetch_bmkg",
        python_callable = task_fetch_bmkg,
    )

    validate = PythonOperator(
        task_id         = "validate_fetch",
        python_callable = task_validate_fetch,
    )

    upload = PythonOperator(
        task_id         = "upload_parquet",
        python_callable = task_upload_parquet,
    )

    manifest = PythonOperator(
        task_id         = "write_manifest",
        python_callable = task_write_manifest,
        outlets         = [BRONZE_DATASET],   # trigger DAG 2 otomatis
    )

    # ── Flow ────────────────────────────────────────────────────────────────
    start >> fetch_data >> validate >> upload >> manifest >> end
