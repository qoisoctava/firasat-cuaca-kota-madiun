"""
DAG 2 — wm_2_bronze_to_silver
Schedule: triggered otomatis oleh DAG 1 via Airflow Dataset

Tugas:
  [1] read_manifest    → baca manifest terbaru dari RustFS
                         untuk tahu file Parquet mana yang perlu diproses
  [2] load_to_staging  → konfigurasi DuckDB httpfs agar bisa baca RustFS
                         hapus data lama untuk batch yang sama (idempotent)
                         INSERT dari Parquet langsung ke stg_weather_forecast
  [3] validate_staging → hitung row yang berhasil diinsert, pastikan masuk akal

Keunggulan utama:
  DuckDB membaca Parquet LANGSUNG dari RustFS via httpfs — tidak perlu
  download file dulu ke container. Query pushdown bekerja secara otomatis.

Upstream : wm_1_bmkg_to_bronze (trigger via Dataset)
Downstream: wm_3_silver_to_gold (trigger via Dataset, dibuat di DAG 3)
"""

import json
import logging
import os
from datetime import datetime, timedelta

import boto3
import duckdb
from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

# ─── Constants ──────────────────────────────────────────────────────────────
RUSTFS_BUCKET  = "weather"
DUCKDB_PATH    = "/opt/airflow/duckdb/weather.duckdb"
MIN_ROWS       = 400    # minimum row yang dianggap wajar setelah insert

BRONZE_DATASET = Dataset("s3://weather/bronze/")
SILVER_DATASET = Dataset("s3://weather/silver/")   # untuk trigger DAG 3 nanti

# ─── Default args ───────────────────────────────────────────────────────────
default_args = {
    "owner":            "wm-pipeline",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          3,
    "retry_delay":      timedelta(minutes=5),
}


# ─── Helper ─────────────────────────────────────────────────────────────────

def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url          = os.environ["RUSTFS_ENDPOINT"],
        aws_access_key_id     = os.environ["RUSTFS_ACCESS_KEY"],
        aws_secret_access_key = os.environ["RUSTFS_SECRET_KEY"],
        region_name           = "us-east-1",
    )

def get_duckdb_conn():
    """
    Buka koneksi ke file DuckDB dan konfigurasi httpfs agar bisa
    membaca Parquet langsung dari RustFS (S3-compatible).

    Konfigurasi penting:
    - s3_endpoint    : alamat RustFS tanpa 'http://' (DuckDB tambahkan sendiri)
    - s3_url_style   : 'path' untuk RustFS/MinIO (bukan 'vhost' seperti AWS S3)
    - s3_use_ssl     : false karena RustFS lokal pakai HTTP bukan HTTPS
    """
    endpoint   = os.environ["RUSTFS_ENDPOINT"].replace("http://", "")
    access_key = os.environ["RUSTFS_ACCESS_KEY"]
    secret_key = os.environ["RUSTFS_SECRET_KEY"]

    con = duckdb.connect(DUCKDB_PATH)

    # Install dan load ekstensi httpfs
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")

    # Konfigurasi koneksi ke RustFS
    con.execute(f"SET s3_endpoint='{endpoint}';")
    con.execute(f"SET s3_access_key_id='{access_key}';")
    con.execute(f"SET s3_secret_access_key='{secret_key}';")
    con.execute("SET s3_use_ssl=false;")
    con.execute("SET s3_url_style='path';")   # wajib untuk RustFS/MinIO

    return con


# ─── Task Functions ─────────────────────────────────────────────────────────

def task_read_manifest(**ctx):
    """
    [1] Baca manifest terbaru dari RustFS.

    Manifest ditulis oleh DAG 1 di path:
        weather/bronze/_manifests/YYYYMMDD_HH.json

    Kita list semua manifest, ambil yang terbaru (sort by nama file),
    lalu push isinya ke XCom agar task berikutnya tahu file mana
    yang perlu diproses.
    """
    client = get_s3_client()

    # List semua file di folder _manifests
    response = client.list_objects_v2(
        Bucket = RUSTFS_BUCKET,
        Prefix = "bronze/_manifests/",
    )

    contents = response.get("Contents", [])
    if not contents:
        raise RuntimeError("[MANIFEST] Tidak ada manifest ditemukan di RustFS.")

    # Sort by nama file (format YYYYMMDD_HH → sort kronologis otomatis)
    manifests  = sorted(
        [obj["Key"] for obj in contents if obj["Key"].endswith(".json")],
    )
    latest_key = manifests[-1]

    # Baca isi manifest
    obj      = client.get_object(Bucket=RUSTFS_BUCKET, Key=latest_key)
    manifest = json.loads(obj["Body"].read().decode("utf-8"))

    logger.info(f"[MANIFEST] Membaca manifest : {latest_key}")
    logger.info(f"[MANIFEST] Target Parquet   : {manifest['object_key']}")
    logger.info(f"[MANIFEST] Batch date       : {manifest['batch_date']}")
    logger.info(f"[MANIFEST] Run hour         : {manifest['run_hour']}")
    logger.info(f"[MANIFEST] Row count        : {manifest['row_count']}")

    ctx["ti"].xcom_push(key="manifest",   value=manifest)
    ctx["ti"].xcom_push(key="object_key", value=manifest["object_key"])
    ctx["ti"].xcom_push(key="batch_date", value=manifest["batch_date"])
    ctx["ti"].xcom_push(key="run_hour",   value=manifest["run_hour"])


def task_load_to_staging(**ctx):
    """
    [2] Baca Parquet dari RustFS dan insert ke staging.stg_weather_forecast.

    Cara kerja:
    - DuckDB membaca file Parquet LANGSUNG dari RustFS via httpfs
      (tidak perlu download ke container dulu)
    - DELETE dulu berdasarkan _batch_date + _run_hour agar idempotent
      (kalau DAG di-re-run, data lama dihapus dan diganti yang baru)
    - INSERT menggunakan nextval('staging.stg_id_seq') untuk generate
      stg_id secara otomatis dan unik
    - _run_hour disimpan per row agar validasi dan filter berikutnya akurat
    """
    ti         = ctx["ti"]
    object_key = ti.xcom_pull(task_ids="read_manifest", key="object_key")
    batch_date = ti.xcom_pull(task_ids="read_manifest", key="batch_date")
    run_hour   = ti.xcom_pull(task_ids="read_manifest", key="run_hour")

    parquet_path = f"s3://{RUSTFS_BUCKET}/{object_key}"
    run_hour_int = int(run_hour)

    con = get_duckdb_conn()

    try:
        # ── Idempotent: hapus data lama untuk batch ini ──────────────────
        deleted = con.execute("""
            DELETE FROM staging.stg_weather_forecast
            WHERE _batch_date = ?
              AND _run_hour   = ?
        """, [batch_date, run_hour_int]).rowcount

        if deleted > 0:
            logger.info(f"[LOAD] {deleted} row lama dihapus (idempotent re-run).")

        # ── INSERT dari Parquet langsung via httpfs ───────────────────────
        con.execute(f"""
            INSERT INTO staging.stg_weather_forecast (
                stg_id,
                location_id,
                utc_datetime,
                local_datetime,
                t,
                hu,
                weather_code,
                weather_desc,
                weather_desc_en,
                ws,
                wd,
                wd_deg,
                wd_to,
                tcc,
                vs,
                vs_text,
                tp,
                analysis_date,
                time_index,
                image_url,
                _run_hour,
                _batch_date
            )
            SELECT
                nextval('staging.stg_id_seq'),
                location_id,
                utc_datetime::TIMESTAMP,
                local_datetime::TIMESTAMP,
                t::DECIMAL(4,1),
                hu::DECIMAL(5,2),
                weather_code::INTEGER,
                weather_desc,
                weather_desc_en,
                ws::DECIMAL(5,2),
                wd,
                wd_deg::INTEGER,
                wd_to,
                tcc::DECIMAL(5,2),
                vs::INTEGER,
                vs_text,
                tp::DECIMAL(5,2),
                analysis_date::TIMESTAMP,
                time_index,
                image_url,
                {run_hour_int},
                '{batch_date}'::DATE
            FROM read_parquet('{parquet_path}')
        """)

        # Hitung row yang baru diinsert untuk batch ini
        row_count = con.execute("""
            SELECT COUNT(*) FROM staging.stg_weather_forecast
            WHERE _batch_date = ?
              AND _run_hour   = ?
        """, [batch_date, run_hour_int]).fetchone()[0]

        ctx["ti"].xcom_push(key="inserted_rows", value=row_count)
        logger.info(f"[LOAD] {row_count} row berhasil diinsert ke staging.")

    finally:
        con.close()


def task_validate_staging(**ctx):
    """
    [3] Validasi hasil insert ke staging.

    Pengecekan:
    - Jumlah row yang diinsert >= MIN_ROWS (400)
    - Tidak ada row dengan location_id NULL
    - Tidak ada row dengan utc_datetime NULL
    """
    ti            = ctx["ti"]
    inserted_rows = ti.xcom_pull(task_ids="load_to_staging", key="inserted_rows")
    batch_date    = ti.xcom_pull(task_ids="read_manifest",   key="batch_date")
    run_hour      = ti.xcom_pull(task_ids="read_manifest",   key="run_hour")
    run_hour_int  = int(run_hour)

    con = get_duckdb_conn()

    try:
        # Cek 1: jumlah row masuk akal
        if inserted_rows < MIN_ROWS:
            raise ValueError(
                f"[VALIDATE] Hanya {inserted_rows} row diinsert — "
                f"minimum {MIN_ROWS}. Ada masalah saat load."
            )

        # Cek 2: tidak ada NULL di kolom kritis
        null_check = con.execute("""
            SELECT
                COUNT(*) FILTER (WHERE location_id IS NULL)  AS null_location,
                COUNT(*) FILTER (WHERE utc_datetime IS NULL) AS null_datetime
            FROM staging.stg_weather_forecast
            WHERE _batch_date = ?
              AND _run_hour   = ?
        """, [batch_date, run_hour_int]).fetchone()

        null_location, null_datetime = null_check

        if null_location > 0:
            raise ValueError(f"[VALIDATE] {null_location} row dengan location_id NULL.")
        if null_datetime > 0:
            raise ValueError(f"[VALIDATE] {null_datetime} row dengan utc_datetime NULL.")

        logger.info(
            f"[VALIDATE] OK — {inserted_rows} row valid di staging "
            f"untuk batch {batch_date} run hour {run_hour}."
        )

    finally:
        con.close()


# ─── DAG Definition ─────────────────────────────────────────────────────────

with DAG(
    dag_id          = "wm_2_bronze_to_silver",
    default_args    = default_args,
    description     = "Baca Parquet dari RustFS Bronze → load ke DuckDB Silver (staging)",
    schedule        = [BRONZE_DATASET],
    start_date      = datetime(2026, 1, 1),
    catchup         = False,
    max_active_runs = 1,
    tags            = ["transform", "silver", "duckdb"],
) as dag:

    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(
        task_id = "end",
        outlets = [SILVER_DATASET],   # trigger DAG 3 otomatis (dibuat nanti)
    )

    read_manifest = PythonOperator(
        task_id         = "read_manifest",
        python_callable = task_read_manifest,
    )

    load_staging = PythonOperator(
        task_id         = "load_to_staging",
        python_callable = task_load_to_staging,
        pool            = "duckdb_pool",
    )

    validate = PythonOperator(
        task_id         = "validate_staging",
        python_callable = task_validate_staging,
        pool            = "duckdb_pool",
    )

    # ── Flow ────────────────────────────────────────────────────────────────
    start >> read_manifest >> load_staging >> validate >> end