"""
DAG 0 — wm_0_init_infrastructure
Schedule : None (trigger manual, hanya dijalankan sekali)

Tugas:
  [1] check_rustfs          → pastikan RustFS bisa diakses
  [2] create_bucket         → buat bucket 'weather' di RustFS
  [3] create_bronze_prefix  → buat prefix 'bronze/' di bucket
  [4] init_duckdb           → buat file .duckdb + schema + tabel kosong
  [5] seed_locations        → isi dim_location dari CSV adm4_madiun.csv
  [6] create_duckdb_pool    → daftarkan Airflow Pool 'duckdb_pool' (kapasitas 1)

Semua task idempotent — aman dijalankan ulang tanpa merusak data yang sudah ada.
"""

import csv
import logging
import os
from datetime import datetime

import boto3
import duckdb
from airflow import DAG
from airflow.models import Pool
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.db import provide_session
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

# ─── Constants ──────────────────────────────────────────────────────────────
RUSTFS_BUCKET  = "weather"
BRONZE_PREFIX  = "bronze/.keep"
DUCKDB_PATH    = "/opt/airflow/duckdb/weather.duckdb"
SEEDS_PATH     = "/opt/airflow/dags/seeds/adm4_madiun.csv"
DUCKDB_POOL    = "duckdb_pool"

# Mapping adm3 → nama kecamatan
KECAMATAN_MAP = {
    "35.77.01": "Kartoharjo",
    "35.77.02": "Manguharjo",
    "35.77.03": "Taman",
}


# ─── Helper ─────────────────────────────────────────────────────────────────

def get_s3_client():
    """Buat boto3 S3 client yang mengarah ke RustFS."""
    return boto3.client(
        "s3",
        endpoint_url          = os.environ["RUSTFS_ENDPOINT"],
        aws_access_key_id     = os.environ["RUSTFS_ACCESS_KEY"],
        aws_secret_access_key = os.environ["RUSTFS_SECRET_KEY"],
        region_name           = "us-east-1",
    )


# ─── Task Functions ─────────────────────────────────────────────────────────

def task_check_rustfs():
    """
    [1] Pastikan RustFS bisa diakses sebelum melakukan apapun.
    Kalau koneksi gagal, DAG berhenti di sini dengan pesan yang jelas.
    """
    client = get_s3_client()
    client.list_buckets()
    logger.info("[CHECK] RustFS dapat diakses.")


def task_create_bucket():
    """
    [2] Buat bucket 'weather' di RustFS.
    Idempotent: kalau bucket sudah ada, skip tanpa error.
    """
    client = get_s3_client()

    try:
        client.create_bucket(Bucket=RUSTFS_BUCKET)
        logger.info(f"[BUCKET] Bucket '{RUSTFS_BUCKET}' berhasil dibuat.")
    except ClientError as e:
        if e.response["Error"]["Code"] == "BucketAlreadyOwnedByYou":
            logger.info(f"[BUCKET] Bucket '{RUSTFS_BUCKET}' sudah ada, skip.")
        else:
            raise


def task_create_bronze_prefix():
    """
    [3] Buat 'folder' bronze/ di bucket weather.
    Object storage tidak punya folder asli — kita upload file kosong
    bernama 'bronze/.keep' sebagai penanda prefix.
    Idempotent: upload ulang hanya overwrite file kosong yang sama.
    """
    client = get_s3_client()
    client.put_object(
        Bucket = RUSTFS_BUCKET,
        Key    = BRONZE_PREFIX,
        Body   = b"",
    )
    logger.info(f"[PREFIX] Prefix 'bronze/' siap di bucket '{RUSTFS_BUCKET}'.")


def task_init_duckdb():
    """
    [4] Inisialisasi file DuckDB:
        - Buat schema 'staging' dan 'gold'
        - Buat tabel staging: stg_weather_forecast
        - Buat tabel gold   : dim_date, dim_time, dim_location,
                              dim_weather_condition,
                              fact_comfort_safety, fact_energy_proxy,
                              fact_agriculture_proxy
    Semua pakai CREATE ... IF NOT EXISTS → idempotent.
    """
    con = duckdb.connect(DUCKDB_PATH)

    con.execute("CREATE SCHEMA IF NOT EXISTS staging;")
    con.execute("CREATE SCHEMA IF NOT EXISTS gold;")
    logger.info("[DUCKDB] Schema 'staging' dan 'gold' siap.")

    # ── Staging ─────────────────────────────────────────────────────────────
    con.execute("""
        CREATE TABLE IF NOT EXISTS staging.stg_weather_forecast (
            stg_id          INTEGER PRIMARY KEY,
            location_id     VARCHAR NOT NULL,       -- adm4
            utc_datetime    TIMESTAMP NOT NULL,
            local_datetime  TIMESTAMP NOT NULL,
            t               DECIMAL(4,1),           -- suhu udara (°C)
            hu              DECIMAL(5,2),           -- kelembapan (%)
            weather_code    INTEGER,                -- kode cuaca numerik BMKG
            weather_desc    VARCHAR,                -- deskripsi bahasa Indonesia
            weather_desc_en VARCHAR,                -- deskripsi bahasa Inggris
            ws              DECIMAL(5,2),           -- kecepatan angin (km/jam)
            wd              VARCHAR,                -- arah angin dari (N/S/E/W)
            wd_deg          INTEGER,                -- arah angin dalam derajat
            wd_to           VARCHAR,                -- arah angin tuju
            tcc             DECIMAL(5,2),           -- tutupan awan (%)
            vs              INTEGER,                -- jarak pandang (meter)
            vs_text         VARCHAR,                -- jarak pandang teks ("> 10 km")
            tp              DECIMAL(5,2),           -- curah hujan (mm)
            analysis_date   TIMESTAMP,              -- waktu produksi prakiraan
            time_index      VARCHAR,                -- label slot waktu BMKG (contoh: "12-13")
            image_url       VARCHAR,                -- URL ikon cuaca BMKG
            _run_hour       INTEGER NOT NULL,         -- jam run pipeline (23 atau 11 UTC)
            _batch_date     DATE NOT NULL,          -- tanggal batch pipeline
            _ingested_at    TIMESTAMP DEFAULT now() -- waktu insert ke staging
        );
    """)
    con.execute("CREATE SEQUENCE IF NOT EXISTS staging.stg_id_seq START 1;")
    logger.info("[DUCKDB] Tabel staging.stg_weather_forecast + sequence siap.")

    # ── Dimensi ─────────────────────────────────────────────────────────────
    con.execute("""
        CREATE TABLE IF NOT EXISTS gold.dim_date (
            date_key    INTEGER PRIMARY KEY,    -- format YYYYMMDD, contoh: 20260622
            date        DATE NOT NULL,
            year        INTEGER,
            month       INTEGER,
            day         INTEGER,
            day_of_week INTEGER,                -- 0=Senin, 6=Minggu
            day_name    VARCHAR,                -- contoh: 'Senin'
            month_name  VARCHAR,                -- contoh: 'Juni'
            quarter     INTEGER,
            is_weekend  BOOLEAN
        );
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS gold.dim_time (
            time_key    INTEGER PRIMARY KEY,    -- format HH (0-23)
            hour        INTEGER NOT NULL,
            time_of_day VARCHAR                 -- 'Pagi', 'Siang', 'Sore', 'Malam'
        );
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS gold.dim_location (
            location_id   VARCHAR PRIMARY KEY,
            location_name VARCHAR NOT NULL,          -- nama kelurahan
            kecamatan     VARCHAR,
            city          VARCHAR,
            province      VARCHAR,
            latitude      DOUBLE,
            longitude     DOUBLE,
            timezone      VARCHAR,
            is_coastal    BOOLEAN
        );
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS gold.dim_weather_condition (
            weather_key     INTEGER PRIMARY KEY,
            weather_code    INTEGER NOT NULL UNIQUE,
            weather_desc    VARCHAR,
            weather_desc_en VARCHAR
        );
    """)
    logger.info("[DUCKDB] Tabel dimensi siap.")

    # ── Fakta ────────────────────────────────────────────────────────────────
    con.execute("""
        CREATE TABLE IF NOT EXISTS gold.fact_comfort_safety (
            fact_id         INTEGER PRIMARY KEY,
            date_key        INTEGER,
            time_key        INTEGER,
            location_id     VARCHAR,
            weather_key     INTEGER,
            temperature     DECIMAL(4,1),
            humidity        DECIMAL(5,2),
            wind_speed      DECIMAL(5,2),
            wind_direction  VARCHAR,
            heat_index      DECIMAL(5,2),
            comfort_label   VARCHAR
        );
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS gold.fact_energy_proxy (
            fact_id         INTEGER PRIMARY KEY,
            date_key        INTEGER,
            time_key        INTEGER,
            location_id     VARCHAR,
            weather_key     INTEGER,
            temperature     DECIMAL(4,1),
            cloud_cover     DECIMAL(5,2),
            wind_speed      DECIMAL(5,2),
            visibility_km   DECIMAL(6,3),
            solar_proxy     VARCHAR,
            wind_proxy      VARCHAR
        );
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS gold.fact_agriculture_proxy (
            fact_id             INTEGER PRIMARY KEY,
            date_key            INTEGER,
            time_key            INTEGER,
            location_id         VARCHAR,
            weather_key         INTEGER,
            temperature         DECIMAL(4,1),
            humidity            DECIMAL(5,2),
            precipitation       DECIMAL(5,2),
            wind_speed          DECIMAL(5,2),
            rain_flag           BOOLEAN,
            irrigation_flag     BOOLEAN
        );
    """)
    logger.info("[DUCKDB] Tabel factdbt run --select dim_date siap.")

    con.close()
    logger.info(f"[DUCKDB] Inisialisasi selesai → {DUCKDB_PATH}")


def task_seed_locations():
    """
    [5] Isi dim_location dari file CSV adm4_madiun.csv.
    Idempotent: pakai INSERT OR IGNORE agar row yang sudah ada tidak di-overwrite.

    Kolom CSV yang dipakai:
        adm4, nama_kelurahan, latitude, longitude, nama_kota

    Kolom yang di-derive atau di-hardcode:
        kecamatan → dari 3 segmen pertama adm4 (35.77.01 → 'Kartoharjo')
        province  → 'Jawa Timur'
        timezone  → 'Asia/Jakarta'
        is_coastal→ FALSE
    """
    con = duckdb.connect(DUCKDB_PATH)

    with open(SEEDS_PATH, newline="", encoding="utf-8") as f:
        reader  = csv.DictReader(f)
        seeded  = 0
        skipped = 0

        for row in reader:
            adm4      = row["adm4"].strip()
            adm3      = ".".join(adm4.split(".")[:3])   # ambil 3 segmen pertama
            kecamatan = KECAMATAN_MAP.get(adm3, "Unknown")

            existing = con.execute(
                "SELECT 1 FROM gold.dim_location WHERE location_id = ?", [adm4]
            ).fetchone()

            if existing:
                skipped += 1
                continue

            con.execute("""
                INSERT INTO gold.dim_location
                    (location_id, location_name, kecamatan, city,
                     province, latitude, longitude, timezone, is_coastal)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                adm4,
                row["nama_kelurahan"].strip(),
                kecamatan,
                row["nama_kota"].strip(),
                "Jawa Timur",
                float(row["latitude"]),
                float(row["longitude"]),
                "Asia/Jakarta",
                False,
            ])
            seeded += 1

    con.close()
    logger.info(f"[SEED] {seeded} kelurahan berhasil di-seed, {skipped} sudah ada (skip).")


@provide_session
def task_create_duckdb_pool(session=None):
    """
    [6] Daftarkan Airflow Pool 'duckdb_pool' dengan kapasitas 1.
    Memastikan task dbt tidak berjalan paralel agar tidak conflict
    saat menulis ke file .duckdb yang sama.
    Idempotent: kalau pool sudah ada, skip.
    """
    existing = session.query(Pool).filter(Pool.pool == DUCKDB_POOL).first()

    if existing:
        logger.info(f"[POOL] Pool '{DUCKDB_POOL}' sudah ada (slots={existing.slots}), skip.")
        return

    pool = Pool(
        pool        = DUCKDB_POOL,
        slots       = 1,
        description = "DuckDB single writer — mencegah concurrent write conflict",
    )
    session.add(pool)
    session.commit()
    logger.info(f"[POOL] Pool '{DUCKDB_POOL}' berhasil dibuat (slots=1).")


# ─── DAG Definition ─────────────────────────────────────────────────────────

with DAG(
    dag_id          = "wm_0_init_infrastructure",
    description     = "One-time setup: RustFS bucket + DuckDB schema + seed lokasi",
    schedule        = None,
    start_date      = datetime(2026, 1, 1),
    catchup         = False,
    max_active_runs = 1,
    tags            = ["init", "setup"],
) as dag:

    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end")

    check_rustfs = PythonOperator(
        task_id         = "check_rustfs",
        python_callable = task_check_rustfs,
    )

    create_bucket = PythonOperator(
        task_id         = "create_bucket",
        python_callable = task_create_bucket,
    )

    create_bronze_prefix = PythonOperator(
        task_id         = "create_bronze_prefix",
        python_callable = task_create_bronze_prefix,
    )

    init_duckdb = PythonOperator(
        task_id         = "init_duckdb",
        python_callable = task_init_duckdb,
    )

    seed_locations = PythonOperator(
        task_id         = "seed_locations",
        python_callable = task_seed_locations,
    )

    create_duckdb_pool = PythonOperator(
        task_id         = "create_duckdb_pool",
        python_callable = task_create_duckdb_pool,
    )

    # ── Flow ────────────────────────────────────────────────────────────────
    (
        start
        >> check_rustfs
        >> create_bucket
        >> create_bronze_prefix
        >> init_duckdb
        >> seed_locations
        >> create_duckdb_pool
        >> end
    )