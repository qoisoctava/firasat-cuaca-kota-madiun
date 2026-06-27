# streamlit/utils/db.py
import duckdb
import pandas as pd

DUCKDB_PATH = "/opt/streamlit/duckdb/weather.duckdb"


def get_connection():
    """
    Buka koneksi DuckDB dalam mode read-only.
    Read-only agar tidak conflict dengan Airflow yang sedang menulis.
    """
    return duckdb.connect(DUCKDB_PATH, read_only=True)


def query(sql: str) -> pd.DataFrame:
    """
    Jalankan query SQL dan return sebagai pandas DataFrame.
    Koneksi dibuka dan ditutup setiap query agar tidak ada
    koneksi yang tergantung terlalu lama.
    """
    con = get_connection()
    try:
        return con.execute(sql).df()
    finally:
        con.close()


# ── Query helpers ────────────────────────────────────────────────────────────

def get_locations() -> pd.DataFrame:
    return query("""
        SELECT
            location_id,
            location_name,
            kecamatan,
            latitude,
            longitude
        FROM gold.dim_location
        ORDER BY kecamatan, location_name
    """)


def get_latest_forecast() -> pd.DataFrame:
    """
    Ambil prakiraan terbaru per lokasi — hanya jam terdekat dari sekarang.
    Dipakai untuk overview peta.
    """
    return query("""
        WITH latest AS (
            SELECT
                f.location_id,
                f.date_key,
                f.time_key,
                f.temperature,
                f.humidity,
                f.wind_speed,
                f.heat_index,
                f.comfort_label,
                d.date,
                t.time_of_day,
                l.location_name,
                l.kecamatan,
                l.latitude,
                l.longitude,
                w.weather_desc_en,
                ROW_NUMBER() OVER (
                    PARTITION BY f.location_id
                    ORDER BY d.date, t.time_key
                ) AS rn
            FROM gold.fact_comfort_safety f
            JOIN gold.dim_date     d ON f.date_key     = d.date_key
            JOIN gold.dim_time     t ON f.time_key     = t.time_key
            JOIN gold.dim_location l ON f.location_id  = l.location_id
            JOIN gold.dim_weather_condition w ON f.weather_key = w.weather_key
        )
        SELECT * EXCLUDE (rn)
        FROM latest
        WHERE rn = 1
    """)


def get_comfort_timeseries(location_id: str) -> pd.DataFrame:
    return query(f"""
        SELECT
            d.date,
            t.hour,
            t.time_of_day,
            f.temperature,
            f.humidity,
            f.heat_index,
            f.comfort_label,
            f.wind_speed
        FROM gold.fact_comfort_safety f
        JOIN gold.dim_date d ON f.date_key = d.date_key
        JOIN gold.dim_time t ON f.time_key = t.time_key
        WHERE f.location_id = '{location_id}'
        ORDER BY d.date, t.hour
    """)


def get_energy_summary() -> pd.DataFrame:
    return query("""
        SELECT
            l.kecamatan,
            l.location_name,
            l.latitude,
            l.longitude,
            AVG(f.cloud_cover)                              AS avg_cloud_cover,
            AVG(f.wind_speed)                               AS avg_wind_speed,
            AVG(f.visibility_km)                            AS avg_visibility_km,
            COUNT(*) FILTER (WHERE f.solar_proxy = 'Tinggi') AS solar_tinggi,
            COUNT(*) FILTER (WHERE f.solar_proxy = 'Sedang') AS solar_sedang,
            COUNT(*) FILTER (WHERE f.solar_proxy = 'Rendah') AS solar_rendah,
            COUNT(*) FILTER (WHERE f.wind_proxy  = 'Kencang') AS wind_kencang,
            COUNT(*) FILTER (WHERE f.wind_proxy  = 'Sedang')  AS wind_sedang,
            COUNT(*) FILTER (WHERE f.wind_proxy  = 'Lemah')   AS wind_lemah
        FROM gold.fact_energy_proxy f
        JOIN gold.dim_location l ON f.location_id = l.location_id
        GROUP BY l.kecamatan, l.location_name, l.latitude, l.longitude
        ORDER BY l.kecamatan, l.location_name
    """)


def get_agriculture_summary() -> pd.DataFrame:
    return query("""
        SELECT
            l.kecamatan,
            l.location_name,
            l.latitude,
            l.longitude,
            AVG(f.temperature)                               AS avg_temperature,
            AVG(f.humidity)                                  AS avg_humidity,
            AVG(f.precipitation)                             AS avg_precipitation,
            COUNT(*) FILTER (WHERE f.rain_flag = true)       AS rain_hours,
            COUNT(*) FILTER (WHERE f.irrigation_flag = true) AS irrigation_hours,
            COUNT(*)                                         AS total_hours
        FROM gold.fact_agriculture_proxy f
        JOIN gold.dim_location l ON f.location_id = l.location_id
        GROUP BY l.kecamatan, l.location_name, l.latitude, l.longitude
        ORDER BY l.kecamatan, l.location_name
    """)