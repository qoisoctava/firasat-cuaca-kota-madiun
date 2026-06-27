"""
api_bmkg.py
Fetcher data cuaca dari API BMKG per kode adm4 Kota Madiun.
Dipanggil oleh DAG wm_1_bmkg_to_bronze.

Catatan:
- Tidak menggunakan proxy (27 kelurahan << 60 req/menit limit BMKG)
- Mengambil semua record prakiraan (~20 per lokasi, ±60 jam ke depan)
- Nama field diselaraskan dengan schema staging.stg_weather_forecast
"""

import csv
import logging
import time
from datetime import datetime
from typing import Dict, List, Optional

import requests

logger = logging.getLogger(__name__)

# ─── Constants ──────────────────────────────────────────────────────────────
API_URL       = "https://api.bmkg.go.id/publik/prakiraan-cuaca"
ADM4_PATH     = "/opt/airflow/dags/seeds/adm4_madiun.csv"
FETCH_DELAY   = 1      # detik antar request, hindari rate limit
MAX_RETRIES   = 3      # jumlah retry jika request gagal
RETRY_BACKOFF = 5      # detik awal backoff, dikali 2 setiap retry

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
}


# ─── Helpers ────────────────────────────────────────────────────────────────

def load_adm4_list(path: str = ADM4_PATH) -> List[str]:
    """
    Baca daftar kode adm4 dari CSV Kota Madiun.
    Return list of string adm4.
    """
    adm4_list = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            adm4_list.append(row["adm4"].strip())

    logger.info(f"[ADM4] {len(adm4_list)} kode adm4 dimuat dari {path}.")
    return adm4_list


def parse_datetime(raw: Optional[str]) -> Optional[str]:
    """
    Parse string datetime dari API ke ISO format string.
    Return None jika input kosong atau format tidak dikenali.
    """
    if not raw:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ"):
        try:
            return datetime.strptime(raw.strip(), fmt).isoformat()
        except ValueError:
            continue
    logger.warning(f"[PARSE] Format datetime tidak dikenali: {raw!r}")
    return None


# ─── Core Functions ─────────────────────────────────────────────────────────

def fetch_weather(adm4: str) -> Optional[Dict]:
    """
    Fetch data cuaca dari API BMKG untuk satu kode adm4.
    Retry hingga MAX_RETRIES kali dengan exponential backoff.
    Return dict payload JSON atau None jika semua attempt gagal.
    """
    backoff = RETRY_BACKOFF

    for attempt in range(1, MAX_RETRIES + 1):
        logger.info(f"[FETCH] Attempt #{attempt} — adm4={adm4}")
        try:
            resp = requests.get(
                API_URL,
                params  = {"adm4": adm4},
                headers = HEADERS,
                timeout = 15,
            )

            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", backoff))
                logger.warning(f"[FETCH] adm4={adm4}: 429 rate limit — tunggu {wait}s...")
                time.sleep(wait)
                backoff *= 2
                continue

            resp.raise_for_status()
            return resp.json()

        except requests.exceptions.RequestException as e:
            logger.error(f"[FETCH] adm4={adm4}: request error — {e}")
            if attempt < MAX_RETRIES:
                logger.info(f"[FETCH] Retry dalam {backoff}s...")
                time.sleep(backoff)
                backoff *= 2

    logger.error(f"[FETCH] Semua {MAX_RETRIES} attempt gagal untuk adm4={adm4}.")
    return None


def extract_records(payload: Dict) -> List[Dict]:
    """
    Flatten nested array cuaca dari payload BMKG menjadi list of dict.
    Mengambil SEMUA record (~20 per lokasi, ±60 jam ke depan).

    Struktur payload BMKG:
        payload["lokasi"]           → metadata lokasi
        payload["data"][0]["cuaca"] → array of arrays, dikelompokkan per hari
            cuaca[0] → sisa hari ini (jumlah bervariasi)
            cuaca[1] → besok (8 record)
            cuaca[2] → lusa (8 record)

    Semua group di-flatten menjadi satu list datar.
    """
    if not payload or "lokasi" not in payload or "data" not in payload:
        logger.warning("[EXTRACT] Payload kosong atau format tidak dikenali.")
        return []

    lokasi           = payload["lokasi"]
    raw_cuaca_groups = payload["data"][0].get("cuaca", [])

    # Flatten array of arrays → list datar
    flat = [item for group in raw_cuaca_groups for item in group]

    if not flat:
        logger.warning(f"[EXTRACT] Tidak ada data cuaca untuk adm4={lokasi.get('adm4')}.")
        return []

    records = []
    for item in flat:
        records.append({
            # ── Identitas lokasi ──────────────────────────────────────────
            "location_id":     lokasi.get("adm4"),

            # ── Waktu ─────────────────────────────────────────────────────
            "utc_datetime":    parse_datetime(item.get("utc_datetime")),
            "local_datetime":  parse_datetime(item.get("local_datetime")),

            # ── Cuaca utama ───────────────────────────────────────────────
            "t":               item.get("t"),
            "hu":              item.get("hu"),
            "weather_code":    item.get("weather"),
            "weather_desc":    item.get("weather_desc"),
            "weather_desc_en": item.get("weather_desc_en"),

            # ── Angin ─────────────────────────────────────────────────────
            "ws":              item.get("ws"),
            "wd":              item.get("wd"),
            "wd_deg":          item.get("wd_deg"),
            "wd_to":           item.get("wd_to"),

            # ── Awan & jarak pandang ──────────────────────────────────────
            "tcc":             item.get("tcc"),
            "vs":              item.get("vs"),
            "vs_text":         item.get("vs_text"),

            # ── Hujan ─────────────────────────────────────────────────────
            "tp":              item.get("tp"),

            # ── Metadata prakiraan ────────────────────────────────────────
            "analysis_date":   parse_datetime(item.get("analysis_date")),
            "time_index":      item.get("time_index"),
            "image_url":       item.get("image"),
        })

    logger.info(f"[EXTRACT] {len(records)} record dari adm4={lokasi.get('adm4')}.")
    return records


def fetch_all(adm4_path: str = ADM4_PATH) -> List[Dict]:
    """
    Entry point utama — dipanggil oleh DAG wm_1_bmkg_to_bronze.
    Iterasi semua adm4 Kota Madiun, fetch, flatten, return list of dict.

    Return:
        List of dict, setiap dict = satu baris prakiraan cuaca per lokasi per waktu.
        Total diharapkan: 27 kelurahan x ~20 record = ~540 record per run.
    """
    adm4_list   = load_adm4_list(adm4_path)
    all_records = []
    failed      = []

    for adm4_code in adm4_list:
        payload = fetch_weather(adm4_code)

        if payload:
            records = extract_records(payload)
            if records:
                all_records.extend(records)
            else:
                failed.append(adm4_code)
        else:
            failed.append(adm4_code)

        time.sleep(FETCH_DELAY)

    logger.info(f"[DONE] {len(all_records)} record dari {len(adm4_list)} adm4.")
    if failed:
        logger.warning(f"[DONE] {len(failed)} adm4 gagal: {failed}")

    return all_records
