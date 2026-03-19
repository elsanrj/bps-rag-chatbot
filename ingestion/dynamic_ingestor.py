"""
dynamic_ingestor.py
Fase 1A - Hari 6: Ingestion Data Dinamis BPS Kota Bandung

Tanggung jawab modul ini:
  - Fetch semua variabel (var) dari WebAPI BPS domain 3273
  - Fetch semua tahun tersedia dari endpoint /list/model/th
  - Fetch data (datacontent) untuk setiap var_id × th_id
  - Simpan hasil mentah ke MongoDB collection `raw_docs`
  - TIDAK melakukan preprocessing/konversi — raw apa adanya

Flow:
  fetch_all_variables()              → list var_id
  fetch_all_tahun()                  → list {th_id, th_label}
  fetch_variable_data(var_id, th_id) → raw datacontent per kombinasi

Jalankan:
  python -m ingestion.dynamic_ingestor

Env variables yang diperlukan (.env):
  BPS_API_KEY    : API key WebAPI BPS
  MONGODB_URI    : Connection string MongoDB Atlas
  MONGODB_DB     : Nama database (contoh: bps_rag)
"""

import os
import time
import logging
import requests

from datetime import datetime
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Konstanta
DOMAIN       = "3273"           # Kota Bandung
BASE_URL     = "https://webapi.bps.go.id/v1/api"
API_KEY      = os.getenv("BPS_API_KEY")
MONGODB_URI  = os.getenv("MONGODB_URI")
MONGODB_DB   = os.getenv("MONGODB_DB")
COLLECTION   = "raw_docs"

REQUEST_DELAY     = 0.5   # detik antar request (hindari rate limit)
RETRY_MAX         = 3     # jumlah percobaan ulang jika request gagal
RETRY_DELAY       = 2.0   # detik sebelum retry

# ---------------------------------------------------------------------------
# HTTP Helper
# ---------------------------------------------------------------------------

def fetch_with_retry(url: str, params: dict = None) -> dict | None:
    """
    GET request dengan retry logic.
    Return dict JSON jika berhasil, None jika gagal setelah RETRY_MAX percobaan.
    """
    for attempt in range(1, RETRY_MAX + 1):
        try:
            response = requests.get(url, params=params, timeout=15)
            response.raise_for_status()
            data = response.json()

            if data.get("status") != "OK":
                logger.warning(f"API status bukan OK: {data.get('status')} | URL: {url}")
                return None

            return data

        except requests.exceptions.Timeout:
            logger.warning(f"Timeout (percobaan {attempt}/{RETRY_MAX}): {url}")
        except requests.exceptions.HTTPError as e:
            logger.warning(f"HTTP error {e.response.status_code} (percobaan {attempt}/{RETRY_MAX}): {url}")
        except requests.exceptions.RequestException as e:
            logger.warning(f"Request error (percobaan {attempt}/{RETRY_MAX}): {e}")
        except ValueError:
            logger.warning(f"Response bukan JSON valid: {url}")
            return None

        if attempt < RETRY_MAX:
            time.sleep(RETRY_DELAY)

    logger.error(f"Gagal setelah {RETRY_MAX} percobaan: {url}")
    return None


# ---------------------------------------------------------------------------
# Step 1: Fetch semua variabel (var_id list)
# ---------------------------------------------------------------------------

def fetch_all_variables() -> list[dict]:
    """
    Fetch semua variabel dari endpoint /list/model/var.
    Handle pagination otomatis (total pages bisa mencapai 250+).

    Return list of dict, masing-masing berisi:
      var_id, title, sub_id, sub_name, subcsa_id, subcsa_name, def, unit, notes
    """
    url = f"{BASE_URL}/list/model/var/domain/{DOMAIN}/key/{API_KEY}/"
    all_vars = []
    page = 1

    logger.info("Mulai fetch daftar variabel (var)...")

    while True:
        data = fetch_with_retry(url, params={"page": page})

        if not data:
            logger.error(f"Gagal fetch halaman {page} dari daftar variabel. Berhenti.")
            break

        # Response data: [pagination_info, [list_of_vars]]
        payload = data.get("data", [])
        if len(payload) < 2:
            logger.warning(f"Format response tidak sesuai di halaman {page}")
            break

        pagination = payload[0]
        var_list   = payload[1]

        if not isinstance(var_list, list) or len(var_list) == 0:
            break

        for var in var_list:
            all_vars.append({
                "var_id"      : var.get("var_id"),
                "title"       : var.get("title"),
                "sub_id"      : var.get("sub_id"),
                "sub_name"    : var.get("sub_name"),
                "subcsa_id"   : var.get("subcsa_id"),
                "subcsa_name" : var.get("subcsa_name"),
                "def"         : var.get("def"),
                "unit"        : var.get("unit"),
                "notes"       : var.get("notes"),
            })

        total_pages = pagination.get("pages", 1)
        logger.info(f"  Halaman {page}/{total_pages} — {len(var_list)} variabel diambil")

        if page >= total_pages:
            break

        page += 1
        time.sleep(REQUEST_DELAY)

    logger.info(f"Total variabel berhasil di-fetch: {len(all_vars)}")
    return all_vars

def fetch_all_years() -> list[dict]:
    """
    Fetch semua tahun tersedia dari endpoint /list/model/th.
    Return list of dict: [{th_id, th}, ...]
    """
    url = f"{BASE_URL}/list/model/th/domain/{DOMAIN}/key/{API_KEY}/"
    all_years = []
    page = 1

    logger.info("Mulai fetch daftar tahun (th)...")

    while True:
        data = fetch_with_retry(url, params={"page": page})

        if not data:
            logger.error(f"Gagal fetch halaman {page} dari daftar tahun. Berhenti.")
            break

        payload = data.get("data", [])
        if len(payload) < 2:
            break

        pagination = payload[0]
        year_list  = payload[1]

        if not isinstance(year_list, list) or len(year_list) == 0:
            break

        for item in year_list:
            all_years.append({
                "th_id" : item.get("th_id"),
                "th"    : item.get("th"),
            })

        total_pages = pagination.get("pages", 1)
        logger.info(f"  Halaman {page}/{total_pages} — {len(year_list)} tahun diambil")

        if page >= total_pages:
            break

        page += 1
        time.sleep(REQUEST_DELAY)

    logger.info(f"Total tahun berhasil di-fetch: {len(all_years)}")
    return all_years

# ---------------------------------------------------------------------------
# Step 2: Fetch data untuk satu var_id (semua tahun tersedia)
# ---------------------------------------------------------------------------

def fetch_variable_data(var_id: int, th_id: int) -> dict | None:
    """
    Fetch data aktual untuk satu var_id pada satu th_id tertentu.
    th_id didapat dari endpoint /list/model/th
    
    """
    url = (
        f"{BASE_URL}/list/model/data/domain/{DOMAIN}"
        f"/var/{var_id}/th/{th_id}/key/{API_KEY}/"
    )
    data = fetch_with_retry(url)

    if not data:
        return None

    if data.get("data-availability") != "available":
        logger.debug(f"Data tidak tersedia untuk var_id={var_id}")
        return None

    return data


# ---------------------------------------------------------------------------
# Step 3: Simpan raw ke MongoDB
# ---------------------------------------------------------------------------

def get_mongo_collection():
    """Inisialisasi koneksi MongoDB dan return collection raw_docs."""
    client = MongoClient(MONGODB_URI)
    db     = client[MONGODB_DB]
    return db[COLLECTION]


def save_raw_variables(var_list: list[dict], collection) -> int:
    """
    Simpan list variabel ke MongoDB (upsert berdasarkan var_id).
    Return jumlah dokumen yang berhasil disimpan.
    """
    if not var_list:
        return 0

    operations = [
        UpdateOne(
            filter={"raw_id": f"var_{v['var_id']}", "source_type": "dynamic_var"},
            update={
                "$set": {
                    "raw_id"      : f"var_{v['var_id']}",
                    "source_type" : "dynamic_var",
                    "fetched_at"  : datetime.utcnow(),
                    "payload"     : v,
                }
            },
            upsert=True,
        )
        for v in var_list
    ]

    try:
        result = collection.bulk_write(operations, ordered=False)
        saved = result.upserted_count + result.modified_count
        logger.info(f"Variabel tersimpan/diperbarui: {saved}")
        return saved
    except BulkWriteError as e:
        logger.error(f"BulkWriteError saat simpan variabel: {e.details}")
        return 0


def save_raw_data(var_id: int, tahun_label: str, th_id: int, raw_data: dict, collection) -> bool:
    """
    Simpan satu raw data response ke MongoDB (upsert berdasarkan var_id + tahun).
    Return True jika berhasil.
    """
    raw_id = f"data_{var_id}_{tahun_label}"

    try:
        collection.update_one(
            filter={"raw_id": raw_id, "source_type": "dynamic_data"},
            update={
                "$set": {
                    "raw_id"      : raw_id,
                    "source_type" : "dynamic_data",
                    "source_url"   : f"{BASE_URL}/list/model/data/domain/{DOMAIN}/var/{var_id}/th/{th_id}/key/API_KEY/",
                    "var_id"      : var_id,
                    "tahun"       : tahun_label,
                    "fetched_at"  : datetime.utcnow(),
                    "payload"     : raw_data,
                }
            },
            upsert=True,
        )
        return True
    except Exception as e:
        logger.error(f"Gagal simpan data var_id={var_id} tahun={tahun_label}: {e}")
        return False


# ---------------------------------------------------------------------------
# Orchestrator utama
# ---------------------------------------------------------------------------

def run(var_limit: int = None, year_limit: int = None):
    """
    Pipeline utama:
      1. Fetch semua variabel
      2. Simpan variabel ke raw_docs
      3. Untuk setiap var_id: fetch data → simpan raw

    Parameter:
      var_limit : Batasi jumlah var yang diproses (berguna saat testing/dev).
                  None = proses semua.
    """
    if not API_KEY:
        logger.error("BPS_API_KEY tidak ditemukan di environment. Hentikan.")
        return

    if not MONGODB_URI:
        logger.error("MONGODB_URI tidak ditemukan di environment. Hentikan.")
        return

    collection = get_mongo_collection()
    logger.info(f"Terhubung ke MongoDB: {MONGODB_DB}.{COLLECTION}")

    # --- Step 1: Fetch variabel --- (choose 1 method)
    # all_vars = fetch_all_variables()
    all_vars = [doc["payload"] for doc in collection.find({"source_type": "dynamic_var"})]

    if not all_vars:
        logger.error("Tidak ada variabel yang berhasil di-fetch. Hentikan.")
        return

    # --- Step 2: Simpan variabel --- (done)
    save_raw_variables(all_vars, collection)

    # --- Step 3: Fetch data per var_id ---
    if var_limit:
        logger.info(f"Mode dev: hanya memproses {var_limit} variabel pertama")
        all_vars = all_vars[:var_limit]

    success_count = 0
    skip_count    = 0
    error_count   = 0

    logger.info(f"Mulai fetch data untuk {len(all_vars)} variabel...")

    # --- Step 2.5: Fetch semua tahun ---
    all_years = fetch_all_years()
    
    all_years = all_years[:year_limit]
    
    if not all_years:
        logger.error("Tidak ada tahun yang berhasil di-fetch. Hentikan.")
        return

    total_combinations = len(all_vars) * len(all_years)
    logger.info(
        f"Mulai fetch data: {len(all_vars)} var × {len(all_years)} tahun "
        f"= {total_combinations} kombinasi"
    )

    counter = 0
    for var in all_vars:
        var_id = var["var_id"]

        for year in all_years:
            th_id       = year["th_id"]
            tahun_label = year["th"]
            counter    += 1

            existing = collection.find_one({
                "source_type" : "dynamic_data",
                "var_id"      : var_id,
                "tahun"       : tahun_label,
            })
            if existing:
                logger.debug(f"[{counter}] var_id={var_id} tahun={tahun_label} sudah ada, skip.")
                skip_count += 1
                time.sleep(REQUEST_DELAY)
                continue

            raw_data = fetch_variable_data(var_id, th_id)

            if raw_data is None:
                error_count += 1
                logger.debug(f"[{counter}] Tidak tersedia: var_id={var_id} tahun={tahun_label}")
                time.sleep(REQUEST_DELAY)
                continue

            ok = save_raw_data(var_id, tahun_label, th_id, raw_data, collection)

            if ok:
                success_count += 1
                logger.info(f"[{counter}/{total_combinations}] OK: var_id={var_id} tahun={tahun_label}")
            else:
                error_count += 1

            time.sleep(REQUEST_DELAY)
            
    # --- Summary ---
    logger.info("=" * 50)
    logger.info("INGESTION SELESAI")
    logger.info(f"  Berhasil  : {success_count}")
    logger.info(f"  Dilewati  : {skip_count} (sudah ada di DB)")
    logger.info(f"  Gagal     : {error_count}")
    logger.info("=" * 50)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # Saat development, batasi ke 20 variabel dulu untuk uji coba cepat
    # Ganti None untuk jalankan semua
    run(var_limit=20, year_limit=5)
