"""
simdasi_ingestor.py
Fase 1B: Ingestion Data SIMDASI BPS Kota Bandung

Tanggung jawab modul ini:
  - Fetch semua subject (mms_id) dari SIMDASI wilayah 3273000
  - Fetch list tabel per mms_id → ambil id_tabel + ketersediaan_tahun
  - Fetch detail tabel per id_tabel × tahun
  - Simpan hasil mentah ke MongoDB collection `raw_docs`
  - TIDAK melakukan preprocessing/konversi — raw apa adanya

Flow tiga langkah:
  [1] /simdasi/id/22 → list subject (mms_id)
  [2] /simdasi/id/24 → list tabel per subject (id_tabel + ketersediaan_tahun)
  [3] /simdasi/id/25 → detail tabel per id_tabel × tahun

Jalankan:
  python -m ingestion.simdasi_ingestor

Env variables yang diperlukan (.env):
  BPS_API_KEY  : API key WebAPI BPS
  MONGODB_URI  : Connection string MongoDB Atlas
  MONGODB_DB   : Nama database (contoh: bps_rag)
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
WILAYAH      = "3273000"        # Kota Bandung
BASE_URL     = "https://webapi.bps.go.id/v1/api"
API_KEY      = os.getenv("BPS_API_KEY")
MONGODB_URI  = os.getenv("MONGODB_URI")
MONGODB_DB   = os.getenv("MONGODB_DB")
COLLECTION   = "raw_docs"

REQUEST_DELAY = 0.5   # detik antar request
RETRY_MAX     = 3     # jumlah percobaan ulang
RETRY_DELAY   = 2.0   # detik sebelum retry


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

            if data.get("data-availability") == "not available":
                logger.debug(f"Data tidak tersedia: {url}")
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


def extract_simdasi_payload(data: dict) -> dict | None:
    """
    Response SIMDASI punya struktur berlapis:
      data → [pagination, {status, condition, message, ..., data: [...]}]
    
    Fungsi ini mengekstrak dict payload inti dari struktur tersebut.
    Return None jika struktur tidak sesuai.

    Selalu verifikasi struktur aktual sebelum akses field — pelajaran dari dynamic data.
    """
    outer = data.get("data", [])

    # outer adalah list: [pagination_dict, payload_dict]
    if not isinstance(outer, list) or len(outer) < 2:
        logger.warning(f"Struktur response SIMDASI tidak sesuai: {list(data.keys())}")
        return None

    payload = outer[1]

    if not isinstance(payload, dict):
        logger.warning(f"Payload SIMDASI bukan dict: {type(payload)}")
        return None

    if payload.get("status") != 200:
        logger.warning(f"Payload status bukan 200: {payload.get('status')}")
        return None

    return payload


# ---------------------------------------------------------------------------
# Step 1: Fetch semua subject (mms_id)
# ---------------------------------------------------------------------------

def fetch_all_subjects() -> list[dict]:
    """
    Fetch semua subject dari endpoint /simdasi/id/22.
    Return list of dict: [{mms_id, bab, subject, tabel: [id_tabel, ...]}, ...]
    """
    url = f"{BASE_URL}/interoperabilitas/datasource/simdasi/id/22/wilayah/{WILAYAH}/key/{API_KEY}/"
    logger.info("Fetch list subject SIMDASI...")

    data = fetch_with_retry(url)
    if not data:
        logger.error("Gagal fetch list subject SIMDASI.")
        return []

    payload = extract_simdasi_payload(data)
    if not payload:
        return []

    subjects = payload.get("data", [])
    if not isinstance(subjects, list):
        logger.warning("Field 'data' di payload subject bukan list.")
        return []

    result = []
    for item in subjects:
        mms_id = item.get("mms_id")
        if mms_id is None:
            continue
        result.append({
            "mms_id"      : mms_id,
            "bab"         : item.get("bab"),
            "subject"     : item.get("subject"),
            "tabel"       : item.get("tabel", []),  # list id_tabel encoded
        })

    logger.info(f"Total subject ditemukan: {len(result)}")
    return result


# ---------------------------------------------------------------------------
# Step 2: Fetch list tabel per mms_id
# ---------------------------------------------------------------------------

def fetch_tables_by_subject(mms_id: int) -> list[dict]:
    """
    Fetch list tabel untuk satu mms_id dari endpoint /simdasi/id/24.
    Return list of dict: [{id_tabel, judul, ketersediaan_tahun, ...}, ...]
    """
    url = (
        f"{BASE_URL}/interoperabilitas/datasource/simdasi/id/24"
        f"/wilayah/{WILAYAH}/id_subjek/{mms_id}/key/{API_KEY}/"
    )

    data = fetch_with_retry(url)
    if not data:
        return []

    payload = extract_simdasi_payload(data)
    if not payload:
        return []

    tables = payload.get("data", [])
    if not isinstance(tables, list):
        return []

    result = []
    for item in tables:
        id_tabel = item.get("id_tabel")
        if not id_tabel:
            continue
        result.append({
            "id_tabel"           : id_tabel,
            "judul"              : item.get("judul"),
            "kode_tabel"         : item.get("kode_tabel"),
            "ketersediaan_tahun" : item.get("ketersediaan_tahun", []),  # array int
            "bab"                : item.get("bab"),
            "subject"            : item.get("subject"),
            "mms_id"             : mms_id,
            "latest_update"      : item.get("latest_update"),
        })

    return result


# ---------------------------------------------------------------------------
# Step 3: Fetch detail tabel per id_tabel × tahun
# ---------------------------------------------------------------------------

def fetch_table_detail(id_tabel: str, tahun: int) -> dict | None:
    """
    Fetch detail satu tabel untuk tahun tertentu dari endpoint /simdasi/id/25.
    Return dict raw response atau None jika tidak tersedia.
    """
    url = (
        f"{BASE_URL}/interoperabilitas/datasource/simdasi/id/25"
        f"/wilayah/{WILAYAH}/tahun/{tahun}/id_tabel/{id_tabel}/key/{API_KEY}/"
    )

    data = fetch_with_retry(url)
    if not data:
        return None

    payload = extract_simdasi_payload(data)
    return payload  # bisa None jika struktur tidak sesuai


# ---------------------------------------------------------------------------
# Storage
# ---------------------------------------------------------------------------

def get_mongo_collection():
    """Inisialisasi koneksi MongoDB dan return collection raw_docs."""
    client = MongoClient(MONGODB_URI)
    db     = client[MONGODB_DB]
    return db[COLLECTION]

def save_raw_subjects(subjects: list[dict], collection) -> int:
    """
    Simpan list subject SIMDASI ke MongoDB (upsert berdasarkan mms_id).
    Return jumlah dokumen yang berhasil disimpan.
    """
    if not subjects:
        return 0

    operations = [
        UpdateOne(
            filter={"raw_id": f"simdasi_subject_{s['mms_id']}", "source_type": "simdasi_subject"},
            update={
                "$set": {
                    "raw_id"      : f"simdasi_subject_{s['mms_id']}",
                    "source_type" : "simdasi_subject",
                    "fetched_at"  : datetime.utcnow(),
                    "payload"     : s,
                }
            },
            upsert=True,
        )
        for s in subjects
    ]

    try:
        result = collection.bulk_write(operations, ordered=False)
        saved = result.upserted_count + result.modified_count
        logger.info(f"Subject tersimpan/diperbarui: {saved}")
        return saved
    except BulkWriteError as e:
        logger.error(f"BulkWriteError saat simpan subject: {e.details}")
        return 0


def save_raw_tables(tables: list[dict], mms_id: int, collection) -> int:
    """
    Simpan list tabel untuk satu mms_id ke MongoDB (upsert berdasarkan id_tabel).
    Return jumlah dokumen yang berhasil disimpan.
    """
    if not tables:
        return 0

    operations = [
        UpdateOne(
            filter={"raw_id": f"simdasi_table_{t['id_tabel']}", "source_type": "simdasi_table"},
            update={
                "$set": {
                    "raw_id"      : f"simdasi_table_{t['id_tabel']}",
                    "source_type" : "simdasi_table",
                    "fetched_at"  : datetime.utcnow(),
                    "payload"     : t,
                }
            },
            upsert=True,
        )
        for t in tables
    ]

    try:
        result = collection.bulk_write(operations, ordered=False)
        return result.upserted_count + result.modified_count
    except BulkWriteError as e:
        logger.error(f"BulkWriteError saat simpan list tabel mms_id={mms_id}: {e.details}")
        return 0

def save_raw_table_detail(
    id_tabel: str,
    tahun: int,
    mms_id: int,
    judul: str,
    payload: dict,
    collection,
) -> bool:
    """
    Simpan satu raw detail tabel ke MongoDB (upsert berdasarkan id_tabel + tahun).
    Return True jika berhasil.

    Field standar raw_docs:
      raw_id, source_type, fetched_at, source_url, payload
    ditambah field konteks:
      id_tabel, tahun, mms_id, judul
    """
    raw_id = f"simdasi_{id_tabel}_{tahun}"

    try:
        collection.update_one(
            filter={"raw_id": raw_id, "source_type": "simdasi"},
            update={
                "$set": {
                    "raw_id"      : raw_id,
                    "source_type" : "simdasi",
                    "id_tabel"    : id_tabel,
                    "tahun"       : tahun,
                    "mms_id"      : mms_id,
                    "judul"       : judul,
                    "fetched_at"  : datetime.utcnow(),
                    "payload"     : payload,
                }
            },
            upsert=True,
        )
        return True
    except Exception as e:
        logger.error(f"Gagal simpan id_tabel={id_tabel} tahun={tahun}: {e}")
        return False


# ---------------------------------------------------------------------------
# Orchestrator utama
# ---------------------------------------------------------------------------

def run(subject_limit: int = None, tahun_limit: int = None):
    """
    Pipeline utama ingestion SIMDASI:
      1. Fetch semua subject (mms_id)
      2. Untuk setiap subject: fetch list tabel
      3. Untuk setiap tabel × tahun tersedia: fetch detail → simpan raw

    Parameter:
      subject_limit : Batasi jumlah subject yang diproses (untuk dev/testing).
      tahun_limit   : Batasi jumlah tahun per tabel (untuk dev/testing).
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

    # --- Step 1: Fetch subjects ---
    subjects = fetch_all_subjects()
    if not subjects:
        logger.error("Tidak ada subject yang berhasil di-fetch. Hentikan.")
        return
    
    save_raw_subjects(subjects, collection)

    if subject_limit:
        logger.info(f"Mode dev: hanya memproses {subject_limit} subject pertama")
        subjects = subjects[:subject_limit]

    success_count = 0
    skip_count    = 0
    error_count   = 0

    # --- Step 2: Loop subject → tabel ---
    for subj in subjects:
        mms_id  = subj["mms_id"]
        subject = subj["subject"]

        logger.info(f"Fetch tabel untuk subject: '{subject}' (mms_id={mms_id})")
        tables = fetch_tables_by_subject(mms_id)

        if not tables:
            logger.warning(f"Tidak ada tabel untuk mms_id={mms_id}")
            time.sleep(REQUEST_DELAY)
            continue
        
        logger.info(f"  {len(tables)} tabel ditemukan untuk mms_id={mms_id}")
        save_raw_tables(tables, mms_id, collection)

        # --- Step 3: Loop tabel × tahun ---
        for tbl in tables:
            id_tabel           = tbl["id_tabel"]
            judul              = tbl["judul"]
            ketersediaan_tahun = tbl["ketersediaan_tahun"]

            if tahun_limit:
                # Ambil N tahun terbaru — list sudah urut ascending, ambil dari belakang
                ketersediaan_tahun = ketersediaan_tahun[-tahun_limit:]

            for tahun in ketersediaan_tahun:
                raw_id = f"simdasi_{id_tabel}_{tahun}"

                # Skip jika sudah ada di MongoDB
                existing = collection.find_one(
                    {"raw_id": raw_id, "source_type": "simdasi"},
                    {"_id": 1}  # projection — hanya butuh tahu ada/tidak
                )
                if existing:
                    logger.debug(f"Skip (sudah ada): {raw_id}")
                    skip_count += 1
                    time.sleep(REQUEST_DELAY)
                    continue


                payload = fetch_table_detail(id_tabel, tahun)

                if payload is None:
                    logger.debug(f"Tidak tersedia: id_tabel={id_tabel} tahun={tahun}")
                    error_count += 1
                    time.sleep(REQUEST_DELAY)
                    continue

                ok = save_raw_table_detail(
                    id_tabel   = id_tabel,
                    tahun      = tahun,
                    mms_id     = mms_id,
                    judul      = judul,
                    payload    = payload,
                    collection = collection,
                )

                if ok:
                    success_count += 1
                    logger.info(f"  OK: id_tabel={id_tabel} tahun={tahun} | {judul[:50]}")
                else:
                    error_count += 1

                time.sleep(REQUEST_DELAY)

    # --- Summary ---
    logger.info("=" * 50)
    logger.info("INGESTION SIMDASI SELESAI")
    logger.info(f"  Berhasil  : {success_count}")
    logger.info(f"  Dilewati  : {skip_count} (sudah ada di DB)")
    logger.info(f"  Gagal     : {error_count}")
    logger.info("=" * 50)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # Mode dev: 2 subject, 3 tahun terbaru per tabel
    # Ganti None, None untuk jalankan semua
    run(subject_limit=2, tahun_limit=3)