"""
brs_ingestor.py
Fase 1B - Hari 11: Ingestion Data BRS (Berita Resmi Statistik) BPS Kota Bandung

Tanggung jawab modul ini:
  - Fetch semua BRS dari WebAPI BPS domain 3273
  - Simpan hasil mentah ke MongoDB collection `raw_docs`
  - TIDAK melakukan preprocessing/konversi — raw apa adanya
  - TIDAK mendownload PDF — hanya abstract dari API (untuk dev awal)

Flow satu langkah:
  [1] GET /list/model/pressrelease/lang/ind/domain/3273
      → ambil brs_id, title, abstract, subcsa, rl_date, pdf (url disimpan
        untuk supplementary di fase berikutnya)

Jalankan:
  python -m ingestion.brs_ingestor

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

DOMAIN        = "3273"
BASE_URL      = "https://webapi.bps.go.id/v1/api"
API_KEY       = os.getenv("BPS_API_KEY")
MONGODB_URI   = os.getenv("MONGODB_URI")
MONGODB_DB    = os.getenv("MONGODB_DB")
COLLECTION    = "raw_docs"

REQUEST_DELAY = 0.5
RETRY_MAX     = 3
RETRY_DELAY   = 2.0


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
# Fetch BRS
# ---------------------------------------------------------------------------

def fetch_all_brs() -> list[dict]:
    """
    Fetch semua BRS dari endpoint /list/model/pressrelease.
    Handle pagination otomatis.

    Verifikasi struktur response sebelum akses field — pelajaran dari dynamic data.
    Response format:
      data → [pagination_dict, [list_of_brs]]

    Return list of dict raw per BRS.
    """
    url = f"{BASE_URL}/list/model/pressrelease/lang/ind/domain/{DOMAIN}/key/{API_KEY}/"
    all_brs = []
    page    = 1

    logger.info("Mulai fetch daftar BRS...")

    while True:
        data = fetch_with_retry(url, params={"page": page})

        if not data:
            logger.error(f"Gagal fetch halaman {page}. Berhenti.")
            break

        # Verifikasi struktur — data adalah list [pagination, [items]]
        payload = data.get("data", [])

        if not isinstance(payload, list) or len(payload) < 2:
            logger.warning(
                f"Struktur response tidak sesuai di halaman {page}: "
                f"{list(data.keys())}"
            )
            break

        pagination = payload[0]
        brs_list   = payload[1]

        if not isinstance(pagination, dict):
            logger.warning(f"Pagination bukan dict di halaman {page}")
            break

        if not isinstance(brs_list, list) or len(brs_list) == 0:
            break

        for item in brs_list:
            if not isinstance(item, dict):
                continue
            all_brs.append(item)

        total_pages = pagination.get("pages", 1)
        logger.info(f"  Halaman {page}/{total_pages} — {len(brs_list)} BRS diambil")

        if page >= total_pages:
            break

        page += 1
        time.sleep(REQUEST_DELAY)

    logger.info(f"Total BRS berhasil di-fetch: {len(all_brs)}")
    return all_brs


# ---------------------------------------------------------------------------
# Storage
# ---------------------------------------------------------------------------

def get_mongo_collection():
    """Inisialisasi koneksi MongoDB dan return collection raw_docs."""
    client = MongoClient(MONGODB_URI)
    db     = client[MONGODB_DB]
    return db[COLLECTION]


def save_raw_brs(brs_list: list[dict], collection) -> int:
    """
    Simpan list BRS ke MongoDB (upsert berdasarkan brs_id).
    Menyimpan seluruh item response API apa adanya di field `payload`.

    Field standar raw_docs:
      raw_id, source_type, brs_id, fetched_at, source_url, payload

    Field yang sengaja TIDAK disimpan di root (hanya di payload):
      thumbnail, slide — tidak relevan untuk RAG
      size             — tidak relevan untuk RAG

    Return jumlah dokumen yang berhasil disimpan.
    """
    if not brs_list:
        return 0

    source_url = (
        f"{BASE_URL}/list/model/pressrelease/lang/ind/domain/{DOMAIN}/key/{API_KEY}/"
    )

    operations = []
    skipped    = 0

    for item in brs_list:
        brs_id = item.get("brs_id")

        if not brs_id:
            logger.warning(f"brs_id tidak ditemukan, skip item: {list(item.keys())}")
            skipped += 1
            continue

        operations.append(
            UpdateOne(
                filter={"raw_id": f"brs_{brs_id}", "source_type": "brs"},
                update={
                    "$set": {
                        "raw_id"      : f"brs_{brs_id}",
                        "source_type" : "brs",
                        "brs_id"      : brs_id,
                        "fetched_at"  : datetime.utcnow(),
                        "source_url"  : source_url,
                        "payload"     : item,   # response API apa adanya
                    }
                },
                upsert=True,
            )
        )

    if not operations:
        logger.warning("Tidak ada operasi valid untuk disimpan.")
        return 0

    if skipped:
        logger.warning(f"{skipped} item dilewati karena brs_id tidak ditemukan.")

    try:
        result = collection.bulk_write(operations, ordered=False)
        saved  = result.upserted_count + result.modified_count
        logger.info(f"BRS tersimpan/diperbarui: {saved}")
        return saved
    except BulkWriteError as e:
        logger.error(f"BulkWriteError saat simpan BRS: {e.details}")
        return 0


# ---------------------------------------------------------------------------
# Orchestrator utama
# ---------------------------------------------------------------------------

def run(limit: int = None):
    """
    Pipeline utama ingestion BRS:
      1. Fetch semua BRS dari API (abstract only, tanpa PDF)
      2. Simpan raw ke MongoDB collection `raw_docs`

    Parameter:
      limit : Batasi jumlah BRS yang disimpan (untuk dev/testing).
              None = simpan semua.
    """
    if not API_KEY:
        logger.error("BPS_API_KEY tidak ditemukan di environment. Hentikan.")
        return

    if not MONGODB_URI:
        logger.error("MONGODB_URI tidak ditemukan di environment. Hentikan.")
        return

    collection = get_mongo_collection()
    logger.info(f"Terhubung ke MongoDB: {MONGODB_DB}.{COLLECTION}")

    # --- Fetch ---
    all_brs = fetch_all_brs()

    if not all_brs:
        logger.error("Tidak ada BRS yang berhasil di-fetch. Hentikan.")
        return

    if limit:
        logger.info(f"Mode dev: hanya menyimpan {limit} BRS pertama")
        all_brs = all_brs[:limit]

    # --- Simpan ---
    saved = save_raw_brs(all_brs, collection)

    # --- Summary ---
    logger.info("=" * 50)
    logger.info("INGESTION BRS SELESAI")
    logger.info(f"  Total di-fetch : {len(all_brs)}")
    logger.info(f"  Tersimpan      : {saved}")
    logger.info("=" * 50)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # Mode dev: simpan 20 BRS pertama
    # Ganti None untuk simpan semua
    run(limit=None)
