"""
statictable_ingestor.py
Fase 1B: Ingestion Data Tabel Statis BPS Kota Bandung

Tanggung jawab modul ini:
  - Fetch 1000 list tabel statis dari WebAPI BPS domain 3273
  - Fetch semua detail setiap tabel (field `table` HTML + metadata)
  - Simpan hasil mentah ke MongoDB collection `raw_docs`
  - TIDAK melakukan preprocessing/konversi — raw apa adanya
  - Gunakan field `table` (HTML), BUKAN `excel` (URL binary)

Flow dua langkah:
  [1] GET /list/model/statictable/lang/ind/domain/3273
      → ambil 1000 table_id + title + subj + updt_date

  [2] Untuk setiap table_id:
      GET /view/domain/3273/model/statictable/lang/ind/id/{table_id}
      → ambil title + table (HTML) + subcsa + cr_date + updt_date + related

Jalankan:
  python -m ingestion.statictable_ingestor

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
# Step 1: Fetch list tabel statis
# ---------------------------------------------------------------------------

def fetch_all_table_list() -> list[dict]:
    """
    Fetch semua list tabel statis dari endpoint /list/model/statictable.
    Handle pagination otomatis (total bisa mencapai 4590+ tabel).

    Verifikasi struktur response sebelum akses field — pelajaran dari dynamic data.
    Response format:
      data → [pagination_dict, [list_of_tables]]

    Return list of dict: [{table_id, title, subj_id, subj, updt_date, size}, ...]
    """
    url = (
        f"{BASE_URL}/list/model/statictable/lang/ind"
        f"/domain/{DOMAIN}/key/{API_KEY}/"
    )
    all_tables = []
    page       = 1

    logger.info("Mulai fetch list tabel statis...")

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

        pagination  = payload[0]
        table_list  = payload[1]

        if not isinstance(pagination, dict):
            logger.warning(f"Pagination bukan dict di halaman {page}")
            break

        if not isinstance(table_list, list) or len(table_list) == 0:
            break

        for item in table_list:
            if not isinstance(item, dict):
                continue
            table_id = item.get("table_id")
            if table_id is None:
                continue
            all_tables.append({
                "table_id"  : table_id,
                "title"     : item.get("title", ""),
                "subj_id"   : item.get("subj_id"),
                "subj"      : item.get("subj", ""),
                "updt_date" : item.get("updt_date", ""),
                "size"      : item.get("size", ""),
                # excel diabaikan — gunakan HTML dari detail endpoint
            })

        # total_pages = pagination.get("pages", 1)
        total_pages = 100 # Override sementara mode dev
        logger.info(f"  Halaman {page}/{total_pages} — {len(table_list)} tabel diambil")

        if page >= total_pages:
            break

        page += 1
        time.sleep(REQUEST_DELAY)

    logger.info(f"Total tabel dalam list: {len(all_tables)}")
    return all_tables


# ---------------------------------------------------------------------------
# Step 2: Fetch detail satu tabel
# ---------------------------------------------------------------------------

def fetch_table_detail(table_id: int) -> dict | None:
    """
    Fetch detail satu tabel dari endpoint /view/model/statictable.
    Return dict raw response atau None jika gagal.

    Field penting yang diambil dari detail:
      table    : konten HTML tabel (BUKAN excel)
      title    : judul tabel (bisa lebih lengkap dari list)
      subcsa   : kategori subjek
      cr_date  : tanggal pembuatan
      updt_date: tanggal update
      related  : tabel-tabel terkait (untuk cross-reference)
    """
    url = (
        f"{BASE_URL}/view/domain/{DOMAIN}/model/statictable"
        f"/lang/ind/id/{table_id}/key/{API_KEY}/"
    )

    data = fetch_with_retry(url)
    if not data:
        return None

    if data.get("data-availability") != "available":
        logger.debug(f"Data tidak tersedia untuk table_id={table_id}")
        return None

    # Verifikasi field `data` ada dan berisi dict
    detail = data.get("data", {})
    if not isinstance(detail, dict):
        logger.warning(f"Field 'data' bukan dict untuk table_id={table_id}: {type(detail)}")
        return None

    return detail


# ---------------------------------------------------------------------------
# Storage
# ---------------------------------------------------------------------------

def get_mongo_collection():
    """Inisialisasi koneksi MongoDB dan return collection raw_docs."""
    client = MongoClient(MONGODB_URI)
    db     = client[MONGODB_DB]
    return db[COLLECTION]


def save_raw_table_list(table_list: list[dict], collection) -> int:
    """
    Simpan list metadata tabel statis ke MongoDB (upsert berdasarkan table_id).
    Konsisten dengan pola save_raw_variables di json_ingestor dan
    save_raw_subjects di simdasi_ingestor — metadata list disimpan terpisah.

    Return jumlah dokumen yang berhasil disimpan.
    """
    if not table_list:
        return 0

    source_url = (
        f"{BASE_URL}/list/model/statictable/lang/ind"
        f"/domain/{DOMAIN}/key/{API_KEY}/"
    )

    operations = [
        UpdateOne(
            filter={
                "raw_id"      : f"statictable_list_{t['table_id']}",
                "source_type" : "statictable_list",
            },
            update={
                "$set": {
                    "raw_id"      : f"statictable_list_{t['table_id']}",
                    "source_type" : "statictable_list",
                    "table_id"    : t["table_id"],
                    "fetched_at"  : datetime.utcnow(),
                    "source_url"  : source_url,
                    "payload"     : t,
                }
            },
            upsert=True,
        )
        for t in table_list
    ]

    try:
        result = collection.bulk_write(operations, ordered=False)
        saved  = result.upserted_count + result.modified_count
        logger.info(f"List tabel tersimpan/diperbarui: {saved}")
        return saved
    except BulkWriteError as e:
        logger.error(f"BulkWriteError saat simpan list tabel: {e.details}")
        return 0


def save_raw_table_detail(
    table_id: int,
    title: str,
    detail: dict,
    source_url: str,
    collection,
) -> bool:
    """
    Simpan detail satu tabel statis ke MongoDB (upsert berdasarkan table_id).

    Field standar raw_docs:
      raw_id, source_type, table_id, title, fetched_at, source_url, payload

    Return True jika berhasil.
    """
    raw_id = f"statictable_{table_id}"

    try:
        collection.update_one(
            filter={"raw_id": raw_id, "source_type": "statictable"},
            update={
                "$set": {
                    "raw_id"      : raw_id,
                    "source_type" : "statictable",
                    "table_id"    : table_id,
                    "title"       : title,
                    "fetched_at"  : datetime.utcnow(),
                    "source_url"  : source_url,
                    "payload"     : detail,   # response API apa adanya
                }
            },
            upsert=True,
        )
        return True
    except Exception as e:
        logger.error(f"Gagal simpan table_id={table_id}: {e}")
        return False


# ---------------------------------------------------------------------------
# Orchestrator utama
# ---------------------------------------------------------------------------

def run(limit: int = None):
    """
    Pipeline utama ingestion tabel statis:
      1. Fetch list semua tabel → simpan ke raw_docs (statictable_list)
      2. Untuk setiap table_id: fetch detail → simpan ke raw_docs (statictable)

    Parameter:
      limit : Batasi jumlah tabel yang di-fetch detailnya (untuk dev/testing).
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

    # --- Step 1: Fetch dan simpan list tabel ---
    all_tables = fetch_all_table_list()

    if not all_tables:
        logger.error("Tidak ada tabel yang berhasil di-fetch. Hentikan.")
        return

    save_raw_table_list(all_tables, collection)

    # --- Step 2: Fetch detail per table_id ---
    if limit:
        logger.info(f"Mode dev: hanya memproses {limit} tabel pertama")
        all_tables = all_tables[:limit]

    success_count = 0
    skip_count    = 0
    error_count   = 0
    total         = len(all_tables)

    logger.info(f"Mulai fetch detail untuk {total} tabel...")

    for i, tbl in enumerate(all_tables, start=1):
        table_id = tbl["table_id"]
        title    = tbl["title"]

        # Skip jika detail sudah ada di MongoDB
        existing = collection.find_one(
            {"raw_id": f"statictable_{table_id}", "source_type": "statictable"},
            {"_id": 1},   # projection — hanya butuh tahu ada/tidak
        )
        if existing:
            logger.debug(f"[{i}/{total}] Skip (sudah ada): table_id={table_id}")
            skip_count += 1
            time.sleep(REQUEST_DELAY)
            continue

        source_url = (
            f"{BASE_URL}/view/domain/{DOMAIN}/model/statictable"
            f"/lang/ind/id/{table_id}/key/{API_KEY}/"
        )

        detail = fetch_table_detail(table_id)

        if detail is None:
            logger.debug(f"[{i}/{total}] Tidak tersedia: table_id={table_id}")
            error_count += 1
            time.sleep(REQUEST_DELAY)
            continue

        ok = save_raw_table_detail(
            table_id   = table_id,
            title      = title,
            detail     = detail,
            source_url = source_url,
            collection = collection,
        )

        if ok:
            success_count += 1
            logger.info(f"[{i}/{total}] OK: table_id={table_id} | {title[:60]}")
        else:
            error_count += 1

        time.sleep(REQUEST_DELAY)

    # --- Summary ---
    logger.info("=" * 50)
    logger.info("INGESTION TABEL STATIS SELESAI")
    logger.info(f"  Berhasil  : {success_count}")
    logger.info(f"  Dilewati  : {skip_count} (sudah ada di DB)")
    logger.info(f"  Gagal     : {error_count}")
    logger.info("=" * 50)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # Mode dev: fetch detail 10 tabel pertama
    # Ganti None untuk proses semua
    run(limit=None)