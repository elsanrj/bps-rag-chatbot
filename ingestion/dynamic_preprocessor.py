"""
dynamic_preprocessor.py
Fase 1A - Hari 6: Preprocessing Data Dinamis → Unified Document Schema

Tanggung jawab modul ini:
  - Baca raw_docs (source_type: dynamic_data + dynamic_var) dari MongoDB
  - Decode key numerik di `datacontent` menjadi kombinasi label bermakna
  - Konversi setiap data point menjadi kalimat deskriptif dalam Bahasa Indonesia
  - Simpan hasil ke MongoDB collection `documents` (unified schema)

Prasyarat:
  - dynamic_ingestor.py sudah dijalankan (raw_docs terisi)
  - cleaner.py tersedia di direktori yang sama

Jalankan:
  python -m ingestion.dynamic_preprocessor

Env variables yang diperlukan (.env):
  MONGODB_URI : Connection string MongoDB Atlas
  MONGODB_DB  : Nama database (contoh: bps_rag)
"""

import os
import re
import logging

from datetime import datetime
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError

from ingestion.cleaner import normalize_whitespace, validate_doc, generate_id, apply_symbol_legend
from ingestion.regions import DISTRICTS, VILLAGES

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

MONGODB_URI   = os.getenv("MONGODB_URI")
MONGODB_DB    = os.getenv("MONGODB_DB")
RAW_COLL      = "raw_docs"
DOC_COLL      = "documents"
BATCH_SIZE    = 100   # dokumen per batch upsert ke MongoDB


# ---------------------------------------------------------------------------
# Decode datacontent
# ---------------------------------------------------------------------------

def build_lookup_maps(raw_data: dict) -> tuple[dict, dict, dict]:
    """
    Bangun tiga lookup map dari response data API untuk decode key datacontent:
      - vervar_map  : {val_str → label}  (dimensi vertikal, misal jenis kelamin/wilayah)
      - turvar_map  : {val_str → label}  (kategori/indikator turunan)
      - tahun_map   : {val_str → label}  (tahun data)
      - turtahun_map : {val_str → label}  (tahun untuk turvar, jika ada)

    Key di datacontent adalah gabungan string dari val-val tersebut,
    sehingga lookup map ini kunci utama proses decode.
    """

    vervar_map = {
        str(item["val"]): item["label"]
        for item in raw_data.get("vervar", [])
        if item.get("val") is not None
    }
    turvar_map = {
        str(item["val"]): item["label"]
        for item in raw_data.get("turvar", [])
        if item.get("val") is not None
    }
    tahun_map = {
        str(item["val"]): item["label"]
        for item in raw_data.get("tahun", [])
        if item.get("val") is not None
    }
    turtahun_map = {
        str(item["val"]): item["label"]
        for item in raw_data.get("turtahun", [])
        if item.get("val") is not None
    }
    

    return vervar_map, turvar_map, tahun_map, turtahun_map


def decode_datacontent_key(
    key: str,
    var_id: int,
    vervar_map: dict,
    turvar_map: dict,
    tahun_map: dict,
    turtahun_map: dict,
) -> tuple[str, str, str, str] | None:
    """
    Decode key datacontent secara berurutan dari kiri:
      Format: {vervar_val}{var_id}{turvar_val}{tahun_val}{turtahun_val}

    Strategi: greedy match dari kiri untuk setiap komponen.
    Return tuple (vervar_label, turvar_label, tahun_label, turtahun_label)
    atau None jika decode gagal.
    """
    remaining = key
    var_id_str = str(var_id)

    # Step 1: vervar_val — prefix sebelum var_id
    vervar_label = None
    for val, label in vervar_map.items():
        if remaining.startswith(val):
            after_vervar = remaining[len(val):]
            if after_vervar.startswith(var_id_str):
                vervar_label = label
                remaining = after_vervar[len(var_id_str):]
                break

    if vervar_label is None:
        logger.info(f"Gagal decode vervar dari key='{key}'")
        return None

    # Step 2: turvar_val — jika tidak ada, val=0
    turvar_label = None
    for val, label in turvar_map.items():
        if remaining.startswith(val):
            turvar_label = label
            remaining = remaining[len(val):]
            break
    if turvar_label is None:
        turvar_label = ""   # turvar tidak wajib ada
        # remaining tidak dipotong — lanjut cari tahun

    # Step 3: tahun_val
    tahun_label = None
    for val, label in tahun_map.items():
        if remaining.startswith(val):
            tahun_label = label
            remaining = remaining[len(val):]
            break

    if tahun_label is None:
        logger.info(f"Gagal decode tahun dari key='{key}' sisa='{remaining}'")
        return None

    # Step 4: turtahun_val — jika tidak ada, val=0
    turtahun_label = None
    for val, label in turtahun_map.items():
        if remaining.startswith(val):
            turtahun_label = label
            remaining = remaining[len(val):]
            break
    if turtahun_label is None:
        turtahun_label = ""   # turtahun tidak wajib ada

    return vervar_label, turvar_label, tahun_label, turtahun_label

# ---------------------------------------------------------------------------
# Konversi ke kalimat deskriptif
# ---------------------------------------------------------------------------

def build_sentence(
    title: str,
    labelvervar: str,
    vervar_label: str,
    turvar_label: str,       # kosong jika turvar_val == 0
    tahun_label: str,
    turtahun_label: str,     # kosong jika turtahun_val == 0
    value: str,
    unit: str,
) -> str:
    """
    Bangun kalimat deskriptif dengan template:
      {title}
      {labelvervar} {vervar_label} kategori {turvar_label} pada {turtahun_label} {tahun_label}
      sebesar {value} {unit}

    Komponen opsional:
      - "kategori {turvar_label}" hanya muncul jika turvar_label tidak kosong
      - "{turtahun_label}" hanya muncul jika turtahun_label tidak kosong
      - "{unit}" hanya muncul jika unit != "Tidak Ada Satuan"
    """
    # Validasi value — skip jika tidak bermakna
    cleaned_value = apply_symbol_legend(value)
    if cleaned_value in ("data tidak tersedia", "tidak dapat ditampilkan"):
        return ""

    # Baris 1: header
    header = title.strip()

    # Baris 2: data point
    turvar_part   = f" kategori {turvar_label}" if turvar_label and turvar_label != "Tidak ada" else ""
    turtahun_part = f" {turtahun_label}" if turtahun_label else ""
    unit_part     = f" {unit}" if unit and unit != "Tidak Ada Satuan" else ""

    data_line = (
        f"{labelvervar} {vervar_label}{turvar_part}"
        f" pada{turtahun_part} {tahun_label}"
        f" sebesar {cleaned_value}{unit_part}"
    )

    return normalize_whitespace(f"{header}. {data_line}")

def resolve_region(title: str, vervar_label: str) -> str:
    """
    Cari nama kecamatan atau kelurahan dalam title secara case-insensitive.
    Return nama wilayah jika ditemukan, default 'Kota Bandung' jika tidak.
    Prioritas: nama terpanjang dicek dulu untuk hindari partial match.
    """
    title_low = title.lower()
    vervar_low = vervar_label.lower() if vervar_label else ""
    
    # Strategi: Urutkan kunci berdasarkan panjang karakter (descending) 
    # agar nama panjang tidak terpotong (misal: "Bandung Kidul" vs "Bandung")
    sorted_villages = sorted(VILLAGES.items(), key=len, reverse=True)
    sorted_districts = sorted(DISTRICTS.items(), key=len, reverse=True)

    # 1. Cek di vervar_label dengan bantuan hint 'vervar label'
    # Jika vervar label menyebutkan kecamatan, cari di map kecamatan dulu            
    if "kecamatan" in vervar_low:
        for name, display in sorted_districts:
            if name in vervar_low: return display
            
        for name, display in sorted_villages:
            if name in vervar_low: return display
            
    else:
        for name, display in sorted_villages:
            if name in vervar_low: return display
        
        for name, display in sorted_districts:
            if name in vervar_low: return display

    # LANGKAH 2: Cek di Judul (Title)
    for name, display in sorted_villages:
        if name in title_low: return display

    for name, display in sorted_districts:
        if name in title_low: return display
        
    return "Kota Bandung"


# ---------------------------------------------------------------------------
# Bangun unified document dari satu raw_data + var_info
# ---------------------------------------------------------------------------

def process_one_variable(raw_doc: dict, var_lookup: dict) -> list[dict]:
    """
    Proses satu raw dynamic_data dokumen dari MongoDB menjadi list unified documents.
    Setiap data point di datacontent menghasilkan satu dokumen terpisah.

    Parameter:
      raw_doc    : Satu dokumen dari collection raw_docs (source_type: dynamic_data)
      var_lookup : Dict {var_id → var_info} dari collection raw_docs dynamic_var

    Return list of unified document dicts (siap upsert ke collection `documents`).
    """
    payload  = raw_doc.get("payload", {})
    var_id   = raw_doc.get("var_id")
    var_info = var_lookup.get(var_id, {})

    title      = var_info.get("title", f"Variabel {var_id}")
    unit       = var_info.get("unit", "")
    subcsa     = var_info.get("subcsa_name", "")
    sub_name   = var_info.get("sub_name", "")

    data_section  = payload
    datacontent   = payload.get("datacontent", {})
    last_update   = payload.get("last_update", "")

    if not datacontent:
        logger.info(f"datacontent kosong untuk var_id={var_id}")
        return []
    
    vervar_map, turvar_map, tahun_map, turtahun_map = build_lookup_maps(payload)
    labelvervar = data_section.get("labelvervar", "")

    unified_docs = []

    for key, value in datacontent.items():
        decoded = decode_datacontent_key(key, var_id, vervar_map, turvar_map, tahun_map, turtahun_map)
        if not decoded:
            continue

        vervar_label, turvar_label, tahun_label, turtahun_label = decoded

        content = build_sentence(
            title        = title,
            labelvervar = labelvervar,
            vervar_label = vervar_label,
            turvar_label = turvar_label,
            tahun_label  = tahun_label,
            turtahun_label = turtahun_label,
            value        = value,
            unit         = unit,
        )
        if not content:
            continue

        # Validasi sebelum lanjut
        candidate = {"content": content, "id": generate_id(content)}
        if not validate_doc(candidate):
            continue

        unified_doc = {
            "id"          : candidate["id"],
            "source_type" : "dynamic",
            "title"       : title,
            "content"     : content,
            "metadata"    : {
                "date"       : last_update[:10] if last_update else None,
                "year"       : int(tahun_label) if tahun_label.isdigit() else None,
                "category"   : subcsa or sub_name,
                "region"     : resolve_region(title, vervar_label),
                "source_url" : None,
                "extra"      : {
                    "var_id"       : var_id,
                    "vervar_label" : vervar_label,
                    "turvar_label" : turvar_label,
                    "datacontent_key" : key,
                },
            },
            "processed_at" : datetime.utcnow(),
        }

        unified_docs.append(unified_doc)

    return unified_docs


# ---------------------------------------------------------------------------
# Simpan ke collection documents
# ---------------------------------------------------------------------------

def save_documents(docs: list[dict], collection) -> int:
    """
    Upsert list unified documents ke MongoDB collection `documents`.
    Upsert berdasarkan field `id` (MD5 hash dari content) — idempotent.
    Return jumlah dokumen yang berhasil disimpan.
    """
    if not docs:
        return 0

    operations = [
        UpdateOne(
            filter={"id": doc["id"]},
            update={"$set": doc},
            upsert=True,
        )
        for doc in docs
    ]

    try:
        result = collection.bulk_write(operations, ordered=False)
        return result.upserted_count + result.modified_count
    except BulkWriteError as e:
        logger.error(f"BulkWriteError: {e.details}")
        return 0


# ---------------------------------------------------------------------------
# Orchestrator utama
# ---------------------------------------------------------------------------

def run():
    """
    Pipeline utama preprocessing dynamic data:
      1. Load semua var_info dari raw_docs (dynamic_var) sebagai lookup
      2. Iterasi semua raw_docs (dynamic_data)
      3. Decode + konversi ke kalimat → unified documents
      4. Upsert ke collection `documents`
    """
    if not MONGODB_URI:
        logger.error("MONGODB_URI tidak ditemukan di environment. Hentikan.")
        return

    client     = MongoClient(MONGODB_URI)
    db         = client[MONGODB_DB]
    raw_coll   = db[RAW_COLL]
    doc_coll   = db[DOC_COLL]

    logger.info(f"Terhubung ke MongoDB: {MONGODB_DB}")

    # --- Step 1: Build var_lookup ---
    logger.info("Memuat var_info dari raw_docs...")
    var_lookup = {}

    projection = {
        "payload.var_id": 1, "payload.title": 1, "payload.def": 1,
        "payload.unit": 1, "payload.subcsa_name": 1, "payload.sub_name": 1,
        "_id": 0
    }
    for raw_var in raw_coll.find({"source_type": "dynamic_var"}, projection):
        payload = raw_var.get("payload", {})
        vid     = payload.get("var_id")
        if vid:
            var_lookup[vid] = payload

    logger.info(f"Var lookup siap: {len(var_lookup)} entri")

    # --- Step 2 & 3: Proses setiap raw data ---
    total_raw    = raw_coll.count_documents({"source_type": "dynamic_data"})
    total_saved  = 0
    total_skip   = 0
    batch        = []

    logger.info(f"Total raw dynamic_data yang akan diproses: {total_raw}")

    for i, raw_doc in enumerate(raw_coll.find({"source_type": "dynamic_data"}), start=1):
        var_id = raw_doc.get("var_id")

        if var_id not in var_lookup:
            logger.warning(f"var_id={var_id} tidak ada di var_lookup, skip.")
            total_skip += 1
            continue
        
        try:
            unified_docs = process_one_variable(raw_doc, var_lookup)
        except Exception as e:
            logger.error(f"Gagal memproses var_id={var_id}. Error: {e}")
            total_skip += 1
            continue

        if not unified_docs:
            total_skip += 1
            continue

        for doc in unified_docs:
            batch.append(doc)
            if len(batch) >= BATCH_SIZE:
                saved = save_documents(batch, doc_coll)
                total_saved += saved
                logger.info(f"  [{i}/{total_raw}] Batch flushed: {saved} dokumen disimpan")
                batch = []

    # Flush sisa batch
    if batch:
        saved = save_documents(batch, doc_coll)
        total_saved += saved

    # --- Summary ---
    logger.info("=" * 50)
    logger.info("PREPROCESSING SELESAI")
    logger.info(f"  Total raw diproses : {total_raw}")
    logger.info(f"  Dokumen dihasilkan : {total_saved}")
    logger.info(f"  Dilewati           : {total_skip}")
    logger.info("=" * 50)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    run()
