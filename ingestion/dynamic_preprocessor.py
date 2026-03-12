"""
dynamic_preprocessor.py
Fase 1A - Hari 6: Preprocessing Data Dinamis → Unified Document Schema

Tanggung jawab modul ini:
  - Baca raw_docs (source_type: dynamic_data + dynamic_var) dari MongoDB
  - Decode key numerik di `datacontent` menjadi kombinasi label bermakna
  - Konversi setiap data point menjadi kalimat deskriptif dalam Bahasa Indonesia
  - Simpan hasil ke MongoDB collection `documents` (unified schema)

Prasyarat:
  - json_ingestor.py sudah dijalankan (raw_docs terisi)
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

from ingestion.cleaner import normalize_whitespace, validate_doc, generate_id

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

    Key di datacontent adalah gabungan string dari val-val tersebut,
    sehingga lookup map ini kunci utama proses decode.
    """
    data_section = raw_data.get("data", {})

    vervar_map = {
        str(item["val"]): item["label"]
        for item in data_section.get("vervar", [])
        if item.get("val") is not None
    }
    turvar_map = {
        str(item["val"]): item["label"]
        for item in data_section.get("turvar", [])
        if item.get("val") is not None
    }
    tahun_map = {
        str(item["val"]): item["label"]
        for item in data_section.get("turtahun", [])
        if item.get("val") is not None
    }

    # Fallback: ambil tahun dari field `tahun` jika `turtahun` kosong
    if not tahun_map:
        tahun_map = {
            str(item["val"]): item["label"]
            for item in data_section.get("tahun", [])
            if item.get("val") is not None
        }

    return vervar_map, turvar_map, tahun_map


def decode_datacontent_key(
    key: str,
    var_id: int,
    vervar_map: dict,
    turvar_map: dict,
    tahun_map: dict,
) -> tuple[str, str, str] | None:
    """
    Decode satu key datacontent numerik menjadi tuple (vervar_label, turvar_label, tahun_label).

    Format key: {vervar_val}{var_id}{turvar_val}{tahun_val}
    Semua komponen disambung tanpa separator — parsing dengan eliminasi var_id.

    Contoh:
      key     = "11331151210"
      var_id  = 133
      → sisa setelah hapus "133" = "1" + "115" + "121" + "0"
      → vervar_val="1", turvar_val="115", tahun_val="121", turtahun_val="0"

    Return None jika decode gagal (key tidak bisa diparsing).
    """
    var_id_str = str(var_id)

    # Cari posisi var_id dalam key
    idx = key.find(var_id_str)
    if idx == -1:
        logger.debug(f"var_id '{var_id_str}' tidak ditemukan dalam key '{key}'")
        return None

    # Bagian sebelum var_id = vervar_val
    vervar_val = key[:idx]

    # Bagian setelah var_id = turvar_val + tahun_val (perlu split lagi)
    remainder = key[idx + len(var_id_str):]

    # Cari turvar_val dan tahun_val dari remainder
    # Strategi: coba semua turvar_val yang ada, ambil yang cocok sebagai prefix
    turvar_label = None
    tahun_label  = None

    for tv_val, tv_label in turvar_map.items():
        if remainder.startswith(tv_val):
            tahun_remainder = remainder[len(tv_val):]
            # Cek apakah sisa setelah turvar adalah tahun yang valid
            for th_val, th_label in tahun_map.items():
                if tahun_remainder.startswith(th_val):
                    turvar_label = tv_label
                    tahun_label  = th_label
                    break
            if turvar_label:
                break

    if not (vervar_val in vervar_map and turvar_label and tahun_label):
        # Fallback: log untuk investigasi manual
        logger.debug(
            f"Decode gagal untuk key='{key}' var_id={var_id} | "
            f"vervar_val='{vervar_val}' | turvar coba dari remainder='{remainder}'"
        )
        return None

    return vervar_map[vervar_val], turvar_label, tahun_label


# ---------------------------------------------------------------------------
# Konversi ke kalimat deskriptif
# ---------------------------------------------------------------------------

def build_sentence(
    title: str,
    definition: str,
    vervar_label: str,
    turvar_label: str,
    tahun_label: str,
    value: float | str,
    unit: str,
) -> str:
    """
    Bangun satu kalimat deskriptif dari komponen-komponen yang sudah di-decode.

    Template:
      [Judul indikator]: [definisi singkat jika ada].
      [vervar_label] di Kota Bandung pada tahun [tahun_label],
      [turvar_label] sebesar [value] [unit].

    Contoh output:
      Persentase Anggota Rumah Tangga Berusia 5 Tahun ke Atas menurut
      Jenis Kelamin KRT dan Penggunaan Teknologi Informasi:
      Menggunakan Telepon Seluler (HP)/Nirkabel atau Komputer.
      Laki-laki di Kota Bandung pada tahun 2021,
      Menggunakan Telepon Seluler sebesar 82.21 Persen.
    """
    parts = []

    # Baris 1: judul + definisi
    header = title.strip()
    if definition and definition.strip() and definition.strip() != header:
        header += f": {definition.strip()}"
    parts.append(header)

    # Baris 2: data point
    value_str = str(value).replace(".", ",")   # format angka Indonesia
    data_line = (
        f"{vervar_label} di Kota Bandung pada tahun {tahun_label}, "
        f"{turvar_label} sebesar {value_str} {unit}."
    )
    parts.append(data_line)

    return normalize_whitespace(" ".join(parts))


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
    definition = var_info.get("def", "")
    unit       = var_info.get("unit", "")
    subcsa     = var_info.get("subcsa_name", "")
    sub_name   = var_info.get("sub_name", "")

    data_section  = payload.get("data", {})
    datacontent   = data_section.get("datacontent", {})
    last_update   = payload.get("last_update", "")

    if not datacontent:
        logger.debug(f"datacontent kosong untuk var_id={var_id}")
        return []

    vervar_map, turvar_map, tahun_map = build_lookup_maps(payload)

    unified_docs = []

    for key, value in datacontent.items():
        decoded = decode_datacontent_key(key, var_id, vervar_map, turvar_map, tahun_map)
        if not decoded:
            continue

        vervar_label, turvar_label, tahun_label = decoded

        content = build_sentence(
            title        = title,
            definition   = definition,
            vervar_label = vervar_label,
            turvar_label = turvar_label,
            tahun_label  = tahun_label,
            value        = value,
            unit         = unit,
        )

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
                "region"     : "Kota Bandung",
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

    for raw_var in raw_coll.find({"source_type": "dynamic_var"}):
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

        unified_docs = process_one_variable(raw_doc, var_lookup)

        if not unified_docs:
            total_skip += 1
            continue

        batch.extend(unified_docs)

        # Flush batch ke MongoDB setiap BATCH_SIZE dokumen
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
